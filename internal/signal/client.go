// Package signal is the agent's outbound WebSocket connection to the
// coordinator. It registers, heartbeats, receives the peer list and forwards
// WebRTC signalling. It reconnects forever with exponential backoff.
package signal

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"unicompute/internal/protocol"
)

type Client struct {
	url  string
	self func() protocol.NodeInfo
	hb   func() protocol.Heartbeat

	OnPeers  func([]protocol.NodeInfo)
	OnSignal func(protocol.Envelope)
	OnStatus func(connected bool, msg string)

	mu   sync.Mutex
	conn *websocket.Conn

	BytesIn  atomic.Int64
	BytesOut atomic.Int64
	Connected atomic.Bool
}

func New(url string, self func() protocol.NodeInfo, hb func() protocol.Heartbeat) *Client {
	return &Client{url: url, self: self, hb: hb}
}

// Run blocks until ctx is cancelled, reconnecting as needed.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.session(ctx)
		c.Connected.Store(false)
		if c.OnStatus != nil {
			msg := "disconnected"
			if err != nil {
				msg = err.Error()
			}
			c.OnStatus(false, msg)
		}
		if ctx.Err() != nil {
			return
		}
		sleep := backoff + time.Duration(rand.Int63n(int64(backoff/2)))
		log.Printf("[signal] reconnecting in %s (%v)", sleep.Round(time.Millisecond), err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
		// Cap low: a coordinator restart should be noticed within ~10 s so
		// the "kill the cloud, bring it back" demo reads cleanly.
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) session(ctx context.Context) error {
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := d.DialContext(ctx, c.url, nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
		conn.Close()
	}()

	if err := c.Send(protocol.Envelope{Type: protocol.SigRegister, Data: protocol.Marshal(c.self())}); err != nil {
		return err
	}
	c.Connected.Store(true)
	if c.OnStatus != nil {
		c.OnStatus(true, "connected")
	}

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := c.Send(protocol.Envelope{Type: protocol.SigHeartbeat, Data: protocol.Marshal(c.hb())}); err != nil {
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		c.BytesIn.Add(int64(len(data)))
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Type {
		case protocol.SigPeers:
			var peers []protocol.NodeInfo
			if json.Unmarshal(env.Data, &peers) == nil && c.OnPeers != nil {
				c.OnPeers(peers)
			}
		case protocol.SigOffer, protocol.SigAnswer, protocol.SigICE:
			if c.OnSignal != nil {
				c.OnSignal(env)
			}
		case protocol.SigError:
			log.Printf("[signal] coordinator error: %s", string(env.Data))
		}
	}
}

// Send writes one envelope. Safe for concurrent use.
func (c *Client) Send(env protocol.Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("not connected to coordinator")
	}
	c.BytesOut.Add(int64(len(b)))
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.conn.WriteMessage(websocket.TextMessage, b)
}
