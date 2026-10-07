// Package peer wraps a single pion/webrtc PeerConnection carrying one ordered,
// reliable DataChannel named "unicompute". Signalling (offer/answer/ICE) goes
// out through the SendSignal callback; the coordinator relays it. Once the
// channel is open, nothing in this package ever talks to the coordinator again.
package peer

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"unicompute/internal/protocol"
)

type Config struct {
	SelfID     string
	STUN       []string
	TURNURL    string
	TURNUser   string
	TURNPass   string
	SendSignal func(protocol.Envelope)
}

// Path describes the ICE candidate pair actually carrying traffic.
type Path struct {
	Type   string `json:"type"`   // human label
	Local  string `json:"local"`  // "srflx 49.37.1.2:51234"
	Remote string `json:"remote"` // "srflx 103.4.5.6:40011"
	Direct bool   `json:"direct"` // false when a TURN relay is in the path
}

type Peer struct {
	RemoteID string
	cfg      Config
	pc       *webrtc.PeerConnection

	dcMu sync.Mutex
	dc   *webrtc.DataChannel

	mu         sync.Mutex
	remoteSet  bool
	pendingICE []webrtc.ICECandidateInit
	offering   bool
	state      string

	lowBuf chan struct{}

	BytesIn  atomic.Int64
	BytesOut atomic.Int64

	onMessage func(webrtc.DataChannelMessage)
	onState   func(string)
}

func buildAPI() *webrtc.API {
	se := webrtc.SettingEngine{}
	// 4 MB SCTP receive window: lets bulk payload transfer fill a residential
	// link instead of stalling on the 256 KB default.
	se.SetSCTPMaxReceiveBufferSize(4 * 1024 * 1024)
	// Fast failure detection so a peer that loses Wi-Fi is noticed in ~5-10 s.
	se.SetICETimeouts(5*time.Second, 10*time.Second, 2*time.Second)
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

// New creates the PeerConnection but sends nothing yet. Call Offer (caller
// side) or HandleOffer (callee side) next.
func New(remoteID string, cfg Config, onMessage func(webrtc.DataChannelMessage), onState func(string)) (*Peer, error) {
	servers := []webrtc.ICEServer{{URLs: cfg.STUN}}
	if cfg.TURNURL != "" {
		servers = append(servers, webrtc.ICEServer{
			URLs:       []string{cfg.TURNURL},
			Username:   cfg.TURNUser,
			Credential: cfg.TURNPass,
		})
	}
	pc, err := buildAPI().NewPeerConnection(webrtc.Configuration{ICEServers: servers})
	if err != nil {
		return nil, err
	}
	p := &Peer{
		RemoteID:  remoteID,
		cfg:       cfg,
		pc:        pc,
		lowBuf:    make(chan struct{}, 1),
		onMessage: onMessage,
		onState:   onState,
		state:     "new",
	}

	// Trickle ICE: every candidate is forwarded the moment it is discovered.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		cfg.SendSignal(protocol.Envelope{
			Type: protocol.SigICE, From: cfg.SelfID, To: remoteID,
			Data: protocol.Marshal(c.ToJSON()),
		})
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		switch s {
		case webrtc.PeerConnectionStateConnected:
			// The transport is up. On the FIRST connect the channel is still
			// opening, so report "negotiated" and let attach's OnOpen promote
			// us. On a RECOVERY (disconnected -> connected after a network
			// blip) the channel is already open and OnOpen will never fire
			// again, so promote straight back to "connected" here.
			if p.channelOpen() {
				p.setState("connected")
			} else {
				p.setState("negotiated")
			}
		default:
			p.setState(s.String())
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) { p.attach(dc) })
	return p, nil
}

// Offer makes this side the caller: creates the channel and sends an SDP offer.
func (p *Peer) Offer() error {
	p.mu.Lock()
	p.offering = true
	p.mu.Unlock()

	ordered := true
	dc, err := p.pc.CreateDataChannel("unicompute", &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return err
	}
	p.attach(dc)

	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	p.cfg.SendSignal(protocol.Envelope{
		Type: protocol.SigOffer, From: p.cfg.SelfID, To: p.RemoteID,
		Data: protocol.Marshal(offer),
	})
	p.setState("offering")
	return nil
}

// HandleOffer makes this side the callee.
func (p *Peer) HandleOffer(sd webrtc.SessionDescription) error {
	if err := p.pc.SetRemoteDescription(sd); err != nil {
		return err
	}
	p.markRemoteSet()
	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := p.pc.SetLocalDescription(answer); err != nil {
		return err
	}
	p.cfg.SendSignal(protocol.Envelope{
		Type: protocol.SigAnswer, From: p.cfg.SelfID, To: p.RemoteID,
		Data: protocol.Marshal(answer),
	})
	p.setState("answering")
	return nil
}

func (p *Peer) HandleAnswer(sd webrtc.SessionDescription) error {
	if err := p.pc.SetRemoteDescription(sd); err != nil {
		return err
	}
	p.markRemoteSet()
	return nil
}

// AddICE queues candidates that arrive before the remote description is set
// (trickle ICE makes that ordering race unavoidable).
func (p *Peer) AddICE(c webrtc.ICECandidateInit) error {
	p.mu.Lock()
	if !p.remoteSet {
		p.pendingICE = append(p.pendingICE, c)
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	return p.pc.AddICECandidate(c)
}

func (p *Peer) markRemoteSet() {
	p.mu.Lock()
	p.remoteSet = true
	pend := p.pendingICE
	p.pendingICE = nil
	p.mu.Unlock()
	for _, c := range pend {
		_ = p.pc.AddICECandidate(c)
	}
}

func (p *Peer) IsOffering() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.offering
}

func (p *Peer) State() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

func (p *Peer) setState(s string) {
	p.mu.Lock()
	p.state = s
	p.mu.Unlock()
	if p.onState != nil {
		p.onState(s)
	}
}

func (p *Peer) attach(dc *webrtc.DataChannel) {
	p.dcMu.Lock()
	p.dc = dc
	p.dcMu.Unlock()

	dc.SetBufferedAmountLowThreshold(512 * 1024)
	dc.OnBufferedAmountLow(func() {
		select {
		case p.lowBuf <- struct{}{}:
		default:
		}
	})
	dc.OnOpen(func() { p.setState("connected") })
	dc.OnClose(func() { p.setState("closed") })
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		p.BytesIn.Add(int64(len(m.Data)))
		if p.onMessage != nil {
			p.onMessage(m)
		}
	})
}

func (p *Peer) channelOpen() bool {
	p.dcMu.Lock()
	dc := p.dc
	p.dcMu.Unlock()
	return dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen
}

func (p *Peer) channel() (*webrtc.DataChannel, error) {
	p.dcMu.Lock()
	dc := p.dc
	p.dcMu.Unlock()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return nil, errors.New("data channel not open")
	}
	return dc, nil
}

// SendJSON sends a control message as a text frame.
func (p *Peer) SendJSON(v any) error {
	dc, err := p.channel()
	if err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	p.BytesOut.Add(int64(len(b)))
	return dc.SendText(string(b))
}

// SendBinary sends a payload chunk with backpressure: it waits while more than
// 1 MB is already queued so a 50 MB transfer cannot exhaust memory.
func (p *Peer) SendBinary(b []byte) error {
	dc, err := p.channel()
	if err != nil {
		return err
	}
	for dc.BufferedAmount() > 1<<20 {
		select {
		case <-p.lowBuf:
		case <-time.After(20 * time.Millisecond):
		}
		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return errors.New("data channel closed during send")
		}
	}
	p.BytesOut.Add(int64(len(b)))
	return dc.Send(b)
}

// Path reports which ICE candidate pair was selected. Empty until connected.
func (p *Peer) Path() Path {
	var out Path
	s := p.pc.SCTP()
	if s == nil {
		return out
	}
	t := s.Transport()
	if t == nil {
		return out
	}
	it := t.ICETransport()
	if it == nil {
		return out
	}
	pair, err := it.GetSelectedCandidatePair()
	if err != nil || pair == nil || pair.Local == nil || pair.Remote == nil {
		return out
	}
	lt, rt := pair.Local.Typ.String(), pair.Remote.Typ.String()
	out.Local = fmt.Sprintf("%s %s:%d", lt, pair.Local.Address, pair.Local.Port)
	out.Remote = fmt.Sprintf("%s %s:%d", rt, pair.Remote.Address, pair.Remote.Port)
	switch {
	case lt == "relay" || rt == "relay":
		out.Type = "RELAY via TURN"
		out.Direct = false
	case lt == "host" && rt == "host":
		out.Type = "DIRECT (same LAN)"
		out.Direct = true
	default:
		out.Type = "DIRECT (NAT-punched " + lt + "↔" + rt + ")"
		out.Direct = true
	}
	return out
}

func (p *Peer) Close() error {
	return p.pc.Close()
}
