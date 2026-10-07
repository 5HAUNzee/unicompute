// The coordinator is the only publicly reachable component. It does exactly
// three things: keeps a registry of connected agents, broadcasts that list,
// and relays WebRTC signalling (offer/answer/ICE) between two agents by ID.
//
// It never carries task data. Its byte counters exist so you can watch them
// stay flat while a 50 MB payload moves peer-to-peer.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"unicompute/internal/protocol"
)

const (
	staleAfter    = 8 * time.Second
	reapEvery     = 2 * time.Second
	broadcastEach = 2 * time.Second
)

type session struct {
	info     protocol.NodeInfo
	conn     *websocket.Conn
	wmu      sync.Mutex
	lastSeen time.Time
	bytesIn  int64 // signalling bytes relayed TO this node
	bytesOut int64 // signalling bytes relayed FROM this node
	joined   time.Time
}

func (s *session) send(env protocol.Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.conn.WriteMessage(websocket.TextMessage, b)
}

type hub struct {
	mu       sync.RWMutex
	sessions map[string]*session
	relayed  int64 // total signalling bytes relayed, ever
	started  time.Time
}

func newHub() *hub {
	return &hub{sessions: map[string]*session{}, started: time.Now()}
}

func (h *hub) snapshot() []protocol.NodeInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]protocol.NodeInfo, 0, len(h.sessions))
	for _, s := range h.sessions {
		n := s.info
		n.LastSeen = s.lastSeen.UnixMilli()
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (h *hub) broadcastPeers() {
	data := protocol.Marshal(h.snapshot())
	h.mu.RLock()
	targets := make([]*session, 0, len(h.sessions))
	for _, s := range h.sessions {
		targets = append(targets, s)
	}
	h.mu.RUnlock()
	for _, s := range targets {
		_ = s.send(protocol.Envelope{Type: protocol.SigPeers, Data: data})
	}
}

func (h *hub) remove(id string, conn *websocket.Conn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.sessions[id]
	if !ok || s.conn != conn {
		return false
	}
	delete(h.sessions, id)
	return true
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin:     func(*http.Request) bool { return true },
}

func (h *hub) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	var sess *session
	defer func() {
		if sess != nil {
			if h.remove(sess.info.ID, conn) {
				log.Printf("[coord] node left   %s (%s)", sess.info.ID, sess.info.Name)
				h.broadcastPeers()
			}
		}
		conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}

		switch env.Type {
		case protocol.SigRegister:
			var info protocol.NodeInfo
			if err := json.Unmarshal(env.Data, &info); err != nil || info.ID == "" {
				_ = writeErr(conn, "register requires a NodeInfo with an id")
				continue
			}
			sess = &session{info: info, conn: conn, lastSeen: time.Now(), joined: time.Now()}
			h.mu.Lock()
			if old, ok := h.sessions[info.ID]; ok && old.conn != conn {
				old.conn.Close() // same node reconnecting: drop the stale socket
			}
			h.sessions[info.ID] = sess
			h.mu.Unlock()
			_ = sess.send(protocol.Envelope{Type: protocol.SigWelcome, Data: protocol.Marshal(map[string]string{"id": info.ID})})
			log.Printf("[coord] node joined %s (%s) %s/%s %d cores nat=%s", info.ID, info.Name, info.OS, info.Arch, info.Cores, info.NATType)
			h.broadcastPeers()

		case protocol.SigHeartbeat:
			if sess == nil {
				continue
			}
			var hb protocol.Heartbeat
			if json.Unmarshal(env.Data, &hb) == nil {
				h.mu.Lock()
				sess.info.CPUPct = hb.CPUPct
				sess.info.MemFree = hb.MemFree
				sess.lastSeen = time.Now()
				h.mu.Unlock()
			}

		case protocol.SigOffer, protocol.SigAnswer, protocol.SigICE:
			if sess == nil {
				continue
			}
			env.From = sess.info.ID // never trust the client's From
			h.mu.RLock()
			target := h.sessions[env.To]
			h.mu.RUnlock()
			if target == nil {
				_ = writeErr(conn, "unknown peer "+env.To)
				continue
			}
			if err := target.send(env); err != nil {
				_ = writeErr(conn, "relay to "+env.To+" failed: "+err.Error())
				continue
			}
			n := int64(len(data))
			h.mu.Lock()
			sess.bytesOut += n
			target.bytesIn += n
			h.relayed += n
			h.mu.Unlock()

		default:
			_ = writeErr(conn, "unknown message type "+env.Type)
		}
	}
}

func writeErr(conn *websocket.Conn, msg string) error {
	b, _ := json.Marshal(protocol.Envelope{Type: protocol.SigError, Data: protocol.Marshal(map[string]string{"message": msg})})
	return conn.WriteMessage(websocket.TextMessage, b)
}

func (h *hub) reaper() {
	t := time.NewTicker(reapEvery)
	defer t.Stop()
	for range t.C {
		var dead []*session
		h.mu.Lock()
		for id, s := range h.sessions {
			if time.Since(s.lastSeen) > staleAfter {
				delete(h.sessions, id)
				dead = append(dead, s)
			}
		}
		h.mu.Unlock()
		for _, s := range dead {
			log.Printf("[coord] node stale  %s (%s) - removed", s.info.ID, s.info.Name)
			s.conn.Close()
		}
		if len(dead) > 0 {
			h.broadcastPeers()
		}
	}
}

func (h *hub) periodicBroadcast() {
	t := time.NewTicker(broadcastEach)
	defer t.Stop()
	for range t.C {
		h.broadcastPeers()
	}
}

type statsNode struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	BytesIn  int64  `json:"signal_bytes_to"`
	BytesOut int64  `json:"signal_bytes_from"`
	Online   string `json:"online_for"`
}

func (h *hub) stats() map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	nodes := make([]statsNode, 0, len(h.sessions))
	for _, s := range h.sessions {
		nodes = append(nodes, statsNode{s.info.ID, s.info.Name, s.bytesIn, s.bytesOut, time.Since(s.joined).Round(time.Second).String()})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return map[string]any{
		"nodes":                len(nodes),
		"total_relayed_bytes":  h.relayed,
		"uptime":               time.Since(h.started).Round(time.Second).String(),
		"sessions":             nodes,
		"note":                 "relayed bytes are SIGNALLING ONLY (offer/answer/ICE). Task data never passes through here.",
	}
}

func jsonHandler(fn func() any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(fn())
	}
}

const statusPage = `<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>UniCompute coordinator</title>
<style>body{font:14px system-ui;margin:28px;color:#111}table{border-collapse:collapse}td,th{padding:4px 12px;border-bottom:1px solid #ddd;text-align:left}code{background:#eee;padding:1px 5px}</style>
<h2>UniCompute coordinator</h2>
<p>Signalling relay only. <b>Total relayed: %d bytes</b> &nbsp;·&nbsp; nodes: %d &nbsp;·&nbsp; uptime %s</p>
<table><tr><th>ID</th><th>Name</th><th>OS</th><th>Cores</th><th>CPU%%</th><th>NAT</th><th>Public IP</th><th>Last seen</th></tr>%s</table>
<p>JSON: <code>/api/nodes</code> &nbsp; <code>/api/stats</code> &nbsp; WebSocket: <code>/ws</code></p>`

func (h *hub) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	rows := ""
	for _, n := range h.snapshot() {
		ago := time.Since(time.UnixMilli(n.LastSeen)).Round(time.Second)
		rows += fmt.Sprintf("<tr><td>%s</td><td>%s</td><td>%s/%s</td><td>%d</td><td>%.0f</td><td>%s</td><td>%s</td><td>%s ago</td></tr>",
			n.ID, n.Name, n.OS, n.Arch, n.Cores, n.CPUPct, n.NATType, n.PublicIP, ago)
	}
	h.mu.RLock()
	relayed, count := h.relayed, len(h.sessions)
	h.mu.RUnlock()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, statusPage, relayed, count, time.Since(h.started).Round(time.Second), rows)
}

func main() {
	listen := flag.String("listen", ":8080", "address to listen on")
	flag.Parse()

	h := newHub()
	go h.reaper()
	go h.periodicBroadcast()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	mux.HandleFunc("/api/nodes", jsonHandler(func() any { return h.snapshot() }))
	mux.HandleFunc("/api/stats", jsonHandler(func() any { return h.stats() }))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("/", h.handleRoot)

	log.Printf("[coord] UniCompute coordinator listening on %s  (ws endpoint: /ws)", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
