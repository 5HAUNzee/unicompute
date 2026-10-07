package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"unicompute/internal/natcheck"
	"unicompute/internal/peer"
	"unicompute/internal/protocol"
)

//go:embed web/index.html
var indexHTML []byte

// ---------------------------------------------------------------------------
// state snapshot sent to the GUI
// ---------------------------------------------------------------------------

type guiPeer struct {
	protocol.NodeInfo
	OnCloud   bool                `json:"on_cloud"`
	P2PState  string              `json:"p2p_state"`
	Connected bool                `json:"connected"`
	Path      peer.Path           `json:"path"`
	RTTMs     int64               `json:"rtt_ms"`
	Telemetry *protocol.Telemetry `json:"telemetry,omitempty"`
	BytesIn   int64               `json:"bytes_in"`
	BytesOut  int64               `json:"bytes_out"`
	Since     int64               `json:"connected_since,omitempty"`
}

type guiState struct {
	Self  protocol.NodeInfo `json:"self"`
	Cloud struct {
		URL       string `json:"url"`
		Connected bool   `json:"connected"`
		Msg       string `json:"msg"`
		BytesIn   int64  `json:"bytes_in"`
		BytesOut  int64  `json:"bytes_out"`
	} `json:"cloud"`
	CoordStats json.RawMessage `json:"coord_stats,omitempty"`
	Peers      []guiPeer       `json:"peers"`
	Tasks      []*taskState    `json:"tasks"`
	P2P        struct {
		BytesIn  int64 `json:"bytes_in"`
		BytesOut int64 `json:"bytes_out"`
	} `json:"p2p"`
	Now int64 `json:"now"`
}

// stateJSON builds and marshals the snapshot under the agent lock so the GUI
// never observes a half-updated task.
func (a *Agent) stateJSON() []byte {
	var st guiState
	st.Self = a.selfInfo()
	st.Cloud.URL = a.cfg.coordinator
	st.Cloud.Connected = a.sig.Connected.Load()
	if m, ok := a.cloudMsg.Load().(string); ok {
		st.Cloud.Msg = m
	}
	st.Cloud.BytesIn = a.sig.BytesIn.Load()
	st.Cloud.BytesOut = a.sig.BytesOut.Load()
	if cs, ok := a.coordStats.Load().(json.RawMessage); ok && len(cs) > 0 {
		st.CoordStats = cs
	}
	st.Now = nowMs()

	a.mu.Lock()
	seen := map[string]bool{}
	for id, n := range a.known {
		gp := guiPeer{NodeInfo: n, OnCloud: true}
		if pc, ok := a.peers[id]; ok {
			fill(&gp, pc)
		}
		st.Peers = append(st.Peers, gp)
		seen[id] = true
	}
	for id, pc := range a.peers { // P2P-connected peers that left the cloud still show
		if seen[id] {
			continue
		}
		gp := guiPeer{NodeInfo: pc.info, OnCloud: false}
		fill(&gp, pc)
		st.Peers = append(st.Peers, gp)
	}
	for _, pc := range a.peers {
		st.P2P.BytesIn += pc.p.BytesIn.Load()
		st.P2P.BytesOut += pc.p.BytesOut.Load()
	}
	for i := len(a.order) - 1; i >= 0; i-- {
		if t := a.tasks[a.order[i]]; t != nil {
			st.Tasks = append(st.Tasks, t)
		}
	}
	b, err := json.Marshal(map[string]any{"type": "state", "state": st})
	a.mu.Unlock()
	if err != nil {
		return []byte(`{"type":"error","message":"state marshal failed"}`)
	}
	return b
}

func fill(gp *guiPeer, pc *peerConn) {
	gp.P2PState = pc.state
	gp.Connected = pc.state == "connected"
	gp.RTTMs = pc.rttMs
	gp.Telemetry = pc.telemetry
	gp.BytesIn = pc.p.BytesIn.Load()
	gp.BytesOut = pc.p.BytesOut.Load()
	if gp.Connected {
		gp.Path = pc.p.Path()
		gp.Since = pc.connectedAt.UnixMilli()
	}
	if gp.Name == "" {
		gp.Name = pc.info.Name
	}
}

// ---------------------------------------------------------------------------
// local WebSocket hub for the browser
// ---------------------------------------------------------------------------

type guiHub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]*sync.Mutex
}

func newGUIHub() *guiHub { return &guiHub{clients: map[*websocket.Conn]*sync.Mutex{}} }

func (h *guiHub) add(c *websocket.Conn) {
	h.mu.Lock()
	h.clients[c] = &sync.Mutex{}
	h.mu.Unlock()
}

func (h *guiHub) remove(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *guiHub) sendTo(c *websocket.Conn, b []byte) {
	h.mu.Lock()
	wm := h.clients[c]
	h.mu.Unlock()
	if wm == nil {
		return
	}
	wm.Lock()
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = c.WriteMessage(websocket.TextMessage, b)
	wm.Unlock()
}

func (h *guiHub) broadcast(b []byte) {
	h.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for c := range h.clients {
		conns = append(conns, c)
	}
	h.mu.Unlock()
	for _, c := range conns {
		h.sendTo(c, b)
	}
}

// stateLoop pushes a fresh snapshot on every notify (coalesced) and at least
// once a second.
func (a *Agent) stateLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.notifyCh:
			time.Sleep(40 * time.Millisecond)
			select {
			case <-a.notifyCh:
			default:
			}
		case <-t.C:
		}
		a.gui.broadcast(a.stateJSON())
	}
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

var guiUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type guiCommand struct {
	Cmd string `json:"cmd"` // connect | disconnect | submit | cancel
	submitReq
	TaskID string `json:"task_id"`
}

func (a *Agent) handleCommand(c guiCommand) (any, error) {
	switch c.Cmd {
	case "connect":
		return map[string]string{"ok": "connecting"}, a.connect(c.PeerID)
	case "disconnect":
		return map[string]string{"ok": "disconnected"}, a.disconnect(c.PeerID)
	case "submit":
		id, err := a.submit(c.submitReq)
		return map[string]string{"task_id": id}, err
	case "cancel":
		return map[string]string{"ok": "cancelled"}, a.cancelTask(c.TaskID)
	}
	return nil, fmt.Errorf("unknown command %q", c.Cmd)
}

func (a *Agent) serveGUI() {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := guiUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		a.gui.add(c)
		defer func() { a.gui.remove(c); c.Close() }()
		a.gui.sendTo(c, a.stateJSON())
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var cmd guiCommand
			if json.Unmarshal(data, &cmd) != nil {
				continue
			}
			if _, err := a.handleCommand(cmd); err != nil {
				b, _ := json.Marshal(map[string]string{"type": "error", "message": err.Error()})
				a.gui.sendTo(c, b)
			}
			a.notify()
		}
	})

	// JSON API — the same operations the GUI uses, for curl-driven tests.
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(a.stateJSON())
	})
	mux.HandleFunc("/api/natcheck", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(natcheck.Run(3 * time.Second))
	})
	mux.HandleFunc("/api/task", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			id := r.URL.Query().Get("id")
			a.mu.Lock()
			t := a.tasks[id]
			var b []byte
			if t != nil {
				b, _ = json.Marshal(t)
			}
			a.mu.Unlock()
			if b == nil {
				http.Error(w, `{"error":"unknown task"}`, 404)
				return
			}
			_, _ = w.Write(b)
			return
		}
		var req submitReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad json"}`, 400)
			return
		}
		id, err := a.submit(req)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"task_id": id})
	})
	for _, name := range []string{"connect", "disconnect", "cancel"} {
		cmd := name
		mux.HandleFunc("/api/"+cmd, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			var c guiCommand
			_ = json.NewDecoder(r.Body).Decode(&c)
			c.Cmd = cmd
			out, err := a.handleCommand(c)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), 400)
				return
			}
			_ = json.NewEncoder(w).Encode(out)
		})
	}

	addr := fmt.Sprintf("127.0.0.1:%d", a.cfg.guiPort)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[gui] cannot listen on %s: %v (is another agent using this port? try --gui-port 7778)", addr, err)
	}
}

// sortedPeerIDs is used by tests and debug output.
func (a *Agent) sortedPeerIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.known))
	for id := range a.known {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
