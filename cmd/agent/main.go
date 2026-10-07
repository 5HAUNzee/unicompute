// unicompute.exe — the peer agent. One binary per PC. It:
//
//   - registers with the coordinator over an OUTBOUND WebSocket (no inbound ports)
//   - learns which other peers exist
//   - opens a DIRECT WebRTC DataChannel to a chosen peer (STUN hole punching,
//     optional TURN fallback), using the coordinator only to relay signalling
//   - sends tasks to peers and executes tasks received from peers OVER THAT
//     CHANNEL, so the coordinator never sees task data
//   - serves a local web GUI on http://localhost:7777
//
// Subcommand:  unicompute.exe natcheck   — classify this machine's NAT and exit.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"

	"unicompute/internal/natcheck"
	"unicompute/internal/peer"
	"unicompute/internal/protocol"
	"unicompute/internal/signal"
)

const version = "0.1.0"

type config struct {
	coordinator string
	name        string
	stun        string
	turn        string
	turnUser    string
	turnPass    string
	guiPort     int
	noBrowser   bool
}

type peerConn struct {
	p           *peer.Peer
	info        protocol.NodeInfo // last known advert, kept even if it leaves the cloud
	state       string
	telemetry   *protocol.Telemetry
	rttMs       int64
	connectedAt time.Time
}

type Agent struct {
	cfg config

	selfMu sync.Mutex
	self   protocol.NodeInfo

	sig      *signal.Client
	cloudMsg atomic.Value // string

	mu    sync.Mutex
	known map[string]protocol.NodeInfo // peers advertised by the coordinator (excluding self)
	peers map[string]*peerConn         // live/attempted P2P connections by remote ID
	tasks map[string]*taskState
	order []string // task ids in creation order
	recv  map[string]*payloadRecv

	coordStats atomic.Value // json.RawMessage
	gui        *guiHub
	notifyCh   chan struct{}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "natcheck" {
		runNatcheck()
		return
	}
	var cfg config
	host, _ := os.Hostname()
	flag.StringVar(&cfg.coordinator, "coordinator", "", "coordinator WebSocket URL, e.g. wss://xyz.trycloudflare.com/ws or ws://1.2.3.4:8080/ws")
	flag.StringVar(&cfg.name, "name", host, "display name for this PC")
	flag.StringVar(&cfg.stun, "stun", "stun:stun.l.google.com:19302,stun:stun1.l.google.com:19302", "comma-separated STUN URLs")
	flag.StringVar(&cfg.turn, "turn", "", "optional TURN URL for relay fallback, e.g. turn:host:3478")
	flag.StringVar(&cfg.turnUser, "turn-user", "", "TURN username")
	flag.StringVar(&cfg.turnPass, "turn-pass", "", "TURN credential")
	flag.IntVar(&cfg.guiPort, "gui-port", 7777, "local port for the web GUI (binds 127.0.0.1 only)")
	flag.BoolVar(&cfg.noBrowser, "no-browser", false, "do not open the GUI in a browser automatically")
	flag.Parse()

	if cfg.coordinator == "" {
		fmt.Fprintln(os.Stderr, "usage:\n  unicompute.exe --coordinator wss://HOST/ws [--name PC-NAME] [--gui-port 7777]\n  unicompute.exe natcheck")
		os.Exit(2)
	}
	log.SetFlags(log.Ltime)
	newAgent(cfg).run()
}

func runNatcheck() {
	fmt.Println("UniCompute NAT check — probing", strings.Join(natcheck.Servers, ", "))
	r := natcheck.Run(3 * time.Second)
	fmt.Printf("\n  local address : %s:%d\n", r.LocalIP, r.LocalPort)
	for i, m := range r.Mapped {
		fmt.Printf("  mapping %d     : %s\n", i+1, m)
	}
	fmt.Printf("  public IP     : %s\n", r.PublicIP)
	fmt.Printf("  NAT type      : %s\n", strings.ToUpper(r.Type))
	fmt.Printf("  direct P2P    : %v\n\n  %s\n", r.Punchable, r.Description)
}

func newAgent(cfg config) *Agent {
	a := &Agent{
		cfg:      cfg,
		known:    map[string]protocol.NodeInfo{},
		peers:    map[string]*peerConn{},
		tasks:    map[string]*taskState{},
		recv:     map[string]*payloadRecv{},
		notifyCh: make(chan struct{}, 1),
	}
	a.cloudMsg.Store("connecting")
	a.gui = newGUIHub()

	vm, _ := mem.VirtualMemory()
	var total, free uint64
	if vm != nil {
		total, free = vm.Total, vm.Available
	}
	nat := natcheck.Run(2 * time.Second)
	a.self = protocol.NodeInfo{
		ID:       "nd_" + randHex(4),
		Name:     cfg.name,
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		Cores:    runtime.NumCPU(),
		MemTotal: total,
		MemFree:  free,
		NATType:  nat.Type,
		PublicIP: nat.PublicIP,
		Version:  version,
	}
	_, _ = cpu.Percent(0, false) // prime the sampler; the first call always returns 0

	a.sig = signal.New(cfg.coordinator, a.selfInfo, a.heartbeat)
	a.sig.OnPeers = a.onPeers
	a.sig.OnSignal = a.onSignal
	a.sig.OnStatus = func(ok bool, msg string) {
		a.cloudMsg.Store(msg)
		if ok {
			log.Printf("[cloud] connected to %s as %s (%s)", cfg.coordinator, a.self.ID, a.self.Name)
		} else {
			log.Printf("[cloud] %s", msg)
		}
		a.notify()
	}
	return a
}

func (a *Agent) run() {
	ctx := context.Background()
	go a.serveGUI()
	go a.sig.Run(ctx)
	go a.telemetryLoop()
	go a.coordStatsLoop()
	go a.stateLoop()

	url := fmt.Sprintf("http://127.0.0.1:%d", a.cfg.guiPort)
	log.Printf("[gui] UniCompute %s - node %s (%s) - GUI at %s", version, a.self.ID, a.self.Name, url)
	log.Printf("[nat] type=%s public=%s", a.self.NATType, a.self.PublicIP)
	if !a.cfg.noBrowser {
		openBrowser(url)
	}
	select {}
}

// ---------------------------------------------------------------------------
// self / cloud
// ---------------------------------------------------------------------------

func (a *Agent) selfInfo() protocol.NodeInfo {
	a.selfMu.Lock()
	defer a.selfMu.Unlock()
	return a.self
}

func (a *Agent) heartbeat() protocol.Heartbeat {
	s := a.selfInfo()
	return protocol.Heartbeat{CPUPct: s.CPUPct, MemFree: s.MemFree, ActiveTasks: a.activeTasks()}
}

func (a *Agent) onPeers(list []protocol.NodeInfo) {
	a.mu.Lock()
	a.known = map[string]protocol.NodeInfo{}
	for _, n := range list {
		if n.ID == a.self.ID {
			continue
		}
		a.known[n.ID] = n
		if pc, ok := a.peers[n.ID]; ok {
			pc.info = n
		}
	}
	a.mu.Unlock()
	a.notify()
}

func (a *Agent) peerName(id string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if pc, ok := a.peers[id]; ok && pc.info.Name != "" {
		return pc.info.Name
	}
	if n, ok := a.known[id]; ok {
		return n.Name
	}
	return id
}

// ---------------------------------------------------------------------------
// P2P peer lifecycle
// ---------------------------------------------------------------------------

func (a *Agent) stunList() []string {
	var out []string
	for _, s := range strings.Split(a.cfg.stun, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// newPeerLocked must be called with a.mu held.
func (a *Agent) newPeerLocked(remoteID string) (*peerConn, error) {
	pc := &peerConn{state: "new", info: a.known[remoteID]}
	cfg := peer.Config{
		SelfID:   a.self.ID,
		STUN:     a.stunList(),
		TURNURL:  a.cfg.turn,
		TURNUser: a.cfg.turnUser,
		TURNPass: a.cfg.turnPass,
		SendSignal: func(env protocol.Envelope) {
			if err := a.sig.Send(env); err != nil {
				log.Printf("[signal] send %s to %s failed: %v", env.Type, env.To, err)
			}
		},
	}
	p, err := peer.New(remoteID, cfg,
		func(m webrtc.DataChannelMessage) { a.onPeerMessage(remoteID, pc, m) },
		func(s string) { a.onPeerState(remoteID, pc, s) },
	)
	if err != nil {
		return nil, err
	}
	pc.p = p
	a.peers[remoteID] = pc
	return pc, nil
}

func (a *Agent) connect(remoteID string) error {
	a.mu.Lock()
	if _, ok := a.known[remoteID]; !ok {
		a.mu.Unlock()
		return fmt.Errorf("peer %s is not on the cloud", remoteID)
	}
	var stale *peerConn
	if pc, ok := a.peers[remoteID]; ok {
		switch pc.p.State() {
		case "new", "offering", "answering", "connecting", "negotiated", "connected":
			a.mu.Unlock()
			return fmt.Errorf("already %s with %s", pc.p.State(), remoteID)
		}
		delete(a.peers, remoteID)
		stale = pc
	}
	pc, err := a.newPeerLocked(remoteID)
	a.mu.Unlock()
	if stale != nil {
		_ = stale.p.Close()
	}
	if err != nil {
		return err
	}
	log.Printf("[p2p] offering to %s", remoteID)
	if err := pc.p.Offer(); err != nil {
		a.dropPeer(remoteID, pc)
		return err
	}
	a.notify()
	return nil
}

func (a *Agent) disconnect(remoteID string) error {
	a.mu.Lock()
	pc := a.peers[remoteID]
	a.mu.Unlock()
	if pc == nil {
		return fmt.Errorf("no connection to %s", remoteID)
	}
	a.dropPeer(remoteID, pc)
	return nil
}

func (a *Agent) dropPeer(remoteID string, pc *peerConn) {
	a.mu.Lock()
	if a.peers[remoteID] == pc {
		delete(a.peers, remoteID)
	}
	for _, t := range a.tasks {
		if t.PeerID == remoteID && (t.Status == "running" || t.Status == "queued") {
			t.Status = "failed"
			t.EndedAt = nowMs()
			t.Result = &protocol.TaskResult{ExitCode: -1, Error: "peer disconnected", DurationMs: t.EndedAt - t.StartedAt}
			if t.cancel != nil {
				t.cancel()
			}
		}
	}
	a.mu.Unlock()
	_ = pc.p.Close()
	a.notify()
}

func (a *Agent) onPeerState(remoteID string, pc *peerConn, s string) {
	a.mu.Lock()
	if a.peers[remoteID] != pc {
		a.mu.Unlock()
		return
	}
	pc.state = s
	if s == "connected" {
		pc.connectedAt = time.Now()
	}
	a.mu.Unlock()
	if s == "connected" {
		path := pc.p.Path()
		log.Printf("[p2p] %s CONNECTED - %s  local=%s  remote=%s", remoteID, path.Type, path.Local, path.Remote)
	} else {
		log.Printf("[p2p] %s -> %s", remoteID, s)
	}
	a.notify()
	if s == "failed" || s == "closed" {
		go a.dropPeer(remoteID, pc)
	}
}

func (a *Agent) onSignal(env protocol.Envelope) {
	switch env.Type {
	case protocol.SigOffer:
		var sd webrtc.SessionDescription
		if json.Unmarshal(env.Data, &sd) != nil {
			return
		}
		a.mu.Lock()
		var toClose *peerConn
		if existing, ok := a.peers[env.From]; ok {
			// Glare: both sides clicked Connect at once. The lower ID keeps
			// its offer; the higher ID abandons its own and answers instead.
			if existing.p.IsOffering() && a.self.ID < env.From {
				a.mu.Unlock()
				log.Printf("[p2p] glare with %s: keeping our offer", env.From)
				return
			}
			delete(a.peers, env.From)
			toClose = existing
		}
		if _, ok := a.known[env.From]; !ok {
			a.known[env.From] = protocol.NodeInfo{ID: env.From, Name: env.From}
		}
		pc, err := a.newPeerLocked(env.From)
		a.mu.Unlock()
		if toClose != nil {
			_ = toClose.p.Close()
		}
		if err != nil {
			log.Printf("[p2p] cannot create peer for %s: %v", env.From, err)
			return
		}
		log.Printf("[p2p] offer from %s — answering", env.From)
		if err := pc.p.HandleOffer(sd); err != nil {
			log.Printf("[p2p] answer to %s failed: %v", env.From, err)
			a.dropPeer(env.From, pc)
		}
		a.notify()

	case protocol.SigAnswer:
		var sd webrtc.SessionDescription
		if json.Unmarshal(env.Data, &sd) != nil {
			return
		}
		a.mu.Lock()
		pc := a.peers[env.From]
		a.mu.Unlock()
		if pc == nil {
			return
		}
		if err := pc.p.HandleAnswer(sd); err != nil {
			log.Printf("[p2p] bad answer from %s: %v", env.From, err)
		}

	case protocol.SigICE:
		var c webrtc.ICECandidateInit
		if json.Unmarshal(env.Data, &c) != nil {
			return
		}
		a.mu.Lock()
		pc := a.peers[env.From]
		a.mu.Unlock()
		if pc != nil {
			_ = pc.p.AddICE(c)
		}
	}
}

// ---------------------------------------------------------------------------
// background loops
// ---------------------------------------------------------------------------

func (a *Agent) telemetryLoop() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		var pct float64
		if v, err := cpu.Percent(0, false); err == nil && len(v) > 0 {
			pct = v[0]
		}
		var free uint64
		if vm, err := mem.VirtualMemory(); err == nil {
			free = vm.Available
		}
		a.selfMu.Lock()
		a.self.CPUPct = pct
		a.self.MemFree = free
		a.selfMu.Unlock()

		tele := protocol.Telemetry{CPUPct: pct, MemFree: free, ActiveTasks: a.activeTasks(), TS: nowMs()}
		a.mu.Lock()
		conns := make([]*peerConn, 0, len(a.peers))
		for _, pc := range a.peers {
			if pc.state == "connected" {
				conns = append(conns, pc)
			}
		}
		a.mu.Unlock()
		for _, pc := range conns {
			_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTelemetry, Data: protocol.Marshal(tele)})
			_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PPing, Data: protocol.Marshal(protocol.Ping{TS: nowMs()})})
		}
		a.notify()
	}
}

func (a *Agent) coordStatsLoop() {
	base := a.cfg.coordinator
	base = strings.Replace(base, "wss://", "https://", 1)
	base = strings.Replace(base, "ws://", "http://", 1)
	base = strings.TrimSuffix(base, "/ws")
	client := &http.Client{Timeout: 3 * time.Second}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		if !a.sig.Connected.Load() {
			a.coordStats.Store(json.RawMessage(nil))
			continue
		}
		resp, err := client.Get(base + "/api/stats")
		if err != nil {
			a.coordStats.Store(json.RawMessage(nil))
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode == 200 && json.Valid(b) {
			a.coordStats.Store(json.RawMessage(b))
		}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (a *Agent) notify() {
	select {
	case a.notifyCh <- struct{}{}:
	default:
	}
}

func (a *Agent) activeTasks() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, t := range a.tasks {
		if t.Status == "running" || t.Status == "queued" {
			n++
		}
	}
	return n
}

func nowMs() int64 { return time.Now().UnixMilli() }

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
