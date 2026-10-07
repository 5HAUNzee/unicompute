package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"unicompute/internal/protocol"
	"unicompute/internal/task"
)

const maxOutput = 32 * 1024

// taskState is what the GUI sees for every task, sent or received.
type taskState struct {
	ID          string               `json:"id"`
	PeerID      string               `json:"peer_id"`
	PeerName    string               `json:"peer_name"`
	Direction   string               `json:"direction"` // sent | received
	Kind        string               `json:"kind"`      // shell | compute | payload
	Command     string               `json:"command,omitempty"`
	N           int64                `json:"n,omitempty"`
	Bytes       int64                `json:"bytes,omitempty"`
	TimeoutSec  int                  `json:"timeout_sec,omitempty"`
	Status      string               `json:"status"` // queued | running | done | failed | cancelled
	Output      string               `json:"output"`
	Result      *protocol.TaskResult `json:"result,omitempty"`
	Path        string               `json:"path,omitempty"`         // P2P path type when submitted
	LocalDigest string               `json:"local_digest,omitempty"` // payload sender's own sha256
	DigestMatch *bool                `json:"digest_match,omitempty"`
	BytesSent   int64                `json:"bytes_sent,omitempty"`
	BytesRecv   int64                `json:"bytes_recv,omitempty"`
	StartedAt   int64                `json:"started_at"`
	EndedAt     int64                `json:"ended_at,omitempty"`
	cancel      context.CancelFunc
}

type payloadRecv struct {
	h     hash.Hash
	n     atomic.Int64
	start time.Time
}

func newTaskID() string { return randHex(protocol.TaskIDLen / 2) }

func (a *Agent) addTask(t *taskState) {
	a.mu.Lock()
	a.tasks[t.ID] = t
	a.order = append(a.order, t.ID)
	if len(a.order) > 200 { // keep the GUI bounded
		old := a.order[0]
		a.order = a.order[1:]
		delete(a.tasks, old)
	}
	a.mu.Unlock()
	a.notify()
}

func (a *Agent) appendOutput(id, text string) {
	a.mu.Lock()
	if t := a.tasks[id]; t != nil {
		t.Output += text
		if len(t.Output) > maxOutput {
			t.Output = "…[truncated]…\n" + t.Output[len(t.Output)-maxOutput:]
		}
	}
	a.mu.Unlock()
	a.notify()
}

// finishTask records a result exactly once and releases the task's context.
func (a *Agent) finishTask(id string, r *protocol.TaskResult) {
	a.mu.Lock()
	t := a.tasks[id]
	if t == nil || t.Status == "done" || t.Status == "failed" || t.Status == "cancelled" {
		a.mu.Unlock()
		return
	}
	t.EndedAt = nowMs()
	if r.DurationMs == 0 {
		r.DurationMs = t.EndedAt - t.StartedAt
	}
	t.Result = r
	switch {
	case strings.HasPrefix(r.Error, "cancelled"):
		t.Status = "cancelled"
	case r.Error != "":
		t.Status = "failed"
	default:
		t.Status = "done" // a non-zero exit code is still a completed task
	}
	if t.Direction == "sent" && t.Kind == "payload" && r.Digest != "" && t.LocalDigest != "" {
		m := r.Digest == t.LocalDigest
		t.DigestMatch = &m
	}
	cancel := t.cancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	a.notify()
}

// ---------------------------------------------------------------------------
// sending side
// ---------------------------------------------------------------------------

type submitReq struct {
	PeerID     string `json:"peer_id"`
	Kind       string `json:"kind"`
	Command    string `json:"command"`
	N          int64  `json:"n"`
	MB         int64  `json:"mb"`
	TimeoutSec int    `json:"timeout_sec"`
}

func (a *Agent) submit(req submitReq) (string, error) {
	a.mu.Lock()
	pc := a.peers[req.PeerID]
	a.mu.Unlock()
	if pc == nil || pc.p.State() != "connected" {
		return "", errors.New("peer is not P2P-connected — click Connect first")
	}
	switch req.Kind {
	case "shell":
		if strings.TrimSpace(req.Command) == "" {
			return "", errors.New("command is empty")
		}
	case "compute":
		if req.N <= 0 {
			req.N = 5_000_000
		}
	case "payload":
		if req.MB <= 0 {
			req.MB = 20
		}
	default:
		return "", fmt.Errorf("unknown task kind %q", req.Kind)
	}
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = 120
	}

	id := newTaskID()
	ctx, cancel := context.WithCancel(context.Background())
	t := &taskState{
		ID: id, PeerID: req.PeerID, PeerName: a.peerName(req.PeerID), Direction: "sent",
		Kind: req.Kind, Command: req.Command, N: req.N, TimeoutSec: req.TimeoutSec,
		Status: "queued", StartedAt: nowMs(), Path: pc.p.Path().Type, cancel: cancel,
	}
	sub := protocol.TaskSubmit{Kind: req.Kind, Command: req.Command, N: req.N, TimeoutSec: req.TimeoutSec}
	if req.Kind == "payload" {
		t.Bytes = req.MB << 20
		sub.Bytes = t.Bytes
	}
	a.addTask(t)

	if err := pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTaskSubmit, TaskID: id, Data: protocol.Marshal(sub)}); err != nil {
		a.finishTask(id, &protocol.TaskResult{ExitCode: -1, Error: err.Error()})
		return id, err
	}
	log.Printf("[task] %s sent to %s: %s", id, t.PeerName, describe(t))

	if req.Kind == "payload" {
		go a.streamPayload(ctx, t, pc)
	}
	// Safety net: if the peer never answers at all.
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(req.TimeoutSec+10) * time.Second):
			a.finishTask(id, &protocol.TaskResult{ExitCode: -1, Error: "no result from peer within timeout"})
		}
	}()
	return id, nil
}

func describe(t *taskState) string {
	switch t.Kind {
	case "shell":
		return "shell `" + t.Command + "`"
	case "compute":
		return fmt.Sprintf("count primes below %d", t.N)
	case "payload":
		return fmt.Sprintf("payload %d MB", t.Bytes>>20)
	}
	return t.Kind
}

// streamPayload pushes t.Bytes of pseudo-random data as 16 KB binary frames,
// each prefixed with the task id, then announces completion.
func (a *Agent) streamPayload(ctx context.Context, t *taskState, pc *peerConn) {
	const chunk = 16 * 1024
	seed := make([]byte, chunk)
	_, _ = rand.Read(seed)
	h := sha256.New()
	var sent int64
	var ctr uint64
	lastNotify := time.Now()
	for sent < t.Bytes && ctx.Err() == nil {
		n := int64(chunk)
		if rem := t.Bytes - sent; rem < n {
			n = rem
		}
		// A fresh slice per frame: the SCTP layer may hold a reference to it.
		frame := make([]byte, protocol.TaskIDLen+int(n))
		copy(frame, t.ID)
		copy(frame[protocol.TaskIDLen:], seed[:n])
		binary.BigEndian.PutUint64(frame[protocol.TaskIDLen:], ctr) // make every chunk distinct
		ctr++
		h.Write(frame[protocol.TaskIDLen:])
		if err := pc.p.SendBinary(frame); err != nil {
			a.finishTask(t.ID, &protocol.TaskResult{ExitCode: -1, Error: "send failed: " + err.Error()})
			return
		}
		sent += n
		a.mu.Lock()
		t.BytesSent = sent
		a.mu.Unlock()
		if time.Since(lastNotify) > 200*time.Millisecond {
			a.notify()
			lastNotify = time.Now()
		}
	}
	if ctx.Err() != nil {
		return
	}
	a.mu.Lock()
	t.LocalDigest = hex.EncodeToString(h.Sum(nil))
	a.mu.Unlock()
	_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PPayloadDone, TaskID: t.ID, Data: protocol.Marshal(protocol.PayloadDone{Bytes: sent})})
	a.notify()
}

func (a *Agent) cancelTask(id string) error {
	a.mu.Lock()
	t := a.tasks[id]
	var pc *peerConn
	if t != nil {
		pc = a.peers[t.PeerID]
	}
	a.mu.Unlock()
	if t == nil {
		return errors.New("unknown task")
	}
	if t.Direction == "sent" && pc != nil {
		_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTaskCancel, TaskID: id})
	}
	if t.cancel != nil {
		t.cancel()
	}
	a.finishTask(id, &protocol.TaskResult{ExitCode: -1, Error: "cancelled by user"})
	return nil
}

// ---------------------------------------------------------------------------
// receiving side: every message that arrives over a DataChannel lands here
// ---------------------------------------------------------------------------

func (a *Agent) onPeerMessage(remoteID string, pc *peerConn, m webrtc.DataChannelMessage) {
	if !m.IsString {
		a.onPayloadChunk(m.Data)
		return
	}
	var msg protocol.P2PMsg
	if err := json.Unmarshal(m.Data, &msg); err != nil {
		return
	}
	switch msg.Type {
	case protocol.P2PPing:
		_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PPong, Data: msg.Data})

	case protocol.P2PPong:
		var p protocol.Ping
		if json.Unmarshal(msg.Data, &p) == nil {
			a.mu.Lock()
			pc.rttMs = nowMs() - p.TS
			a.mu.Unlock()
		}

	case protocol.P2PTelemetry:
		var t protocol.Telemetry
		if json.Unmarshal(msg.Data, &t) == nil {
			a.mu.Lock()
			pc.telemetry = &t
			a.mu.Unlock()
			a.notify()
		}

	case protocol.P2PTaskSubmit:
		a.onTaskSubmit(remoteID, pc, msg)

	case protocol.P2PTaskAccepted:
		a.mu.Lock()
		if t := a.tasks[msg.TaskID]; t != nil && t.Status == "queued" {
			t.Status = "running"
		}
		a.mu.Unlock()
		a.notify()

	case protocol.P2PTaskOutput:
		var o protocol.TaskOutput
		if json.Unmarshal(msg.Data, &o) == nil {
			a.appendOutput(msg.TaskID, o.Text)
		}

	case protocol.P2PTaskResult:
		var r protocol.TaskResult
		if json.Unmarshal(msg.Data, &r) == nil {
			log.Printf("[task] %s result from %s: exit=%d %s %s", msg.TaskID, pc.info.Name, r.ExitCode, r.Summary, r.Error)
			a.finishTask(msg.TaskID, &r)
		}

	case protocol.P2PTaskCancel:
		a.mu.Lock()
		t := a.tasks[msg.TaskID]
		a.mu.Unlock()
		if t != nil && t.cancel != nil {
			t.cancel()
		}

	case protocol.P2PPayloadDone:
		a.onPayloadDone(pc, msg)
	}
}

func (a *Agent) onTaskSubmit(remoteID string, pc *peerConn, msg protocol.P2PMsg) {
	var sub protocol.TaskSubmit
	if err := json.Unmarshal(msg.Data, &sub); err != nil {
		return
	}
	base := context.Background()
	var ctx context.Context
	var cancel context.CancelFunc
	if sub.TimeoutSec > 0 {
		ctx, cancel = context.WithTimeout(base, time.Duration(sub.TimeoutSec)*time.Second)
	} else {
		ctx, cancel = context.WithCancel(base)
	}
	t := &taskState{
		ID: msg.TaskID, PeerID: remoteID, PeerName: a.peerName(remoteID), Direction: "received",
		Kind: sub.Kind, Command: sub.Command, N: sub.N, Bytes: sub.Bytes, TimeoutSec: sub.TimeoutSec,
		Status: "running", StartedAt: nowMs(), Path: pc.p.Path().Type, cancel: cancel,
	}
	a.addTask(t)
	log.Printf("[task] %s received from %s: %s", t.ID, t.PeerName, describe(t))
	_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTaskAccepted, TaskID: t.ID})

	out := func(stream, text string) {
		a.appendOutput(t.ID, text)
		_ = pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTaskOutput, TaskID: t.ID, Data: protocol.Marshal(protocol.TaskOutput{Stream: stream, Text: text})})
	}

	switch sub.Kind {
	case "shell":
		go func() {
			r := task.RunShell(ctx, sub.Command, out)
			a.completeReceived(t, pc, protocol.TaskResult{ExitCode: r.ExitCode, Error: r.Err, Summary: r.Summary})
		}()
	case "compute":
		go func() {
			r := task.RunPrimes(ctx, sub.N, out)
			a.completeReceived(t, pc, protocol.TaskResult{ExitCode: r.ExitCode, Error: r.Err, Summary: r.Summary})
		}()
	case "payload":
		pr := &payloadRecv{h: sha256.New(), start: time.Now()}
		a.mu.Lock()
		a.recv[t.ID] = pr
		a.mu.Unlock()
		go func() {
			<-ctx.Done()
			a.mu.Lock()
			_, pending := a.recv[t.ID]
			delete(a.recv, t.ID)
			a.mu.Unlock()
			if pending {
				msg := "cancelled: " + ctx.Err().Error()
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					msg = "timed out waiting for payload"
				}
				a.completeReceived(t, pc, protocol.TaskResult{ExitCode: -1, Error: msg, Bytes: pr.n.Load()})
			}
		}()
	default:
		a.completeReceived(t, pc, protocol.TaskResult{ExitCode: -1, Error: "unknown task kind " + sub.Kind})
	}
}

func (a *Agent) completeReceived(t *taskState, pc *peerConn, r protocol.TaskResult) {
	r.DurationMs = nowMs() - t.StartedAt
	a.finishTask(t.ID, &r)
	if err := pc.p.SendJSON(protocol.P2PMsg{Type: protocol.P2PTaskResult, TaskID: t.ID, Data: protocol.Marshal(r)}); err != nil {
		log.Printf("[task] %s: could not return result: %v", t.ID, err)
	}
}

func (a *Agent) onPayloadChunk(data []byte) {
	if len(data) < protocol.TaskIDLen {
		return
	}
	id := string(data[:protocol.TaskIDLen])
	a.mu.Lock()
	pr := a.recv[id]
	t := a.tasks[id]
	a.mu.Unlock()
	if pr == nil {
		return
	}
	pr.h.Write(data[protocol.TaskIDLen:])
	n := pr.n.Add(int64(len(data) - protocol.TaskIDLen))
	if t != nil && n%(4<<20) < 16*1024 {
		a.mu.Lock()
		t.BytesRecv = n
		a.mu.Unlock()
		a.notify()
	}
}

func (a *Agent) onPayloadDone(pc *peerConn, msg protocol.P2PMsg) {
	a.mu.Lock()
	pr := a.recv[msg.TaskID]
	delete(a.recv, msg.TaskID)
	t := a.tasks[msg.TaskID]
	a.mu.Unlock()
	if pr == nil || t == nil {
		return
	}
	n := pr.n.Load()
	dur := time.Since(pr.start).Seconds()
	digest := hex.EncodeToString(pr.h.Sum(nil))
	a.mu.Lock()
	t.BytesRecv = n
	a.mu.Unlock()
	mbps := float64(n) / (1 << 20) / dur
	a.completeReceived(t, pc, protocol.TaskResult{
		ExitCode: 0, Digest: digest, Bytes: n,
		Summary: fmt.Sprintf("received %.1f MB over P2P in %.2fs (%.1f MB/s) | sha256 %s...", float64(n)/(1<<20), dur, mbps, digest[:16]),
	})
}
