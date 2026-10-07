// Package protocol defines every message that crosses a process boundary in the
// UniCompute prototype: signalling (agent <-> coordinator over WebSocket) and
// peer traffic (agent <-> agent over a WebRTC DataChannel).
package protocol

import "encoding/json"

// ---------------------------------------------------------------------------
// Signalling: agent <-> coordinator. The coordinator never inspects Data for
// offer/answer/ice; it only forwards by To.
// ---------------------------------------------------------------------------

const (
	SigRegister  = "register"  // agent -> coord, Data = NodeInfo
	SigHeartbeat = "heartbeat" // agent -> coord, Data = Heartbeat
	SigWelcome   = "welcome"   // coord -> agent, Data = {"id": ...}
	SigPeers     = "peers"     // coord -> agent, Data = []NodeInfo
	SigOffer     = "offer"     // relayed, Data = webrtc.SessionDescription
	SigAnswer    = "answer"    // relayed, Data = webrtc.SessionDescription
	SigICE       = "ice"       // relayed, Data = webrtc.ICECandidateInit
	SigError     = "error"     // coord -> agent, Data = {"message": ...}
)

// Envelope is the single frame type on the signalling WebSocket.
type Envelope struct {
	Type string          `json:"type"`
	From string          `json:"from,omitempty"`
	To   string          `json:"to,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// NodeInfo is what every agent advertises about itself.
type NodeInfo struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	OS       string  `json:"os"`
	Arch     string  `json:"arch"`
	Cores    int     `json:"cores"`
	MemTotal uint64  `json:"mem_total"`
	MemFree  uint64  `json:"mem_free"`
	CPUPct   float64 `json:"cpu_pct"`
	NATType  string  `json:"nat_type"`
	PublicIP string  `json:"public_ip"`
	Version  string  `json:"version"`
	LastSeen int64   `json:"last_seen"` // unix ms, set by the coordinator
}

// Heartbeat carries only the gauges that change.
type Heartbeat struct {
	CPUPct      float64 `json:"cpu_pct"`
	MemFree     uint64  `json:"mem_free"`
	ActiveTasks int     `json:"active_tasks"`
}

// ---------------------------------------------------------------------------
// Peer traffic: agent <-> agent over ONE ordered, reliable DataChannel.
// Text frames carry P2PMsg JSON. Binary frames carry payload chunks:
// [TaskIDLen bytes task id][data...]
// ---------------------------------------------------------------------------

const (
	P2PTaskSubmit   = "task_submit"   // Data = TaskSubmit
	P2PTaskAccepted = "task_accepted" // no data
	P2PTaskOutput   = "task_output"   // Data = TaskOutput
	P2PTaskResult   = "task_result"   // Data = TaskResult
	P2PTaskCancel   = "task_cancel"   // no data
	P2PPayloadDone  = "payload_done"  // Data = {"bytes": n}
	P2PTelemetry    = "telemetry"     // Data = Telemetry
	P2PPing         = "ping"          // Data = Ping
	P2PPong         = "pong"          // Data = Ping (echoed)
)

// TaskIDLen is the fixed width of a task id, so binary frames can carry it as
// a plain prefix with no framing.
const TaskIDLen = 12

type P2PMsg struct {
	Type   string          `json:"type"`
	TaskID string          `json:"task_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

type TaskSubmit struct {
	Kind       string `json:"kind"` // shell | compute | payload
	Command    string `json:"command,omitempty"`
	N          int64  `json:"n,omitempty"`     // compute: count primes below N
	Bytes      int64  `json:"bytes,omitempty"` // payload: how many bytes will follow
	TimeoutSec int    `json:"timeout_sec,omitempty"`
}

type TaskOutput struct {
	Stream string `json:"stream"` // stdout | stderr | progress
	Text   string `json:"text"`
}

type TaskResult struct {
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
	Summary    string `json:"summary,omitempty"`
	Digest     string `json:"digest,omitempty"` // payload: sha256 hex computed by the executor
	Bytes      int64  `json:"bytes,omitempty"`  // payload: bytes the executor received
}

type PayloadDone struct {
	Bytes int64 `json:"bytes"`
}

type Telemetry struct {
	CPUPct      float64 `json:"cpu_pct"`
	MemFree     uint64  `json:"mem_free"`
	ActiveTasks int     `json:"active_tasks"`
	TS          int64   `json:"ts"`
}

type Ping struct {
	TS int64 `json:"ts"` // unix ms at sender
}

// Marshal is a convenience for building Envelope/P2PMsg Data fields.
func Marshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
