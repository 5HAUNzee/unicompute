# UniCompute — P2P compute prototype

Two or more Windows PCs on **different networks** find each other through a tiny
cloud coordinator, open a **direct WebRTC DataChannel** between themselves, and
then send tasks and results **peer-to-peer**. The coordinator only relays the
connection handshake; once the channel is up you can kill the coordinator and
tasks still complete.

```
  PC A (home broadband)              CLOUD                PC B (other network)
  ┌─────────────────┐        ┌───────────────────┐        ┌─────────────────┐
  │ unicompute.exe  │──WSS──▶│   coordinator     │◀──WSS──│ unicompute.exe  │
  │ GUI :7777       │        │ registry+signal   │        │ GUI :7777       │
  └────────┬────────┘        └───────────────────┘        └────────┬────────┘
           │              STUN: stun.l.google.com                  │
           └═══════════ DIRECT P2P DataChannel (tasks+results) ════┘
```

Two binaries, no installer, no Docker, no admin rights:

| Binary | Runs on | Purpose |
|---|---|---|
| `coordinator.exe` | one publicly reachable machine | registry + signalling relay + stats page |
| `unicompute.exe` | every participating PC | peer agent + task executor + local web GUI |

---

## 1. Build (once, on any machine with Go)

Install Go 1.22+ from <https://go.dev/dl/> (or `winget install GoLang.Go`), then:

```powershell
cd Unicompute
go build -o bin\coordinator.exe .\cmd\coordinator
go build -o bin\unicompute.exe  .\cmd\agent
```

Copy `bin\unicompute.exe` to every test PC. Build locally rather than
downloading the `.exe` from somewhere — an unsigned binary that arrived over the
network triggers Windows SmartScreen, which is just noise during a test.

---

## 2. Put the coordinator somewhere both PCs can reach

PCs on different networks need a common public address. Three options:

### Option A — Cloudflare quick tunnel (≈2 min, free, no signup) — *use this for today's test*

On any machine (your laptop is fine):

```powershell
.\bin\coordinator.exe --listen :8080
```

In a second terminal, download `cloudflared` from
<https://github.com/cloudflare/cloudflared/releases> (the `-windows-amd64.exe`),
rename it `cloudflared.exe`, and run:

```powershell
.\cloudflared.exe tunnel --url http://localhost:8080
```

It prints a line like:

```
https://quiet-river-1234.trycloudflare.com
```

Your coordinator URL for the agents is that host with `wss://` and `/ws`:

```
wss://quiet-river-1234.trycloudflare.com/ws
```

Open the `https://` URL in a browser — you should see the coordinator status page.
The URL changes every time you restart `cloudflared`; that's fine for a test.

### Option B — free container host (≈15 min, stable URL)

`cmd/coordinator/Dockerfile` builds a 10 MB static image. Deploy it to Render,
Fly.io or Railway's free tier, expose port 8080, and use
`wss://<your-app>.onrender.com/ws` (or equivalent).

### Option C — your own VPS

```bash
./coordinator --listen :8080
```

Open TCP 8080 (or put it behind nginx/caddy for TLS) and use
`ws://<ip>:8080/ws` or `wss://<domain>/ws`.

---

## 3. Pre-flight on every PC

```powershell
.\unicompute.exe natcheck
```

Example output:

```
  local address : 192.168.1.20:60312
  mapping 1     : 49.37.112.8:60312
  mapping 2     : 49.37.112.8:60312
  public IP     : 49.37.112.8
  NAT type      : CONE
  direct P2P    : true
```

| NAT type | Meaning | What to expect |
|---|---|---|
| `open` / `cone` | endpoint-independent mapping | ✅ direct P2P will work |
| `symmetric` | different port per destination (common on **mobile hotspots** and campus Wi-Fi) | ⚠️ hole punching usually fails → needs a TURN relay (see §6) |
| `blocked` | no STUN reply at all | ❌ outbound UDP is blocked; WebRTC cannot work on this network |

**For a clean demo use two home broadband connections.** Confirm the two PCs
show *different* public IPs — that proves they are on different networks.

---

## 4. Run

On **each** PC:

```powershell
.\unicompute.exe --coordinator wss://quiet-river-1234.trycloudflare.com/ws --name LAPTOP-A
```

- The GUI opens automatically at <http://127.0.0.1:7777>.
- If Windows Firewall asks about `unicompute.exe`, click **Allow** (private networks).
  Outbound UDP is permitted by default, so no inbound rule is required.
- Two agents on the same PC? Give the second one `--gui-port 7778`.

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `--coordinator` | *(required)* | `wss://host/ws` or `ws://ip:8080/ws` |
| `--name` | hostname | display name |
| `--gui-port` | 7777 | local GUI port (binds 127.0.0.1 only) |
| `--stun` | Google STUN | comma-separated STUN URLs |
| `--turn`, `--turn-user`, `--turn-pass` | *(none)* | optional TURN relay fallback |
| `--no-browser` | false | don't auto-open the GUI |

---

## 5. Use the GUI

```
┌──────────────────────────────────────────────────────────────────────┐
│ UniCompute   ● cloud: connected   NAT: cone   LAPTOP-A · nd_a1b2…    │
├──────────────────────────────┬───────────────────────────────────────┤
│ PEERS ON THE CLOUD           │ SEND A TASK OVER P2P                  │
│ ┌──────────────────────────┐ │  Target [LAPTOP-B ▾]  Kind [shell ▾]  │
│ │ LAPTOP-B  ● P2P CONNECTED│ │  Command [hostname        ] [▶ Run]   │
│ │ nd_c3d4 · windows · 8c   │ ├───────────────────────────────────────┤
│ │ DIRECT (NAT-punched      │ │ TASKS                                 │
│ │   srflx↔srflx) · 38 ms   │ │  7f3a2b9c  → LAPTOP-B  done  shell    │
│ │ [Use as target][Disconn] │ │  hostname · via DIRECT (NAT-punched)  │
│ └──────────────────────────┘ │  ┌───────────────────────────────┐    │
│                              │  │ LAPTOP-B                      │    │
│                              │  └───────────────────────────────┘    │
│                              │  exit 0 · 41 ms                       │
├──────────────────────────────┴───────────────────────────────────────┤
│ P2P ↑52.4 MB ↓1.2 KB   Cloud ↑7.1 KB ↓6.8 KB   Coordinator relayed 14 KB │
└──────────────────────────────────────────────────────────────────────┘
```

1. Wait for the other PC to appear under **Peers on the cloud**.
2. Click **Connect**. Within a few seconds it shows **● P2P CONNECTED** and the
   path: `DIRECT (NAT-punched srflx↔srflx)` is what you want.
3. Pick a task kind and click **▶ Run on peer**:

| Kind | What happens on the target PC | Why it's in the test |
|---|---|---|
| **shell** | runs the command via `cmd /C`, streams output, returns the exit code | `hostname` proves *where* it ran |
| **compute** | counts primes below N by trial division, streams progress | real CPU work, verifiable result, visible duration |
| **payload** | receives N MB over the DataChannel and returns its SHA-256 | proves bulk data flows P2P; the GUI shows ✓ MATCH against the sender's own hash |

Known prime counts for verification: below 1,000,000 → **78,498** · 2,000,000 →
**148,933** · 5,000,000 → **348,513** · 10,000,000 → **664,579**.

---

## 6. Proving the cloud is out of the data path

Three independent checks, all visible without extra tools:

1. **Footer counters.** Run a 50 MB payload task. `P2P ↑` climbs to 50 MB;
   `Cloud ↑/↓` and `Coordinator relayed` do not move.
2. **Kill the coordinator mid-task.** Start `compute` with N = 30,000,000
   (≈20–60 s), then stop `coordinator.exe` (Ctrl-C). The header turns red
   (`cloud: disconnected`) — **and the task still completes with its result.**
3. **Path type.** The peer card shows the selected ICE pair. `srflx` or `host`
   on both ends = direct. `relay` = TURN in the path (see below).

### If you see `RELAY via TURN` or the connection never reaches CONNECTED

One or both networks has a symmetric NAT or blocks UDP. Options:

- Test from two different *home* connections instead of a hotspot or campus Wi-Fi.
- Add a TURN relay so the demo still works (clearly labelled as relayed in the GUI):

```powershell
.\unicompute.exe --coordinator wss://… --turn turn:openrelay.metered.ca:80 --turn-user openrelayproject --turn-pass openrelayproject
```

(Public test relay; fine for a demo, not for anything real. Better: run
`coturn` on the same VPS as the coordinator.)

---

## 7. JSON API (for scripted tests)

Every GUI action is also an HTTP call on the agent (`127.0.0.1:7777`):

```powershell
curl http://127.0.0.1:7777/api/state                       # full snapshot
curl http://127.0.0.1:7777/api/natcheck                    # run NAT check now
curl -X POST http://127.0.0.1:7777/api/connect -d '{"peer_id":"nd_c3d4e5f6"}'
curl -X POST http://127.0.0.1:7777/api/task    -d '{"peer_id":"nd_c3d4e5f6","kind":"shell","command":"hostname"}'
curl "http://127.0.0.1:7777/api/task?id=7f3a2b9c1d2e"
```

Coordinator: `GET /api/nodes`, `GET /api/stats`, and `/` is a live status page.

---

## 8. Layout

```
cmd/coordinator/   signalling server (registry, relay, stats)      ~250 lines
cmd/agent/         peer agent: main.go (lifecycle), tasks.go, gui.go, web/index.html
internal/protocol/ every message type on the wire
internal/signal/   outbound WebSocket client with reconnect
internal/peer/     pion/webrtc wrapper: one ordered DataChannel, STUN/TURN, path info
internal/task/     shell + prime-count executors
internal/natcheck/ STUN-based NAT classification
TESTPLAN.md        30 test cases with pass criteria
```

What is deliberately **not** here: Docker/gVisor sandboxing, PTY terminal,
HTTP reverse proxy, load balancing, autoscaling, storage. This prototype answers
one question — *can PCs on different networks connect through the cloud and
then exchange work directly?* — and nothing else.
