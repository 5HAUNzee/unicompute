# UniCompute prototype — test plan

Two Windows PCs, **A** and **B**, on different networks. Coordinator reachable
by both (see README §2). Tick each box as it passes. Record NAT types and public
IPs from T0 at the top of your results — they explain any T1.5/T5 outcome.

```
PC A name: ________  NAT: ________  public IP: ____________
PC B name: ________  NAT: ________  public IP: ____________
coordinator URL: ______________________________________
```

---

## T0 — Pre-flight

| # | Step | Pass when | ☐ |
|---|---|---|---|
| T0.1 | Run `unicompute.exe natcheck` on A and on B | A NAT type is printed on each; note it above | ☐ |
| T0.2 | Compare the two `public IP` lines | They **differ** → the PCs are genuinely on different networks | ☐ |
| T0.3 | Open the coordinator's `https://…` URL in a browser | Status page loads, `nodes: 0` | ☐ |
| T0.4 | Start `unicompute.exe --coordinator wss://…/ws --name A` on A | GUI opens; header shows **cloud: connected**; own name/cores/RAM correct | ☐ |
| T0.5 | Same on B | Same | ☐ |
| T0.6 | Refresh the coordinator status page | Both nodes listed with the right names and NAT types | ☐ |

## T1 — Discovery and P2P establishment

| # | Step | Pass when | ☐ |
|---|---|---|---|
| T1.1 | Look at **Peers on the cloud** on A | B is listed with correct name, OS, cores, RAM, NAT type | ☐ |
| T1.2 | Same on B | A is listed | ☐ |
| T1.3 | Close B's agent (Ctrl-C) | B disappears from A's list within ~8 s | ☐ |
| T1.4 | Restart B's agent | B reappears on A without any action on A | ☐ |
| T1.5 | On A, click **Connect** on B's card | Card goes `offering…` → **● P2P CONNECTED** within ~5 s | ☐ |
| T1.6 | Read the path line on B's card | Shows **DIRECT (NAT-punched srflx↔srflx)** ✅. If it shows `RELAY via TURN` or never connects, see T5 | ☐ |
| T1.7 | Read the RTT | A plausible number (10–150 ms between Indian cities; <5 ms on one LAN) | ☐ |
| T1.8 | Look at B's GUI | B shows A as **● P2P CONNECTED** too — the link is symmetric | ☐ |
| T1.9 | Watch B's card on A for 10 s | `CPU n% (live via P2P)` updates every 2 s — telemetry is flowing over the DataChannel | ☐ |
| T1.10 | Click **Disconnect**, then **Connect** again | Reconnects cleanly | ☐ |

## T2 — Task execution and result return

All submitted from A unless stated. Target = B.

| # | Step | Pass when | ☐ |
|---|---|---|---|
| **T2.1** | kind **shell**, command `hostname` | **Output is B's hostname, not A's.** This is the core proof. | ☐ |
| T2.2 | shell: `echo %COMPUTERNAME% & wmic cpu get name` | B's actual computer name and CPU model | ☐ |
| T2.3 | shell: `dir C:\` | A directory listing from B | ☐ |
| T2.4 | shell: `exit 42` | Result shows **exit 42** (status `done`) | ☐ |
| T2.5 | shell: `for /L %i in (1,1,8) do @(echo tick %i & ping -n 2 127.0.0.1 >nul)` | Lines appear **one per second**, streamed, not all at the end. (`ping -n 2` is a 1 s sleep; `timeout /t` exits instantly when stdin is not a console, so don't use it here.) | ☐ |
| T2.6 | shell: `ping -n 30 127.0.0.1`, **timeout 5** | Cancelled after ~5 s; `ping` is gone from B's Task Manager | ☐ |
| T2.7 | shell: `ping -n 30 127.0.0.1`, then click **Cancel** | Stops immediately; status `cancelled` | ☐ |
| T2.8 | kind **compute**, N = 5,000,000 | Result `primes below 5000000 = 348513`, progress lines 10%…100%, duration 1–10 s | ☐ |
| T2.9 | compute N = 10,000,000 | `664579` | ☐ |
| T2.10 | compute to a *slower* PC (if you have one) | Noticeably longer duration — proves it ran there | ☐ |
| T2.11 | kind **payload**, 20 MB | Progress bar fills; result shows remote sha256, local sha256, **✓ MATCH**, throughput in MB/s | ☐ |
| T2.12 | payload 100 MB | Same, ✓ MATCH; note the MB/s | ☐ |
| T2.13 | From **B**, send `hostname` to A | A's hostname appears on B — bidirectional | ☐ |
| T2.14 | Submit two `compute` tasks back-to-back | Both run concurrently and both complete | ☐ |
| T2.15 | `curl -X POST http://127.0.0.1:7777/api/task -d "{\"peer_id\":\"<B id>\",\"kind\":\"shell\",\"command\":\"hostname\"}"` | Returns `{"task_id":…}`; `GET /api/task?id=…` shows the result | ☐ |

## T3 — Proof that the cloud is out of the data path ⭐

| # | Step | Pass when | ☐ |
|---|---|---|---|
| T3.1 | Note the footer: `Cloud ↑/↓` and `Coordinator relayed`. Run payload 50 MB. | `P2P ↑` reaches 50 MB. **Cloud counters and relayed total do not move.** | ☐ |
| **T3.2** | Start compute N = 30,000,000 on B. While it runs, **Ctrl-C the coordinator.** | Header turns red `cloud: disconnected`. **The task keeps running and the result arrives.** | ☐ |
| T3.3 | With the coordinator still down, send another task to B | Works — no cloud needed once connected | ☐ |
| T3.4 | Restart the coordinator | Both agents reconnect; the P2P link was never interrupted; peer list repopulates | ☐ |
| T3.5 | On B during a payload task: `netstat -ano \| findstr UDP \| findstr <PID of unicompute.exe>` | A UDP socket with traffic to **A's public IP**, not the coordinator's | ☐ |
| T3.6 | On the coordinator status page, read **Total relayed** after a full session | A few KB (offer + answer + ICE), regardless of how many MB of payload moved | ☐ |

## T4 — Multi-peer and resilience

| # | Step | Pass when | ☐ |
|---|---|---|---|
| T4.1 | Add PC **C** (third network, or `--gui-port 7778` on A/B as a stand-in) | All three see each other; A connects to both | ☐ |
| T4.2 | Send tasks to B and C at the same time | Both execute; results don't cross | ☐ |
| T4.3 | Run compute on C and watch C's card on A | C's CPU% rises in A's GUI — telemetry over P2P | ☐ |
| T4.4 | Start a 60 s task on B, then **disable B's Wi-Fi** | A marks it failed with `peer disconnected` within ~15 s (ICE: 5 s to `disconnected`, 10 s more to `failed`). **Not a silent hang.** | ☐ |
| T4.5 | Re-enable B's Wi-Fi | B reappears; Connect works; a new task succeeds | ☐ |
| T4.6 | Close B's agent window mid-task | Same as T4.4 — clean failure | ☐ |
| T4.7 | Both click **Connect** on each other within the same second | Exactly one connection forms (glare resolved by ID), both show CONNECTED | ☐ |

## T5 — Negative and fallback

| # | Step | Pass when | ☐ |
|---|---|---|---|
| T5.1 | Put one PC on a **mobile hotspot** and run `natcheck` | Reports `symmetric` (typical for CGNAT) | ☐ |
| T5.2 | Try Connect from that PC without `--turn` | Either connects (lucky) or stays in `connecting…` then `failed` — a clear state, not a hang | ☐ |
| T5.3 | Add `--turn turn:openrelay.metered.ca:80 --turn-user openrelayproject --turn-pass openrelayproject` and retry | Connects; card shows **RELAY via TURN** in amber — honest labelling, not fake success | ☐ |
| T5.4 | Block outbound UDP for `unicompute.exe` in Windows Firewall, run `natcheck` | `blocked` with a clear message | ☐ |
| T5.5 | Start an agent with a wrong coordinator URL | Header shows the error; retries with backoff; no crash; GUI still works | ☐ |
| T5.6 | Send a task to a peer that is listed but not connected | GUI refuses: "peer is not P2P-connected — click Connect first" | ☐ |
| T5.7 | Start a second agent on the same PC without `--gui-port` | Exits with a clear "port in use, try --gui-port 7778" message | ☐ |

---

## Result summary

```
T0 pre-flight      ___ / 6
T1 discovery/P2P   ___ / 10
T2 tasks           ___ / 15
T3 cloud-out-of-path  ___ / 6    ← the ones that matter for the thesis
T4 resilience      ___ / 7
T5 negative        ___ / 7

Path type observed between A and B:  ________________________
Payload throughput A→B:  ______ MB/s     B→A:  ______ MB/s
RTT A↔B:  ______ ms
```
