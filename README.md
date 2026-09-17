# measurement

Browser-based network measurement: UDP-style probes between a web page and a server over
WebTransport, with pluggable congestion control in both directions.

Unreliable datagrams between a web page and a server over **WebTransport**
(QUIC DATAGRAM frames), with pluggable congestion control in both directions.

```
browser (web/)                                   server (server/, aioquic)
  worker.js ── up DATA (JS CC: web/cc/) ───────▶ session.py ── ACK
            ◀─ down DATA (Python CC: appcc/) ─── session.py
            ── ACK ─────────────────────────────▶
  control: newline-delimited JSON on one bidirectional stream (start / finish / results)
```

## Run (local)

```sh
cd server
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python gen_cert.py            # ECDSA cert valid ≤14 days + web/cert-hash.json
.venv/bin/python server.py --cc null    # UDP 4433; --cc reno|cubic keeps aioquic's CC under yours
.venv/bin/python static_server.py       # http://localhost:8000
```

Open http://localhost:8000 in Chrome (or Firefox). Pick a mode (`up`, `down`, `both`),
choose a controller and its JSON params, then click Run.
- Each run is saved to `server/logs/<time>_s<id>.json`. The file holds server and browser summaries, 100 ms timelines, and per-packet records.
- **Download JSON** saves the browser's copy of the results.

The certificate hash stops working after `--days` (default 10). Re-run `gen_cert.py` when it expires.

## Deploy (Ubuntu server with nginx and a domain)

nginx serves the page over HTTPS with a Let's Encrypt certificate from certbot. The probe server
(systemd) uses that same certificate on UDP 4433, so no hash pinning is needed. nginx isn't
involved in the WebTransport traffic.

Prerequisites:
- A DNS record for the domain pointing at the server.
- nginx running and reachable on TCP 80/443.
- `server/.venv` created on the server.

Then run:

```sh
sudo deploy/install.sh probe.example.edu you@example.edu    # optional 3rd arg: UDP port (4433)
```

The script:
- adds **only** a new site, `browser-cc-probe-<domain>.conf`, and reverts it if `nginx -t` fails
- copies `web/` to `/srv/browser-cc-probe/web`
- gets the certificate with `certbot --webroot`
- starts `browser-cc-probe.service`

A certbot deploy hook copies each renewed certificate to `/etc/browser-cc-probe/` and restarts
the server.

- Update code or page: rsync, then re-run the same command.
- Switch QUIC CC: `echo 'PROBE_ARGS=--cc reno' | sudo tee /etc/default/browser-cc-probe && sudo systemctl restart browser-cc-probe`
- Logs: `journalctl -u browser-cc-probe -f`; results in `server/logs/`.

## Where congestion control lives

| Direction | Your controller | Underneath it |
|---|---|---|
| server → browser | `server/appcc/*.py`, chosen by the browser in `start.down_cc` | aioquic's QUIC CC: `--cc null` disables it; `reno`/`cubic` keep it |
| browser → server | `web/cc/*.js` | Chrome/Firefox's own QUIC CC. **You can't turn it off.** |

Signs that a lower layer is the real bottleneck:
- **Up:** `up.blocked_ticks` / `max_pending_writes`. These count datagram writes Chrome hasn't accepted yet.
- **Down:** `down.blocked_ticks` / `timeline[].quic.pending_datagrams`. These count datagrams aioquic has queued.

Adding a controller:
- **Python:** subclass `appcc.base.CongestionController` and add it to `appcc/__init__.py:REGISTRY`.
- **JS:** extend `web/cc/base.js` and add it to `web/cc/index.js`.
- **UI:** add an `<option>` in `index.html`.
- **QUIC-level (server):** register with aioquic in `quiccc/`.

The interface is the same on both sides:
`onPacketSent`, `onAck(rtt, srtt, minRtt)`, `onLoss(seqs)`, `pacingRateBps()`, `cwndBytes()`.
Loss is detected in `SenderCore` in `transport_stats.py` / `stats.js`, by reordering (3 packets) or by timeout.

## WebRTC DataChannel version

`web/rtc.html` runs the same experiment over an unordered, non-retransmitting **data channel**
instead of WebTransport datagrams, sharing the probe format, controllers, ACKs and analysis.

```sh
.venv/bin/python rtc_server.py --port 8080   # signaling (HTTP POST /offer); media is UDP via ICE
```
Then open `rtc.html` and point "Signaling URL" at it (`/rtc/offer` when deployed behind nginx).

Differences from the WebTransport page:
- **Signaling instead of certificates:** one HTTP POST exchanges SDP; DTLS is authenticated by the
  fingerprint inside it, so no CA certificate or cert hash is involved.
- **SCTP, not QUIC:** `ordered: false, maxRetransmits: 0` gives datagram-like delivery, but SCTP
  still congestion-controls the channel and adds its own receive-window flow control. Server-side
  SCTP cwnd, flight size and RTT come from aiortc and are charted.
- **Backpressure** is `bufferedAmount` on both sides (the page pauses above a configurable limit)
  rather than QUIC's datagram queue.
- **Main-thread sending:** `RTCPeerConnection` has no Worker API, so the page sends directly and
  takes pacing ticks from a small worker (`web/tick-worker.js`) — main-thread timers are throttled
  in background tabs, which otherwise starves the sender.
- **No packet-number loss split:** SCTP gives the receiver no equivalent of QUIC packet numbers, so
  "dropped before send" cannot be separated from network loss; IAT, pacing and delay signals remain.
- **Control messages are chunked** (16 KB) because data channels cap a single message at 64 KB.

Deployment adds `browser-cc-rtc.service` and an nginx `location /rtc/` proxy; ICE needs the
ephemeral UDP range open (`ufw allow 32768:60999/udp`, added by `deploy/install.sh`).

## Acknowledgements

The receiver acknowledges probe packets in one of two modes, chosen on the page and used in both directions:
- **`packet`:** one 22-byte ACK per DATA packet, echoing its send timestamp.
- **`block`** (default): an `ACK_BLOCK` datagram sent every `interval_ms` (20) or after `every_n`
  (16) packets. It carries:
  - the largest sequence number received, with its receive time and the ack delay
  - up to 16 received ranges within `[low, largest]`
  - receive timestamps for every packet newly received since the previous ACK, spread over several
    datagrams if needed

  The sender takes one RTT sample per ACK (largest newly acked packet, minus the ack delay) and
  declares loss only inside `[low, largest]`. The loss timeout allows for the receiver's ACK
  interval, without which low-rate runs report large spurious losses.

Implementations: `server/ack.py` + `SenderCore.on_ack_block`, and `web/ack.js` +
`SenderCore.onAckBlock`. Both produce byte-identical ACKs.

## Sending above the path's capacity (mobile Safari)

Safari does not drop queued datagrams (it ignores `outgoingMaxAge`), so a fixed rate above the
path's capacity grows its queue without bound: app RTT climbs for the whole run and everything
after it — ACKs, control messages, the results upload — queues behind it. On a phone uplink that
looks like "the run never ends and srtt keeps rising".

Safety nets in `web/worker.js`:
- **Queue guard** (page field, default 2000 ms): stop sending when `srtt − minRtt` exceeds it, and
  report `up_stopped_early: {reason: "queue_guard", …}`. Set 0 to disable.
- **Capped waits:** the end-of-run pause for late ACKs is at most 3 s, and every control message
  wait times out (20 s) instead of hanging; results are shown even when the server never replies.
- **Capped records:** at most 20,000 per-packet records per direction (`records_truncated` counts
  the rest), to bound the size of the results upload.
- The server saves a partial log if the peer disappears mid-run, and caps its own end-of-run wait.

## Is the browser's QUIC congestion control limiting the upload?

Each run's results include:
- **`browser_quic`:** browser QUIC stats from `WebTransport.getStats()`, when they're actually
  populated (see the Safari notes).
- **`server.up_analysis`:** server-side analysis of the received packets:

| Signal | How | Meaning |
|---|---|---|
| Inter-arrival times | IAT percentiles and coefficient of variation; back-to-back fraction; fraction of same-tick bursts that arrive re-spaced | Evenly paced arrivals below your send rate = the browser's QUIC pacer / a bottleneck |
| Forward-delay excess | `recv_ts − send_ts` above its minimum | Network queueing **plus** time in the browser's local queue |
| Server QUIC RTT excess | aioquic latest RTT − min RTT | Network queueing (the browser's ACKs normally skip its datagram queue) |
| Local queue (lower bound) | forward excess − QUIC RTT excess | Time spent queued inside the browser |
| Loss split | missing probe sequence numbers vs missing QUIC packet numbers (`quic_instrument.py`) | Probes missing without a QUIC packet gap were dropped inside the browser before sending |

Time bins where the local-queue lower bound exceeds 2 ms are flagged `quic_limited`. The page shows
all of this in the "Is QUIC congestion control limiting browser → server?" section.

To reproduce a ceiling locally, emulate a slow uplink in front of the server:
`server.py --emulate-up-mbps 10 --emulate-up-queue-ms 50 [--emulate-up-loss 0.01]`.
It only shapes browser → server traffic (`server/netem.py`).

Measured with a 10 Mbps emulated uplink and the browser sending 20–30 Mbps:

| | Chrome 151 | Safari 26.6.2 |
|---|---|---|
| Arrivals | paced at 0.82 ms (≈ 9.7 Mbps), 99.96% of bursts re-spaced | same |
| Local drops before sending | about half of all probes (`outgoingMaxAge` = 100 ms respected) | none: the queue grows without bound |
| Forward-delay excess | ~145 ms (~100 ms local + ~45 ms network) | grows to ~5 s |
| `getStats()` | not available | fields present but all zero |

## Emulating a bad path on macOS (dummynet)

```sh
sudo dnctl pipe 1 config delay 25ms plr 0.01 bw 20Mbit/s
echo "dummynet in quick proto udp from any to any port 4433 pipe 1
dummynet out quick proto udp from any port 4433 to any pipe 1" | sudo pfctl -f -
sudo pfctl -E
# undo: sudo pfctl -f /etc/pf.conf; sudo dnctl -q flush
```

## Findings so far (Chrome 151, macOS, localhost)

- **2 Mbps fixed rate, both directions:** 997/1000 up and 1000/1000 down, 0 loss, RTT about 0.4 ms.
- **AIMD (1→100 Mbps), both directions at once:** about 50 Mbps each way. The limit is the Python server; its single thread adds delay, and AIMD backs off with a clean sawtooth.
- **Chrome WebTransport quirks:**
  - `datagrams.outgoingHighWaterMark` defaults to **1**, and `writer.desiredSize` never reflects a larger value. Backpressure is therefore tracked by counting pending writes.
  - `transport.getStats()` and `datagrams.createWritable()` don't exist, so no browser-side QUIC RTT or loss stats are available.
  - `maxDatagramSize` is 1024. Payloads are clamped to `maxDatagramSize - 16`.

## Safari (26.4+) compatibility

aioquic only implements Chrome's WebTransport draft, so `server/server.py` adds four adjustments.
Each was found by tracing Safari 26.6.2 with qlog:

1. **`max_udp_payload_size = 1500`.** aioquic can't decrypt packets larger than 1,500 bytes and
   drops them silently, but it never tells peers that limit. Safari sizes packets to the path MTU
   (about 16 KB on loopback), so its larger stream writes were dropped and resent forever.
2. **Only the draft-07 max-sessions setting (`0xc671706a`).** Without it, Safari cancels before
   sending CONNECT (`H3_REQUEST_CANCELLED`). If the server advertises any draft-13+ `WT_*` setting
   (`0x14e9cd29`, `0x2b61`, `0x2b64`, `0x2b65`) or `WT_ENABLED` (`0x2c7cf000`), Safari also cancels.
3. **Flow-control capsules** (`WT_MAX_DATA`, `WT_MAX_STREAMS_BIDI`/`UNI`) sent right after the
   CONNECT response. Chrome ignores them.
4. **Both upgrade tokens accepted:** `webtransport` and `webtransport-h3`.

Also: Safari's `performance.now()` is coarser than Chrome's, so browser-measured RTTs on
localhost round to 0–1 ms.

Full investigation notes, test matrix and debugging workflow: [docs/safari-webtransport.md](docs/safari-webtransport.md).

Pass `--qlog-dir DIR` to `server.py` to trace connections. Each file is written when the connection closes.

## Caveats / next steps

- Browser timers are coarse (about 1–4 ms), so pacing sends small bursts (up to about 10 ms of tokens).
- Throughput from the Python server tops out around tens of Mbps. Port the server to Rust (quinn has a public `ControllerFactory`) for faster paths.
- ACKs go one per packet for now. Batch them before running at high rates over real paths.
- Before running internet-wide measurements:
  - Deploy servers with public IPs, each with its own cert hash or a CA-signed cert.
  - Serve the page over HTTPS.
  - Add consent and IRB review.
  - Consider a WebRTC DataChannel fallback for networks that block UDP.
