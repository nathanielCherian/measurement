# Safari WebTransport notes

What we learned getting the aioquic probe server (`server/server.py`) to work with Safari.
Investigated 2026-09-17 against **Safari 26.6.2 on macOS 26.6.2**; Chrome 151 was used as the control.

## TL;DR

Safari 26.4+ supports WebTransport. Its implementation (Apple's Network.framework) is stricter
than Chrome's and fails **silently**. aioquic 1.2.0 needed four changes:

| # | Symptom in Safari | Cause | Fix in `server.py` |
|---|---|---|---|
| 1 | `WebTransportError: source=session` right after connecting; the server never sees a CONNECT request | Server SETTINGS lacked draft-07 `SETTINGS_WEBTRANSPORT_MAX_SESSIONS` (`0xc671706a`), **or** included newer `WT_*` settings | Advertise `0xc671706a = 1` and **nothing newer** |
| 2 | Session works, but a stream write over about 1 KB is never delivered; the page hangs with no error | Safari sent QUIC packets over 1,500 bytes; aioquic can't decrypt them and drops them silently | Advertise transport parameter `max_udp_payload_size = 1500` |
| 3 | (From the literature) `createBidirectionalStream()` never resolves | Safari implements session flow control and waits for stream/data grants | Send `WT_MAX_DATA`, `WT_MAX_STREAMS_BIDI`, `WT_MAX_STREAMS_UNI` capsules after the `200` |
| 4 | (Defensive) CONNECT rejected by our own path check | Newer drafts use the upgrade token `webtransport-h3` | Accept `webtransport` and `webtransport-h3` |

Chrome is unaffected by all four: it ignores the extra setting, the capsules, and the payload-size limit.

## 1. SETTINGS: Safari cancels before sending CONNECT

**Symptom.** The page logs `session closed: WebTransportError: source=session` within a few
milliseconds of `new WebTransport()`. Server log:

```
Stream 0 reset by peer (error code 268, final size 0)     # 0x10c H3_REQUEST_CANCELLED, nothing sent
Connection close received (code 0x100, reason )           # H3_NO_ERROR
```

`final size 0` means Safari opened the request stream and cancelled it **without sending HEADERS**.
It decides from the server's SETTINGS, which it receives right after the handshake.

**What Safari sends** (for comparison; a GREASE setting with a random id is also included):

```
0x1 QPACK_MAX_TABLE_CAPACITY = 16383     0x7 QPACK_BLOCKED_STREAMS = 100
0x33 H3_DATAGRAM = 1
0xc671706a (draft-07 WT max sessions) = 1
0x14e9cd29 (draft-14 WT_MAX_SESSIONS) = 1
0x2b61 WT_INITIAL_MAX_DATA = 8388608
0x2b64 WT_INITIAL_MAX_STREAMS_UNI = 100   0x2b65 WT_INITIAL_MAX_STREAMS_BIDI = 100
```

Note that Safari itself does **not** send `ENABLE_CONNECT_PROTOCOL`, the draft-02 `0x2b603742`,
or draft-15 `WT_ENABLED` (`0x2c7cf000`).

**What works as a server** (verified): aioquic's defaults plus the draft-07 setting.

```
0x1 = 4096, 0x7 = 16, 0x8 ENABLE_CONNECT_PROTOCOL = 1, 0x21 (GREASE) = 1, 0x33 H3_DATAGRAM = 1,
0x2b603742 (draft-02 ENABLE_WEBTRANSPORT) = 1, 0xc671706a = 1
```

**Server SETTINGS we tested** (all with the capsules sent):

| Server advertises, in addition to aioquic defaults | Safari |
|---|---|
| nothing (plain aioquic) | cancels |
| `0xc671706a`, `0x14e9cd29`, `0x2b61`, `0x2b64`, `0x2b65` (mirroring Safari's own set) | cancels |
| … plus `0x2c7cf000` (`WT_ENABLED`) | cancels |
| … with webtransport-go's values (`2^62-1`, `2^60`) | cancels |
| … with `0x2b61 = 8388608` (Safari's own value) | cancels |
| … without `0xc671706a`, `0x21`; without `0x2b603742` / `0x2c7cf000` (various combinations) | cancels |
| minimal: `0x8`, `0x33`, `0x14e9cd29`, `0x2b61`, `0x2b64`, `0x2b65` only | cancels |
| `0xc671706a` + `0x2b61`/`0x2b64`/`0x2b65` (no `0x14e9cd29`, no `WT_ENABLED`) | cancels |
| `0xc671706a` + `0x2b61` only | cancels |
| **`0xc671706a` only** (value 1 or 16) | **connects** |

Two public reports suggested adding the draft-14 settings ([hyperium/h3#347],
[quic-go/webtransport-go#355], [webtransport-go PR #261]). **For aioquic that made Safari fail.**
We didn't find out why a server advertising draft-13+ flow-control settings is rejected. Possibly
Safari then expects other draft-13+ behavior from the server that aioquic doesn't have. Treat it as
empirical.

**How we found it.** A quiche-based server (`@fails-components/webtransport` on npm) did get
Safari to send CONNECT. Dumping its SETTINGS with an aioquic client showed it advertises only
draft-02/07-style settings (`0x2b603742`, `0xc671706a = 16`, `0xffd277`), with no draft-13+ settings.

**Ruled out along the way** (Safari still cancelled after each change):
- `max_datagram_frame_size` 65536 vs 65535
- Sending SETTINGS in 0.5-RTT packets vs after `HandshakeCompleted`. Safari didn't acknowledge the
  0.5-RTT packet, but fixing that alone didn't help. We kept the change.
- QPACK dynamic table instructions on the encoder stream, and not opening QPACK streams at all
- Number of `NEW_CONNECTION_ID` frames issued (7 vs 1)
- The `sec-webtransport-http3-draft: draft02` response header. Irrelevant, because Safari never
  sends CONNECT when it rejects the SETTINGS.

## 2. Oversized packets: stream writes silently stall

**Symptom.** The session opens, datagrams flow both ways, and small control messages work. Then a
larger write, e.g. the end-of-run `results` upload, never reaches the server. In the page,
`writer.write()` resolves, but the server never replies. There's no error and no close. Safari
keeps the connection alive.

**Evidence (server qlog).**

```
RECV stream 4 offset=27    len=1027
datagrams_received length=9193
packet_dropped trigger=payload_decrypt_error length=9185    <-- 9 KB QUIC packet
RECV stream 4 offset=10207 len=874                           <-- gap 1054..10207 never arrives
datagrams_received length=9193 / packet_dropped ...          <-- Safari retransmits the same size forever
```

**Cause.** aioquic's C AEAD code decrypts into a fixed buffer, `#define PACKET_LENGTH_MAX 1500`
in `aioquic/_crypto.c`, and fails on anything larger. aioquic never advertises
`max_udp_payload_size`, so peers may assume the default of 65527. Chrome sends packets of at most
about 1,450 bytes and never hits this. Safari sizes packets to the path MTU; on loopback
(`lo0`, MTU 16384) that produced 9 KB packets.

**Fix.** Wrap `aioquic.quic.connection.QuicTransportParameters` so the server always sends
`max_udp_payload_size = 1500`. It's constructed in exactly one place:
`QuicConnection._serialize_transport_parameters`. After the fix, uploads of 1 KB, 10 KB, 70 KB
and 1 MB all succeed.

**Relevance on real networks.** Over the internet the path MTU is usually 1,500 or less, so this
mostly affects localhost testing. But jumbo-frame LANs or VPN setups could hit it too, so keep the fix.

## 3. Flow-control capsules

Safari 26.4+ implements WebTransport session flow control (draft-13 style). It won't make new
streams usable until the server grants them. We send these right after the `200` response, in a
DATA frame on the CONNECT stream:

```
WT_MAX_DATA          0x190b4d3d = 2^32
WT_MAX_STREAMS_BIDI  0x190b4d3f = 100
WT_MAX_STREAMS_UNI   0x190b4d40 = 100
```

Sources: [hyperium/h3#347]; the retraction note on [WebKit bug 312697] ("This is not a Safari
bug": the servers hadn't sent `WT_MAX_STREAMS`). We didn't separately confirm that removing the
capsules breaks Safari with our server; they were present in every working configuration.

Safari has not been seen sending `WT_DATA_BLOCKED`. When the upload stalled (issue 2), the only
capsule it sent was `CLOSE_WEBTRANSPORT_SESSION` (`0x2843`), on `wt.close()`.

## Other Safari behaviors worth knowing

- **Keeps the QUIC connection open after `wt.close()`.** It sends `CLOSE_WEBTRANSPORT_SESSION`
  but no CONNECTION_CLOSE, so the server sits until its 30 s idle timeout. The server now closes
  the connection on that capsule, which also flushes qlog files.
- **`serverCertificateHashes` works** with our ECDSA P-256, 10-day certificate, so Safari can be
  tested against localhost.
- **API differences from Chrome 151:**
  - `datagrams.writable` is `undefined`; use `datagrams.createWritable()`. `web/worker.js`
    handles both.
  - `transport.getStats()` exists and returns `atSendCapacity`, `bytesLost`, `bytesReceived`,
    `bytesSent`, `datagrams`, `minRtt`, … but every value is 0 (see below). Chrome 151 has no
    `getStats()`.
  - `transport.congestionControl` reports `"default"`; Chrome reports `undefined`.
  - `datagrams.maxDatagramSize` is 65535; Chrome reports 1024.
- **Coarse timer.** Safari's `performance.now()` resolution rounds browser-measured RTTs on
  localhost to 0–1 ms. Server-side RTTs are unaffected.
- **Errors carry almost no information.** `WebTransportError` has `source=session`, an empty
  message and no stream error code in every failure we saw. The macOS unified log wasn't readable
  from our sandboxed shell. The server-side qlog is the only reliable window.
- Chrome sends `sec-webtransport-http3-draft02: 1` in its CONNECT request; the server echoes
  `sec-webtransport-http3-draft: draft02` only then.

## Behavior at a bottleneck (compared with Chrome)

Tested with the server's emulated 10 Mbps uplink (`--emulate-up-mbps 10`) while the page sent 20 Mbps
upstream with `outgoingMaxAge = 100`:

- **`outgoingMaxAge` appears to be ignored.** Chrome drops datagrams that wait longer than
  100 ms (about half the probes never reached the wire). Safari dropped **none** and queued
  everything: forward delay grew to ~5 s and app RTT p95 reached 5 s. All 12,420 delivered probes
  arrived, just late.
- **`getStats()` is a stub.** It returns `atSendCapacity`, `bytesSent`, `smoothedRtt`, `datagrams`,
  … but every value is `0` (or `false`), even during traffic. The page marks such samples
  `populated: false` and ignores them.
- **Its QUIC ACKs seem to wait behind the datagram queue.** The server's QUIC RTT to Safari climbed
  in steps to ~3.7 s. Chrome's stayed at network level (~43 ms). So the server-side "forward excess
  − QUIC RTT excess" local-queue estimate is only a loose lower bound for Safari. Pacing regularity
  (IAT) and growing forward delay remain reliable indicators.
- **Pacing is the same as Chrome's.** At the bottleneck both deliver evenly spaced packets
  (IAT p50 0.82 ms), and bursts sent in one timer tick arrive re-spaced.
- **Very slow runs.** With multi-second RTTs the worker's end-of-run grace period (3 × srtt) makes
  runs take much longer than the configured duration. Don't close the tab early.
- **Same on iOS.** Reported on iPhone Safari against a real uplink: the run never finished and srtt
  grew for the whole run — the unbounded queue again, since a fixed rate above the uplink's capacity
  is never trimmed by the browser. The worker now stops when the standing queue passes the queue
  guard (default 2 s), caps the grace period at 3 s, and times out every control wait; the page
  reports `up_stopped_early`. Verified on macOS Safari behind a 10 Mbps emulated uplink at 20 Mbps:
  the guard fired at 4.1 s (srtt 2,011 ms vs min RTT 7.9 ms) and the run saved 10 s after start.

## Debugging workflow that worked

1. **Reproduce locally.** Safari on the same Mac, the page served from `http://localhost`, a
   cert hash, and the server on `127.0.0.1:4433`.
2. **Auto-run page.** A test page with an `?autorun` hook that POSTs its log to a tiny local HTTP
   endpoint, opened with `open -a Safari "http://localhost:8001/...?autorun=1&r=$RANDOM"`.
   **Close old test tabs between runs** (AppleScript: close tabs whose URL starts with the test
   origin). Stale tabs keep reporting, and they kept WebTransport sessions alive, which polluted
   results.
3. **Trace the server.** Run `server.py --qlog-dir DIR` and read frames and `packet_dropped` events
   from the qlog. A qlog file is only written when the connection closes.
4. **Bisect with small pages** (the main thread vs a Worker, datagram volume and duration, stream
   upload sizes, `getStats` polling) before blaming the real worker. Most suspects were ruled out
   this way, e.g. Workers, `outgoingMaxAge`/`outgoingHighWaterMark`, and `getStats` were all fine.
5. **Compare with another QUIC stack** Safari accepts (the quiche-based npm package) by dumping
   its SETTINGS and transport parameters with an aioquic client.

## Open questions

- Why do draft-13+ `WT_*` settings from the server make Safari cancel? Would a full draft-13+
  implementation (e.g. webtransport-go ≥ 0.11) behave differently from aioquic plus those settings?
- Are the flow-control capsules strictly required once issue 1 is fixed? Our 2 KB–1 MB control
  messages never came close to the granted limits.
- iOS Safari hasn't been tested. [quic-go/webtransport-go#355] reports iOS-specific SETTINGS
  sensitivity.
- The deployment at `m.nathanielc.com` (real internet path, Let's Encrypt cert) needs re-testing
  in Safari with the fixed server.

[hyperium/h3#347]: https://github.com/hyperium/h3/issues/347
[quic-go/webtransport-go#355]: https://github.com/quic-go/webtransport-go/issues/355
[webtransport-go PR #261]: https://github.com/quic-go/webtransport-go/pull/261
[WebKit bug 312697]: https://bugs.webkit.org/show_bug.cgi?id=312697
