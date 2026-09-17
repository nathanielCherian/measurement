"""One WebRTC session = one experiment (the DataChannel twin of session.py).

Same control protocol and probe packets; the transport underneath is an
unreliable SCTP data channel instead of QUIC datagrams:

  browser -> server  "control" channel: ordered, reliable (NDJSON)
  both ways          "probe" channel:   unordered, maxRetransmits 0

SCTP applies its own congestion control to the probe channel, exactly like QUIC
does to datagrams, so the same "is the transport limiting us?" questions apply.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import protocol as proto
from ack import AckGenerator
from appcc import make_cc
from transport_stats import ReceiverStats, SenderCore
from up_analysis import analyze_up

log = logging.getLogger("rtc")

_WALL_OFFSET = time.time() - time.monotonic()

SAMPLE_MS = 100
SCTP_SAMPLE_MS = 20
MAX_GRACE_S = 3.0
MAX_SIZE = 1100
# Stop feeding the channel when SCTP has this much queued: its congestion
# control is the bottleneck and more queueing only adds delay.
BUFFERED_LIMIT = 256 * 1024
# Data channels cap a single message (64 KB in aiortc), so NDJSON control
# messages are chunked; the peer reassembles on newlines.
CONTROL_CHUNK = 16000


def now_ms() -> float:
    return (time.monotonic() + _WALL_OFFSET) * 1000


class RtcSession:
    def __init__(self, pc, log_dir: str, peer: str) -> None:
        self.pc = pc
        self.log_dir = log_dir
        self.peer = peer
        self.created = now_ms()
        self.start_ms: Optional[float] = None

        self._control_buf = ""
        self.control = None
        self.probe = None
        self.config: Dict[str, Any] = {}
        self.down: Optional[SenderCore] = None
        self.down_timeline: List[Dict[str, Any]] = []
        self.down_blocked_ticks = 0
        self.up = ReceiverStats()
        self.up_acks: Optional[AckGenerator] = None
        self.up_sctp_samples: List[Dict[str, Any]] = []
        self._task: Optional[asyncio.Task] = None
        self._up_task: Optional[asyncio.Task] = None
        self.saved = False
        self.closed = False

    # ---- channels --------------------------------------------------------

    def attach(self, channel) -> None:
        if channel.label == "control":
            self.control = channel
            channel.on("message", self._on_control_message)
        elif channel.label == "probe":
            self.probe = channel
            channel.on("message", self.on_probe_message)
        else:
            log.warning("unexpected data channel %r", channel.label)

    def send_control(self, msg: Dict[str, Any]) -> None:
        if self.control is None or self.closed or self.control.readyState != "open":
            return
        text = json.dumps(msg) + "\n"
        for i in range(0, len(text), CONTROL_CHUNK):
            self.control.send(text[i : i + CONTROL_CHUNK])

    def _on_control_message(self, message) -> None:
        # Messages are chunks of an NDJSON stream: a large "results" message is
        # split by the sender to stay under the data channel's message size cap.
        if isinstance(message, bytes):
            message = message.decode()
        self._control_buf += message
        while "\n" in self._control_buf:
            line, self._control_buf = self._control_buf.split("\n", 1)
            if line.strip():
                self._on_control(json.loads(line))

    def _on_control(self, msg: Dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "start":
            self.config = msg
            self.config["size"] = min(int(msg.get("size", 1000)), MAX_SIZE)
            log.info("session start %s", json.dumps(msg))
            self.start_ms = now_ms()
            self.up_acks = AckGenerator(proto.FLOW_UP, msg.get("ack"), self._send_probe, now_ms)
            self.send_control(
                {"type": "started", "server_time_ms": now_ms(), "transport": "webrtc-datachannel", "size": self.config["size"]}
            )
            if msg.get("mode") in ("down", "both"):
                self._task = asyncio.ensure_future(self._run_down())
            if msg.get("mode") in ("up", "both"):
                self._up_task = asyncio.ensure_future(self._sample_up_sctp())
        elif kind == "finish":
            if self._up_task:
                self._up_task.cancel()
            self.send_control({"type": "server_report", **self.report(brief=True)})
        elif kind == "results":
            path = self._save(msg)
            self.saved = True
            self.send_control({"type": "saved", "file": os.path.relpath(path)})
        else:
            log.warning("unknown control message %r", kind)

    def _send_probe(self, data: bytes) -> None:
        if self.probe is not None and self.probe.readyState == "open":
            self.probe.send(data)

    # ---- probe traffic ---------------------------------------------------

    def on_probe_message(self, data) -> None:
        t = now_ms()
        if isinstance(data, str):
            return
        pkt = proto.decode(data)
        if isinstance(pkt, proto.Data) and pkt.flow == proto.FLOW_UP:
            self.up.on_data(pkt.seq, pkt.send_ts, t, pkt.size)
            if self.up_acks:
                self.up_acks.on_packet(pkt.seq, pkt.send_ts, t)
        elif isinstance(pkt, proto.Ack) and pkt.flow == proto.FLOW_DOWN and self.down:
            self.down.on_ack(t, pkt.seq, pkt.echo_send_ts, pkt.recv_ts)
        elif isinstance(pkt, proto.AckBlock) and pkt.flow == proto.FLOW_DOWN and self.down:
            self.down.on_ack_block(t, pkt)

    async def _run_down(self) -> None:
        cfg = self.config
        cc_cfg = cfg.get("down_cc") or {"name": "fixed", "params": {"rate_mbps": 1}}
        cc = make_cc(cc_cfg["name"], cc_cfg.get("params", {}))
        ack_cfg = cfg.get("ack") or {}
        budget = float(ack_cfg.get("interval_ms", 0)) if ack_cfg.get("mode") == "block" else 0.0
        core = self.down = SenderCore(cc, ack_delay_budget_ms=budget)
        size = cfg["size"]
        start = last = last_sample = now_ms()
        end = start + float(cfg.get("duration_s", 10)) * 1000
        tokens = float(size)

        while not self.closed:
            t = now_ms()
            if t >= end:
                break
            rate = cc.pacing_rate_bps()
            burst = max(2 * size, rate / 8 * 0.01)
            tokens = min(tokens + rate / 8 * (t - last) / 1000, burst)
            last = t
            core.check_timeouts(t)

            while tokens >= size and core.can_send(size):
                if self.probe is None or self.probe.bufferedAmount >= BUFFERED_LIMIT:
                    self.down_blocked_ticks += 1
                    tokens = 0.0
                    break
                seq = core.on_send(t, size)
                self._send_probe(proto.encode_data(proto.FLOW_DOWN, seq, t, size))
                tokens -= size

            if t - last_sample >= SAMPLE_MS:
                last_sample = t
                self.down_timeline.append(
                    {
                        "t": t - start,
                        "cc": cc.state(),
                        "srtt": core.srtt,
                        "inflight": core.inflight_bytes,
                        "sent": core.sent,
                        "acked": core.acked,
                        "lost": core.lost,
                        "sctp": self._sctp_state(),
                    }
                )
            await asyncio.sleep(0.001)

        await asyncio.sleep(min(max(0.5, 3 * (core.srtt or 100) / 1000), MAX_GRACE_S))
        core.check_timeouts(now_ms() + 1e9)
        self.send_control({"type": "down_done", **core.summary()})

    def _sctp(self):
        return getattr(self.pc, "sctp", None)

    def _sctp_state(self) -> Dict[str, Any]:
        sctp = self._sctp()
        if sctp is None:
            return {}
        return {
            "cwnd": getattr(sctp, "_cwnd", None),
            "ssthresh": getattr(sctp, "_ssthresh", None),
            "flight_size": getattr(sctp, "_flight_size", None),
            "srtt_ms": (sctp._srtt * 1000) if getattr(sctp, "_srtt", None) else None,
            "buffered": self.probe.bufferedAmount if self.probe else None,
        }

    async def _sample_up_sctp(self) -> None:
        """SCTP RTT while the browser uploads (the analogue of the QUIC RTT
        sampler in session.py). SCTP SACKs share the association with the probe
        data, so this is a looser reference than QUIC's."""
        min_rtt = None
        while not self.closed:
            sctp = self._sctp()
            srtt = getattr(sctp, "_srtt", None) if sctp else None
            if srtt:
                srtt_ms = srtt * 1000
                min_rtt = srtt_ms if min_rtt is None else min(min_rtt, srtt_ms)
                self.up_sctp_samples.append(
                    {"t_ms": now_ms(), "latest_rtt_ms": srtt_ms, "min_rtt_ms": min_rtt, "srtt_ms": srtt_ms}
                )
            await asyncio.sleep(SCTP_SAMPLE_MS / 1000)

    # ---- reporting -------------------------------------------------------

    def report(self, brief: bool) -> Dict[str, Any]:
        out: Dict[str, Any] = {"up": self.up.summary() if self.up.received else None}
        if out["up"] is not None and self.up_acks:
            out["up"]["acks_sent"] = self.up_acks.acks_sent
            out["up"]["ack_mode"] = self.up_acks.mode
        if self.up.received and self.up.records is not None:
            # No packet-number equivalent here, so no local-drop split: SCTP
            # gives us no per-packet transmission record for the peer.
            out["up_analysis"] = analyze_up(self.up.records, None, self.up_sctp_samples, self.start_ms or self.created)
        if self.down:
            out["down"] = {
                **self.down.summary(),
                "blocked_ticks": self.down_blocked_ticks,
                "timeline": self.down_timeline,
            }
        else:
            out["down"] = None
        if not brief:
            out["up_records"] = self.up.records
            out["down_records"] = self.down.records if self.down else None
        return out

    def _save(self, client_results: Optional[Dict[str, Any]]) -> str:
        os.makedirs(self.log_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(self.created / 1000))
        path = os.path.join(self.log_dir, f"{stamp}_rtc.json")
        with open(path, "w") as f:
            json.dump(
                {
                    "transport": "webrtc-datachannel",
                    "peer": self.peer,
                    "config": self.config,
                    "server": self.report(brief=False),
                    "client": client_results,
                },
                f,
            )
        log.info("saved %s", path)
        return path

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for task in (self._task, self._up_task):
            if task:
                task.cancel()
        if self.up_acks:
            self.up_acks.close()
        if not self.saved and (self.up.received or self.down):
            try:
                self._save(None)
            except Exception:  # noqa: BLE001
                log.exception("could not save partial results")
