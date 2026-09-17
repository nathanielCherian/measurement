"""One WebTransport session = one experiment.

Control: newline-delimited JSON on the client-opened bidirectional stream.
  client -> server  {"type":"start", "mode":"up|down|both", "duration_s", "size",
                     "down_cc": {"name", "params"}, ...}
  server -> client  {"type":"started", "server_time_ms", "quic_cc", ...}
  client -> server  {"type":"finish"}             after the run + a grace period
  server -> client  {"type":"server_report", "down": {...}, "up": {...}}
  client -> server  {"type":"results", ...}       browser-side data, saved to the log
  server -> client  {"type":"saved", "file"}
Probe traffic: DATA/ACK datagrams (protocol.py).
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Callable, Dict, Optional

import protocol as proto
from appcc import make_cc
from ack import AckGenerator
from transport_stats import ReceiverStats, SenderCore
from up_analysis import analyze_up

log = logging.getLogger("session")

_WALL_OFFSET = time.time() - time.monotonic()

SAMPLE_MS = 100
QUIC_SAMPLE_MS = 20
MAX_PENDING_DATAGRAMS = 64  # beyond this, aioquic's own cwnd/pacer is the bottleneck
MAX_SIZE = 1100


def now_ms() -> float:
    """Wall-clock ms with monotonic behaviour (for timestamps and pacing)."""
    return (time.monotonic() + _WALL_OFFSET) * 1000


def loop_time_to_ms(t: float) -> float:
    """asyncio loop.time() (time.monotonic) -> the now_ms() clock."""
    return (t + _WALL_OFFSET) * 1000


class Session:
    def __init__(
        self,
        session_id: int,
        h3,
        quic,
        transmit: Callable[[], None],
        log_dir: str,
        quic_cc: str,
        peer: Any,
        shaper_stats: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> None:
        self.id = session_id
        self.h3 = h3
        self.quic = quic
        self.transmit = transmit
        self.log_dir = log_dir
        self.quic_cc = quic_cc
        self.peer = peer
        self.shaper_stats = shaper_stats
        self.created = now_ms()

        self.control_stream: Optional[int] = None
        self._buf = b""
        self.config: Dict[str, Any] = {}
        self.down: Optional[SenderCore] = None
        self.down_timeline: list = []
        self.down_blocked_ticks = 0
        self.up = ReceiverStats()
        self.up_acks: Optional[AckGenerator] = None
        self.up_quic_samples: list = []
        self._up_task: Optional[asyncio.Task] = None
        self.start_ms: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self.closed = False

    # ---- control stream -------------------------------------------------

    def on_stream_data(self, stream_id: int, data: bytes, ended: bool) -> None:
        if self.control_stream is None:
            self.control_stream = stream_id
        if stream_id != self.control_stream:
            return
        self._buf += data
        while b"\n" in self._buf:
            line, self._buf = self._buf.split(b"\n", 1)
            if line.strip():
                self._on_control(json.loads(line))

    def send_control(self, msg: Dict[str, Any]) -> None:
        if self.control_stream is None or self.closed:
            return
        self.quic.send_stream_data(
            self.control_stream, json.dumps(msg).encode() + b"\n"
        )
        self.transmit()

    def _on_control(self, msg: Dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "start":
            self.config = msg
            self.config["size"] = min(int(msg.get("size", 1000)), MAX_SIZE)
            log.info("session %d start %s", self.id, json.dumps(msg))
            self.send_control(
                {
                    "type": "started",
                    "server_time_ms": now_ms(),
                    "quic_cc": self.quic_cc,
                    "size": self.config["size"],
                }
            )
            self.start_ms = now_ms()
            self.up_acks = AckGenerator(
                proto.FLOW_UP,
                msg.get("ack"),
                lambda buf: (self.h3.send_datagram(self.id, buf), self.transmit()),
                now_ms,
            )
            if msg.get("mode") in ("down", "both"):
                self._task = asyncio.ensure_future(self._run_down())
            if msg.get("mode") in ("up", "both"):
                self.quic.rx_packets = []
                self._up_task = asyncio.ensure_future(self._sample_up_quic())
        elif kind == "finish":
            self.quic.rx_packets_final = self.quic.rx_packets
            self.quic.rx_packets = None
            if self._up_task:
                self._up_task.cancel()
            self.send_control({"type": "server_report", **self.report(brief=True)})
        elif kind == "results":
            path = self._save(msg)
            self.send_control({"type": "saved", "file": os.path.relpath(path)})
        else:
            log.warning("unknown control message %r", kind)

    # ---- datagrams ------------------------------------------------------

    def on_datagram(self, data: bytes, meta: Optional[tuple] = None) -> None:
        # meta = (QUIC packet number, arrival loop time) from quic_instrument
        t = loop_time_to_ms(meta[1]) if meta and meta[1] is not None else now_ms()
        pkt = proto.decode(data)
        if isinstance(pkt, proto.Data) and pkt.flow == proto.FLOW_UP:
            self.up.on_data(pkt.seq, pkt.send_ts, t, pkt.size, pn=meta[0] if meta else None)
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
        core = self.down = SenderCore(cc)
        size = cfg["size"]
        start = last = last_sample = now_ms()
        end = start + float(cfg.get("duration_s", 10)) * 1000
        tokens = float(size)

        while not self.closed:
            t = now_ms()
            if t >= end:
                break
            rate = cc.pacing_rate_bps()
            # allow ~10 ms worth of burst, since asyncio timers are ~1 ms coarse
            burst = max(2 * size, rate / 8 * 0.01)
            tokens = min(tokens + rate / 8 * (t - last) / 1000, burst)
            last = t
            core.check_timeouts(t)

            sent = 0
            while tokens >= size and core.can_send(size):
                if len(self.quic._datagrams_pending) >= MAX_PENDING_DATAGRAMS:
                    self.down_blocked_ticks += 1
                    tokens = 0.0
                    break
                seq = core.on_send(t, size)
                self.h3.send_datagram(self.id, proto.encode_data(proto.FLOW_DOWN, seq, t, size))
                tokens -= size
                sent += 1
            if sent:
                self.transmit()

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
                        "quic": self._quic_state(),
                    }
                )
            await asyncio.sleep(0.001)

        # let trailing ACKs arrive, then sweep remaining packets as lost
        await asyncio.sleep(max(0.5, 3 * (core.srtt or 100) / 1000))
        core.check_timeouts(now_ms() + 1e9)
        self.send_control({"type": "down_done", **core.summary()})

    async def _sample_up_quic(self) -> None:
        """Server-side QUIC RTT while the browser uploads: network RTT that
        excludes the browser's local send queue (its ACKs aren't congestion controlled)."""
        loss = self.quic._loss
        while not self.closed:
            if loss._rtt_initialized:
                self.up_quic_samples.append(
                    {
                        "t_ms": now_ms(),
                        "latest_rtt_ms": loss._rtt_latest * 1000,
                        "min_rtt_ms": loss._rtt_min * 1000,
                        "srtt_ms": loss._rtt_smoothed * 1000,
                    }
                )
            await asyncio.sleep(QUIC_SAMPLE_MS / 1000)

    def _quic_state(self) -> Dict[str, Any]:
        loss = getattr(self.quic, "_loss", None)
        if loss is None:
            return {}
        return {
            "cwnd": loss.congestion_window,
            "inflight": loss.bytes_in_flight,
            "srtt_ms": loss._rtt_smoothed * 1000 if loss._rtt_initialized else None,
            "pending_datagrams": len(self.quic._datagrams_pending),
        }

    # ---- reporting ------------------------------------------------------

    def report(self, brief: bool) -> Dict[str, Any]:
        out: Dict[str, Any] = {"up": self.up.summary() if self.up.received else None}
        if out["up"] is not None and self.up_acks:
            out["up"]["acks_sent"] = self.up_acks.acks_sent
            out["up"]["ack_mode"] = self.up_acks.mode
        if self.up.received and self.up.records is not None:
            rx = getattr(self.quic, "rx_packets_final", None) or self.quic.rx_packets
            out["up_analysis"] = analyze_up(
                self.up.records,
                [(pn, loop_time_to_ms(t)) for pn, t in rx] if rx else None,
                self.up_quic_samples,
                self.start_ms or self.created,
            )
        if self.shaper_stats:
            out["netem"] = self.shaper_stats()
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
        path = os.path.join(self.log_dir, f"{stamp}_s{self.id}.json")
        doc = {
            "session_id": self.id,
            "peer": str(self.peer),
            "quic_cc": self.quic_cc,
            "config": self.config,
            "server": self.report(brief=False),
            "client": client_results,
        }
        with open(path, "w") as f:
            json.dump(doc, f)
        log.info("session %d saved %s", self.id, path)
        return path

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self._task:
            self._task.cancel()
        if self._up_task:
            self._up_task.cancel()
        if self.up_acks:
            self.up_acks.close()
