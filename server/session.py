"""One WebTransport session = one experiment.

Control: newline-delimited JSON on the client-opened bidirectional stream.
  client -> server  {"type":"start", "mode":"up|down|both", "duration_s", "size",
                     "down_cc": {"name", "params"},
                     "trains": {...}, "rtt": {"direction", "interval_ms", "size", "duration_s"}}
  server -> client  {"type":"started", "server_time_ms", "quic_cc", ...}
  client -> server  {"type":"finish"}             after the run + a grace period
  server -> client  {"type":"server_report", "down": {...}, "up": {...}}
  client -> server  {"type":"results", ...}       browser-side data, saved to the log
  server -> client  {"type":"saved", "file"}
Probe traffic: DATA/ACK datagrams, TRAIN bursts (trains.py) and PING/PONG for the
steady-stream RTT monitor (rtt.py), all in protocol.py.
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
from rtt import RttMonitor, RttStreamReceiver, run_rtt_stream
from saturation import summarize_saturation, window_report
from trains import TrainReceiver, send_trains
from transport_stats import ReceiverStats, SenderCore
from up_analysis import analyze_up

log = logging.getLogger("session")

_WALL_OFFSET = time.time() - time.monotonic()

MAX_GRACE_S = 3.0  # cap on the end-of-run wait for late ACKs
SAMPLE_MS = 100
QUIC_SAMPLE_MS = 20
MAX_PENDING_DATAGRAMS = 64  # beyond this, aioquic's own cwnd/pacer is the bottleneck
MAX_SIZE = 1100
LOG_RECORD_CAP = 50_000  # beyond this, per-packet rows are left out of the saved log


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

        self.saved = False
        self.control_stream: Optional[int] = None
        self._buf = b""
        self.config: Dict[str, Any] = {}
        self.down: Optional[SenderCore] = None
        self.down_timeline: list = []
        self.down_blocked_ticks = 0
        self.up = ReceiverStats()
        self.up_acks: Optional[AckGenerator] = None
        self.up_trains = TrainReceiver()
        self._trains_task: Optional[asyncio.Task] = None
        # RTT monitor: the browser's stream is parsed here (up_rtt) and echoed
        # back; a server-initiated stream is tracked by down_rtt.
        self.up_rtt = RttStreamReceiver()
        self.down_rtt = RttMonitor()
        self._rtt_task: Optional[asyncio.Task] = None
        self.up_quic_samples: list = []
        self._sat_task: Optional[asyncio.Task] = None
        self.loop_lag_ms: list = []
        self.cpu_fraction: list = []
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
            trains_cfg = msg.get("trains")
            if trains_cfg and trains_cfg.get("direction") in ("down", "both"):
                self._trains_task = asyncio.ensure_future(self._run_trains(trains_cfg))
            rtt_cfg = msg.get("rtt")
            if rtt_cfg and rtt_cfg.get("direction") in ("down", "both"):
                self._rtt_task = asyncio.ensure_future(self._run_rtt(rtt_cfg))
            if msg.get("mode") in ("down", "both"):
                self._task = asyncio.ensure_future(self._run_down())
            if msg.get("mode") in ("up", "both"):
                self.quic.rx_packets = []
                self._up_task = asyncio.ensure_future(self._sample_up_quic())
            if msg.get("saturate"):
                self._sat_task = asyncio.ensure_future(self._report_progress(msg["saturate"]))
        elif kind == "finish":
            self.quic.rx_packets_final = self.quic.rx_packets
            self.quic.rx_packets = None
            if self._up_task:
                self._up_task.cancel()
            if self._sat_task:
                self._sat_task.cancel()
            # The page sends its own offered-rate counters with "finish", so the
            # report can say whether the sender or the browser was the limit.
            if msg.get("client_offered") and self.config.get("saturate") is not None:
                self.config["saturate"]["client_offered"] = msg["client_offered"]
            self.send_control({"type": "server_report", **self.report(brief=True)})
        elif kind == "results":
            path = self._save(msg)
            self.saved = True
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
        elif isinstance(pkt, proto.Train) and pkt.flow == proto.FLOW_UP:
            self.up_trains.on_packet(pkt, t)
        elif isinstance(pkt, proto.Ping) and pkt.flow == proto.FLOW_UP:
            # Turn it around first (any delay here inflates the browser's RTT),
            # then parse what its timestamp says about the upstream path.
            self.h3.send_datagram(self.id, proto.encode_pong(pkt.flow, pkt.seq, pkt.send_ts, t))
            self.transmit()
            self.up_rtt.on_ping(pkt, t)
        elif isinstance(pkt, proto.Pong) and pkt.flow == proto.FLOW_DOWN:
            self.down_rtt.on_pong(pkt, t)
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
        await asyncio.sleep(min(max(0.5, 3 * (core.srtt or 100) / 1000), MAX_GRACE_S))
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

    async def _report_progress(self, cfg: Dict[str, Any]) -> None:
        """Tell the page what is actually arriving, while it is arriving.

        A saturation run is only meaningful live: the page has no idea how much
        of what it writes survives the browser's own send queue, so the server's
        view is the measurement and it has to come back during the run.
        """
        interval = float(cfg.get("progress_ms", 500)) / 1000
        last = now_ms()
        loop = asyncio.get_event_loop()
        while not self.closed:
            # Event-loop lag and CPU: a Python QUIC receiver is expensive per
            # packet, and a server that cannot keep up looks exactly like a slow
            # path from the browser's side. Measuring it here is the only way to
            # tell "the browser's CC stopped at this rate" from "our server did".
            before, cpu_before = loop.time(), time.process_time()
            await asyncio.sleep(interval)
            lag_ms = max(0.0, (loop.time() - before - interval) * 1000)
            cpu_frac = (time.process_time() - cpu_before) / max(1e-9, loop.time() - before)
            self.loop_lag_ms.append(lag_ms)
            self.cpu_fraction.append(cpu_frac)
            t = now_ms()
            if self.up.records is None:
                continue
            win = window_report(self.up.records, last, t)
            last = t
            self.send_control({
                "type": "up_progress",
                "t_ms": t - (self.start_ms or self.created),
                **win,
                "quic": self._quic_state(),
                "packets_total": self.up.received,
                "server_loop_lag_ms": lag_ms,
                "server_cpu_fraction": cpu_frac,
            })

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

    async def _run_trains(self, cfg: Dict[str, Any]) -> None:
        """Send packet trains browser-ward; the page measures their spacing."""
        result = await send_trains(
            lambda buf: (self.h3.send_datagram(self.id, buf), self.transmit()),
            proto.FLOW_DOWN,
            now_ms,
            train_len=int(cfg.get("train_len", 16)),
            trains=int(cfg.get("trains", 50)),
            gap_ms=float(cfg.get("gap_ms", 200)),
            size=min(int(cfg.get("size", 1000)), MAX_SIZE),
            is_open=lambda: not self.closed,
        )
        self.send_control({"type": "trains_done", **result})

    async def _run_rtt(self, cfg: Dict[str, Any]) -> None:
        """Steady PING stream browser-ward; the page echoes each one back."""
        result = await run_rtt_stream(
            lambda buf: (self.h3.send_datagram(self.id, buf), self.transmit()),
            proto.FLOW_DOWN,
            now_ms,
            self.down_rtt,
            interval_ms=float(cfg.get("interval_ms", 50)),
            duration_s=float(cfg.get("duration_s", 30)),
            size=min(int(cfg.get("size", proto.ECHO_HEADER_SIZE)), MAX_SIZE),
            is_open=lambda: not self.closed,
        )
        await asyncio.sleep(min(MAX_GRACE_S, max(0.5, 3 * (self.down_rtt.srtt or 100) / 1000)))
        self.send_control({"type": "rtt_done", **result})

    # ---- reporting ------------------------------------------------------

    def report(self, brief: bool) -> Dict[str, Any]:
        out: Dict[str, Any] = {"up": self.up.summary() if self.up.received else None}
        if self.up_trains.packets:
            out["up_trains"] = self.up_trains.summary()
        if self.up_rtt.received:
            out["up_rtt"] = self.up_rtt.summary(keep_records=not brief)
        if self.down_rtt.sent:
            out["down_rtt"] = self.down_rtt.summary(keep_records=not brief)
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
        if out.get("up_analysis") and self.config.get("saturate"):
            out["saturation"] = summarize_saturation(
                out["up_analysis"],
                client_offered=self.config.get("saturate", {}).get("client_offered"),
                server_load={"loop_lag_ms": self.loop_lag_ms, "cpu_fraction": self.cpu_fraction},
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
            # A saturation run has too many rows to serialise; the 100 ms bins in
            # up_analysis carry the same story at a thousandth of the size.
            records = self.up.records
            out["up_records"] = records if records is not None and len(records) <= LOG_RECORD_CAP else None
            out["up_records_omitted"] = records is not None and len(records) > LOG_RECORD_CAP
            out["up_records_truncated"] = getattr(self.up, "records_truncated", False)
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
        if self._trains_task:
            self._trains_task.cancel()
        if self._rtt_task:
            self._rtt_task.cancel()
        if self._sat_task:
            self._sat_task.cancel()
        if not self.saved and not self.config.get("reference") and (self.up.received or self.down or self.up_rtt.received):
            # The peer vanished mid-run (common on mobile): keep what we have.
            try:
                self._save(None)
            except Exception:  # noqa: BLE001 - never fail teardown
                log.exception("session %d: could not save partial results", self.id)
        if self._task:
            self._task.cancel()
        if self._up_task:
            self._up_task.cancel()
        if self.up_acks:
            self.up_acks.close()
