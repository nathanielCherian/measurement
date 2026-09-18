"""The RTT monitor as a command-line tool: raw UDP, no browser, no QUIC.

`web/rtt.html` measures browser + QUIC + path. This measures just the path, with
the same arithmetic - it imports `RttMonitor` and `RttStreamReceiver` from
`server/rtt.py`, so every field means exactly what it means on the page and the
two can be compared line for line.

    # on the server
    python3 tools/udp_rtt.py serve --port 4445

    # on your machine
    python3 tools/udp_rtt.py send --host probe.example.edu --port 4445 --seconds 30

Each packet carries the sender's clock at hand-off; the responder turns it
around immediately, stamping its arrival. One exchange gives

    rtt      = pong arrival - ping send        (one clock, no sync needed)
    up leg   = echo_recv_ts - send_ts          (+ the clock offset)
    down leg = pong arrival - echo_recv_ts     (- the clock offset)

The offset is unknown but constant, so each leg is read against its own
minimum: `up_excess` climbing while `down_excess` stays flat puts the queue on
the path out of here. The responder also parses the timestamps inside the
stream it receives, which needs no reply at all, so one-way delay and arrival
spacing survive even when the return path is broken.

Add `--load-mbps` to run a saturating UDP flood beside the probe stream (the
command-line version of the load buttons on the page): if one-way delay climbs
while the load runs and falls when it stops, the bottleneck queue is
bufferbloated and the gap measures how deep. That flood has no congestion
control - keep it short, and point it only at links you own.

Read next to the browser: if delay here stays flat while the page's RTT climbs
over the same path at the same rate, the queue is inside the browser, not in
the network.
"""

import argparse
import asyncio
import json
import os
import signal
import socket
import struct
import sys
import time
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "server"))

import protocol as proto  # noqa: E402  (needs the server dir on the path)
from rtt import RttMonitor, RttStreamReceiver, run_rtt_stream  # noqa: E402

# Control packets use a first byte no probe packet can have (protocol.py uses
# 1..6), so the same socket carries both without ambiguity.
CTRL_MAGIC = 0xFE
CTRL = struct.Struct("!BBI")  # magic, opcode, run_id
OP_REPORT = 1
OP_RESET = 2
REPORT_RETRIES = 6
DEFAULT_SIZE = 64          # a delay probe should not be a load test
LOAD_SIZE = 1200

_WALL_OFFSET = time.time() - time.monotonic()


def now_ms() -> float:
    return (time.monotonic() + _WALL_OFFSET) * 1000


# ---- responder ---------------------------------------------------------------


class Responder(asyncio.DatagramProtocol):
    """Echoes PINGs and reads what their timestamps say about the path here."""

    def __init__(self, log_dir: Optional[str]) -> None:
        self.rx = RttStreamReceiver()
        self.log_dir = log_dir
        self.transport = None
        self.load_packets = 0
        self.load_bytes = 0
        self.load_first: Optional[float] = None
        self.load_last: Optional[float] = None
        self.peer = None

    def connection_made(self, transport) -> None:
        self.transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
            except OSError:
                pass

    def datagram_received(self, data: bytes, addr) -> None:
        t = now_ms()
        if data and data[0] == CTRL_MAGIC and len(data) >= CTRL.size:
            _, op, _run = CTRL.unpack_from(data)
            if op == OP_RESET:
                self.reset()
                self.transport.sendto(b'{"reset":true}', addr)
                print(f"reset; probing from {addr[0]}")
            elif op == OP_REPORT:
                # Never truncate: a clipped JSON body is unparseable and the
                # sender would silently show no server-side numbers at all. Drop
                # the per-bin timeline instead; the full version is saved here.
                self.transport.sendto(json.dumps(self.report(compact=True)).encode(), addr)
                self.save()
            return

        pkt = proto.decode(data)
        if isinstance(pkt, proto.Ping):
            # Turn it around before doing anything else: whatever we do first
            # lands in the other end's RTT sample.
            self.transport.sendto(proto.encode_pong(pkt.flow, pkt.seq, pkt.send_ts, t), addr)
            self.rx.on_ping(pkt, t)
            if self.peer != addr[0]:
                self.peer = addr[0]
                print(f"probing from {addr[0]}")
        elif isinstance(pkt, proto.Data):
            # Background load: counted, never echoed - echoing it would put the
            # load on the return path too and measure something else.
            self.load_packets += 1
            self.load_bytes += len(data)
            self.load_first = self.load_first if self.load_first is not None else t
            self.load_last = t

    def reset(self) -> None:
        self.rx = RttStreamReceiver()
        self.load_packets = self.load_bytes = 0
        self.load_first = self.load_last = None

    def report(self, compact: bool = False) -> Dict[str, Any]:
        stream = self.rx.summary()
        if compact and stream:
            stream = {k: v for k, v in stream.items() if k != "timeline"}
        out: Dict[str, Any] = {"stream": stream}
        if self.load_packets and self.load_first is not None:
            span = max(1.0, (self.load_last or 0) - self.load_first)
            out["load"] = {
                "packets": self.load_packets,
                "bytes": self.load_bytes,
                "span_ms": span,
                "bps": self.load_bytes * 8 * 1000 / span,
            }
        return out

    def save(self) -> None:
        if not self.log_dir or not self.rx.received:
            return
        os.makedirs(self.log_dir, exist_ok=True)
        path = os.path.join(self.log_dir, time.strftime("%Y%m%d-%H%M%S") + "_udp_rtt.json")
        full = {"stream": self.rx.summary(keep_records=True), **{k: v for k, v in self.report().items() if k != "stream"}}
        with open(path, "w") as f:
            json.dump(full, f)
        print(f"saved {path}")


async def serve(args) -> None:
    loop = asyncio.get_event_loop()
    await loop.create_datagram_endpoint(lambda: Responder(args.log_dir), local_addr=(args.host, args.port))
    print(f"udp rtt responder on {args.host}:{args.port} (ctrl-c to stop)")
    await asyncio.Future()


# ---- sender ------------------------------------------------------------------


class Prober(asyncio.DatagramProtocol):
    def __init__(self, monitor: RttMonitor) -> None:
        self.monitor = monitor
        self.transport = None
        self.report: Optional[Dict[str, Any]] = None

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:
        t = now_ms()
        if data[:1] == b"{":
            try:
                self.report = json.loads(data.decode())
            except json.JSONDecodeError:
                pass
            return
        pkt = proto.decode(data)
        if isinstance(pkt, proto.Pong):
            self.monitor.on_pong(pkt, t)


async def send_load(transport, target, mbps: float, stop: asyncio.Event, size: int = LOAD_SIZE) -> Dict[str, Any]:
    """Saturating stream beside the probe, to fill the bottleneck queue."""
    rate_bps = mbps * 1e6
    tokens = float(size)
    sent = 0
    seq = 0
    start = last = time.perf_counter()
    while not stop.is_set():
        t = time.perf_counter()
        tokens = min(tokens + rate_bps / 8 * (t - last), rate_bps / 8 * 0.05) if rate_bps else 1e9
        last = t
        burst = 0
        while tokens >= size and burst < 128:
            transport.sendto(proto.encode_data(proto.FLOW_UP, seq, now_ms(), size), target)
            seq += 1
            sent += 1
            tokens -= size
            burst += 1
        await asyncio.sleep(0.001)
    elapsed = max(1e-6, time.perf_counter() - start)
    return {"packets": sent, "bytes": sent * size, "offered_bps": sent * size * 8 / elapsed, "seconds": elapsed}


async def run_send(args) -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    monitor = RttMonitor()
    target = (socket.gethostbyname(args.host), args.port)
    transport, protocol = await loop.create_datagram_endpoint(lambda: Prober(monitor), remote_addr=target)
    run_id = int(time.time()) & 0xFFFFFFFF
    transport.sendto(CTRL.pack(CTRL_MAGIC, OP_RESET, run_id))
    await asyncio.sleep(0.2)

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    print(f"probing {args.host}:{args.port} every {args.interval_ms:g} ms with {args.size} B packets"
          f"{f', plus {args.load_mbps:g} Mbps of load' if args.load_mbps else ''}"
          f"{f' for {args.seconds:g}s' if args.seconds else ' until ctrl-c'}")
    print(f"{'time':>6}  {'rtt':>8}  {'min':>8}  {'srtt':>8}  {'up+':>7}  {'down+':>7}  {'loss':>6}")

    load_task = None
    if args.load_mbps:
        load_task = asyncio.ensure_future(send_load(transport, target, args.load_mbps, stop))

    async def tick_status() -> None:
        start = now_ms()
        while not stop.is_set():
            await asyncio.sleep(1.0)
            m = monitor
            if not m.replies:
                continue
            up_ex = (m._ups[-1] - m.min_up) if m.min_up is not None and m._ups else 0.0
            dn_ex = (m._downs[-1] - m.min_down) if m.min_down is not None and m._downs else 0.0
            # Only count a packet lost once it is overdue; sent-minus-replied
            # would report everything still in flight as loss.
            overdue = m.outstanding(now_ms(), max(200.0, 3 * (m.srtt or 100.0)))
            loss = 100.0 * overdue / m.sent if m.sent else 0.0
            print(f"{(now_ms() - start) / 1000:6.1f}  {m._rtts[-1]:7.2f}ms {m.min_rtt:7.2f}ms "
                  f"{m.srtt:7.2f}ms {up_ex:6.2f}ms {dn_ex:6.2f}ms {loss:5.1f}%")

    status = asyncio.ensure_future(tick_status())
    duration = args.seconds if args.seconds else 1e9
    stream = asyncio.ensure_future(run_rtt_stream(
        lambda buf: transport.sendto(buf),
        proto.FLOW_UP, now_ms, monitor,
        interval_ms=args.interval_ms, duration_s=duration, size=args.size,
        is_open=lambda: not stop.is_set(),
    ))
    done, _ = await asyncio.wait({stream, asyncio.ensure_future(stop.wait())}, return_when=asyncio.FIRST_COMPLETED)
    stop.set()
    send_result = await stream if stream in done else {"sent": monitor.sent}
    status.cancel()
    load_result = await load_task if load_task else None

    await asyncio.sleep(min(1.0, 5 * (monitor.srtt or 100) / 1000))  # let the tail come home

    for _ in range(REPORT_RETRIES):
        transport.sendto(CTRL.pack(CTRL_MAGIC, OP_REPORT, run_id))
        await asyncio.sleep(0.3)
        if protocol.report:
            break
    transport.close()

    results = {
        "tool": "udp_rtt.py",
        "host": args.host, "port": args.port,
        "interval_ms": args.interval_ms, "size": args.size,
        "send": send_result,
        "load": load_result,
        "monitor": monitor.summary(keep_records=args.keep_records),
        "responder": protocol.report,
    }
    print_summary(results)
    return results


def print_summary(r: Dict[str, Any]) -> None:
    m = r.get("monitor") or {}
    if not m:
        print("\n  no replies - is the responder running, and is the port open?")
        return
    resp = (r.get("responder") or {}).get("stream") or {}
    p = lambda d, k: (d.get(k) or {})  # noqa: E731
    print()
    print(f"  packets        {m['sent']} sent, {m['replies']} replied, {m['lost']} lost ({m['loss_pct']:.2f}%)")
    print(f"  rtt            min {m['min_rtt_ms']:.2f} ms  p50 {p(m, 'rtt_ms').get('p50', 0):.2f}  "
          f"p95 {p(m, 'rtt_ms').get('p95', 0):.2f}  max {p(m, 'rtt_ms').get('max', 0):.2f}")
    print(f"  queue above min  p50 {p(m, 'queue_ms').get('p50', 0):.2f} ms  p95 {p(m, 'queue_ms').get('p95', 0):.2f} ms")
    print(f"  |delta rtt|    p95 {p(m, 'ipdv_abs_ms').get('p95', 0):.2f} ms")
    if m.get("up_excess_ms") and m.get("down_excess_ms"):
        up95, dn95 = p(m, "up_excess_ms").get("p95", 0), p(m, "down_excess_ms").get("p95", 0)
        print(f"  leg excess p95 up {up95:.2f} ms  down {dn95:.2f} ms  -> queue is "
              f"{'outbound from here' if up95 > dn95 else 'on the return path'}")
    if resp:
        print(f"  at the server  one-way delay above min p95 {p(resp, 'owd_excess_ms').get('p95', 0):.2f} ms, "
              f"jitter {resp.get('jitter_ms', 0):.2f} ms, loss {resp.get('loss_pct', 0):.2f}%")
        print(f"                 arrival spacing p50 {p(resp, 'iat_ms').get('p50', 0):.2f} ms "
              f"(sent {p(resp, 'send_iat_ms').get('p50', 0):.2f} ms apart)")
    if r.get("load"):
        got = ((r.get("responder") or {}).get("load") or {}).get("bps")
        print(f"  load           offered {r['load']['offered_bps'] / 1e6:.1f} Mbps"
              + (f", delivered {got / 1e6:.1f} Mbps" if got else ""))
    if r.get("send", {}).get("send_gap_ms"):
        gap = r["send"]["send_gap_ms"]
        print(f"  send schedule  gap p50 {gap['p50']:.2f} ms, p95 {gap['p95']:.2f} ms "
              f"(asked for {r['interval_ms']:g} ms)")
    print()
    print("  Next to web/rtt.html over the same path: delay that is flat here but climbs there is "
          "queueing inside the browser, not in the network.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="run the echo responder (on the server)")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=4445)
    p_serve.add_argument("--log-dir", default=None, help="write each run's full report here")
    p_serve.set_defaults(func=lambda a: asyncio.run(serve(a)))

    p_send = sub.add_parser("send", help="probe the responder (on your machine)")
    p_send.add_argument("--host", required=True)
    p_send.add_argument("--port", type=int, default=4445)
    p_send.add_argument("--interval-ms", type=float, default=50.0)
    p_send.add_argument("--size", type=int, default=DEFAULT_SIZE)
    p_send.add_argument("--seconds", type=float, default=30.0, help="0 runs until ctrl-c")
    p_send.add_argument("--load-mbps", type=float, default=0.0,
                        help="run a saturating UDP flood beside the probe (0 = none)")
    p_send.add_argument("--keep-records", action="store_true", help="include per-packet rows in --out")
    p_send.add_argument("--out", default=None)
    p_send.set_defaults(func=lambda a: _send(a))

    args = parser.parse_args()
    args.func(args)


def _send(args) -> None:
    results = asyncio.run(run_send(args))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=1)
        print(f"  saved {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
