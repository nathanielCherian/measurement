"""Raw UDP send/receive baseline: what the path carries with no browser in it.

The browser pages measure "browser + QUIC + path + server" all at once. This is
the control: plain UDP datagrams from a laptop to a sink on the server, no
congestion control, no encryption, no browser. Whatever the browser reached, if
this reaches far more over the same path, the shortfall was not the network.

    # on the server
    python3 tools/udp_probe.py serve --port 4444

    # on your machine
    python3 tools/udp_probe.py send --host probe.example.edu --port 4444 \\
        --mbps 0 --seconds 10          # 0 = flat out

Both ends are plain asyncio and need nothing installed. Open the port first
(`sudo ufw allow 4444/udp`), and remember this is an unresponsive flood: it has
no congestion control at all, so do not point it at a shared link you care about
and keep the runs short.

What comes back:

* **delivered rate** at the sink, in 100 ms bins, and its steady-state median
* **loss** from sequence gaps, and where it happened - the sender counts its own
  failed sends (`ENOBUFS`: the kernel could not take the packet, which is the
  local equivalent of the browser dropping datagrams), so loss on the wire is
  `sent - received - send_failures`
* **arrival spacing** (IAT percentiles), to see pacing or bunching
* **one-way delay excess** above the run's minimum, which is the queue the path
  built while we filled it

Interpreting it next to the browser pages:

| Raw UDP | Browser (capacity.html) | Reading |
|---|---|---|
| high | low, with browser drops | the browser's CC is the limit |
| high | low, no drops anywhere | the page or its write window |
| ~same as browser | ~same | the path (or the sink) is the limit |
| low, with loss | - | the path really is that small |

The sink is deliberately cheap: one recvfrom, a few integers, no crypto and no
per-packet Python object churn beyond what is needed, so that it is not the
thing being measured. If the sink's own CPU is pegged the report says so.
"""

import argparse
import asyncio
import json
import os
import socket
import struct
import time
from typing import Any, Dict, List, Optional

MAGIC = 0x55445042  # "UDPB"
HEADER = struct.Struct("!IIdI")  # magic, seq, send_ts (s), run_id
HEADER_SIZE = HEADER.size
CONTROL = struct.Struct("!II")  # magic, opcode
OP_REPORT_REQUEST = 1
OP_RESET = 2
BIN_MS = 100.0
DEFAULT_SIZE = 1200  # under a 1500 B MTU with IPv4+UDP headers
REPORT_RETRIES = 6


def now() -> float:
    return time.time()


def percentiles(xs: List[float]) -> Optional[Dict[str, float]]:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return {
        "min": s[0],
        "p25": s[int(0.25 * (n - 1))],
        "p50": s[int(0.5 * (n - 1))],
        "p75": s[int(0.75 * (n - 1))],
        "p95": s[int(0.95 * (n - 1))],
        "max": s[-1],
        "mean": sum(s) / n,
    }


# ---- sink -------------------------------------------------------------------


class Sink:
    """Counts what arrives. Keeps per-packet work to a minimum on purpose."""

    def __init__(self) -> None:
        self.reset(0)

    def reset(self, run_id: int) -> None:
        self.run_id = run_id
        self.packets = 0
        self.bytes = 0
        self.first_recv: Optional[float] = None
        self.last_recv: Optional[float] = None
        self.min_seq: Optional[int] = None
        self.max_seq = -1
        self.reordered = 0
        self.duplicates = 0
        self.seen: set = set()
        self.bins: Dict[int, List[int]] = {}  # bin -> [packets, bytes]
        self.iat: List[float] = []
        self.owd: List[float] = []
        self.min_owd: Optional[float] = None
        self.cpu_start = time.process_time()
        self.wall_start = time.perf_counter()

    def on_packet(self, seq: int, send_ts: float, size: int, t: float) -> None:
        if seq in self.seen:
            self.duplicates += 1
            return
        self.seen.add(seq)
        self.packets += 1
        self.bytes += size
        if self.first_recv is None:
            self.first_recv = t
            self.min_seq = seq
        else:
            self.iat.append((t - self.last_recv) * 1000)
        self.last_recv = t
        if seq < self.max_seq:
            self.reordered += 1
        self.max_seq = max(self.max_seq, seq)

        owd = (t - send_ts) * 1000  # + clock offset; only the excess is meaningful
        self.min_owd = owd if self.min_owd is None else min(self.min_owd, owd)
        self.owd.append(owd)

        b = int((t - self.first_recv) * 1000 // BIN_MS)
        row = self.bins.get(b)
        if row is None:
            self.bins[b] = [1, size]
        else:
            row[0] += 1
            row[1] += size

    def summary(self) -> Dict[str, Any]:
        if not self.packets:
            return {"packets": 0}
        span = max(1e-6, self.last_recv - self.first_recv)
        expected = self.max_seq - (self.min_seq or 0) + 1
        bins = [
            {"t": b * BIN_MS, "packets": v[0], "bps": v[1] * 8 * 1000 / BIN_MS}
            for b, v in sorted(self.bins.items())
        ]
        # Drop the first and last bin: they are partial by construction.
        steady = [b["bps"] for b in bins[1:-1]] or [b["bps"] for b in bins]
        cpu = (time.process_time() - self.cpu_start) / max(1e-9, time.perf_counter() - self.wall_start)
        min_owd = self.min_owd or 0.0
        return {
            "run_id": self.run_id,
            "packets": self.packets,
            "bytes": self.bytes,
            "seq_range": [self.min_seq, self.max_seq],
            "expected": expected,
            "missing": max(0, expected - self.packets),
            "loss_pct": 100.0 * max(0, expected - self.packets) / expected if expected else None,
            "reordered": self.reordered,
            "duplicates": self.duplicates,
            "span_s": span,
            "mean_bps": self.bytes * 8 / span,
            "steady_bps": percentiles(steady),
            "iat_ms": percentiles(self.iat),
            "owd_excess_ms": percentiles([o - min_owd for o in self.owd]),
            "bins": len(bins),
            "sink_cpu_fraction": cpu,
            # If the sink itself is pegged, it is the bottleneck, not the path.
            "sink_busy": cpu > 0.85,
        }

    def full_report(self) -> Dict[str, Any]:
        out = self.summary()
        out["timeline"] = [
            {"t": b * BIN_MS, "packets": v[0], "bps": v[1] * 8 * 1000 / BIN_MS}
            for b, v in sorted(self.bins.items())
        ]
        return out


class SinkProtocol(asyncio.DatagramProtocol):
    def __init__(self, log_dir: Optional[str]) -> None:
        self.sink = Sink()
        self.log_dir = log_dir
        self.transport = None

    def connection_made(self, transport) -> None:
        self.transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            try:  # a flood arrives faster than Python wakes up; ask for room
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
            except OSError:
                pass
            print(f"rcvbuf {sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF) // 1024} KB")

    def datagram_received(self, data: bytes, addr) -> None:
        t = now()
        if len(data) == CONTROL.size:
            magic, op = CONTROL.unpack(data)
            if magic != MAGIC:
                return
            if op == OP_REPORT_REQUEST:
                report = self.sink.full_report()
                if self.log_dir and report.get("packets"):
                    os.makedirs(self.log_dir, exist_ok=True)
                    path = os.path.join(self.log_dir, time.strftime("%Y%m%d-%H%M%S") + "_udp_sink.json")
                    with open(path, "w") as f:
                        json.dump(report, f)
                    report["saved"] = path
                    print(f"saved {path}")
                # The timeline can exceed an MTU, so only the summary goes back.
                self.transport.sendto(json.dumps(self.sink.summary()).encode()[:1200], addr)
                print(json.dumps(self.sink.summary())[:400])
            elif op == OP_RESET:
                self.sink.reset(0)
                self.transport.sendto(b'{"reset":true}', addr)
                print(f"reset, receiving from {addr[0]}")
            return

        if len(data) < HEADER_SIZE:
            return
        magic, seq, send_ts, run_id = HEADER.unpack_from(data)
        if magic != MAGIC:
            return
        if run_id != self.sink.run_id:
            self.sink.reset(run_id)  # a new run started
            print(f"run {run_id} from {addr[0]}")
        self.sink.on_packet(seq, send_ts, len(data), t)


async def serve(args) -> None:
    loop = asyncio.get_event_loop()
    await loop.create_datagram_endpoint(
        lambda: SinkProtocol(args.log_dir), local_addr=(args.host, args.port)
    )
    print(f"udp sink on {args.host}:{args.port} (ctrl-c to stop)")
    await asyncio.Future()


# ---- sender -----------------------------------------------------------------


def send_run(args) -> Dict[str, Any]:
    """Blast UDP at the sink, then ask it what it got."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    except OSError:
        pass
    target = (socket.gethostbyname(args.host), args.port)
    run_id = int(time.time()) & 0xFFFFFFFF
    payload = bytearray(args.size)

    rate_bps = args.mbps * 1e6
    tokens = float(args.size)
    sent = failures = 0
    start = time.perf_counter()
    last = start
    end = start + args.seconds
    bins: Dict[int, int] = {}

    print(f"sending {args.size} B datagrams to {args.host}:{args.port} for {args.seconds}s"
          f"{f' at {args.mbps} Mbps' if rate_bps else ' flat out'}")

    while True:
        t = time.perf_counter()
        if t >= end:
            break
        if rate_bps:
            tokens = min(tokens + rate_bps / 8 * (t - last), rate_bps / 8 * 0.05)
        last = t

        burst = 0
        while burst < 64 and (not rate_bps or tokens >= args.size):
            HEADER.pack_into(payload, 0, MAGIC, sent & 0xFFFFFFFF, now(), run_id)
            try:
                sock.sendto(payload, target)
                sent += 1
                bins[int((t - start) * 1000 // BIN_MS)] = bins.get(int((t - start) * 1000 // BIN_MS), 0) + 1
            except BlockingIOError:
                failures += 1          # send buffer full: the kernel is the limit
                break
            except OSError as exc:     # ENOBUFS and friends
                failures += 1
                if failures % 10000 == 1:
                    print(f"  send error: {exc}")
                break
            if rate_bps:
                tokens -= args.size
            burst += 1
        if not rate_bps and burst == 0:
            time.sleep(0)  # yield when the socket is backed up

    elapsed = time.perf_counter() - start
    offered_bps = sent * args.size * 8 / elapsed
    print(f"sent {sent} packets ({sent * args.size / 1e6:.1f} MB) in {elapsed:.1f}s "
          f"= {offered_bps / 1e6:.1f} Mbps, {failures} send failures")

    # Ask the sink for its side. UDP, so retry the request a few times.
    sock.setblocking(True)
    sock.settimeout(1.0)
    report = None
    for _ in range(REPORT_RETRIES):
        try:
            sock.sendto(CONTROL.pack(MAGIC, OP_REPORT_REQUEST), target)
            data, _ = sock.recvfrom(65535)
            report = json.loads(data.decode())
            break
        except (socket.timeout, json.JSONDecodeError):
            continue
    sock.close()

    out = {
        "sender": {
            "host": args.host, "port": args.port, "size": args.size,
            "seconds": args.seconds, "target_mbps": args.mbps,
            "sent_packets": sent, "send_failures": failures,
            "elapsed_s": elapsed, "offered_bps": offered_bps,
            "send_rate_bins": [{"t": b * BIN_MS, "bps": n * args.size * 8 * 1000 / BIN_MS}
                               for b, n in sorted(bins.items())],
        },
        "sink": report,
    }
    print_report(out)
    return out


def print_report(out: Dict[str, Any]) -> None:
    s, r = out["sender"], out.get("sink")
    print()
    print(f"  offered        {s['offered_bps'] / 1e6:8.2f} Mbps  ({s['sent_packets']} packets)")
    if not r or not r.get("packets"):
        print("  no report from the sink - is it running, and is the port open?")
        return
    steady = (r.get("steady_bps") or {}).get("p50")
    print(f"  delivered      {r['mean_bps'] / 1e6:8.2f} Mbps  (steady p50 "
          f"{steady / 1e6 if steady else float('nan'):.2f} Mbps, {r['packets']} packets)")
    print(f"  lost on wire   {r['missing']:8d}      ({r['loss_pct']:.2f}% of what was sent)")
    print(f"  send failures  {s['send_failures']:8d}      (never left this machine)")
    if r.get("iat_ms"):
        print(f"  arrival IAT    p50 {r['iat_ms']['p50']:.3f} ms  p95 {r['iat_ms']['p95']:.3f} ms")
    if r.get("owd_excess_ms"):
        print(f"  one-way delay  p50 +{r['owd_excess_ms']['p50']:.1f} ms  p95 +{r['owd_excess_ms']['p95']:.1f} ms "
              f"above the run's minimum")
    if r.get("reordered"):
        print(f"  reordered      {r['reordered']}")
    if r.get("sink_busy"):
        print(f"  NOTE: the sink was CPU-bound ({100 * r['sink_cpu_fraction']:.0f}%): this is its "
              f"ceiling, not the path's.")
    print()
    print("  Compare with capacity.html over the same path: if this is much higher, the browser "
          "(or its QUIC stack) was the limit, not the network.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="run the sink (on the server)")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=4444)
    p_serve.add_argument("--log-dir", default=None, help="write each run's full report here")
    p_serve.set_defaults(func=lambda a: asyncio.run(serve(a)))

    p_send = sub.add_parser("send", help="blast packets at the sink (on your machine)")
    p_send.add_argument("--host", required=True)
    p_send.add_argument("--port", type=int, default=4444)
    p_send.add_argument("--size", type=int, default=DEFAULT_SIZE, help="datagram size in bytes")
    p_send.add_argument("--seconds", type=float, default=10.0)
    p_send.add_argument("--mbps", type=float, default=0.0, help="target rate; 0 sends flat out")
    p_send.add_argument("--ramp", default=None,
                        help="comma-separated Mbps steps, e.g. 1,2,5,10,20,50 (each --seconds long)")
    p_send.add_argument("--out", default=None, help="write the run's JSON here")
    p_send.set_defaults(func=run_send)

    args = parser.parse_args()
    args.func(args)


def run_send(args) -> None:
    runs = []
    if args.ramp:
        for step in [float(x) for x in args.ramp.split(",")]:
            args.mbps = step
            runs.append(send_run(args))
            time.sleep(0.5)
        print("\n  ramp summary (offered -> delivered):")
        for r in runs:
            sink = r.get("sink") or {}
            got = (sink.get("steady_bps") or {}).get("p50")
            print(f"    {r['sender']['target_mbps']:7.1f} Mbps -> "
                  f"{got / 1e6 if got else float('nan'):7.2f} Mbps"
                  f"  loss {sink.get('loss_pct', float('nan')):5.2f}%")
    else:
        runs.append(send_run(args))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(runs if len(runs) > 1 else runs[0], f, indent=1)
        print(f"  saved {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
