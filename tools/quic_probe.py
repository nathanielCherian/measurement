"""The capacity experiment without a browser: a Python WebTransport client.

`tools/udp_probe.py` removes QUIC and the browser at the same time, so a gap
between it and `capacity.html` has two possible causes. This closes that gap: it
speaks the same WebTransport protocol to the same server, sends the same DATA
datagrams, and makes the server produce the same saturation report - the only
thing missing is the browser.

    # against the local server
    server/.venv/bin/python tools/quic_probe.py --url https://127.0.0.1:4433/probe --insecure

    # against a deployed one
    python3 tools/quic_probe.py --url https://probe.example.edu:4433/probe --seconds 15

Reading the three tools together, over one path:

    raw UDP  (udp_probe)   the path, with no QUIC and no browser
    QUIC     (this script) the path + QUIC + the server's Python receive loop
    browser  (capacity)    the path + QUIC + the server + the browser

Each step down tells you what that layer cost. If QUIC here already lands near
the browser's number, the browser is not what is limiting you - the server's
per-packet cost is (aioquic decrypts every packet in Python). If this is far
above the browser, the browser really is the ceiling.

Needs aioquic, so run it with `server/.venv/bin/python`. Use `--insecure` for
the self-signed development certificate.
"""

import argparse
import asyncio
import json
import os
import ssl
import sys
import time
from typing import Any, Dict, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "server"))

import protocol as proto  # noqa: E402  (needs the server dir on the path)
from aioquic.asyncio.client import connect  # noqa: E402
from aioquic.asyncio.protocol import QuicConnectionProtocol  # noqa: E402
from aioquic.h3.connection import H3_ALPN, H3Connection  # noqa: E402
from aioquic.h3.events import (  # noqa: E402

    HeadersReceived,
    WebTransportStreamDataReceived,
)
from aioquic.quic.configuration import QuicConfiguration  # noqa: E402
from aioquic.quic.events import QuicEvent, StreamDataReceived  # noqa: E402

SAMPLE_MS = 250


class WebTransportClient(QuicConnectionProtocol):
    """Enough of a WebTransport client to run the probe protocol."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic, enable_webtransport=True)
        self.session_id: Optional[int] = None
        self.control_stream: Optional[int] = None
        self.connected = asyncio.Event()
        self._buf = b""
        self._waiters: Dict[str, asyncio.Future] = {}
        self.messages: list = []

    # ---- session setup ---------------------------------------------------

    def connect_session(self, authority: str, path: str) -> None:
        self.session_id = self._quic.get_next_available_stream_id(is_unidirectional=False)
        self._http.send_headers(
            stream_id=self.session_id,
            headers=[
                (b":method", b"CONNECT"),
                (b":protocol", b"webtransport"),
                (b":scheme", b"https"),
                (b":authority", authority.encode()),
                (b":path", path.encode()),
                (b"sec-webtransport-http3-draft02", b"1"),
            ],
        )
        self.transmit()

    def open_control(self) -> None:
        """A client-opened bidirectional WebTransport stream; the server takes
        the first one it sees as the session's control stream."""
        self.control_stream = self._http.create_webtransport_stream(self.session_id, is_unidirectional=False)
        self.transmit()

    def send_control(self, msg: Dict[str, Any]) -> None:
        self._quic.send_stream_data(self.control_stream, json.dumps(msg).encode() + b"\n")
        self.transmit()

    async def wait_for(self, kind: str, timeout: float = 30.0) -> Dict[str, Any]:
        for i, m in enumerate(self.messages):
            if m.get("type") == kind:
                return self.messages.pop(i)
        fut = asyncio.get_event_loop().create_future()
        self._waiters[kind] = fut
        return await asyncio.wait_for(fut, timeout)

    def send_datagram(self, data: bytes) -> None:
        self._http.send_datagram(self.session_id, data)

    # ---- events ----------------------------------------------------------

    def quic_event_received(self, event: QuicEvent) -> None:
        # The server writes control JSON straight onto the WebTransport stream
        # with send_stream_data, and aioquic's client H3 does not surface a
        # stream the client itself created as a WebTransport stream, so read it
        # at the QUIC level. Symmetric with how the server sends it.
        if isinstance(event, StreamDataReceived) and event.stream_id == self.control_stream:
            self._feed(event.data)
            return
        for h3_event in self._http.handle_event(event):
            if isinstance(h3_event, HeadersReceived) and h3_event.stream_id == self.session_id:
                status = dict(h3_event.headers).get(b":status")
                if status == b"200":
                    self.connected.set()
                else:
                    print(f"session refused: :status={status!r}")
            elif isinstance(h3_event, WebTransportStreamDataReceived) and \
                    h3_event.stream_id == self.control_stream:
                self._feed(h3_event.data)

    def _feed(self, data: bytes) -> None:
        self._buf += data
        while b"\n" in self._buf:
            line, self._buf = self._buf.split(b"\n", 1)
            if not line.strip():
                continue
            msg = json.loads(line)
            waiter = self._waiters.pop(msg.get("type"), None)
            if waiter and not waiter.done():
                waiter.set_result(msg)
            else:
                self.messages.append(msg)


async def run(args) -> Dict[str, Any]:
    from urllib.parse import urlparse

    url = urlparse(args.url)
    host, port, path = url.hostname, url.port or 443, url.path or "/probe"

    config = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN, max_datagram_frame_size=65536)
    if args.insecure:
        config.verify_mode = ssl.CERT_NONE
    if args.ca:
        config.load_verify_locations(args.ca)

    async with connect(host, port, configuration=config, create_protocol=WebTransportClient) as client:
        client.connect_session(f"{host}:{port}", path)
        await asyncio.wait_for(client.connected.wait(), 10)
        client.open_control()
        print(f"connected to {args.url}")

        client.send_control({
            "type": "start", "mode": "up", "duration_s": args.seconds, "size": args.size,
            "ack": {"mode": "none"},
            "saturate": {"progress_ms": 500, "mode": "saturate"},
            "client_time_ms": time.time() * 1000, "user_agent": f"quic_probe.py/aioquic",
        })
        await client.wait_for("started")

        quic = client._quic
        sent = 0
        blocked = 0
        start = time.perf_counter()
        end = start + args.seconds
        last_print = start
        rate_bps = args.mbps * 1e6
        tokens = float(args.size)
        last = start

        while True:
            t = time.perf_counter()
            if t >= end:
                break
            if rate_bps:
                tokens = min(tokens + rate_bps / 8 * (t - last), rate_bps / 8 * 0.05)
            last = t

            burst = 0
            while burst < 64 and (not rate_bps or tokens >= args.size):
                # aioquic queues datagrams without bound, so stop feeding it
                # once a batch is waiting: past that we are measuring our own
                # queue, exactly the mistake the browser page had to avoid.
                if len(quic._datagrams_pending) >= args.pending:
                    blocked += 1
                    break
                client.send_datagram(proto.encode_data(proto.FLOW_UP, sent, time.time() * 1000, args.size))
                sent += 1
                if rate_bps:
                    tokens -= args.size
                burst += 1
            if burst:
                client.transmit()
            if t - last_print >= 1.0:
                last_print = t
                print(f"  {t - start:4.1f}s  sent {sent * args.size / 1e6:7.1f} MB "
                      f"({sent * args.size * 8 / (t - start) / 1e6:6.1f} Mbps offered)")
            await asyncio.sleep(0)

        elapsed = time.perf_counter() - start
        offered_bps = sent * args.size * 8 / elapsed
        print(f"sent {sent} datagrams in {elapsed:.1f}s = {offered_bps / 1e6:.1f} Mbps offered")

        await asyncio.sleep(1.0)  # let the tail arrive
        client.send_control({
            "type": "finish",
            "client_offered": {
                "mode": "saturate", "offered_packets": sent, "offered_bytes": sent * args.size,
                "offered_bps": offered_bps, "write_stall_ticks": blocked, "packet_size": args.size,
                "duration_ms": elapsed * 1000, "client": "quic_probe.py",
            },
        })
        report = await client.wait_for("server_report", 30)
        results = {
            "tool": "quic_probe.py", "url": args.url, "size": args.size, "seconds": args.seconds,
            "offered_bps": offered_bps, "sent": sent, "queue_blocked_ticks": blocked,
            "server": report,
        }
        client.send_control({"type": "results", **results})
        try:
            saved = await client.wait_for("saved", 10)
            print(f"server saved {saved.get('file')}")
        except asyncio.TimeoutError:
            pass
        print_summary(results)
        return results


def print_summary(r: Dict[str, Any]) -> None:
    sat = (r.get("server") or {}).get("saturation") or {}
    steady = (sat.get("steady_rate_bps") or {}).get("p50")
    load = sat.get("server_load") or {}
    print()
    print(f"  offered        {r['offered_bps'] / 1e6:8.2f} Mbps  ({r['sent']} datagrams)")
    if steady:
        print(f"  delivered      {steady / 1e6:8.2f} Mbps  (steady p50 at the server)")
    print(f"  dropped locally {sat.get('dropped_in_browser', 0):8}      (never left this process)")
    print(f"  lost on wire   {sat.get('quic_packets_missing', 0):8}")
    if load:
        cpu = (load.get("cpu_fraction") or {}).get("p95")
        lag = (load.get("loop_lag_ms") or {}).get("p95")
        print(f"  server         CPU p95 {100 * cpu:.0f}%, loop lag p95 {lag:.1f} ms"
              f"{'  <-- the server is the bottleneck' if load.get('server_busy') else ''}")
    v = sat.get("verdict") or {}
    if v:
        print(f"  verdict        {v.get('limited_by')}")
    print()
    print("  Next to capacity.html on the same path: a similar number means the browser was not the "
          "limit; a much higher one means it was.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default="https://127.0.0.1:4433/probe")
    p.add_argument("--seconds", type=float, default=15.0)
    p.add_argument("--size", type=int, default=1000)
    p.add_argument("--mbps", type=float, default=0.0, help="target rate; 0 sends flat out")
    p.add_argument("--pending", type=int, default=64,
                   help="datagrams queued in aioquic before we pause (the analogue of the page's write window)")
    p.add_argument("--insecure", action="store_true", help="skip certificate verification (self-signed dev cert)")
    p.add_argument("--ca", default=None, help="CA bundle to verify the server certificate against")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    results = asyncio.run(run(args))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=1)
        print(f"  saved {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
