"""Packet trains over plain HTTP POSTs (the TCP/TLS counterpart of trains.py).

    python http_trains.py --port 8081

The page fires `train_len` POSTs back to back; this server timestamps each one
as its body finishes arriving and reports the spacing, exactly like the
datagram version.

What this measures differently from WebTransport/WebRTC trains:

* **TCP, not UDP.** Loss is repaired below us, so dispersion reflects
  retransmission and congestion-window behaviour as well as the bottleneck.
* **Several connections.** Browsers open up to ~6 sockets per origin (more with
  HTTP/2 multiplexing on one), so a "burst" may be spread across connections
  that each have their own congestion window. The report groups arrivals by
  peer port so you can see how many were used.
* **Request overhead.** Each POST carries headers, so bytes on the wire exceed
  the payload; the implied-rate estimate is a lower bound.

Two shapes:

* ``/trains/post`` - one POST per packet. Simple, but the browser spreads a
  burst over its connection pool (6 sockets per origin on HTTP/1.1), so the
  arrivals interleave flows that each have their own congestion window.
* ``/trains/bulk`` - **one POST per train**: a single request whose body is
  ``train_len * size`` bytes, so the bytes go back to back down one connection.
  The server timestamps the body as it arrives, chunk by chunk, which is the
  closest thing to per-packet arrival times TCP will give an application.
  Caveats: a chunk is "what one read returned", so the kernel may coalesce
  several segments into one; and a cold connection is in slow start, so early
  trains measure congestion-window growth rather than the path.

It also carries the **load generator** the RTT page uses: a bulk transfer in
either direction against this same host, so you can watch what a saturating TCP
flow does to the one-way delay of the datagram stream running beside it
(bufferbloat: the queue it fills is the same queue the probes traverse).

Endpoints (CORS open):
  POST /trains/post?c=<client>&t=<train>&i=<index>&n=<len>&ts=<send_ms>  body = padding
  POST /trains/bulk?c=<client>&t=<train>&n=<len>&size=<bytes>&ts=<send_ms>  body = bulk
  GET  /trains/report?c=<client>[&save=1]  -> summary JSON, optionally written to logs/
  POST /trains/load/upload            body = anything; read and discarded -> {bytes, ms, mbps}
  GET  /trains/load/download?bytes=N  -> N bytes of padding, streamed as fast as TCP allows
"""

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

import protocol as proto
from trains import TrainReceiver

HERE = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger("http-trains")

_WALL_OFFSET = time.time() - time.monotonic()

# Smaller reads give finer arrival timestamps. The kernel can still coalesce
# segments, and on loopback the whole body is usually there at once, so bulk
# dispersion only means something over a real path.
READ_CHUNK = 4096

# Load generator: big enough chunks that the kernel, not this loop, is the limit.
LOAD_CHUNK = 64 * 1024
MAX_LOAD_BYTES = 2 * 1024 * 1024 * 1024
_LOAD_PAD = bytes(LOAD_CHUNK)

CORS = (
    "Access-Control-Allow-Origin: *\r\n"
    "Access-Control-Allow-Headers: content-type\r\n"
    "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n"
)

receivers: Dict[str, TrainReceiver] = {}
bulk_trains: Dict[str, list] = {}  # client -> [{train_id, chunks: [[t, bytes]], ...}]
connections: Dict[str, Dict[str, int]] = {}  # client -> {peer port: requests}


def now_ms() -> float:
    return (time.monotonic() + _WALL_OFFSET) * 1000


def summarize_bulk(trains: list) -> Optional[dict]:
    """Per-train dispersion of a bulk upload, from the chunk arrival times."""
    if not trains:
        return None
    from trains import _percentiles

    per_train = []
    for t in trains:
        chunks = t["chunks"]
        if len(chunks) < 2:
            per_train.append({**{k: v for k, v in t.items() if k != "chunks"}, "chunks": len(chunks),
                              "dispersion_ms": None, "implied_rate_bps": None, "iat_ms": []})
            continue
        times = [c[0] for c in chunks]
        dispersion = times[-1] - times[0]
        # bytes after the first chunk arrived in `dispersion` ms
        later_bytes = sum(c[1] for c in chunks[1:])
        per_train.append(
            {
                "train_id": t["train_id"],
                "peer_port": t["peer_port"],
                "bytes": t["bytes"],
                "chunks": len(chunks),
                "chunk_sizes": [c[1] for c in chunks],
                "dispersion_ms": dispersion,
                "iat_ms": [times[i] - times[i - 1] for i in range(1, len(times))],
                "implied_rate_bps": (later_bytes * 8 * 1000 / dispersion) if dispersion else None,
            }
        )
    complete = [t for t in per_train if t["dispersion_ms"]]
    return {
        "trains": len(per_train),
        "connections": len({t["peer_port"] for t in per_train}),
        "chunks_per_train": _percentiles([float(t["chunks"]) for t in per_train]),
        "dispersion_ms": _percentiles([t["dispersion_ms"] for t in complete]),
        "implied_rate_bps": _percentiles([t["implied_rate_bps"] for t in complete]),
        "iat_ms": _percentiles([x for t in per_train for x in t["iat_ms"]]),
        "per_train": per_train,
    }


async def handle_load(method: str, path: str, q: dict, body_len: int, reader, writer) -> None:
    """Saturating transfer in one direction, to load the path under test.

    Upload: swallow the body as fast as it arrives. Download: stream padding
    until the client has what it asked for or goes away (the page aborts the
    fetch when you stop the load, which shows up here as a reset).
    """
    if method == "OPTIONS":
        writer.write(f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
        return

    if path.endswith("/upload"):
        start = now_ms()
        got, remaining = 0, body_len
        while remaining > 0:
            data = await reader.read(min(remaining, LOAD_CHUNK))
            if not data:
                break
            got += len(data)
            remaining -= len(data)
        ms = now_ms() - start
        payload = json.dumps({"bytes": got, "ms": ms, "mbps": (got * 8 / ms / 1000) if ms else None}).encode()
        writer.write(
            f"HTTP/1.1 200 OK\r\n{CORS}Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload
        )
        return

    if path.endswith("/download"):
        total = max(0, min(int(q.get("bytes", 32 * 1024 * 1024)), MAX_LOAD_BYTES))
        writer.write(
            f"HTTP/1.1 200 OK\r\n{CORS}Content-Type: application/octet-stream\r\n"
            f"Cache-Control: no-store\r\nContent-Length: {total}\r\n\r\n".encode()
        )
        sent = 0
        while sent < total:
            n = min(LOAD_CHUNK, total - sent)
            writer.write(_LOAD_PAD[:n])
            await writer.drain()  # backpressure: don't buffer the whole file here
            sent += n
        return

    writer.write(f"HTTP/1.1 404 Not Found\r\n{CORS}Content-Length: 0\r\n\r\n".encode())


async def handle(reader, writer, log_dir: str) -> None:
    peer = writer.get_extra_info("peername")
    try:
        while True:  # keep-alive: one socket can carry many requests
            request_line = await reader.readline()
            if not request_line:
                return
            try:
                method, target, _ = request_line.decode().split(" ", 2)
            except ValueError:
                return
            headers = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"", b"\n"):
                    break
                name, _, value = line.decode().partition(":")
                headers[name.strip().lower()] = value.strip()

            url = urlparse(target)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            body_len = int(headers.get("content-length", 0))
            if url.path.startswith("/trains/load/"):
                await handle_load(method, url.path, q, body_len, reader, writer)
                await writer.drain()
                continue
            if url.path == "/trains/bulk" and body_len:
                # Timestamp the body as it lands: each read is roughly one
                # delivery of segments from the kernel.
                chunks = []
                remaining = body_len
                first = None
                while remaining > 0:
                    data = await reader.read(min(remaining, READ_CHUNK))
                    if not data:
                        break
                    t = now_ms()
                    first = first if first is not None else t
                    chunks.append([t, len(data)])
                    remaining -= len(data)
                client = q.get("c", "default")
                bulk_trains.setdefault(client, []).append(
                    {
                        "train_id": int(q.get("t", 0)),
                        "send_ts": float(q.get("ts", 0.0)),
                        "bytes": body_len - remaining,
                        "packets": int(q.get("n", 0)),
                        "packet_size": int(q.get("size", 0)),
                        "peer_port": peer[1],
                        "chunks": chunks,
                    }
                )
                ports = connections.setdefault(client, {})
                ports[str(peer[1])] = ports.get(str(peer[1]), 0) + 1
                body = b""
            else:
                body = await reader.readexactly(body_len) if body_len else b""
            arrival = now_ms()  # after the whole body is in

            if method == "OPTIONS":
                writer.write(f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
            elif method == "POST" and url.path == "/trains/post":
                client = q.get("c", "default")
                rx = receivers.setdefault(client, TrainReceiver())
                rx.on_packet(
                    proto.Train(
                        flow=proto.FLOW_UP,
                        train_id=int(q.get("t", 0)),
                        index=int(q.get("i", 0)),
                        train_len=int(q.get("n", 1)),
                        send_ts=float(q.get("ts", 0.0)),
                        size=body_len + len(request_line) + sum(len(k) + len(v) + 4 for k, v in headers.items()),
                    ),
                    arrival,
                )
                ports = connections.setdefault(client, {})
                ports[str(peer[1])] = ports.get(str(peer[1]), 0) + 1
                writer.write(f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
            elif method == "POST" and url.path == "/trains/bulk":
                # The body was already read above only for small requests; for
                # bulk we re-read it in chunks with timestamps (see below).
                writer.write(f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
            elif method == "GET" and url.path == "/trains/report":
                client = q.get("c", "default")
                rx = receivers.get(client)
                report = {
                    "transport": "http-post",
                    "client": client,
                    "connections": connections.get(client, {}),
                    "trains": rx.summary() if rx else None,
                    "bulk": summarize_bulk(bulk_trains.get(client, [])),
                }
                if q.get("save") and rx:
                    os.makedirs(log_dir, exist_ok=True)
                    path = os.path.join(log_dir, time.strftime("%Y%m%d-%H%M%S") + "_http_trains.json")
                    with open(path, "w") as f:
                        json.dump(report, f)
                    report["saved"] = os.path.relpath(path)
                    log.info("saved %s", path)
                receivers.pop(client, None)
                connections.pop(client, None)
                bulk_trains.pop(client, None)
                payload = json.dumps(report).encode()
                writer.write(
                    f"HTTP/1.1 200 OK\r\n{CORS}Content-Type: application/json\r\n"
                    f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload
                )
            else:
                writer.write(f"HTTP/1.1 404 Not Found\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception:  # noqa: BLE001
        log.exception("request failed")
    finally:
        writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--log-dir", default=os.path.join(HERE, "logs"))
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)

    server = await asyncio.start_server(lambda r, w: handle(r, w, args.log_dir), args.host, args.port)
    log.info("HTTP train receiver on http://%s:%d/trains/post", args.host, args.port)
    async with server:
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
