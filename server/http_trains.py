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

Endpoints (CORS open):
  POST /trains/post?c=<client>&t=<train>&i=<index>&n=<len>&ts=<send_ms>  body = padding
  GET  /trains/report?c=<client>[&save=1]  -> summary JSON, optionally written to logs/
"""

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Dict
from urllib.parse import parse_qs, urlparse

import protocol as proto
from trains import TrainReceiver

HERE = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger("http-trains")

_WALL_OFFSET = time.time() - time.monotonic()

CORS = (
    "Access-Control-Allow-Origin: *\r\n"
    "Access-Control-Allow-Headers: content-type\r\n"
    "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n"
)

receivers: Dict[str, TrainReceiver] = {}
connections: Dict[str, Dict[str, int]] = {}  # client -> {peer port: requests}


def now_ms() -> float:
    return (time.monotonic() + _WALL_OFFSET) * 1000


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
            elif method == "GET" and url.path == "/trains/report":
                client = q.get("c", "default")
                rx = receivers.get(client)
                report = {
                    "transport": "http-post",
                    "client": client,
                    "connections": connections.get(client, {}),
                    "trains": rx.summary() if rx else None,
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
