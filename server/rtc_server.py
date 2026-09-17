"""WebRTC DataChannel probe server (the twin of server.py).

    python rtc_server.py --port 8080

Signaling: POST /offer  {"sdp": ..., "type": "offer"} -> {"sdp": ..., "type": "answer"}
No TLS here: DTLS inside WebRTC is authenticated by the fingerprint in the SDP,
so the page needs no certificate for the media path. Serve the page over
HTTPS and proxy this endpoint (see deploy/nginx-site.conf).
"""

import argparse
import asyncio
import json
import logging
import os
from typing import Set

from aiortc import RTCPeerConnection, RTCSessionDescription

from rtc_session import RtcSession

HERE = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger("rtc-server")

CORS = (
    "Access-Control-Allow-Origin: *\r\n"
    "Access-Control-Allow-Headers: content-type\r\n"
    "Access-Control-Allow-Methods: POST, OPTIONS\r\n"
)

pcs: Set[RTCPeerConnection] = set()


async def handle_offer(params: dict, log_dir: str, peer: str) -> dict:
    pc = RTCPeerConnection()
    pcs.add(pc)
    session = RtcSession(pc, log_dir, peer)
    log.info("offer from %s", peer)

    @pc.on("datachannel")
    def on_datachannel(channel):
        log.info("data channel %r (ordered=%s maxRetransmits=%s)", channel.label, channel.ordered, channel.maxRetransmits)
        session.attach(channel)

    @pc.on("connectionstatechange")
    async def on_state():
        log.info("connection state %s", pc.connectionState)
        if pc.connectionState in ("failed", "closed", "disconnected"):
            session.close()
            await pc.close()
            pcs.discard(pc)

    await pc.setRemoteDescription(RTCSessionDescription(sdp=params["sdp"], type=params["type"]))
    await pc.setLocalDescription(await pc.createAnswer())
    return {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}


async def http_handler(reader, writer, log_dir: str) -> None:
    """Minimal HTTP/1.1 server: just the signaling endpoint (plus CORS preflight)."""
    try:
        request_line = await reader.readline()
        if not request_line:
            return
        method, path, _ = request_line.decode().split(" ", 2)
        headers = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"", b"\n"):
                break
            name, _, value = line.decode().partition(":")
            headers[name.strip().lower()] = value.strip()

        if method == "OPTIONS":
            writer.write(f"HTTP/1.1 204 No Content\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
        elif method == "POST" and path.rstrip("/").endswith("/offer"):
            body = await reader.readexactly(int(headers.get("content-length", 0)))
            peer = str(writer.get_extra_info("peername"))
            answer = json.dumps(await handle_offer(json.loads(body), log_dir, peer)).encode()
            writer.write(
                f"HTTP/1.1 200 OK\r\n{CORS}Content-Type: application/json\r\n"
                f"Content-Length: {len(answer)}\r\n\r\n".encode() + answer
            )
        else:
            writer.write(f"HTTP/1.1 404 Not Found\r\n{CORS}Content-Length: 0\r\n\r\n".encode())
        await writer.drain()
    except Exception:  # noqa: BLE001 - one bad request must not kill the server
        log.exception("request failed")
    finally:
        writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080, help="signaling port (HTTP)")
    parser.add_argument("--log-dir", default=os.path.join(HERE, "logs"))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    server = await asyncio.start_server(
        lambda r, w: http_handler(r, w, args.log_dir), args.host, args.port
    )
    log.info("signaling on http://%s:%d/offer (media over UDP, ports chosen by ICE)", args.host, args.port)
    async with server:
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
