"""WebTransport (HTTP/3) probe server.

    python server.py --port 4433 --cc null
"""

import argparse
import asyncio
import logging
import os
from typing import Dict, Optional

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.buffer import Buffer, BufferReadError, encode_uint_var
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import (
    DataReceived,
    DatagramReceived,
    H3Event,
    HeadersReceived,
    WebTransportStreamDataReceived,
)
from aioquic.quic import connection as aioquic_connection
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, HandshakeCompleted, QuicEvent
from aioquic.quic.logger import QuicFileLogger

import quic_instrument
import quiccc
from netem import IngressShaper, NetemConfig, SharedLink
from session import Session

quic_instrument.install()

HERE = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger("server")

# ---- Browser compatibility -------------------------------------------------
# aioquic implements WebTransport draft-02, which Chrome speaks. Safari 26.4+
# (Network.framework) needs the adjustments below; each was confirmed against
# Safari 26.6.2.

# 1. aioquic decrypts into a fixed 1500-byte buffer (PACKET_LENGTH_MAX in
#    aioquic/_crypto.c) and drops larger packets, but never advertises
#    max_udp_payload_size. Safari sizes packets to the path MTU (16K on
#    loopback), so its larger stream writes were dropped and resent forever.
MAX_UDP_PAYLOAD_SIZE = 1500
_QuicTransportParameters = aioquic_connection.QuicTransportParameters


def _transport_parameters_with_payload_limit(*args, **kwargs):
    kwargs.setdefault("max_udp_payload_size", MAX_UDP_PAYLOAD_SIZE)
    return _QuicTransportParameters(*args, **kwargs)


aioquic_connection.QuicTransportParameters = _transport_parameters_with_payload_limit

# 2. Safari refuses to send the CONNECT request unless the server advertises
#    draft-07 WebTransport max sessions, and also refuses (H3_REQUEST_CANCELLED)
#    if the server advertises any draft-13+ WT_* settings (0x14e9cd29, 0x2b61,
#    0x2b64, 0x2b65) or draft-15 WT_ENABLED (0x2c7cf000). So add only this one.
SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07 = 0xC671706A

# 3. Session flow-control grants Safari expects after the CONNECT response
#    before it opens streams. Unknown capsules are ignored by Chrome (RFC 9297).
CAPSULE_WT_MAX_DATA = 0x190B4D3D
CAPSULE_WT_MAX_STREAMS_BIDI = 0x190B4D3F
CAPSULE_WT_MAX_STREAMS_UNI = 0x190B4D40
CAPSULE_CLOSE_WEBTRANSPORT_SESSION = 0x2843
WT_MAX_DATA = 1 << 32
WT_MAX_STREAMS = 100

# 4. Upgrade token: "webtransport" through draft-12 (Chrome), "webtransport-h3"
#    from draft-13 on.
WEBTRANSPORT_PROTOCOLS = (b"webtransport", b"webtransport-h3")


def encode_capsule(capsule_type: int, value: int) -> bytes:
    payload = encode_uint_var(value)
    return encode_uint_var(capsule_type) + encode_uint_var(len(payload)) + payload


def capsule_types(data: bytes):
    buf = Buffer(data=data)
    try:
        while not buf.eof():
            capsule_type = buf.pull_uint_var()
            buf.seek(buf.tell() + buf.pull_uint_var())
            yield capsule_type
    except BufferReadError:
        return


class CompatH3Connection(H3Connection):
    def _get_local_settings(self) -> Dict[int, int]:
        settings = super()._get_local_settings()
        settings[SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07] = 1
        return settings


class WebTransportProtocol(QuicConnectionProtocol):
    log_dir = os.path.join(HERE, "logs")
    quic_cc = "null"
    netem = NetemConfig()
    netem_link = SharedLink()  # one emulated uplink for every connection

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http: Optional[H3Connection] = None
        self._sessions: Dict[int, Session] = {}
        self._shaper = (
            IngressShaper(
                self.netem,
                lambda data, addr: QuicConnectionProtocol.datagram_received(self, data, addr),
                self.netem_link,
            )
            if self.netem.enabled
            else None
        )

    def datagram_received(self, data, addr) -> None:
        if self._shaper:
            self._shaper(data, addr)
        else:
            super().datagram_received(data, addr)

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, HandshakeCompleted):
            # Creating the H3 connection sends SETTINGS immediately. Waiting for
            # the handshake keeps them out of 0.5-RTT packets, which Safari
            # did not acknowledge.
            self._http = CompatH3Connection(self._quic, enable_webtransport=True)
        elif isinstance(event, ConnectionTerminated):
            log.info("connection terminated: %s", event.reason_phrase or event.error_code)
            for s in self._sessions.values():
                s.close()
            self._sessions.clear()

        if self._http is not None:
            for h3_event in self._http.handle_event(event):
                self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            headers = dict(event.headers)
            method = headers.get(b":method")
            path = headers.get(b":path", b"").decode()
            protocol = headers.get(b":protocol")
            if method == b"CONNECT" and protocol in WEBTRANSPORT_PROTOCOLS and path.startswith("/probe"):
                response = [(b":status", b"200")]
                if b"sec-webtransport-http3-draft02" in headers:
                    # Chrome's draft-02 negotiation; newer drafts don't use this header.
                    response.append((b"sec-webtransport-http3-draft", b"draft02"))
                self._http.send_headers(event.stream_id, response)
                self._http.send_data(
                    event.stream_id,
                    encode_capsule(CAPSULE_WT_MAX_DATA, WT_MAX_DATA)
                    + encode_capsule(CAPSULE_WT_MAX_STREAMS_BIDI, WT_MAX_STREAMS)
                    + encode_capsule(CAPSULE_WT_MAX_STREAMS_UNI, WT_MAX_STREAMS),
                    end_stream=False,
                )
                peer = self._quic._network_paths[0].addr
                self._sessions[event.stream_id] = Session(
                    event.stream_id,
                    self._http,
                    self._quic,
                    self.transmit,
                    self.log_dir,
                    self.quic_cc,
                    peer,
                    shaper_stats=self._shaper.stats if self._shaper else None,
                )
                log.info(
                    "session %d opened from %s; user-agent %s",
                    event.stream_id,
                    peer,
                    headers.get(b"user-agent", b"?").decode(),
                )
            else:
                log.warning(
                    "rejected request (404): %s",
                    {k.decode(): v.decode() for k, v in event.headers},
                )
                self._http.send_headers(event.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
        elif isinstance(event, DataReceived) and event.stream_id in self._sessions:
            # Capsules on the CONNECT stream. One session per connection, so a
            # closed session ends the connection (Safari otherwise keeps it open).
            if CAPSULE_CLOSE_WEBTRANSPORT_SESSION in capsule_types(event.data) or event.stream_ended:
                log.info("session %d closed by peer", event.stream_id)
                self._quic.close()
                self.transmit()
        elif isinstance(event, DatagramReceived):
            meta = self._quic.rx_datagram_meta.popleft() if self._quic.rx_datagram_meta else None
            session = self._sessions.get(event.stream_id)
            if session:
                session.on_datagram(event.data, meta)
        elif isinstance(event, WebTransportStreamDataReceived):
            session = self._sessions.get(event.session_id)
            if session:
                session.on_stream_data(event.stream_id, event.data, event.stream_ended)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="::", help="bind address (:: = dual-stack)")
    parser.add_argument("--port", type=int, default=4433)
    parser.add_argument("--cert", default=os.path.join(HERE, "certs", "cert.pem"))
    parser.add_argument("--key", default=os.path.join(HERE, "certs", "key.pem"))
    parser.add_argument("--cc", default="null", choices=quiccc.NAMES, help="QUIC-level congestion control")
    parser.add_argument("--log-dir", default=os.path.join(HERE, "logs"))
    parser.add_argument("--qlog-dir", help="write a qlog trace per connection (written when it closes)")
    parser.add_argument("--emulate-up-mbps", type=float, help="emulate a browser->server bottleneck of this rate")
    parser.add_argument("--emulate-up-queue-ms", type=float, default=50.0, help="bottleneck queue (max queueing delay)")
    parser.add_argument("--emulate-up-loss", type=float, default=0.0, help="random loss rate for browser->server packets")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    config = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=False,
        max_datagram_frame_size=65535,
        congestion_control_algorithm=args.cc,
        idle_timeout=30.0,
    )
    config.load_cert_chain(args.cert, args.key)
    if args.qlog_dir:
        os.makedirs(args.qlog_dir, exist_ok=True)
        config.quic_logger = QuicFileLogger(args.qlog_dir)

    WebTransportProtocol.log_dir = args.log_dir
    WebTransportProtocol.quic_cc = args.cc
    WebTransportProtocol.netem = NetemConfig(args.emulate_up_mbps, args.emulate_up_queue_ms, args.emulate_up_loss)

    await serve(args.host, args.port, configuration=config, create_protocol=WebTransportProtocol)
    log.info(
        "listening on udp [%s]:%d, QUIC cc=%s, up emulation: %s",
        args.host, args.port, args.cc, WebTransportProtocol.netem.describe(),
    )
    await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
