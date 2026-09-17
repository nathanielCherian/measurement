"""Expose QUIC receive-side details that aioquic keeps internal.

After `install()`, every QuicConnection gets:
  rx_packets:     list of (packet_number, arrival_time_s) for every 1-RTT packet
                  (only while `rx_packets` is not None; set it to [] to start recording)
  rx_datagram_meta: deque of (packet_number, arrival_time_s), one entry per DATAGRAM
                  frame, in the same order as the DatagramReceived events

Packet numbers let the server tell network loss (a QUIC packet number never
arrives) from datagrams the browser dropped locally before sending them (the
application sequence number is missing but no packet number is).
"""

from collections import deque

from aioquic import tls
from aioquic.quic import connection as qconn
from aioquic.quic import crypto as qcrypto

_installed = False


def install() -> None:
    global _installed
    if _installed:
        return
    _installed = True

    orig_decrypt = qcrypto.CryptoPair.decrypt_packet

    def decrypt_packet(self, packet, encrypted_offset, expected_packet_number):
        result = orig_decrypt(self, packet, encrypted_offset, expected_packet_number)
        self.last_packet_number = result[2]
        return result

    qcrypto.CryptoPair.decrypt_packet = decrypt_packet

    orig_init = qconn.QuicConnection.__init__

    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self.rx_packets = None
        self.rx_datagram_meta = deque()

    qconn.QuicConnection.__init__ = __init__

    orig_payload_received = qconn.QuicConnection._payload_received

    def _payload_received(self, context, plain, crypto_frame_required=False):
        if self.rx_packets is not None and context.epoch == tls.Epoch.ONE_RTT:
            pn = getattr(self._cryptos[context.epoch], "last_packet_number", None)
            if pn is not None:
                self.rx_packets.append((pn, context.time))
        return orig_payload_received(self, context, plain, crypto_frame_required=crypto_frame_required)

    qconn.QuicConnection._payload_received = _payload_received

    orig_handle_datagram = qconn.QuicConnection._handle_datagram_frame

    def _handle_datagram_frame(self, context, frame_type, buf):
        n_events = len(self._events)
        orig_handle_datagram(self, context, frame_type, buf)
        if len(self._events) > n_events:
            pn = getattr(self._cryptos[context.epoch], "last_packet_number", None)
            self.rx_datagram_meta.append((pn, context.time))

    qconn.QuicConnection._handle_datagram_frame = _handle_datagram_frame

    # QuicConnection.__init__ builds its frame-handler table from self._handle_*,
    # so patching the class before any connection exists is sufficient.
