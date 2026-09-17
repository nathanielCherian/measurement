from typing import Iterable

from aioquic.quic.congestion.base import QuicCongestionControl, register_congestion_control
from aioquic.quic.packet_builder import QuicSentPacket

# Large enough that aioquic's cwnd check and pacer never limit sending.
HUGE_WINDOW = 1 << 40


class NullCongestionControl(QuicCongestionControl):
    """Disables QUIC-level congestion control so that the application-level
    controller (appcc/) is the only thing deciding the send rate.

    aioquic applies the congestion window and pacer to DATAGRAM frames too,
    so without this the QUIC controller would sit underneath ours."""

    def __init__(self, *, max_datagram_size: int) -> None:
        super().__init__(max_datagram_size=max_datagram_size)
        self.congestion_window = HUGE_WINDOW

    def on_packet_acked(self, *, now: float, packet: QuicSentPacket) -> None:
        self.bytes_in_flight -= packet.sent_bytes

    def on_packet_sent(self, *, packet: QuicSentPacket) -> None:
        self.bytes_in_flight += packet.sent_bytes

    def on_packets_expired(self, *, packets: Iterable[QuicSentPacket]) -> None:
        for packet in packets:
            self.bytes_in_flight -= packet.sent_bytes

    def on_packets_lost(self, *, now: float, packets: Iterable[QuicSentPacket]) -> None:
        for packet in packets:
            self.bytes_in_flight -= packet.sent_bytes

    def on_rtt_measurement(self, *, now: float, rtt: float) -> None:
        pass


register_congestion_control("null", NullCongestionControl)
