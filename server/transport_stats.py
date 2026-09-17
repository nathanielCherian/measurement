"""Sender-side ACK/loss/RTT bookkeeping and receiver-side statistics.
Mirrored in web/stats.js."""

from collections import OrderedDict
from typing import Any, Dict, List, Optional

from appcc.base import CongestionController

REORDER_THRESHOLD = 3  # packets
BIN_MS = 100


class SenderCore:
    """Tracks sent-but-unacked packets, estimates RTT, declares losses
    (packet-reordering threshold or timeout) and drives the controller."""

    def __init__(self, cc: CongestionController, keep_records: bool = True) -> None:
        self.cc = cc
        self.next_seq = 0
        self.unacked: "OrderedDict[int, tuple]" = OrderedDict()  # seq -> (send_ts, size)
        self.inflight_bytes = 0
        self.srtt: Optional[float] = None
        self.rttvar = 0.0
        self.min_rtt: Optional[float] = None
        self.sent = self.acked = self.lost = self.late_acks = 0
        self.bytes_sent = 0
        self.rtts: List[float] = []
        self.records: Optional[List[list]] = [] if keep_records else None

    def on_send(self, now: float, size: int) -> int:
        seq = self.next_seq
        self.next_seq += 1
        self.unacked[seq] = (now, size)
        self.inflight_bytes += size
        self.sent += 1
        self.bytes_sent += size
        self.cc.on_packet_sent(now, seq, size)
        return seq

    def on_ack(self, now: float, seq: int, echo_send_ts: float, recv_ts: float) -> None:
        entry = self.unacked.pop(seq, None)
        if entry is None:
            # ACK for a packet already declared lost (spurious loss) or a duplicate.
            self.late_acks += 1
            return
        _, size = entry
        self.inflight_bytes -= size
        self.acked += 1

        rtt = now - echo_send_ts
        self.rtts.append(rtt)
        if self.srtt is None:
            self.srtt, self.rttvar = rtt, rtt / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
            self.srtt = 0.875 * self.srtt + 0.125 * rtt
        self.min_rtt = rtt if self.min_rtt is None else min(self.min_rtt, rtt)
        if self.records is not None:
            self.records.append([seq, echo_send_ts, recv_ts, now])  # [seq, send_ts, recv_ts, ack_arrival]

        self.cc.on_ack(now, seq, size, rtt, self.srtt, self.min_rtt)

        lost = []
        for s in self.unacked:
            if s + REORDER_THRESHOLD < seq:
                lost.append(s)
            else:
                break
        self._declare_lost(now, lost)

    def on_ack_block(self, now: float, ack) -> None:
        """Handle a protocol.AckBlock: every unacked packet inside ack.ranges is
        acknowledged; one RTT sample comes from the largest packet if it is
        newly acked (minus the receiver's ack delay)."""
        self.acks_received = getattr(self, "acks_received", 0) + 1
        recv_ts = dict(ack.timestamps)

        def covered(seq: int) -> bool:
            return any(first <= seq <= last for first, last in ack.ranges)

        newly = [s for s in self.unacked if s <= ack.largest and covered(s)]
        if not newly:
            return

        rtt = None
        if ack.largest in self.unacked:
            send_ts, _ = self.unacked[ack.largest]
            rtt = max(0.0, now - send_ts - ack.ack_delay_ms)
            self.rtts.append(rtt)
            if self.srtt is None:
                self.srtt, self.rttvar = rtt, rtt / 2
            else:
                self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
                self.srtt = 0.875 * self.srtt + 0.125 * rtt
            self.min_rtt = rtt if self.min_rtt is None else min(self.min_rtt, rtt)

        for seq in newly:
            send_ts, size = self.unacked.pop(seq)
            self.inflight_bytes -= size
            self.acked += 1
            if self.records is not None:
                self.records.append([seq, send_ts, recv_ts.get(seq), now])
            if self.srtt is not None:
                self.cc.on_ack(now, seq, size, rtt if rtt is not None else self.srtt, self.srtt, self.min_rtt)

        # Lost: inside the span the ACK describes, not covered, and 3+ below largest.
        lost = []
        for seq in self.unacked:
            if seq + REORDER_THRESHOLD >= ack.largest:
                break
            if seq >= ack.low:
                lost.append(seq)
        self._declare_lost(now, lost)

    def check_timeouts(self, now: float) -> None:
        if self.srtt is None:
            threshold = 1000.0
        else:
            threshold = max(2 * self.srtt, self.srtt + 4 * self.rttvar, 25.0)
        lost = []
        for s, (ts, _) in self.unacked.items():
            if now - ts > threshold:
                lost.append(s)
            else:
                break
        self._declare_lost(now, lost)

    def _declare_lost(self, now: float, seqs: List[int]) -> None:
        if not seqs:
            return
        for s in seqs:
            _, size = self.unacked.pop(s)
            self.inflight_bytes -= size
        self.lost += len(seqs)
        self.cc.on_loss(now, seqs, self.srtt)

    def can_send(self, size: int) -> bool:
        cwnd = self.cc.cwnd_bytes()
        return cwnd is None or self.inflight_bytes + size <= cwnd

    def summary(self) -> Dict[str, Any]:
        return {
            "sent": self.sent,
            "acked": self.acked,
            "lost": self.lost,
            "late_acks": self.late_acks,
            "bytes_sent": self.bytes_sent,
            "rtt_ms": percentiles(self.rtts),
            "min_rtt_ms": self.min_rtt,
            "srtt_ms": self.srtt,
            "acks_received": getattr(self, "acks_received", None),
        }


class ReceiverStats:
    """Counts arrivals, loss (by sequence gaps), reordering, jitter and goodput."""

    def __init__(self, keep_records: bool = True) -> None:
        self.received = 0
        self.bytes = 0
        self.max_seq = -1
        self.reordered = 0
        self.duplicates = 0
        self.seen = set()
        self.jitter = 0.0
        self._last_transit: Optional[float] = None
        self.first_recv: Optional[float] = None
        self.bins: Dict[int, int] = {}
        self.records: Optional[List[list]] = [] if keep_records else None

    def on_data(self, seq: int, send_ts: float, recv_ts: float, size: int, pn: Optional[int] = None) -> None:
        if seq in self.seen:
            self.duplicates += 1
            return
        self.seen.add(seq)
        self.received += 1
        self.bytes += size
        if seq < self.max_seq:
            self.reordered += 1
        self.max_seq = max(self.max_seq, seq)

        # RFC 3550 interarrival jitter; clock offset cancels out.
        transit = recv_ts - send_ts
        if self._last_transit is not None:
            self.jitter += (abs(transit - self._last_transit) - self.jitter) / 16
        self._last_transit = transit

        if self.first_recv is None:
            self.first_recv = recv_ts
        b = int((recv_ts - self.first_recv) // BIN_MS)
        self.bins[b] = self.bins.get(b, 0) + size
        if self.records is not None:
            self.records.append([seq, send_ts, recv_ts, size, pn])

    def summary(self) -> Dict[str, Any]:
        expected = self.max_seq + 1
        n_bins = max(self.bins) + 1 if self.bins else 0
        return {
            "received": self.received,
            "expected": expected,
            "loss_rate": (1 - self.received / expected) if expected else None,
            "bytes": self.bytes,
            "reordered": self.reordered,
            "duplicates": self.duplicates,
            "jitter_ms": self.jitter,
            "goodput_bps_bins": [self.bins.get(i, 0) * 8 * 1000 / BIN_MS for i in range(n_bins)],
            "bin_ms": BIN_MS,
        }


def percentiles(xs: List[float]) -> Optional[Dict[str, float]]:
    if not xs:
        return None
    s = sorted(xs)

    def p(q: float) -> float:
        return s[min(len(s) - 1, int(q * len(s)))]

    return {"min": s[0], "p50": p(0.5), "p95": p(0.95), "max": s[-1], "mean": sum(s) / len(s)}
