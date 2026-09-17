"""Receiver-side acknowledgement generation (mirrored in web/ack.js).

mode "packet": one ACK datagram per received DATA packet (echoes send_ts).
mode "block":  ACK_BLOCK datagrams, sent when `every_n` packets are pending or
               `interval_ms` after the first pending packet, whichever is first.
mode "none":   no ACKs at all. For saturation tests, where the sender is trying
               to fill its uplink and ACK traffic would only add work at both
               ends; the server sees every packet anyway.
"""

import asyncio
from typing import Callable, Dict, List, Optional, Set, Tuple

import protocol as proto

RANGE_WINDOW = 4096  # sequence numbers below largest considered for ranges


class AckGenerator:
    def __init__(
        self,
        flow: int,
        config: Optional[Dict],
        send: Callable[[bytes], None],
        now_ms: Callable[[], float],
    ) -> None:
        config = config or {}
        self.flow = flow
        self.mode = config.get("mode", "packet")
        self.interval_ms = float(config.get("interval_ms", 20))
        self.every_n = int(config.get("every_n", 16))
        self.send = send
        self.now_ms = now_ms

        self.received: Set[int] = set()
        self.largest = -1
        self.largest_recv_ts = 0.0
        self.pending: List[Tuple[int, float]] = []
        self.ack_id = 0
        self.acks_sent = 0
        self._timer: Optional[asyncio.TimerHandle] = None

    def on_packet(self, seq: int, send_ts: float, recv_ts: float) -> None:
        if self.mode == "none":
            return
        if self.mode == "packet":
            self.send(proto.encode_ack(self.flow, seq, send_ts, recv_ts))
            self.acks_sent += 1
            return

        self.received.add(seq)
        if seq > self.largest:
            self.largest, self.largest_recv_ts = seq, recv_ts
        self.pending.append((seq, recv_ts))
        if len(self.pending) >= self.every_n:
            self.flush()
        elif self._timer is None:
            self._timer = asyncio.get_event_loop().call_later(self.interval_ms / 1000, self.flush)

    def _ranges(self) -> Tuple[List[Tuple[int, int]], int]:
        floor = max(0, self.largest - RANGE_WINDOW + 1)
        ranges: List[Tuple[int, int]] = []
        seq = self.largest
        while seq >= floor and len(ranges) < proto.MAX_ACK_RANGES:
            last = seq
            while seq >= floor and seq in self.received:
                seq -= 1
            ranges.append((seq + 1, last))
            while seq >= floor and seq not in self.received:
                seq -= 1
        # Stopped early: only [first of last range, largest] is described.
        low = ranges[-1][0] if seq >= floor else floor
        # Forget sequence numbers that can no longer appear in a range.
        if len(self.received) > 2 * RANGE_WINDOW:
            self.received = {s for s in self.received if s >= floor}
        return ranges, low

    def flush(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self.mode != "block" or self.largest < 0:
            return
        ranges, low = self._ranges()
        capacity = proto.block_ts_capacity(len(ranges))
        pending, self.pending = self.pending, []
        delay = max(0.0, self.now_ms() - self.largest_recv_ts)
        chunks = [pending[i : i + capacity] for i in range(0, len(pending), capacity)] or [[]]
        for chunk in chunks:
            self.send(
                proto.encode_ack_block(
                    self.flow, self.ack_id, self.largest, self.largest_recv_ts, delay, low, ranges, chunk
                )
            )
            self.ack_id += 1
            self.acks_sent += 1

    def close(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
