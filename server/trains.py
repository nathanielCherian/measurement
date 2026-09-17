"""Packet-train experiment (mirrored in web/trains.js).

A train is `train_len` packets handed to the transport back to back, repeated
every `gap_ms`. The receiver records arrival times and reports, per train:

* **dispersion** - last arrival minus first arrival
* **IAT** - inter-arrival times inside the train
* **implied rate** - (train_len - 1) * size * 8 / dispersion, the classic
  packet-train capacity estimate: the bottleneck serialises the burst, so the
  spacing on arrival reflects its rate
* **send-side spread** - the same span measured from the sender's own
  timestamps. The sender stamps each packet as it hands it to the transport, so
  a large send-side spread means the browser's pacer (or SCTP) spread the burst
  before it ever reached the wire, and the arrival spacing says nothing about
  the network.

Trains measure the path (or the local pacer) rather than throughput, so they
stay well below capacity: a 16-packet train of 1000 B every 200 ms is 0.64 Mbps.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

import protocol as proto


def _percentiles(xs: List[float]) -> Optional[Dict[str, float]]:
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


class TrainReceiver:
    """Collects arrivals per train and summarises the spacing."""

    def __init__(self) -> None:
        self.trains: Dict[int, List[list]] = {}  # train_id -> [[index, send_ts, recv_ts, size]]
        self.expected_len: Dict[int, int] = {}
        self.packets = 0

    def on_packet(self, pkt: proto.Train, recv_ts: float) -> None:
        self.packets += 1
        self.expected_len[pkt.train_id] = pkt.train_len
        self.trains.setdefault(pkt.train_id, []).append([pkt.index, pkt.send_ts, recv_ts, pkt.size])

    def summary(self, keep_records: bool = True) -> Optional[Dict[str, Any]]:
        if not self.trains:
            return None
        per_train = []
        for train_id in sorted(self.trains):
            rows = sorted(self.trains[train_id], key=lambda r: r[2])  # arrival order
            expected = self.expected_len[train_id]
            recv = [r[2] for r in rows]
            sent = [r[1] for r in rows]
            size = rows[0][3]
            iat = [recv[i] - recv[i - 1] for i in range(1, len(recv))]
            dispersion = recv[-1] - recv[0] if len(recv) > 1 else None
            send_spread = max(sent) - min(sent) if len(sent) > 1 else None
            implied = ((len(recv) - 1) * size * 8 * 1000 / dispersion) if dispersion else None
            per_train.append(
                {
                    "train_id": train_id,
                    "expected": expected,
                    "received": len(rows),
                    "reordered": sum(1 for i in range(1, len(rows)) if rows[i][0] < rows[i - 1][0]),
                    "dispersion_ms": dispersion,
                    "send_spread_ms": send_spread,
                    "iat_ms": iat if keep_records else None,
                    "iat_p50_ms": _percentiles(iat)["p50"] if iat else None,
                    "implied_rate_bps": implied,
                    "first_recv_ts": recv[0],
                }
            )

        complete = [t for t in per_train if t["received"] == t["expected"] and t["dispersion_ms"]]
        return {
            "trains": len(per_train),
            "complete_trains": len(complete),
            "packets": self.packets,
            "lost_packets": sum(t["expected"] - t["received"] for t in per_train),
            "reordered_packets": sum(t["reordered"] for t in per_train),
            "dispersion_ms": _percentiles([t["dispersion_ms"] for t in complete]),
            "send_spread_ms": _percentiles([t["send_spread_ms"] for t in per_train if t["send_spread_ms"] is not None]),
            "iat_ms": _percentiles([x for t in per_train for x in (t["iat_ms"] or [])]),
            "implied_rate_bps": _percentiles([t["implied_rate_bps"] for t in complete]),
            "per_train": per_train,
        }


async def send_trains(
    send: Callable[[bytes], None],
    flow: int,
    now_ms: Callable[[], float],
    train_len: int,
    trains: int,
    gap_ms: float,
    size: int,
    is_open: Callable[[], bool] = lambda: True,
) -> Dict[str, Any]:
    """Send `trains` bursts of `train_len` packets, `gap_ms` apart."""
    sent = 0
    spreads = []
    for train_id in range(trains):
        if not is_open():
            break
        first = now_ms()
        for index in range(train_len):
            send(proto.encode_train(flow, train_id, index, train_len, now_ms(), size))
            sent += 1
        spreads.append(now_ms() - first)
        await asyncio.sleep(gap_ms / 1000)
    return {"sent": sent, "trains": len(spreads), "send_spread_ms": _percentiles(spreads)}
