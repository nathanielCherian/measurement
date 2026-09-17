"""Steady-stream RTT monitoring (mirrored in web/rtt.js).

One side sends PING packets at a fixed interval, each carrying the reading of
its own clock at hand-off. The other side turns each one around immediately as
a PONG, stamping its own arrival time, so a single exchange yields

    rtt      = pong arrival - ping send        (one clock, no sync needed)
    up leg   = echo_recv_ts - send_ts          (+ the clock offset)
    down leg = pong arrival - echo_recv_ts     (- the clock offset)

The offset between the two clocks is unknown but (over a run) constant, so the
legs mean nothing in absolute terms - only against their own minimum.
`up_excess = up - min(up)` is the delay the forward direction added beyond its
best case in this run, and likewise for the reverse. That split is the point of
the exercise: a rising `up_excess` with a flat `down_excess` says the queue is
on the browser's uplink (very often the browser's own send queue), which is
exactly the ambiguity a plain RTT number leaves open.

Two halves, both used on both ends:

* `RttMonitor` - the initiator: matches PONGs to PINGs, keeps srtt/min-RTT and
  per-sample records for plotting.
* `RttStreamReceiver` - the responder: parses the timestamps *inside* the
  stream it receives. It never needs the reply, so it works even when the
  return path is broken: arrival spacing vs send spacing, one-way delay excess,
  RFC 3550 jitter, and loss/reordering from the sequence numbers.

The stream is deliberately thin (a 64-byte packet every 50 ms is 10 kbps) so it
measures the path's standing delay rather than creating queueing of its own.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

import protocol as proto
from trains import _percentiles

# RFC 6298 smoothing, same constants as transport_stats.SenderCore.
ALPHA = 1 / 8
BETA = 1 / 4
# RFC 3550 interarrival jitter gain.
JITTER_GAIN = 1 / 16
BIN_MS = 100.0


def _bin_timeline(rows: List[Dict[str, Any]], start: float, keys: List[str]) -> List[Dict[str, Any]]:
    """Group per-packet rows into BIN_MS bins, median per key (for plots)."""
    bins: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        bins.setdefault(int((r["t"] - start) // BIN_MS), []).append(r)
    out = []
    for idx in sorted(bins):
        group = bins[idx]
        row: Dict[str, Any] = {"t": idx * BIN_MS, "packets": len(group)}
        for key in keys:
            vals = [g[key] for g in group if g.get(key) is not None]
            row[key] = _percentiles(vals)["p50"] if vals else None
        out.append(row)
    return out


class RttMonitor:
    """Initiator side: sends PINGs, matches the PONGs that come back."""

    def __init__(self, keep_records: bool = True) -> None:
        self.sent = 0
        self.replies = 0
        self.duplicates = 0
        self.reordered = 0
        self.pending: Dict[int, float] = {}  # seq -> our send time
        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        self.min_rtt: Optional[float] = None
        self.min_up: Optional[float] = None
        self.min_down: Optional[float] = None
        self.max_seq = -1
        self.records: Optional[List[Dict[str, Any]]] = [] if keep_records else None
        self._rtts: List[float] = []
        self._ups: List[float] = []
        self._downs: List[float] = []
        self._ipdv: List[float] = []
        self._prev_rtt: Optional[float] = None
        self.first_send: Optional[float] = None

    def on_send(self, seq: int, t: float) -> None:
        self.sent += 1
        self.pending[seq] = t
        if self.first_send is None:
            self.first_send = t

    def on_pong(self, pkt: proto.Pong, t: float) -> Optional[Dict[str, Any]]:
        if self.pending.pop(pkt.seq, None) is None and pkt.seq <= self.max_seq:
            self.duplicates += 1  # already accounted for (or never ours)
            return None
        # RTT comes from the echoed timestamp, so it survives a lost record.
        rtt = t - pkt.send_ts
        self.replies += 1
        if pkt.seq < self.max_seq:
            self.reordered += 1
        self.max_seq = max(self.max_seq, pkt.seq)

        if self.srtt is None:
            self.srtt, self.rttvar = rtt, rtt / 2
        else:
            self.rttvar = (1 - BETA) * self.rttvar + BETA * abs(self.srtt - rtt)
            self.srtt = (1 - ALPHA) * self.srtt + ALPHA * rtt
        self.min_rtt = rtt if self.min_rtt is None else min(self.min_rtt, rtt)
        if self._prev_rtt is not None:
            self._ipdv.append(rtt - self._prev_rtt)
        self._prev_rtt = rtt

        up = pkt.echo_recv_ts - pkt.send_ts  # + clock offset
        down = t - pkt.echo_recv_ts  # - clock offset
        self.min_up = up if self.min_up is None else min(self.min_up, up)
        self.min_down = down if self.min_down is None else min(self.min_down, down)
        self._rtts.append(rtt)
        self._ups.append(up)
        self._downs.append(down)

        rec = {
            "seq": pkt.seq,
            "t": pkt.send_ts,
            "rtt_ms": rtt,
            "queue_ms": rtt - self.min_rtt,
            "up_ms": up,
            "down_ms": down,
            "srtt_ms": self.srtt,
        }
        if self.records is not None:
            self.records.append(rec)
        return rec

    def outstanding(self, t: float, timeout_ms: float) -> int:
        """PINGs with no reply yet, older than `timeout_ms` (i.e. lost)."""
        return sum(1 for ts in self.pending.values() if t - ts > timeout_ms)

    def summary(self, keep_records: bool = False) -> Optional[Dict[str, Any]]:
        if not self.replies:
            return None
        min_rtt = self.min_rtt or 0.0
        rows = self.records or []
        out: Dict[str, Any] = {
            "sent": self.sent,
            "replies": self.replies,
            "lost": max(0, self.sent - self.replies),
            "loss_pct": 100.0 * max(0, self.sent - self.replies) / self.sent if self.sent else None,
            "reordered": self.reordered,
            "duplicates": self.duplicates,
            "min_rtt_ms": self.min_rtt,
            "srtt_ms": self.srtt,
            "rttvar_ms": self.rttvar,
            "rtt_ms": _percentiles(self._rtts),
            # standing queue: how far above the best RTT of the run we sat
            "queue_ms": _percentiles([r - min_rtt for r in self._rtts]),
            # |consecutive RTT change|: short-term variation, offset-free
            "ipdv_abs_ms": _percentiles([abs(d) for d in self._ipdv]),
            # each leg relative to its own minimum (the clock offset cancels)
            "up_excess_ms": _percentiles([u - self.min_up for u in self._ups]) if self.min_up is not None else None,
            "down_excess_ms": _percentiles([d - self.min_down for d in self._downs]) if self.min_down is not None else None,
            "timeline": _bin_timeline(rows, self.first_send or 0.0, ["rtt_ms", "queue_ms", "srtt_ms"]),
        }
        if keep_records:
            out["records"] = rows
        return out


class RttStreamReceiver:
    """Responder side: what the timestamps *in* the stream say, reply aside."""

    def __init__(self, keep_records: bool = True) -> None:
        self.received = 0
        self.duplicates = 0
        self.reordered = 0
        self.first_seq: Optional[int] = None
        self.max_seq = -1
        self.jitter = 0.0  # RFC 3550 interarrival jitter, ms
        self.min_owd: Optional[float] = None
        self.records: Optional[List[Dict[str, Any]]] = [] if keep_records else None
        self._seen: set = set()
        self._prev: Optional[tuple] = None  # (send_ts, recv_ts) of the last arrival
        self._owds: List[float] = []
        self._iat: List[float] = []
        self._send_iat: List[float] = []
        self._ipdv: List[float] = []
        self.first_recv: Optional[float] = None

    def on_ping(self, pkt: proto.Ping, recv_ts: float) -> Dict[str, Any]:
        if pkt.seq in self._seen:
            self.duplicates += 1
        self._seen.add(pkt.seq)
        self.received += 1
        if self.first_seq is None:
            self.first_seq = pkt.seq
            self.first_recv = recv_ts
        if pkt.seq < self.max_seq:
            self.reordered += 1
        self.max_seq = max(self.max_seq, pkt.seq)

        owd = recv_ts - pkt.send_ts  # one-way delay + clock offset
        self.min_owd = owd if self.min_owd is None else min(self.min_owd, owd)
        iat = send_iat = d = None
        if self._prev is not None:
            send_iat = pkt.send_ts - self._prev[0]
            iat = recv_ts - self._prev[1]
            d = iat - send_iat  # RFC 3550 D(i-1, i): spacing change in flight
            self.jitter += (abs(d) - self.jitter) * JITTER_GAIN
            self._iat.append(iat)
            self._send_iat.append(send_iat)
            self._ipdv.append(d)
        self._prev = (pkt.send_ts, recv_ts)
        self._owds.append(owd)

        rec = {
            "seq": pkt.seq,
            "t": recv_ts,
            "send_ts": pkt.send_ts,
            "owd_ms": owd,
            "owd_excess_ms": owd - self.min_owd,
            "iat_ms": iat,
            "send_iat_ms": send_iat,
            "ipdv_ms": d,
            "jitter_ms": self.jitter,
        }
        if self.records is not None:
            self.records.append(rec)
        return rec

    def summary(self, keep_records: bool = False) -> Optional[Dict[str, Any]]:
        if not self.received:
            return None
        expected = self.max_seq - (self.first_seq or 0) + 1
        lost = max(0, expected - len(self._seen))
        min_owd = self.min_owd or 0.0
        out: Dict[str, Any] = {
            "received": self.received,
            "expected": expected,
            "lost": lost,
            "loss_pct": 100.0 * lost / expected if expected else None,
            "reordered": self.reordered,
            "duplicates": self.duplicates,
            "jitter_ms": self.jitter,
            # arrival spacing vs the spacing the sender stamped into the packets:
            # if send_iat is steady and iat is not, the delay happened in flight
            # (or in the sender's own queue, which is downstream of the stamp).
            "iat_ms": _percentiles(self._iat),
            "send_iat_ms": _percentiles(self._send_iat),
            "ipdv_abs_ms": _percentiles([abs(d) for d in self._ipdv]),
            # one-way delay above the minimum seen: the queue this direction built
            "owd_excess_ms": _percentiles([o - min_owd for o in self._owds]),
            "timeline": _bin_timeline(
                self.records or [], self.first_recv or 0.0, ["owd_excess_ms", "iat_ms", "jitter_ms"]
            ),
        }
        if keep_records:
            out["records"] = self.records
        return out


async def run_rtt_stream(
    send: Callable[[bytes], None],
    flow: int,
    now_ms: Callable[[], float],
    monitor: RttMonitor,
    interval_ms: float,
    duration_s: float,
    size: int,
    is_open: Callable[[], bool] = lambda: True,
) -> Dict[str, Any]:
    """Send PINGs on a fixed schedule until `duration_s` is up.

    The schedule is absolute (start + n * interval) rather than sleep-per-packet,
    so a late wake-up doesn't push the whole stream out of step.
    """
    start = now_ms()
    end = start + duration_s * 1000
    n = 0
    while is_open():
        t = now_ms()
        if t >= end:
            break
        monitor.on_send(n, t)
        send(proto.encode_ping(flow, n, t, size))
        n += 1
        await asyncio.sleep(max(0.0, (start + n * interval_ms - now_ms()) / 1000))
    return {"sent": n, "interval_ms": interval_ms, "size": size}
