"""Server-side analysis of browser -> server probe traffic.

Answers "is the browser's QUIC congestion control limiting the sender?" from
what the server can observe, in any browser:

* Inter-arrival times (IAT) of received probe datagrams, and whether packets the
  browser sent in one burst (same send_ts) arrive re-spaced (a sign of the
  browser's QUIC pacer, or of a bottleneck link).
* Forward-delay excess: (recv_ts - send_ts) above its minimum. Includes network
  queueing on the forward path AND time spent in the browser's local send queue.
* Server QUIC RTT excess: latest RTT above min RTT. The browser's QUIC ACKs are
  not congestion controlled, so this sees network queueing but not the
  browser's local datagram queue.
  => local_queue_ms_lb = forward excess - QUIC RTT excess (a lower bound).
* Loss split: application sequence numbers that never arrived, versus QUIC packet
  numbers that never arrived. Missing sequence numbers beyond missing packet
  numbers were dropped inside the browser before being sent.
"""

import bisect
from typing import Any, Dict, List, Optional, Sequence, Tuple

BIN_MS = 100
# Arrivals closer than this are "back to back" (no pacing between them).
BURST_GAP_MS = 0.2
# A bin is flagged when the local queue lower bound exceeds this.
LOCAL_QUEUE_FLAG_MS = 2.0


def _percentiles(xs: Sequence[float]) -> Optional[Dict[str, float]]:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    mean = sum(s) / n
    var = sum((x - mean) ** 2 for x in s) / n
    return {
        "p10": s[int(0.1 * (n - 1))],
        "p50": s[int(0.5 * (n - 1))],
        "p90": s[int(0.9 * (n - 1))],
        "p99": s[int(0.99 * (n - 1))],
        "mean": mean,
        "cv": (var ** 0.5) / mean if mean > 0 else None,
    }


def analyze_up(
    records: List[list],
    rx_packets: Optional[List[Tuple[int, float]]],
    quic_samples: List[Dict[str, Any]],
    start_ms: float,
) -> Optional[Dict[str, Any]]:
    """
    records:      [seq, send_ts_ms (browser clock), recv_ts_ms (server clock), size, pn]
                  in arrival order
    rx_packets:   [(packet_number, arrival_ms)] for all 1-RTT packets from the browser
    quic_samples: [{"t_ms": server clock, "latest_rtt_ms", "min_rtt_ms", "srtt_ms"}]
    start_ms:     session start on the server clock (bin 0)
    """
    if len(records) < 2:
        return None

    recv = [r[2] for r in records]
    sent = [r[1] for r in records]
    fwd = [r[2] - r[1] for r in records]
    fwd_min = min(fwd)

    # ---- inter-arrival times --------------------------------------------------
    iat = [recv[i] - recv[i - 1] for i in range(1, len(recv))]
    send_gap = [sent[i] - sent[i - 1] for i in range(1, len(sent))]
    same_tick = [i for i, g in enumerate(send_gap) if g == 0]
    same_tick_spread = [iat[i] for i in same_tick]
    respaced = sum(1 for x in same_tick_spread if x > BURST_GAP_MS)

    # ---- QUIC packet-number loss -----------------------------------------------
    pn_summary = None
    if rx_packets:
        pns = sorted({pn for pn, _ in rx_packets})
        pn_expected = pns[-1] - pns[0] + 1
        pn_missing = pn_expected - len(pns)
        seqs = sorted({r[0] for r in records})
        app_expected = seqs[-1] + 1
        app_missing = app_expected - len(seqs)
        pn_summary = {
            "quic_packets_received": len(pns),
            "quic_packets_missing": pn_missing,
            "app_packets_missing": app_missing,
            # Upper bound on network-lost probe packets is quic_packets_missing
            # (some missing QUIC packets carried only ACKs or several datagrams).
            "dropped_before_send_estimate": max(0, app_missing - pn_missing),
        }

    # ---- per-bin timeline -------------------------------------------------------
    n_bins = int((recv[-1] - start_ms) // BIN_MS) + 1
    bins: List[Dict[str, Any]] = [
        {"t": i * BIN_MS, "n": 0, "bytes": 0, "iat": [], "fwd": [], "seq_min": None, "seq_max": None}
        for i in range(max(n_bins, 1))
    ]
    for i, r in enumerate(records):
        b = int((r[2] - start_ms) // BIN_MS)
        if b < 0:
            continue
        bn = bins[b]
        bn["n"] += 1
        bn["bytes"] += r[3]
        bn["fwd"].append(fwd[i] - fwd_min)
        if i > 0:
            bn["iat"].append(iat[i - 1])
        bn["seq_min"] = r[0] if bn["seq_min"] is None else min(bn["seq_min"], r[0])
        bn["seq_max"] = r[0] if bn["seq_max"] is None else max(bn["seq_max"], r[0])

    pn_times = sorted((t, pn) for pn, t in (rx_packets or []))
    pn_time_keys = [t for t, _ in pn_times]
    q_times = [s["t_ms"] for s in quic_samples]

    timeline = []
    flagged = 0
    for idx, bn in enumerate(bins):
        t0 = start_ms + bn["t"]
        t1 = t0 + BIN_MS
        # QUIC RTT sample closest to the middle of the bin
        q = None
        if quic_samples:
            j = bisect.bisect_left(q_times, t0 + BIN_MS / 2)
            j = min(max(j, 0), len(quic_samples) - 1)
            q = quic_samples[j]
        rtt_excess = None
        if q and q.get("latest_rtt_ms") is not None and q.get("min_rtt_ms") is not None:
            rtt_excess = max(0.0, q["latest_rtt_ms"] - q["min_rtt_ms"])

        fwd_excess = min(bn["fwd"]) if bn["fwd"] else None  # queue floor within the bin
        local_lb = None
        if fwd_excess is not None and rtt_excess is not None:
            local_lb = max(0.0, fwd_excess - rtt_excess)

        # packet numbers seen in this bin, for per-bin network loss
        lo = bisect.bisect_left(pn_time_keys, t0)
        hi = bisect.bisect_left(pn_time_keys, t1)
        bin_pns = [pn for _, pn in pn_times[lo:hi]]
        pn_gap = (max(bin_pns) - min(bin_pns) + 1 - len(set(bin_pns))) if bin_pns else None
        app_gap = (bn["seq_max"] - bn["seq_min"] + 1 - bn["n"]) if bn["n"] else None

        flag = local_lb is not None and local_lb > LOCAL_QUEUE_FLAG_MS
        flagged += flag
        timeline.append(
            {
                "t": bn["t"],
                "recv_bps": bn["bytes"] * 8 * 1000 / BIN_MS,
                "packets": bn["n"],
                "iat_p50_ms": _percentiles(bn["iat"])["p50"] if bn["iat"] else None,
                "iat_p90_ms": _percentiles(bn["iat"])["p90"] if bn["iat"] else None,
                "fwd_excess_ms": fwd_excess,
                "quic_rtt_excess_ms": rtt_excess,
                "quic_srtt_ms": q.get("srtt_ms") if q else None,
                "local_queue_ms_lb": local_lb,
                "quic_pn_gap": pn_gap,
                "app_seq_gap": app_gap,
                "quic_limited": flag,
            }
        )

    return {
        "packets": len(records),
        "iat_ms": _percentiles(iat),
        "send_gap_ms": _percentiles(send_gap),
        "same_tick_pairs": len(same_tick),
        "same_tick_respaced_fraction": (respaced / len(same_tick)) if same_tick else None,
        "back_to_back_fraction": sum(1 for x in iat if x <= BURST_GAP_MS) / len(iat),
        "fwd_excess_ms": _percentiles([f - fwd_min for f in fwd]),
        "loss_split": pn_summary,
        "bins_flagged_quic_limited": flagged,
        "bins": len(timeline),
        "bin_ms": BIN_MS,
        "timeline": timeline,
    }
