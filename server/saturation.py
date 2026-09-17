"""Turn a flood of browser -> server datagrams into "the browser's CC rate".

The experiment is blunt: the page writes datagrams as fast as the transport
accepts them, and this module reads what actually arrived. The subtlety is that
"bytes per second at the server" on its own does not say *what* limited the
sender, and there are four different answers:

1. **The browser's congestion control.** QUIC DATAGRAM frames are congestion
   controlled but never retransmitted, so once the cwnd is full the browser
   drops them *in its own queue*, before they touch the network. Those show up
   as application sequence numbers that never arrived while the QUIC packet
   numbers around them are all present - `dropped_before_send_estimate` in
   up_analysis. This is the signal we actually want: it proves the ceiling was
   the browser, not the path.
2. **The path.** Packets left the browser and died in the network: QUIC packet
   numbers are missing too. The browser's CC will then converge to roughly the
   path's capacity, so the rate is still "the CC's rate", but it is telling you
   about the link, not about Chrome.
3. **The page.** If the JavaScript send loop cannot fill the transport, the
   measurement is of your own loop. `offered_bps` from the client, next to
   `delivered_bps` here, catches that: if nothing was dropped locally and
   offered == delivered, the sender was never the bottleneck and the number is
   a floor, not a ceiling.
4. **The server.** A slow receiver looks exactly like a slow path. The IAT and
   `local_queue_ms_lb` fields in up_analysis are the cross-check.

So the report always pairs the rate with the reason, and `verdict` says which of
the four the run actually measured.

Rate estimation skips the ramp: a QUIC connection starts in slow start and
doubles roughly every RTT, so the first second or two is not the steady state.
`steady_from_ms` cuts the ramp off, and the knee - where delivered rate stops
following the ramp - is reported separately for the stepped mode.
"""

from typing import Any, Dict, List, Optional

# _percentiles here is the trains.py one: min/p25/p50/p75/p95/max/mean.
from trains import _percentiles

# Ignore this much of the run when estimating the steady-state rate: slow start
# needs a few RTTs, and on a long-RTT path more than a few.
DEFAULT_RAMP_MS = 2000.0
# A bin holding less than this fraction of the median is a gap (the sender
# stalled, the tab was throttled), not a sample of the CC's rate.
MIN_BIN_FRACTION = 0.05


def _rate_stats(bins: List[Dict[str, Any]], key: str = "recv_bps") -> Optional[Dict[str, float]]:
    return _percentiles([b[key] for b in bins if b.get(key) is not None])


def summarize_saturation(
    up_analysis: Optional[Dict[str, Any]],
    client_offered: Optional[Dict[str, Any]] = None,
    ramp_ms: float = DEFAULT_RAMP_MS,
) -> Optional[Dict[str, Any]]:
    """Steady-state rate plus the evidence for what limited it."""
    if not up_analysis or not up_analysis.get("timeline"):
        return None
    timeline = up_analysis["timeline"]
    bin_ms = up_analysis.get("bin_ms", 100)

    all_rates = _rate_stats(timeline)
    if not all_rates:
        return None

    # Steady state: after the ramp, ignoring near-empty bins.
    floor = all_rates["p50"] * MIN_BIN_FRACTION
    steady_bins = [b for b in timeline if b["t"] >= ramp_ms and b["recv_bps"] > floor]
    if len(steady_bins) < 3:  # short run: fall back to everything that moved
        steady_bins = [b for b in timeline if b["recv_bps"] > floor]
    steady = _rate_stats(steady_bins)

    # Time to reach 90% of the steady-state rate - the slow-start ramp.
    ramp_to_90 = None
    if steady:
        target = 0.9 * steady["p50"]
        for b in timeline:
            if b["recv_bps"] >= target:
                ramp_to_90 = b["t"]
                break

    # cwnd implied by the steady rate and the RTT the server measured, i.e. how
    # many bytes the browser kept in flight: rate * RTT.
    srtts = [b["quic_srtt_ms"] for b in steady_bins if b.get("quic_srtt_ms")]
    srtt_p50 = _percentiles(srtts)["p50"] if srtts else None
    cwnd_bytes = (steady["p50"] / 8) * (srtt_p50 / 1000) if steady and srtt_p50 else None

    loss = up_analysis.get("loss_split") or {}
    local_dropped = loss.get("dropped_before_send_estimate")
    network_missing = loss.get("quic_packets_missing")
    app_missing = loss.get("app_packets_missing")
    delivered = up_analysis.get("packets", 0)
    offered_packets = (client_offered or {}).get("offered_packets")
    offered_bps = (client_offered or {}).get("offered_bps")

    flagged = up_analysis.get("bins_flagged_quic_limited", 0)
    out: Dict[str, Any] = {
        "bin_ms": bin_ms,
        "bins": len(timeline),
        "steady_bins": len(steady_bins),
        "ramp_ms": ramp_ms,
        "rate_bps": all_rates,
        "steady_rate_bps": steady,
        "peak_bin_bps": all_rates["max"],
        "ramp_to_90pct_ms": ramp_to_90,
        "quic_srtt_p50_ms": srtt_p50,
        "implied_cwnd_bytes": cwnd_bytes,
        "delivered_packets": delivered,
        "offered_packets": offered_packets,
        "offered_bps": offered_bps,
        "dropped_in_browser": local_dropped,
        "quic_packets_missing": network_missing,
        "app_packets_missing": app_missing,
        "bins_flagged_quic_limited": flagged,
    }
    out["verdict"] = _verdict(out)
    return out


def _verdict(s: Dict[str, Any]) -> Dict[str, Any]:
    """Which of the four bottlenecks this run actually measured."""
    steady = (s.get("steady_rate_bps") or {}).get("p50")
    local = s.get("dropped_in_browser") or 0
    net = s.get("quic_packets_missing") or 0
    delivered = s.get("delivered_packets") or 0
    offered = s.get("offered_packets")

    local_frac = local / offered if offered else None
    net_frac = net / (delivered + net) if (delivered + net) else None

    if offered and delivered and local < 0.001 * offered and net < 0.001 * delivered:
        who = "sender"
        why = ("Nothing was dropped in the browser and nothing was lost in the network, so the page "
               "never managed to overload anything. This rate is a floor on the browser's CC, not its "
               "ceiling - raise the packet size or the pending-write window.")
    elif local > net:
        who = "browser-cc"
        why = ("Packets went missing inside the browser (their QUIC packet numbers were never used) "
               "while the network delivered what was sent. The browser's congestion control refused "
               "the excess: this rate is its ceiling on this path.")
    elif net > 0:
        who = "path"
        why = ("Packets were lost in the network, so the browser's CC backed off to fit the path. "
               "The rate is what the path allowed; on a faster path the browser would go higher.")
    else:
        who = "unclear"
        why = "Neither local drops nor network loss dominated; treat the rate as indicative only."

    return {
        "limited_by": who,
        "explanation": why,
        "steady_rate_bps": steady,
        "local_drop_fraction": local_frac,
        "network_loss_fraction": net_frac,
    }


def window_report(records: List[list], since_ms: float, now: float) -> Dict[str, Any]:
    """Live progress: what arrived in the last window, for the page's chart.

    `records` is ReceiverStats.records ([seq, send_ts, recv_ts, size, pn]) in
    arrival order, so the window is a suffix scan from the end.
    """
    packets = 0
    data_bytes = 0
    for r in reversed(records):
        if r[2] < since_ms:
            break
        packets += 1
        data_bytes += r[3]
    span_ms = max(1.0, now - since_ms)
    return {
        "packets": packets,
        "bytes": data_bytes,
        "bps": data_bytes * 8 * 1000 / span_ms,
        "window_ms": span_ms,
    }
