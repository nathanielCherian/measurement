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


# Above this the server's event loop is behind, and above the CPU fraction it is
# simply out of cycles: either way the rate is the receiver's, not the browser's.
SERVER_LAG_MS = 20.0
SERVER_CPU_BUSY = 0.85


def summarize_saturation(
    up_analysis: Optional[Dict[str, Any]],
    client_offered: Optional[Dict[str, Any]] = None,
    ramp_ms: float = DEFAULT_RAMP_MS,
    server_load: Optional[Dict[str, Any]] = None,
    app_loss: Optional[Dict[str, Any]] = None,
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

    # The median of the bins that carried something is the rate *while sending*,
    # which is not the rate achieved if delivery came in bursts separated by
    # stalls (a data channel backing up behind its own send queue does exactly
    # that). The mean over every bin, empty ones included, is what actually got
    # through, so report both and say when they disagree.
    mean_rate = sum(b["recv_bps"] for b in timeline) / len(timeline) if timeline else None
    bursty = bool(steady and mean_rate and mean_rate < 0.6 * steady["p50"])

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

    # QUIC packet numbers are what let us say *where* a packet died. Over an
    # SCTP data channel there is no equivalent, so the loss split is absent and
    # the verdict must not pretend otherwise.
    loss = up_analysis.get("loss_split") or {}
    split_available = bool(up_analysis.get("loss_split"))
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
        "mean_rate_bps": mean_rate,
        "bursty": bursty,
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
        "loss_split_available": split_available,
    }
    if not split_available and app_loss:
        # All we have is application-level loss: sequence numbers that never
        # arrived, with no way to tell a local drop from a network one.
        out["app_loss_rate"] = app_loss.get("loss_rate")
        out["app_received"] = app_loss.get("received")
        out["app_expected"] = app_loss.get("expected")
    if server_load:
        lag = _percentiles(server_load.get("loop_lag_ms") or [])
        cpu = _percentiles(server_load.get("cpu_fraction") or [])
        out["server_load"] = {
            "loop_lag_ms": lag,
            "cpu_fraction": cpu,
            # aioquic decrypts every packet in Python, so a busy loop here is the
            # most likely explanation for a rate that looks like a browser limit.
            "server_busy": bool((lag and lag["p95"] > SERVER_LAG_MS) or (cpu and cpu["p95"] > SERVER_CPU_BUSY)),
        }
    out["verdict"] = _verdict(out)
    if bursty:
        out["verdict"]["explanation"] += (
            f" Delivery was bursty: while it was arriving the rate was "
            f"{steady['p50'] / 1e6:.1f} Mbps, but averaged over the run only "
            f"{mean_rate / 1e6:.1f} Mbps got through - the sender spent much of the run stalled, so "
            f"read the mean as the throughput and the median as the burst rate."
        )
    return out


def _verdict(s: Dict[str, Any]) -> Dict[str, Any]:
    """Which of the four bottlenecks this run actually measured."""
    steady = (s.get("steady_rate_bps") or {}).get("p50")
    local = s.get("dropped_in_browser") or 0
    net = s.get("quic_packets_missing") or 0
    delivered = s.get("delivered_packets") or 0
    offered = s.get("offered_packets")

    # Without a packet-number split these fractions have no meaning: "0% dropped
    # in the browser" would be an assertion we cannot make, not a measurement.
    split = s.get("loss_split_available")
    local_frac = local / offered if (split and offered) else None
    net_frac = net / (delivered + net) if (split and (delivered + net)) else None

    load = s.get("server_load") or {}
    if load.get("server_busy"):
        lag = (load.get("loop_lag_ms") or {}).get("p95")
        cpu = (load.get("cpu_fraction") or {}).get("p95")
        # Name only the thing that actually tripped, so the sentence cannot end
        # up citing a 0 ms lag as evidence.
        reasons = []
        if lag is not None and lag > SERVER_LAG_MS:
            reasons.append(f"its event loop fell behind by up to {lag:.0f} ms")
        if cpu is not None and cpu > SERVER_CPU_BUSY:
            reasons.append(f"it spent {100 * cpu:.0f}% of the run pinned on CPU")
        return {
            "limited_by": "server",
            "explanation": (
                f"This server could not keep up: {' and '.join(reasons)}. Every packet is processed in "
                "Python here - QUIC decryption for WebTransport, the SCTP stack for data channels - which "
                "costs far more per packet than the kernel's TCP path, so a rate measured against it is the "
                "*server's* ceiling, not the browser's. Compare against a server that is not the bottleneck "
                "before concluding anything about the browser."
            ),
            "steady_rate_bps": steady,
            "local_drop_fraction": local_frac,
            "network_loss_fraction": net_frac,
        }

    if not s.get("loss_split_available"):
        rate = s.get("app_loss_rate")
        lost_note = (f"{100 * rate:.2f}% of the packets sent never arrived"
                     if rate else "nothing was lost end to end")
        if rate and rate > 0.001:
            who, why = "unclear", (
                f"This transport gives the receiver no packet-number equivalent, so a missing "
                f"sequence number cannot be attributed: {lost_note}, but whether the browser's SCTP "
                f"stack dropped it or the network did is not observable from here. The rate is what "
                f"got through; to place the limit, watch the page's bufferedAmount stalls (its own "
                f"send queue filling) or re-run over WebTransport, where QUIC packet numbers make "
                f"the split possible."
            )
        else:
            who, why = "sender", (
                f"Nothing was lost ({lost_note}), so nothing was overloaded and this rate is a floor "
                f"on what the transport would carry, not a ceiling. Raise the packet size or the "
                f"send window, or use WebTransport, where the loss split can say where the limit is."
            )
        return {
            "limited_by": who, "explanation": why, "steady_rate_bps": steady,
            "local_drop_fraction": None, "network_loss_fraction": None,
        }

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
