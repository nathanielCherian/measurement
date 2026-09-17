"""Application-level congestion controllers (mirrored in web/cc/).

Interface (all times in ms):
    on_packet_sent(now, seq, size)
    on_ack(now, seq, size, rtt, srtt, min_rtt)
    on_loss(now, seqs, srtt)
    pacing_rate_bps() -> float
    cwnd_bytes() -> Optional[float]    None means rate-only (no window)
    state() -> dict                    snapshot for logs

To add one: subclass CongestionController, then add it to REGISTRY.
"""

from typing import Any, Dict

from .aimd import Aimd
from .base import CongestionController
from .fixed import FixedRate

REGISTRY = {cls.name: cls for cls in (FixedRate, Aimd)}


def make_cc(name: str, params: Dict[str, Any]) -> CongestionController:
    try:
        cls = REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown app CC {name!r}; have {sorted(REGISTRY)}")
    return cls(params or {})
