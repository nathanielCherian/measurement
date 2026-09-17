"""QUIC-level congestion controllers, registered with aioquic by name.

Built into aioquic: "reno", "cubic". Added here: "null".
To add one: subclass aioquic.quic.congestion.base.QuicCongestionControl,
call register_congestion_control("<name>", cls), and import the module below.
"""

import aioquic.quic.congestion.cubic  # noqa: F401  (registers "cubic")
import aioquic.quic.congestion.reno  # noqa: F401  (registers "reno")

from . import null_cc  # noqa: F401

NAMES = ["null", "reno", "cubic"]
