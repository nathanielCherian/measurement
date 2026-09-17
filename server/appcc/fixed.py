from .base import CongestionController


class FixedRate(CongestionController):
    """Constant bitrate, ignores all feedback. Params: rate_mbps."""

    name = "fixed"

    def __init__(self, params):
        super().__init__(params)
        self.rate_bps = float(params.get("rate_mbps", 1.0)) * 1e6

    def pacing_rate_bps(self) -> float:
        return self.rate_bps
