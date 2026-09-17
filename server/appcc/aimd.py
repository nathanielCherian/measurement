from .base import CongestionController


class Aimd(CongestionController):
    """Rate-based AIMD with slow start and a delay signal. Keep in sync with
    web/cc/aimd.js.

    Every epoch (max(srtt, min_epoch_ms)) without congestion the rate grows:
    doubles in slow start, else + ai_mbps. A loss, or an RTT sample above
    min_rtt + max(delay_thresh_ms, delay_thresh_frac * min_rtt), cuts the rate
    by beta (at most once per epoch) and ends slow start.

    Params: start_mbps, min_mbps, max_mbps, ai_mbps, beta, min_epoch_ms,
            delay_thresh_ms, delay_thresh_frac
    """

    name = "aimd"

    def __init__(self, params):
        super().__init__(params)
        p = params
        self.rate_bps = float(p.get("start_mbps", 1.0)) * 1e6
        self.min_bps = float(p.get("min_mbps", 0.1)) * 1e6
        self.max_bps = float(p.get("max_mbps", 200.0)) * 1e6
        self.ai_bps = float(p.get("ai_mbps", 0.5)) * 1e6
        self.beta = float(p.get("beta", 0.5))
        self.min_epoch_ms = float(p.get("min_epoch_ms", 50.0))
        self.delay_thresh_ms = float(p.get("delay_thresh_ms", 10.0))
        self.delay_thresh_frac = float(p.get("delay_thresh_frac", 0.25))

        self.slow_start = True
        self.epoch_start = None
        self.congested_in_epoch = False
        self.last_decrease = -1e18
        self.last_signal = None

    def _epoch_ms(self, srtt):
        return max(srtt or 0.0, self.min_epoch_ms)

    def _decrease(self, now, srtt, why):
        if now - self.last_decrease < self._epoch_ms(srtt):
            return
        self.rate_bps = max(self.min_bps, self.rate_bps * self.beta)
        self.slow_start = False
        self.last_decrease = now
        self.congested_in_epoch = True
        self.last_signal = why

    def on_ack(self, now, seq, size, rtt, srtt, min_rtt):
        if rtt > min_rtt + max(self.delay_thresh_ms, self.delay_thresh_frac * min_rtt):
            self._decrease(now, srtt, "delay")

        if self.epoch_start is None:
            self.epoch_start = now
        elif now - self.epoch_start >= self._epoch_ms(srtt):
            if not self.congested_in_epoch:
                if self.slow_start:
                    self.rate_bps *= 2
                else:
                    self.rate_bps += self.ai_bps
                self.rate_bps = min(self.rate_bps, self.max_bps)
            self.epoch_start = now
            self.congested_in_epoch = False

    def on_loss(self, now, seqs, srtt):
        self._decrease(now, srtt, "loss")

    def pacing_rate_bps(self):
        return self.rate_bps

    def state(self):
        return {
            "rate_bps": self.rate_bps,
            "slow_start": self.slow_start,
            "last_signal": self.last_signal,
        }
