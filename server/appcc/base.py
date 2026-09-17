from typing import Any, Dict, List, Optional


class CongestionController:
    name = "base"

    def __init__(self, params: Dict[str, Any]) -> None:
        self.params = params

    def on_packet_sent(self, now: float, seq: int, size: int) -> None:
        pass

    def on_ack(
        self, now: float, seq: int, size: int, rtt: float, srtt: float, min_rtt: float
    ) -> None:
        pass

    def on_loss(self, now: float, seqs: List[int], srtt: Optional[float]) -> None:
        pass

    def pacing_rate_bps(self) -> float:
        raise NotImplementedError

    def cwnd_bytes(self) -> Optional[float]:
        return None

    def state(self) -> Dict[str, Any]:
        return {"rate_bps": self.pacing_rate_bps(), "cwnd": self.cwnd_bytes()}
