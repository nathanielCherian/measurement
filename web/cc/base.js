// Application-level congestion controller interface (mirrors server/appcc/).
// All times in ms.
//   onPacketSent(now, seq, size)
//   onAck(now, seq, size, rtt, srtt, minRtt)
//   onLoss(now, seqs, srtt)
//   pacingRateBps() -> number
//   cwndBytes() -> number | null     null = rate-only, no window
//   state() -> object                snapshot for logs
// To add one: extend CongestionController and register it in cc/index.js.

export class CongestionController {
  static ccName = 'base';
  constructor(params = {}) { this.params = params; }
  onPacketSent(now, seq, size) {}
  onAck(now, seq, size, rtt, srtt, minRtt) {}
  onLoss(now, seqs, srtt) {}
  pacingRateBps() { throw new Error('not implemented'); }
  cwndBytes() { return null; }
  state() { return { rate_bps: this.pacingRateBps(), cwnd: this.cwndBytes() }; }
}
