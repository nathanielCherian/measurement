import { CongestionController } from './base.js';

// Rate-based AIMD with slow start and a delay signal. Keep in sync with
// server/appcc/aimd.py.
//
// Every epoch (max(srtt, min_epoch_ms)) without congestion the rate grows:
// doubles in slow start, else + ai_mbps. A loss, or an RTT sample above
// minRtt + max(delay_thresh_ms, delay_thresh_frac * minRtt), cuts the rate by
// beta (at most once per epoch) and ends slow start.
export class Aimd extends CongestionController {
  static ccName = 'aimd';
  constructor(p = {}) {
    super(p);
    this.rateBps = (p.start_mbps ?? 1) * 1e6;
    this.minBps = (p.min_mbps ?? 0.1) * 1e6;
    this.maxBps = (p.max_mbps ?? 200) * 1e6;
    this.aiBps = (p.ai_mbps ?? 0.5) * 1e6;
    this.beta = p.beta ?? 0.5;
    this.minEpochMs = p.min_epoch_ms ?? 50;
    this.delayThreshMs = p.delay_thresh_ms ?? 10;
    this.delayThreshFrac = p.delay_thresh_frac ?? 0.25;

    this.slowStart = true;
    this.epochStart = null;
    this.congestedInEpoch = false;
    this.lastDecrease = -Infinity;
    this.lastSignal = null;
  }

  epochMs(srtt) { return Math.max(srtt ?? 0, this.minEpochMs); }

  decrease(now, srtt, why) {
    if (now - this.lastDecrease < this.epochMs(srtt)) return;
    this.rateBps = Math.max(this.minBps, this.rateBps * this.beta);
    this.slowStart = false;
    this.lastDecrease = now;
    this.congestedInEpoch = true;
    this.lastSignal = why;
  }

  onAck(now, seq, size, rtt, srtt, minRtt) {
    if (rtt > minRtt + Math.max(this.delayThreshMs, this.delayThreshFrac * minRtt)) {
      this.decrease(now, srtt, 'delay');
    }
    if (this.epochStart === null) {
      this.epochStart = now;
    } else if (now - this.epochStart >= this.epochMs(srtt)) {
      if (!this.congestedInEpoch) {
        this.rateBps = this.slowStart ? this.rateBps * 2 : this.rateBps + this.aiBps;
        this.rateBps = Math.min(this.rateBps, this.maxBps);
      }
      this.epochStart = now;
      this.congestedInEpoch = false;
    }
  }

  onLoss(now, seqs, srtt) { this.decrease(now, srtt, 'loss'); }

  pacingRateBps() { return this.rateBps; }

  state() {
    return { rate_bps: this.rateBps, slow_start: this.slowStart, last_signal: this.lastSignal };
  }
}
