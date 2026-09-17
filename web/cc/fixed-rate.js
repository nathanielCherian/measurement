import { CongestionController } from './base.js';

// Constant bitrate, ignores all feedback. Params: rate_mbps.
export class FixedRate extends CongestionController {
  static ccName = 'fixed';
  constructor(params = {}) {
    super(params);
    this.rateBps = (params.rate_mbps ?? 1) * 1e6;
  }
  pacingRateBps() { return this.rateBps; }
}
