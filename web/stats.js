// Sender-side ACK/loss/RTT bookkeeping and receiver-side statistics.
// Mirrors server/transport_stats.py.

export const REORDER_THRESHOLD = 3;
export const MIN_LOSS_TIMEOUT_MS = 25;
export const BIN_MS = 100;

export class SenderCore {
  constructor(cc, keepRecords = true, maxRecords = Infinity, ackDelayBudgetMs = 0) {
    this.cc = cc;
    // A block-ACK receiver holds packets for up to its ACK interval; without
    // allowing for that the sender reports spurious loss.
    this.minTimeoutMs = MIN_LOSS_TIMEOUT_MS + 2 * ackDelayBudgetMs;
    this.nextSeq = 0;
    this.unacked = new Map(); // seq -> [sendTs, size], insertion (= seq) order
    this.inflightBytes = 0;
    this.srtt = null;
    this.rttvar = 0;
    this.minRtt = null;
    this.sent = 0; this.acked = 0; this.lost = 0; this.lateAcks = 0;
    this.bytesSent = 0;
    this.rtts = [];
    this.records = keepRecords ? [] : null;
    this.maxRecords = maxRecords;
    this.recordsTruncated = 0;
  }

  addRecord(row) {
    if (this.records === null) return;
    if (this.records.length < this.maxRecords) this.records.push(row);
    else this.recordsTruncated++;
  }

  onSend(now, size) {
    const seq = this.nextSeq++;
    this.unacked.set(seq, [now, size]);
    this.inflightBytes += size;
    this.sent++;
    this.bytesSent += size;
    this.cc.onPacketSent(now, seq, size);
    return seq;
  }

  onAck(now, seq, echoSendTs, recvTs) {
    const entry = this.unacked.get(seq);
    if (!entry) { this.lateAcks++; return; }
    this.unacked.delete(seq);
    const size = entry[1];
    this.inflightBytes -= size;
    this.acked++;

    const rtt = now - echoSendTs;
    this.rtts.push(rtt);
    if (this.srtt === null) {
      this.srtt = rtt; this.rttvar = rtt / 2;
    } else {
      this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - rtt);
      this.srtt = 0.875 * this.srtt + 0.125 * rtt;
    }
    this.minRtt = this.minRtt === null ? rtt : Math.min(this.minRtt, rtt);
    this.addRecord([seq, echoSendTs, recvTs, now]);

    this.cc.onAck(now, seq, size, rtt, this.srtt, this.minRtt);

    const lost = [];
    for (const s of this.unacked.keys()) {
      if (s + REORDER_THRESHOLD < seq) lost.push(s); else break;
    }
    this.declareLost(now, lost);
  }

  // Handle a decoded ACK_BLOCK: every unacked packet inside ack.ranges is acked;
  // one RTT sample from the largest packet if newly acked (minus ack delay).
  onAckBlock(now, ack) {
    this.acksReceived = (this.acksReceived ?? 0) + 1;
    const recvTs = new Map(ack.timestamps);
    const covered = (seq) => ack.ranges.some(([first, last]) => first <= seq && seq <= last);
    const newly = [];
    for (const s of this.unacked.keys()) {
      if (s > ack.largest) break;
      if (covered(s)) newly.push(s);
    }
    if (!newly.length) return;

    let rtt = null;
    const largestEntry = this.unacked.get(ack.largest);
    if (largestEntry) {
      rtt = Math.max(0, now - largestEntry[0] - ack.ackDelayMs);
      this.rtts.push(rtt);
      if (this.srtt === null) { this.srtt = rtt; this.rttvar = rtt / 2; }
      else {
        this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - rtt);
        this.srtt = 0.875 * this.srtt + 0.125 * rtt;
      }
      this.minRtt = this.minRtt === null ? rtt : Math.min(this.minRtt, rtt);
    }

    for (const s of newly) {
      const [sendTs, size] = this.unacked.get(s);
      this.unacked.delete(s);
      this.inflightBytes -= size;
      this.acked++;
      this.addRecord([s, sendTs, recvTs.get(s) ?? null, now]);
      if (this.srtt !== null) this.cc.onAck(now, s, size, rtt ?? this.srtt, this.srtt, this.minRtt);
    }

    const lost = [];
    for (const s of this.unacked.keys()) {
      if (s + REORDER_THRESHOLD >= ack.largest) break;
      if (s >= ack.low) lost.push(s);
    }
    this.declareLost(now, lost);
  }

  checkTimeouts(now) {
    const threshold = this.srtt === null
      ? Math.max(1000, this.minTimeoutMs)
      : Math.max(2 * this.srtt, this.srtt + 4 * this.rttvar, this.minTimeoutMs);
    const lost = [];
    for (const [s, [ts]] of this.unacked) {
      if (now - ts > threshold) lost.push(s); else break;
    }
    this.declareLost(now, lost);
  }

  declareLost(now, seqs) {
    if (!seqs.length) return;
    for (const s of seqs) {
      this.inflightBytes -= this.unacked.get(s)[1];
      this.unacked.delete(s);
    }
    this.lost += seqs.length;
    this.cc.onLoss(now, seqs, this.srtt);
  }

  canSend(size) {
    const cwnd = this.cc.cwndBytes();
    return cwnd === null || this.inflightBytes + size <= cwnd;
  }

  summary() {
    return {
      sent: this.sent, acked: this.acked, lost: this.lost, late_acks: this.lateAcks,
      bytes_sent: this.bytesSent, rtt_ms: percentiles(this.rtts),
      min_rtt_ms: this.minRtt, srtt_ms: this.srtt, acks_received: this.acksReceived ?? null,
    };
  }
}

export class ReceiverStats {
  constructor(keepRecords = true, maxRecords = Infinity) {
    this.received = 0; this.bytes = 0; this.maxSeq = -1;
    this.reordered = 0; this.duplicates = 0;
    this.seen = new Set();
    this.jitter = 0; this.lastTransit = null;
    this.firstRecv = null;
    this.bins = new Map();
    this.records = keepRecords ? [] : null;
    this.maxRecords = maxRecords;
    this.recordsTruncated = 0;
  }

  onData(seq, sendTs, recvTs, size) {
    if (this.seen.has(seq)) { this.duplicates++; return; }
    this.seen.add(seq);
    this.received++;
    this.bytes += size;
    if (seq < this.maxSeq) this.reordered++;
    this.maxSeq = Math.max(this.maxSeq, seq);

    // RFC 3550 interarrival jitter; clock offset cancels out.
    const transit = recvTs - sendTs;
    if (this.lastTransit !== null) this.jitter += (Math.abs(transit - this.lastTransit) - this.jitter) / 16;
    this.lastTransit = transit;

    if (this.firstRecv === null) this.firstRecv = recvTs;
    const b = Math.floor((recvTs - this.firstRecv) / BIN_MS);
    this.bins.set(b, (this.bins.get(b) ?? 0) + size);
    if (this.records !== null) {
      if (this.records.length < this.maxRecords) this.records.push([seq, sendTs, recvTs, size]);
      else this.recordsTruncated++;
    }
  }

  summary() {
    const expected = this.maxSeq + 1;
    const nBins = this.bins.size ? Math.max(...this.bins.keys()) + 1 : 0;
    return {
      received: this.received, expected,
      loss_rate: expected ? 1 - this.received / expected : null,
      bytes: this.bytes, reordered: this.reordered, duplicates: this.duplicates,
      jitter_ms: this.jitter,
      goodput_bps_bins: Array.from({ length: nBins }, (_, i) => ((this.bins.get(i) ?? 0) * 8 * 1000) / BIN_MS),
      bin_ms: BIN_MS,
    };
  }
}

export function percentiles(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const p = (q) => s[Math.min(s.length - 1, Math.floor(q * s.length))];
  return { min: s[0], p50: p(0.5), p95: p(0.95), max: s[s.length - 1], mean: s.reduce((a, b) => a + b, 0) / s.length };
}

// ---- WebTransport getStats() (browser QUIC state) ---------------------------
// Spec fields (WebTransportConnectionStats); browsers expose different subsets.
// Chrome 151 has no getStats(). Safari 26.6.2 returns the fields but every value
// is 0 even during traffic, so samples are marked `populated` only when the
// counters actually move.

export function normalizeQuicStats(raw, t, appSrtt) {
  const d = raw.datagrams ?? {};
  const num = (v) => (typeof v === 'number' ? v : null);
  const populated = [raw.bytesSent, raw.packetsSent, raw.bytesReceived, raw.smoothedRtt].some((v) => typeof v === 'number' && v > 0);
  return {
    t,
    populated,
    smoothedRtt: num(raw.smoothedRtt),
    minRtt: num(raw.minRtt),
    rttVariation: num(raw.rttVariation),
    atSendCapacity: typeof raw.atSendCapacity === 'boolean' ? raw.atSendCapacity : null,
    estimatedSendRate: num(raw.estimatedSendRate),
    bytesSent: num(raw.bytesSent),
    packetsSent: num(raw.packetsSent),
    packetsLost: num(raw.packetsLost),
    datagramsExpiredOutgoing: num(d.expiredOutgoing),
    datagramsLostOutgoing: num(d.lostOutgoing),
    datagramsDroppedIncoming: num(d.droppedIncoming),
    appSrtt: appSrtt ?? null,
    // time spent in the browser's local queue, as seen by our app RTT but not QUIC's
    appMinusQuicRtt: populated && appSrtt != null && typeof raw.smoothedRtt === 'number' ? appSrtt - raw.smoothedRtt : null,
    rawKeys: Object.keys(raw),
  };
}

export function summarizeQuicStats(samples) {
  if (!samples.length) return { exposed: false };
  const fields = samples[samples.length - 1].rawKeys;
  const good = samples.filter((s) => s.populated);
  if (!good.length) return { exposed: true, populated: false, samples: samples.length, fields };
  const vals = (k) => good.map((s) => s[k]).filter((v) => v !== null && v !== undefined);
  const last = (k) => { const v = vals(k); return v.length ? v[v.length - 1] : null; };
  const cap = vals('atSendCapacity');
  return {
    exposed: true,
    populated: true,
    samples: samples.length,
    populatedSamples: good.length,
    fields,
    atSendCapacityFraction: cap.length ? cap.filter(Boolean).length / cap.length : null,
    estimatedSendRateBps: percentiles(vals('estimatedSendRate')),
    smoothedRttMs: percentiles(vals('smoothedRtt')),
    minRttMs: last('minRtt'),
    appMinusQuicRttMs: percentiles(vals('appMinusQuicRtt')),
    datagramsExpiredOutgoing: last('datagramsExpiredOutgoing'),
    datagramsLostOutgoing: last('datagramsLostOutgoing'),
    datagramsDroppedIncoming: last('datagramsDroppedIncoming'),
    packetsLost: last('packetsLost'),
  };
}
