// Steady-stream RTT monitoring - the browser half of server/rtt.py.
//
// One side sends PING packets at a fixed interval, each stamped with its own
// clock at hand-off; the other turns each one around immediately, stamping its
// arrival time. So one exchange gives the round trip and, separately, each leg:
//
//   rtt      = pong arrival - ping send
//   up leg   = echo_recv_ts - send_ts      (+ clock offset)
//   down leg = pong arrival - echo_recv_ts (- clock offset)
//
// The offset is unknown but constant, so the legs only mean something relative
// to their own minimum: up_excess rising while down_excess stays flat says the
// queue is on the uplink. See server/rtt.py for the full reasoning; the classes
// here are line-for-line equivalents so both ends report the same fields.

import * as proto from './protocol.js';

const ALPHA = 1 / 8;
const BETA = 1 / 4;
const JITTER_GAIN = 1 / 16;
const BIN_MS = 100;

export function percentiles(xs) {
  const s = xs.filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
  if (!s.length) return null;
  const p = (q) => s[Math.floor(q * (s.length - 1))];
  return { min: s[0], p25: p(0.25), p50: p(0.5), p75: p(0.75), p95: p(0.95), max: s[s.length - 1],
    mean: s.reduce((a, b) => a + b, 0) / s.length };
}

function binTimeline(rows, start, keys) {
  const bins = new Map();
  for (const r of rows) {
    const idx = Math.floor((r.t - start) / BIN_MS);
    if (!bins.has(idx)) bins.set(idx, []);
    bins.get(idx).push(r);
  }
  return [...bins.keys()].sort((a, b) => a - b).map((idx) => {
    const group = bins.get(idx);
    const row = { t: idx * BIN_MS, packets: group.length };
    for (const k of keys) row[k] = percentiles(group.map((g) => g[k]).filter((v) => v != null))?.p50 ?? null;
    return row;
  });
}

// ---- initiator: sends PINGs, matches the PONGs ------------------------------

export class RttMonitor {
  constructor(keepRecords = true) {
    this.sent = 0; this.replies = 0; this.duplicates = 0; this.reordered = 0;
    this.pending = new Map();           // seq -> our send time
    this.srtt = null; this.rttvar = null; this.minRtt = null;
    this.minUp = null; this.minDown = null;
    this.maxSeq = -1;
    this.records = keepRecords ? [] : null;
    this.rtts = []; this.ups = []; this.downs = []; this.ipdv = [];
    this.prevRtt = null; this.firstSend = null;
    // Send side, to catch what the browser does to the stream *after* we hand
    // it over: the gap we actually achieved between hand-offs, how long the
    // transport took to accept each write, and how deep its own queue got.
    this.sendRecords = [];
    this.sendBySeq = new Map();
    this.sendGaps = []; this.writeMs = []; this.queueDepth = [];
    this.prevSend = null;
  }

  onSend(seq, t) {
    this.sent++;
    this.pending.set(seq, t);
    if (this.firstSend == null) this.firstSend = t;
    const gap = this.prevSend == null ? null : t - this.prevSend;
    this.prevSend = t;
    if (gap != null) this.sendGaps.push(gap);
    const rec = { seq, t, gap_ms: gap, write_ms: null, queue: null };
    this.sendRecords.push(rec);
    this.sendBySeq.set(seq, rec);
    return rec;
  }

  // `writeMs` is the delay before the transport accepted the write (null where
  // the API is synchronous, as on a data channel); `queue` is its own backlog
  // at hand-off - writer.desiredSize for WebTransport, bufferedAmount for RTC.
  onWriteDone(seq, writeMs, queue = null) {
    const rec = this.sendBySeq.get(seq);
    if (!rec) return;
    if (writeMs != null) { rec.write_ms = writeMs; this.writeMs.push(writeMs); }
    if (queue != null) { rec.queue = queue; this.queueDepth.push(queue); }
  }

  onPong(pkt, t) {
    const had = this.pending.delete(pkt.seq);
    if (!had && pkt.seq <= this.maxSeq) { this.duplicates++; return null; }
    const rtt = t - pkt.sendTs;         // echoed timestamp: no lookup needed
    this.replies++;
    if (pkt.seq < this.maxSeq) this.reordered++;
    this.maxSeq = Math.max(this.maxSeq, pkt.seq);

    if (this.srtt == null) { this.srtt = rtt; this.rttvar = rtt / 2; }
    else {
      this.rttvar = (1 - BETA) * this.rttvar + BETA * Math.abs(this.srtt - rtt);
      this.srtt = (1 - ALPHA) * this.srtt + ALPHA * rtt;
    }
    this.minRtt = this.minRtt == null ? rtt : Math.min(this.minRtt, rtt);
    if (this.prevRtt != null) this.ipdv.push(rtt - this.prevRtt);
    this.prevRtt = rtt;

    const up = pkt.echoRecvTs - pkt.sendTs;
    const down = t - pkt.echoRecvTs;
    this.minUp = this.minUp == null ? up : Math.min(this.minUp, up);
    this.minDown = this.minDown == null ? down : Math.min(this.minDown, down);
    this.rtts.push(rtt); this.ups.push(up); this.downs.push(down);

    const rec = {
      seq: pkt.seq, t: pkt.sendTs, rtt_ms: rtt, queue_ms: rtt - this.minRtt,
      up_ms: up, down_ms: down, srtt_ms: this.srtt,
      up_excess_ms: up - this.minUp, down_excess_ms: down - this.minDown,
    };
    this.records?.push(rec);
    return rec;
  }

  outstanding(t, timeoutMs) {
    let n = 0;
    for (const ts of this.pending.values()) if (t - ts > timeoutMs) n++;
    return n;
  }

  summary(keepRecords = false) {
    if (!this.replies) return null;
    const minRtt = this.minRtt ?? 0;
    const lost = Math.max(0, this.sent - this.replies);
    const out = {
      sent: this.sent, replies: this.replies, lost,
      loss_pct: this.sent ? (100 * lost) / this.sent : null,
      reordered: this.reordered, duplicates: this.duplicates,
      min_rtt_ms: this.minRtt, srtt_ms: this.srtt, rttvar_ms: this.rttvar,
      rtt_ms: percentiles(this.rtts),
      queue_ms: percentiles(this.rtts.map((r) => r - minRtt)),
      ipdv_abs_ms: percentiles(this.ipdv.map(Math.abs)),
      up_excess_ms: this.minUp == null ? null : percentiles(this.ups.map((u) => u - this.minUp)),
      down_excess_ms: this.minDown == null ? null : percentiles(this.downs.map((d) => d - this.minDown)),
      // send side: what we asked the browser for vs what it did with it
      send_gap_ms: percentiles(this.sendGaps),
      write_ms: percentiles(this.writeMs),
      queue_depth: percentiles(this.queueDepth),
      timeline: binTimeline(this.records ?? [], this.firstSend ?? 0, ['rtt_ms', 'queue_ms', 'srtt_ms']),
    };
    if (keepRecords) { out.records = this.records; out.send_records = this.sendRecords; }
    return out;
  }
}

// ---- responder: reads the timestamps inside the stream ----------------------

export class RttStreamReceiver {
  constructor(keepRecords = true) {
    this.received = 0; this.duplicates = 0; this.reordered = 0;
    this.firstSeq = null; this.maxSeq = -1;
    this.jitter = 0; this.minOwd = null;
    this.records = keepRecords ? [] : null;
    this.seen = new Set();
    this.prev = null;
    this.owds = []; this.iat = []; this.sendIat = []; this.ipdv = [];
    this.firstRecv = null;
  }

  onPing(pkt, recvTs) {
    if (this.seen.has(pkt.seq)) this.duplicates++;
    this.seen.add(pkt.seq);
    this.received++;
    if (this.firstSeq == null) { this.firstSeq = pkt.seq; this.firstRecv = recvTs; }
    if (pkt.seq < this.maxSeq) this.reordered++;
    this.maxSeq = Math.max(this.maxSeq, pkt.seq);

    const owd = recvTs - pkt.sendTs;    // one-way delay + clock offset
    this.minOwd = this.minOwd == null ? owd : Math.min(this.minOwd, owd);
    let iat = null, sendIat = null, d = null;
    if (this.prev) {
      sendIat = pkt.sendTs - this.prev[0];
      iat = recvTs - this.prev[1];
      d = iat - sendIat;                // RFC 3550 D(i-1, i)
      this.jitter += (Math.abs(d) - this.jitter) * JITTER_GAIN;
      this.iat.push(iat); this.sendIat.push(sendIat); this.ipdv.push(d);
    }
    this.prev = [pkt.sendTs, recvTs];
    this.owds.push(owd);

    const rec = {
      seq: pkt.seq, t: recvTs, send_ts: pkt.sendTs, owd_ms: owd,
      owd_excess_ms: owd - this.minOwd, iat_ms: iat, send_iat_ms: sendIat,
      ipdv_ms: d, jitter_ms: this.jitter,
    };
    this.records?.push(rec);
    return rec;
  }

  summary(keepRecords = false) {
    if (!this.received) return null;
    const expected = this.maxSeq - (this.firstSeq ?? 0) + 1;
    const lost = Math.max(0, expected - this.seen.size);
    const minOwd = this.minOwd ?? 0;
    const out = {
      received: this.received, expected, lost,
      loss_pct: expected ? (100 * lost) / expected : null,
      reordered: this.reordered, duplicates: this.duplicates,
      jitter_ms: this.jitter,
      iat_ms: percentiles(this.iat),
      send_iat_ms: percentiles(this.sendIat),
      ipdv_abs_ms: percentiles(this.ipdv.map(Math.abs)),
      owd_excess_ms: percentiles(this.owds.map((o) => o - minOwd)),
      timeline: binTimeline(this.records ?? [], this.firstRecv ?? 0, ['owd_excess_ms', 'iat_ms', 'jitter_ms']),
    };
    if (keepRecords) out.records = this.records;
    return out;
  }
}

// ---- the send loop ----------------------------------------------------------

// One packet per tick. The tick source must be a worker (see tick-worker.js):
// main-thread timers are throttled to ~1/s in a background tab, which would
// stall the stream. Timer wake-ups are never exact, so the gaps we actually
// achieved are reported too - and since each packet carries the timestamp of
// the moment it was handed over, a ragged send schedule shows up on the
// receiver as `send_iat` and never gets mistaken for network jitter.
export async function runRttStream({ send, flow, nowMs, monitor, intervalMs, durationS, size, tick, isOpen, onSample }) {
  const start = nowMs();
  const end = start + durationS * 1000;
  let n = 0;
  while (isOpen()) {
    const t = nowMs();
    if (t >= end) break;
    monitor.onSend(n, t);          // records the gap we actually achieved
    send(proto.encodePing(flow, n, t, size), n);
    n++;
    onSample?.(n, t - start);
    await tick.next();
  }
  return { sent: n, interval_ms: intervalMs, size, send_gap_ms: percentiles(monitor.sendGaps) };
}
