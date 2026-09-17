// Receiver-side acknowledgement generation (mirrors server/ack.py).
// mode "packet": one ACK per DATA packet. mode "block": ACK_BLOCK when every_n
// packets are pending or interval_ms after the first pending packet.

import * as proto from './protocol.js';

const RANGE_WINDOW = 4096;

export class AckGenerator {
  constructor(flow, config, send, nowMs) {
    config = config ?? {};
    this.flow = flow;
    this.mode = config.mode ?? 'packet';
    this.intervalMs = config.interval_ms ?? 20;
    this.everyN = config.every_n ?? 16;
    this.send = send;
    this.nowMs = nowMs;
    this.received = new Set();
    this.largest = -1;
    this.largestRecvTs = 0;
    this.pending = [];
    this.ackId = 0;
    this.acksSent = 0;
    this.timer = null;
  }

  onPacket(seq, sendTs, recvTs) {
    if (this.mode === 'packet') {
      this.send(proto.encodeAck(this.flow, seq, sendTs, recvTs));
      this.acksSent++;
      return;
    }
    this.received.add(seq);
    if (seq > this.largest) { this.largest = seq; this.largestRecvTs = recvTs; }
    this.pending.push([seq, recvTs]);
    if (this.pending.length >= this.everyN) this.flush();
    else if (this.timer === null) this.timer = setTimeout(() => this.flush(), this.intervalMs);
  }

  ranges() {
    const floor = Math.max(0, this.largest - RANGE_WINDOW + 1);
    const ranges = [];
    let seq = this.largest;
    while (seq >= floor && ranges.length < proto.MAX_ACK_RANGES) {
      const last = seq;
      while (seq >= floor && this.received.has(seq)) seq--;
      ranges.push([seq + 1, last]);
      while (seq >= floor && !this.received.has(seq)) seq--;
    }
    const low = seq >= floor ? ranges[ranges.length - 1][0] : floor;
    if (this.received.size > 2 * RANGE_WINDOW) {
      for (const s of this.received) if (s < floor) this.received.delete(s);
    }
    return { ranges, low };
  }

  flush() {
    if (this.timer !== null) { clearTimeout(this.timer); this.timer = null; }
    if (this.mode !== 'block' || this.largest < 0) return;
    const { ranges, low } = this.ranges();
    const capacity = proto.blockTsCapacity(ranges.length);
    const pending = this.pending;
    this.pending = [];
    const delay = Math.max(0, this.nowMs() - this.largestRecvTs);
    const chunks = [];
    for (let i = 0; i < pending.length; i += capacity) chunks.push(pending.slice(i, i + capacity));
    if (!chunks.length) chunks.push([]);
    for (const chunk of chunks) {
      this.send(proto.encodeAckBlock(this.flow, this.ackId++, this.largest, this.largestRecvTs, delay, low, ranges, chunk));
      this.acksSent++;
    }
  }

  close() {
    if (this.timer !== null) { clearTimeout(this.timer); this.timer = null; }
  }
}
