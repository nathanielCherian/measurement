// Runs one experiment over WebTransport, off the main thread so UI work does
// not disturb pacing or receive timestamps.
//
// main -> worker: {type:'run', config}
// worker -> main: {type:'log'|'progress'|'results'|'error', ...}

import { makeCC } from './cc/index.js';
import { AckGenerator } from './ack.js';
import * as proto from './protocol.js';
import { normalizeQuicStats, ReceiverStats, SenderCore, summarizeQuicStats } from './stats.js';

const SAMPLE_MS = 100;
// End-of-run safety nets. Safari never drops queued datagrams (it ignores
// outgoingMaxAge), so sending above the path's capacity grows its queue without
// bound: srtt climbs for the whole run and the results upload queues behind it.
const MAX_GRACE_MS = 3000;       // cap on the "wait for late ACKs" pause
const CONTROL_TIMEOUT_MS = 20000; // never wait forever for a control message
const MAX_RECORDS = 20000;       // per direction, to bound the results upload
const PROGRESS_MS = 250;
const STATS_POLL_MS = 250;

const nowMs = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (msg) => self.postMessage(msg);
const logLine = (text) => post({ type: 'log', text });

self.onmessage = (e) => {
  if (e.data.type === 'run') {
    run(e.data.config).catch((err) => post({ type: 'error', text: describeError(err) }));
  }
};

function describeError(err) {
  // WebTransportError's toString() is just its name; the useful parts are fields.
  const parts = [err?.name ?? 'Error', err?.message];
  if (err?.source) parts.push(`source=${err.source}`);
  if (err?.streamErrorCode != null) parts.push(`streamErrorCode=${err.streamErrorCode}`);
  return parts.filter(Boolean).join(': ') + (err?.stack ? `\n${err.stack}` : '');
}

function b64ToBytes(b64) {
  return Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
}

// Newline-delimited JSON control channel over a bidirectional stream.
class Control {
  constructor(stream) {
    this.writer = stream.writable.getWriter();
    this.waiters = new Map();
    this.inbox = [];
    this.readLoop(stream.readable);
  }
  send(msg) {
    return this.writer.write(new TextEncoder().encode(JSON.stringify(msg) + '\n'));
  }
  async readLoop(readable) {
    const reader = readable.pipeThrough(new TextDecoderStream()).getReader();
    let buf = '';
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return;
      buf += value;
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i);
        buf = buf.slice(i + 1);
        if (!line.trim()) continue;
        const msg = JSON.parse(line);
        const w = this.waiters.get(msg.type);
        if (w) { this.waiters.delete(msg.type); w(msg); } else this.inbox.push(msg);
      }
    }
  }
  waitFor(type, timeoutMs = 10000) {
    const idx = this.inbox.findIndex((m) => m.type === type);
    if (idx >= 0) return Promise.resolve(this.inbox.splice(idx, 1)[0]);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { this.waiters.delete(type); reject(new Error(`timeout waiting for ${type}`)); }, timeoutMs);
      this.waiters.set(type, (m) => { clearTimeout(t); resolve(m); });
    });
  }
}

async function run(cfg) {
  const options = {};
  if (cfg.certHashB64) {
    options.serverCertificateHashes = [{ algorithm: 'sha-256', value: b64ToBytes(cfg.certHashB64) }];
  }
  if (cfg.browserCongestionControl && cfg.browserCongestionControl !== 'default') {
    options.congestionControl = cfg.browserCongestionControl;
  }

  logLine(`connecting to ${cfg.url}`);
  const transport = new WebTransport(cfg.url, options);
  transport.closed.catch((err) => logLine(`session closed: ${describeError(err)}`));
  await transport.ready;
  logLine(`connected (congestionControl=${transport.congestionControl ?? 'n/a'}, ` +
    `maxDatagramSize=${transport.datagrams.maxDatagramSize ?? 'n/a'})`);

  const dg = transport.datagrams;
  if (cfg.outgoingMaxAgeMs && 'outgoingMaxAge' in dg) dg.outgoingMaxAge = cfg.outgoingMaxAgeMs;
  // Chrome defaults the datagram high-water mark to 1 and its writer.desiredSize
  // ignores later changes, so backpressure is tracked with our own count of
  // unsettled writes, capped at queueLimit.
  const queueLimit = cfg.outgoingHighWaterMark || 256;
  if ('outgoingHighWaterMark' in dg) dg.outgoingHighWaterMark = queueLimit;
  const writable = typeof dg.createWritable === 'function' ? dg.createWritable() : dg.writable;
  const writer = writable.getWriter();
  let writeErrors = 0;
  let pendingWrites = 0;
  let maxPendingWrites = 0;
  const sendDatagram = (buf) => {
    pendingWrites++;
    maxPendingWrites = Math.max(maxPendingWrites, pendingWrites);
    writer.write(buf).catch(() => { writeErrors++; }).finally(() => { pendingWrites--; });
  };

  const control = new Control(await transport.createBidirectionalStream());

  const size = Math.min(cfg.size, dg.maxDatagramSize ? dg.maxDatagramSize - 16 : cfg.size);
  const wantUp = cfg.mode === 'up' || cfg.mode === 'both';
  const wantDown = cfg.mode === 'down' || cfg.mode === 'both';

  // ---- receive path -------------------------------------------------------
  const down = new ReceiverStats(true, MAX_RECORDS);
  const downAcks = new AckGenerator(proto.FLOW_DOWN, cfg.ack, sendDatagram, nowMs);
  let upCore = null;
  let closed = false;
  (async () => {
    const reader = dg.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        const t = nowMs();
        const pkt = proto.decode(value);
        if (!pkt) continue;
        if (pkt.type === proto.DATA && pkt.flow === proto.FLOW_DOWN) {
          down.onData(pkt.seq, pkt.sendTs, t, pkt.size);
          downAcks.onPacket(pkt.seq, pkt.sendTs, t);
        } else if (pkt.type === proto.ACK && pkt.flow === proto.FLOW_UP && upCore) {
          upCore.onAck(t, pkt.seq, pkt.echoSendTs, pkt.recvTs);
        } else if (pkt.type === proto.ACK_BLOCK && pkt.flow === proto.FLOW_UP && upCore) {
          upCore.onAckBlock(t, pkt);
        }
      }
    } catch (err) {
      if (!closed) logLine(`datagram reader stopped: ${err}`);
    }
  })();

  // ---- browser WebTransport stats (QUIC state, where exposed) ----------------
  const wtStats = [];
  let t0 = nowMs();
  const statsExposed = typeof transport.getStats === 'function';
  logLine(statsExposed ? 'browser exposes WebTransport getStats()' : 'browser does not expose WebTransport getStats()');
  const statsTimer = setInterval(async () => {
    if (!statsExposed) return;
    try {
      const raw = JSON.parse(JSON.stringify(await transport.getStats()));
      wtStats.push(normalizeQuicStats(raw, nowMs() - t0, upCore?.srtt));
    } catch { /* not supported */ }
  }, STATS_POLL_MS);

  // ---- start ----------------------------------------------------------------
  await control.send({
    type: 'start', mode: cfg.mode, duration_s: cfg.durationS, size,
    down_cc: cfg.downCC, ack: cfg.ack, client_time_ms: nowMs(), user_agent: navigator.userAgent,
  });
  const started = await control.waitFor('started');
  logLine(`server started: quic_cc=${started.quic_cc}, clock offset ≈ ${(started.server_time_ms - nowMs()).toFixed(1)} ms`);

  t0 = nowMs();
  const progressTimer = setInterval(() => {
    const last = wtStats[wtStats.length - 1];
    post({
      type: 'progress',
      t: nowMs() - t0,
      up: upCore && {
        rate_bps: upCore.cc.pacingRateBps(), sent: upCore.sent, acked: upCore.acked,
        lost: upCore.lost, srtt: upCore.srtt,
      },
      down: wantDown ? { received: down.received, expected: down.maxSeq + 1, bytes: down.bytes, jitter: down.jitter } : null,
      wt: last ?? null,
      statsExposed,
      pendingWrites,
    });
  }, PROGRESS_MS);

  // ---- up sender (browser CC) -------------------------------------------------
  const upTimeline = [];
  let blockedTicks = 0;
  let upStopped = null;
  if (wantUp) {
    const cc = makeCC(cfg.upCC.name, cfg.upCC.params);
    upCore = new SenderCore(cc, true, MAX_RECORDS,
      cfg.ack?.mode === 'block' ? (cfg.ack.interval_ms ?? 0) : 0);
    // Standing queue above this ends the run early: the sender is far above the
    // path's capacity and everything after it (ACKs, control messages) is stuck
    // behind the queue.
    const queueGuardMs = cfg.queueGuardMs ?? 2000;
    const end = t0 + cfg.durationS * 1000;
    let last = t0;
    let lastSample = t0;
    let tokens = size;
    for (;;) {
      const t = nowMs();
      if (t >= end) break;
      const rate = cc.pacingRateBps();
      // timers in workers are ~1-4 ms coarse, so allow ~10 ms of burst
      const burst = Math.max(2 * size, (rate / 8) * 0.01);
      tokens = Math.min(tokens + ((rate / 8) * (t - last)) / 1000, burst);
      last = t;
      upCore.checkTimeouts(t);

      while (tokens >= size && upCore.canSend(size)) {
        // A full datagram queue means the browser's own QUIC CC is the bottleneck.
        if (pendingWrites >= queueLimit) {
          blockedTicks++;
          tokens = 0;
          break;
        }
        const seq = upCore.onSend(t, size);
        sendDatagram(proto.encodeData(proto.FLOW_UP, seq, t, size));
        tokens -= size;
      }

      if (
        upCore.srtt !== null && upCore.minRtt !== null &&
        upCore.srtt - upCore.minRtt > queueGuardMs
      ) {
        upStopped = { reason: 'queue_guard', t: t - t0, srtt: upCore.srtt, minRtt: upCore.minRtt };
        logLine(`stopping early: standing queue ${(upCore.srtt - upCore.minRtt).toFixed(0)} ms ` +
          `(srtt ${upCore.srtt.toFixed(0)} ms, min ${upCore.minRtt.toFixed(1)} ms) — sending faster than the path allows`);
        break;
      }

      if (t - lastSample >= SAMPLE_MS) {
        lastSample = t;
        upTimeline.push({
          t: t - t0, cc: cc.state(), srtt: upCore.srtt, inflight: upCore.inflightBytes,
          sent: upCore.sent, acked: upCore.acked, lost: upCore.lost, pendingWrites,
        });
      }
      await sleep(1);
    }
    await sleep(Math.min(Math.max(500, 3 * (upCore.srtt ?? 100)), MAX_GRACE_MS));
    upCore.checkTimeouts(nowMs() + 1e9);
  }

  let downDone = null;
  if (wantDown) {
    downDone = await control.waitFor('down_done', cfg.durationS * 1000 + 15000).catch((e) => {
      logLine(`no down_done: ${e.message}`);
      return null;
    });
  }

  clearInterval(progressTimer);
  clearInterval(statsTimer);

  await control.send({ type: 'finish' });
  const serverReport = await control.waitFor('server_report', CONTROL_TIMEOUT_MS).catch((e) => {
    logLine(`no server_report: ${e.message}`);
    return {};
  });

  const results = {
    config: cfg,
    user_agent: navigator.userAgent,
    started,
    t0,
    max_datagram_size: dg.maxDatagramSize ?? null,
    congestion_control: transport.congestionControl ?? null,
    write_errors: writeErrors,
    up_stopped_early: upStopped,
    max_pending_writes: maxPendingWrites,
    up: upCore && {
      ...upCore.summary(), blocked_ticks: blockedTicks, timeline: upTimeline,
      records: upCore.records, records_truncated: upCore.recordsTruncated,
    },
    down: wantDown ? {
      ...down.summary(), acks_sent: downAcks.acksSent, ack_mode: downAcks.mode,
      records: down.records, records_truncated: down.recordsTruncated,
    } : null,
    down_done: downDone,
    webtransport_stats: wtStats,
    browser_quic: summarizeQuicStats(wtStats),
  };

  post({ type: 'results', results, serverReport, savedFile: null, pending: true });
  await control.send({ type: 'results', ...results });
  const saved = await control.waitFor('saved', CONTROL_TIMEOUT_MS).catch((e) => {
    logLine(`results not confirmed saved: ${e.message}`);
    return null;
  });
  closed = true;
  downAcks.close();
  transport.close();

  post({ type: 'results', results, serverReport, savedFile: saved?.file ?? null });
}
