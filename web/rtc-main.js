// WebRTC DataChannel version of the probe (see main.js/worker.js for the
// WebTransport one). Same probe packets, congestion controllers, ACKs and
// statistics; the transport is an unreliable SCTP data channel.
//
// RTCPeerConnection is not available in Workers, so sending runs on the main
// thread; a worker only supplies pacing ticks (main-thread timers are throttled
// in background tabs).

import { AckGenerator } from './ack.js';
import { drawChart } from './chart.js';
import { makeCC } from './cc/index.js';
import * as proto from './protocol.js';
import { ReceiverStats, SenderCore } from './stats.js';

const $ = (id) => document.getElementById(id);
const form = $('form');
const nowMs = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Ticks from a worker: main-thread timers are throttled in background tabs,
// which starves pacing (17 packets instead of 1500 in one test).
function ticker(intervalMs = 1) {
  const worker = new Worker('tick-worker.js');
  let resolve = null;
  worker.onmessage = () => { const r = resolve; resolve = null; r?.(); };
  worker.postMessage({ type: 'start', intervalMs });
  return {
    next: () => new Promise((r) => { resolve = r; }),
    stop: () => { worker.postMessage({ type: 'stop' }); worker.terminate(); },
  };
}

const SAMPLE_MS = 100;
const PROGRESS_MS = 250;
const MAX_GRACE_MS = 3000;
const CONTROL_TIMEOUT_MS = 20000;
const MAX_RECORDS = 20000;
const CONTROL_CHUNK = 16000;

let lastResults = null;
let live = [];

if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(location.hostname)) {
  form.elements.signalUrl.value = `${location.origin}/rtc/offer`;
}

function log(text) {
  $('log').textContent += `[${new Date().toLocaleTimeString()}] ${text}\n`;
  $('log').scrollTop = $('log').scrollHeight;
}

const fmt = {
  mbps: (bps) => (bps == null ? '–' : `${(bps / 1e6).toFixed(2)} Mbps`),
  ms: (v) => (v == null ? '–' : `${v.toFixed(2)} ms`),
  pct: (v) => (v == null ? '–' : `${(v * 100).toFixed(2)}%`),
  n: (v) => (v == null ? '–' : String(v)),
};

function renderStats(items) {
  $('stats').innerHTML = items
    .map(([label, value]) => `<div class="stat"><b>${value}</b><span>${label}</span></div>`)
    .join('');
}

// Newline-delimited JSON over the reliable "control" channel.
class Control {
  constructor(channel) {
    this.channel = channel;
    this.waiters = new Map();
    this.inbox = [];
    this.buf = '';
    channel.onmessage = (e) => {
      this.buf += typeof e.data === 'string' ? e.data : new TextDecoder().decode(e.data);
      let i;
      while ((i = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        if (!line.trim()) continue;
        const msg = JSON.parse(line);
        const w = this.waiters.get(msg.type);
        if (w) { this.waiters.delete(msg.type); w(msg); } else this.inbox.push(msg);
      }
    };
  }
  send(msg) {
    // NDJSON, chunked: data channels cap a single message (64 KB in aiortc),
    // and the server reassembles on newlines.
    const text = JSON.stringify(msg) + '\n';
    for (let i = 0; i < text.length; i += CONTROL_CHUNK) this.channel.send(text.slice(i, i + CONTROL_CHUNK));
  }
  waitFor(type, timeoutMs = CONTROL_TIMEOUT_MS) {
    const idx = this.inbox.findIndex((m) => m.type === type);
    if (idx >= 0) return Promise.resolve(this.inbox.splice(idx, 1)[0]);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { this.waiters.delete(type); reject(new Error(`timeout waiting for ${type}`)); }, timeoutMs);
      this.waiters.set(type, (m) => { clearTimeout(t); resolve(m); });
    });
  }
}

const channelOpen = (ch) => new Promise((resolve, reject) => {
  if (ch.readyState === 'open') return resolve();
  ch.onopen = () => resolve();
  ch.onerror = (e) => reject(new Error(`channel ${ch.label} failed: ${e?.error?.message ?? e}`));
});

const iceComplete = (pc) => new Promise((resolve) => {
  if (pc.iceGatheringState === 'complete') return resolve();
  pc.onicegatheringstatechange = () => { if (pc.iceGatheringState === 'complete') resolve(); };
  setTimeout(resolve, 3000); // proceed with what we have
});

async function run(cfg) {
  const pc = new RTCPeerConnection({ iceServers: cfg.stunUrl ? [{ urls: cfg.stunUrl }] : [] });
  const control = pc.createDataChannel('control', { ordered: true });
  // The probe channel is the DataChannel equivalent of QUIC datagrams:
  // unordered and never retransmitted. SCTP still congestion-controls it.
  const probe = pc.createDataChannel('probe', { ordered: false, maxRetransmits: 0 });
  probe.binaryType = 'arraybuffer';

  const down = new ReceiverStats(true, MAX_RECORDS);
  const downAcks = new AckGenerator(proto.FLOW_DOWN, cfg.ack, (buf) => probe.send(buf), nowMs);
  let upCore = null;

  probe.onmessage = (e) => {
    const t = nowMs();
    const pkt = proto.decode(new Uint8Array(e.data));
    if (!pkt) return;
    if (pkt.type === proto.DATA && pkt.flow === proto.FLOW_DOWN) {
      down.onData(pkt.seq, pkt.sendTs, t, pkt.size);
      downAcks.onPacket(pkt.seq, pkt.sendTs, t);
    } else if (pkt.type === proto.ACK && pkt.flow === proto.FLOW_UP && upCore) {
      upCore.onAck(t, pkt.seq, pkt.echoSendTs, pkt.recvTs);
    } else if (pkt.type === proto.ACK_BLOCK && pkt.flow === proto.FLOW_UP && upCore) {
      upCore.onAckBlock(t, pkt);
    }
  };

  log(`signaling via ${cfg.signalUrl}`);
  await pc.setLocalDescription(await pc.createOffer());
  await iceComplete(pc);
  const answer = await (await fetch(cfg.signalUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sdp: pc.localDescription.sdp, type: pc.localDescription.type }),
  })).json();
  await pc.setRemoteDescription(answer);
  await Promise.all([channelOpen(control), channelOpen(probe)]);
  log(`data channels open (probe: ordered=${probe.ordered}, maxRetransmits=${probe.maxRetransmits})`);

  const ctl = new Control(control);
  const size = cfg.size;
  const wantUp = cfg.mode === 'up' || cfg.mode === 'both';
  const wantDown = cfg.mode === 'down' || cfg.mode === 'both';

  ctl.send({
    type: 'start', mode: cfg.mode, duration_s: cfg.durationS, size,
    down_cc: cfg.downCC, ack: cfg.ack, client_time_ms: nowMs(), user_agent: navigator.userAgent,
  });
  const started = await ctl.waitFor('started');
  log(`server started (${started.transport}); clock offset ≈ ${(started.server_time_ms - nowMs()).toFixed(1)} ms`);

  const t0 = nowMs();
  let maxBuffered = 0;
  const progressTimer = setInterval(() => {
    live.push({
      t: nowMs() - t0,
      up: upCore && {
        rate_bps: upCore.cc.pacingRateBps(), sent: upCore.sent, acked: upCore.acked,
        lost: upCore.lost, srtt: upCore.srtt,
      },
      down: wantDown ? { received: down.received, expected: down.maxSeq + 1, bytes: down.bytes } : null,
      buffered: probe.bufferedAmount,
    });
    renderLive();
  }, PROGRESS_MS);

  // ---- up sender ------------------------------------------------------------
  const upTimeline = [];
  let blockedTicks = 0;
  let upStopped = null;
  if (wantUp) {
    const cc = makeCC(cfg.upCC.name, cfg.upCC.params);
    upCore = new SenderCore(cc, true, MAX_RECORDS,
      cfg.ack?.mode === 'block' ? (cfg.ack.interval_ms ?? 0) : 0);
    const queueGuardMs = cfg.queueGuardMs ?? 2000;
    const end = t0 + cfg.durationS * 1000;
    let last = t0, lastSample = t0, tokens = size;
    const tick = ticker(1);

    for (;;) {
      const t = nowMs();
      if (t >= end) break;
      const rate = cc.pacingRateBps();
      const burst = Math.max(2 * size, (rate / 8) * 0.01);
      tokens = Math.min(tokens + ((rate / 8) * (t - last)) / 1000, burst);
      last = t;
      upCore.checkTimeouts(t);

      while (tokens >= size && upCore.canSend(size)) {
        // SCTP's send buffer: when it grows, SCTP congestion control (not our
        // controller) is deciding the rate.
        maxBuffered = Math.max(maxBuffered, probe.bufferedAmount);
        if (probe.bufferedAmount >= cfg.bufferedLimit) { blockedTicks++; tokens = 0; break; }
        const seq = upCore.onSend(t, size);
        probe.send(proto.encodeData(proto.FLOW_UP, seq, t, size));
        tokens -= size;
      }

      if (upCore.srtt !== null && upCore.minRtt !== null && upCore.srtt - upCore.minRtt > queueGuardMs) {
        upStopped = { reason: 'queue_guard', t: t - t0, srtt: upCore.srtt, minRtt: upCore.minRtt };
        log(`stopping early: standing queue ${(upCore.srtt - upCore.minRtt).toFixed(0)} ms`);
        break;
      }

      if (t - lastSample >= SAMPLE_MS) {
        lastSample = t;
        upTimeline.push({
          t: t - t0, cc: cc.state(), srtt: upCore.srtt, inflight: upCore.inflightBytes,
          sent: upCore.sent, acked: upCore.acked, lost: upCore.lost, buffered: probe.bufferedAmount,
        });
      }
      await tick.next();
    }
    tick.stop();
    await sleep(Math.min(Math.max(500, 3 * (upCore.srtt ?? 100)), MAX_GRACE_MS));
    upCore.checkTimeouts(nowMs() + 1e9);
  }

  let downDone = null;
  if (wantDown) {
    downDone = await ctl.waitFor('down_done', cfg.durationS * 1000 + 15000).catch((e) => {
      log(`no down_done: ${e.message}`); return null;
    });
  }
  clearInterval(progressTimer);

  ctl.send({ type: 'finish' });
  const serverReport = await ctl.waitFor('server_report').catch((e) => { log(`no server_report: ${e.message}`); return {}; });

  const results = {
    transport: 'webrtc-datachannel',
    config: cfg,
    user_agent: navigator.userAgent,
    started,
    max_buffered: maxBuffered,
    up_stopped_early: upStopped,
    up: upCore && {
      ...upCore.summary(), blocked_ticks: blockedTicks, timeline: upTimeline,
      records: upCore.records, records_truncated: upCore.recordsTruncated,
    },
    down: wantDown ? {
      ...down.summary(), acks_sent: downAcks.acksSent, ack_mode: downAcks.mode,
      records: down.records, records_truncated: down.recordsTruncated,
    } : null,
    down_done: downDone,
  };

  lastResults = { browser: results, server: serverReport };
  renderFinal(results, serverReport);
  $('download').disabled = false;

  ctl.send({ type: 'results', ...results });
  const saved = await ctl.waitFor('saved').catch((e) => { log(`results not confirmed saved: ${e.message}`); return null; });
  log(`done${saved?.file ? `; server saved ${saved.file}` : ''}`);
  downAcks.close();
  pc.close();
}

function renderLive() {
  const m = live[live.length - 1];
  if (!m) return;
  const prev = live[live.length - 2];
  const downBps = m.down && prev?.down ? ((m.down.bytes - prev.down.bytes) * 8 * 1000) / (m.t - prev.t) : null;
  renderStats([
    ['elapsed', `${(m.t / 1000).toFixed(1)} s`],
    ...(m.up ? [
      ['up CC rate', fmt.mbps(m.up.rate_bps)],
      ['up sent / acked / lost', `${m.up.sent} / ${m.up.acked} / ${m.up.lost}`],
      ['up srtt', fmt.ms(m.up.srtt)],
    ] : []),
    ...(m.down ? [['down received', fmt.mbps(downBps)]] : []),
    ['probe channel bufferedAmount', fmt.n(m.buffered)],
  ]);
  drawChart($('rateChart'), $('rateLegend'), [
    { name: 'up CC rate', color: '#2563eb', points: live.filter((p) => p.up).map((p) => [p.t / 1000, p.up.rate_bps / 1e6]) },
  ], 'Mbps');
  drawChart($('rttChart'), $('rttLegend'), [
    { name: 'up srtt (app)', color: '#2563eb', points: live.filter((p) => p.up?.srtt != null).map((p) => [p.t / 1000, p.up.srtt]) },
  ], 'ms');
}

function renderFinal(r, s) {
  const a = s.up_analysis;
  renderStats([
    ...(r.up ? [
      ['up sent / acked / lost', `${r.up.sent} / ${r.up.acked} / ${r.up.lost}`],
      ['up loss @server', fmt.pct(s.up?.loss_rate)],
      ['up RTT p50 / p95', `${fmt.ms(r.up.rtt_ms?.p50)} / ${fmt.ms(r.up.rtt_ms?.p95)}`],
      ['up blocked by SCTP buffer', `${r.up.blocked_ticks} ticks`],
      ['max bufferedAmount', fmt.n(r.max_buffered)],
    ] : []),
    ...(r.down ? [
      ['down received / expected', `${r.down.received} / ${r.down.expected}`],
      ['down loss @browser', fmt.pct(r.down.loss_rate)],
      ['down RTT p50 / p95 (server)', `${fmt.ms(s.down?.rtt_ms?.p50)} / ${fmt.ms(s.down?.rtt_ms?.p95)}`],
      ['server blocked by SCTP buffer', `${s.down?.blocked_ticks ?? 0} ticks`],
    ] : []),
    ['ACK mode', r.config.ack?.mode ?? 'packet'],
    ...(r.up_stopped_early ? [['stopped early', r.up_stopped_early.reason]] : []),
    ...(a ? [
      ['server: IAT p50 / p90', `${fmt.ms(a.iat_ms?.p50)} / ${fmt.ms(a.iat_ms?.p90)}`],
      ['server: back-to-back arrivals', fmt.pct(a.back_to_back_fraction)],
      ['server: forward-delay excess p50', fmt.ms(a.fwd_excess_ms?.p50)],
      ['server: bins flagged SCTP-limited', `${a.bins_flagged_quic_limited} / ${a.bins}`],
    ] : []),
  ]);

  const bins = (arr, binMs) => (arr ?? []).map((v, i) => [(i * binMs) / 1000, v / 1e6]);
  const tl = (arr, f) => (arr ?? []).map((p) => [p.t / 1000, f(p)]);
  drawChart($('rateChart'), $('rateLegend'), [
    { name: 'up CC rate (browser)', color: '#2563eb', points: tl(r.up?.timeline, (p) => p.cc.rate_bps / 1e6) },
    { name: 'up goodput @server', color: '#60a5fa', points: bins(s.up?.goodput_bps_bins, s.up?.bin_ms) },
    { name: 'down CC rate (server)', color: '#16a34a', points: tl(s.down?.timeline, (p) => p.cc.rate_bps / 1e6) },
    { name: 'down goodput @browser', color: '#86efac', points: bins(r.down?.goodput_bps_bins, r.down?.bin_ms) },
  ], 'Mbps');
  drawChart($('rttChart'), $('rttLegend'), [
    { name: 'up srtt (browser app)', color: '#2563eb', points: tl(r.up?.timeline, (p) => p.srtt) },
    { name: 'down srtt (server app)', color: '#16a34a', points: tl(s.down?.timeline, (p) => p.srtt) },
    { name: 'server SCTP srtt', color: '#f59e0b', points: tl(s.down?.timeline, (p) => p.sctp?.srtt_ms) },
  ], 'ms');
  drawChart($('sctpChart'), $('sctpLegend'), [
    { name: 'browser: probe bufferedAmount (KB)', color: '#9333ea', points: tl(r.up?.timeline, (p) => (p.buffered ?? 0) / 1024) },
    { name: 'server: SCTP cwnd (KB)', color: '#dc2626', points: tl(s.down?.timeline, (p) => (p.sctp?.cwnd ?? 0) / 1024) },
    { name: 'server: SCTP flight (KB)', color: '#0891b2', points: tl(s.down?.timeline, (p) => (p.sctp?.flight_size ?? 0) / 1024) },
  ], 'KB');
}

$('download').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(lastResults, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(blob),
    download: `rtc-probe-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
  });
  a.click();
  URL.revokeObjectURL(a.href);
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = form.elements;
  const config = {
    signalUrl: f.signalUrl.value,
    stunUrl: f.stunUrl.value.trim() || null,
    mode: f.mode.value,
    durationS: Number(f.durationS.value),
    size: Number(f.size.value),
    upCC: { name: f.upCCName.value, params: JSON.parse(f.upCCParams.value || '{}') },
    downCC: { name: f.downCCName.value, params: JSON.parse(f.downCCParams.value || '{}') },
    ack: { mode: f.ackMode.value, interval_ms: Number(f.ackIntervalMs.value), every_n: Number(f.ackEveryN.value) },
    queueGuardMs: Number(f.queueGuardMs.value) || Infinity,
    bufferedLimit: Number(f.bufferedLimit.value),
  };
  if (!('RTCPeerConnection' in window)) { log('WebRTC is not supported in this browser'); return; }
  $('run').disabled = true;
  $('download').disabled = true;
  $('status').textContent = 'running…';
  live = [];
  try {
    await run(config);
    $('status').textContent = 'done';
  } catch (err) {
    log(`ERROR: ${err?.stack ?? err}`);
    $('status').textContent = 'failed';
  } finally {
    $('run').disabled = false;
  }
});
