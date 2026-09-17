// How fast will the browser's own congestion control let us send?
//
// The method is blunt on purpose: write datagrams as fast as the transport will
// accept them and let the server count what arrives. What makes the number mean
// something is the bookkeeping around it, because "bytes per second at the
// server" has four possible explanations (see server/saturation.py):
//
//   * the browser's CC refused the excess  -> the ceiling we want
//   * the network dropped it               -> the path, not the browser
//   * this page could not write fast enough -> a floor, not a ceiling
//   * the server could not keep up          -> not a measurement at all
//
// So the page tracks what it offered (writes attempted, bytes handed over) and
// the server reports what it received plus the loss split: application packets
// that vanished while their QUIC packet numbers were all present were dropped
// inside the browser, which is the proof that the browser was the limit.
//
// Two modes:
//   saturate - write flat out for the whole run; simplest ceiling measurement
//   ramp     - step the offered rate up and watch where delivered stops
//              following. The knee is the CC's rate, found without hammering
//              the link for the whole run.

import { drawChart } from './chart.js';
import * as proto from './protocol.js';
import { percentiles } from './rtt.js';
import { connectWebRTC, connectWebTransport } from './transport.js';

const $ = (id) => document.getElementById(id);
const form = $('form');
const nowMs = () => performance.timeOrigin + performance.now();

// Chrome's outgoingHighWaterMark/desiredSize do not work as a backpressure
// signal (desiredSize reads a constant 1), so the send loop keeps its own count
// of writes that have not resolved yet. This is the depth of that window: big
// enough to keep the transport busy, small enough that we are not the queue.
const DEFAULT_PENDING = 64;
const OFFER_SAMPLE_MS = 250;
// Saturation is not polite: cap the run so a phone on a metered link cannot
// burn through data because a tab was left open.
const MAX_DURATION_S = 120;

let lastResults = null;
let stopRun = false;

if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(location.hostname)) {
  const f = form.elements;
  f.url.value = `https://${location.hostname}:4433/probe`;
  f.signalUrl.value = `${location.origin}/rtc/offer`;
  f.loadUrl.value = `${location.origin}/trains/load`;
}

function log(text) {
  $('log').textContent += `[${new Date().toLocaleTimeString()}] ${text}\n`;
  $('log').scrollTop = $('log').scrollHeight;
}

const fmt = {
  mbps: (v) => (v == null ? '–' : `${(v / 1e6).toFixed(2)} Mbps`),
  ms: (v) => (v == null ? '–' : `${v.toFixed(2)} ms`),
  n: (v) => (v == null ? '–' : v.toLocaleString()),
  pct: (v) => (v == null ? '–' : `${(100 * v).toFixed(2)} %`),
  bytes: (v) => (v == null ? '–' : `${(v / 1e6).toFixed(1)} MB`),
};

function ticker(intervalMs) {
  const worker = new Worker('tick-worker.js');
  let resolve = null;
  worker.onmessage = () => { const r = resolve; resolve = null; r?.(); };
  worker.postMessage({ type: 'start', intervalMs });
  return {
    next: () => new Promise((r) => { resolve = r; }),
    stop: () => { worker.postMessage({ type: 'stop' }); worker.terminate(); },
  };
}

// ---- send loop --------------------------------------------------------------

// Everything the page knows about its own sending. `offered` is what we handed
// to the transport; `accepted` is what it acknowledged taking. The gap between
// them is the transport's queue - the gap between accepted and what the server
// received is the browser dropping datagrams its CC would not send.
const offer = {
  packets: 0, bytes: 0, accepted: 0, pending: 0, stalls: 0, rejected: 0,
  samples: [],   // [t_s, offered Mbps]
  start: null,
};

function resetOffer() {
  Object.assign(offer, { packets: 0, bytes: 0, accepted: 0, pending: 0, stalls: 0, rejected: 0,
    samples: [], start: null });
}

// SCTP gives no per-write completion signal, so backpressure there is
// bufferedAmount; beyond this we are queueing in the browser rather than
// measuring what it will send.
const BUFFERED_LIMIT = 256 * 1024;
// Never hand over more than this in one tick, or the loop starves the event
// loop and the write promises never get a chance to resolve.
const MAX_PER_TICK = 256;

async function blast(conn, cfg) {
  const tick = ticker(1);
  const end = nowMs() + cfg.durationS * 1000;
  offer.start = nowMs();

  // WebTransport: a write resolves when the datagram is accepted, so the count
  // of unresolved writes is real backpressure. A data channel just buffers, so
  // its own bufferedAmount is the only signal.
  const backpressured = cfg.transport === 'webtransport'
    ? () => offer.pending >= cfg.pending
    : () => (conn.queue?.() ?? 0) >= BUFFERED_LIMIT;

  let lastSample = offer.start;
  let bytesAtSample = 0;
  let seq = 0;
  let tokens = 0;              // ramp mode only
  let lastToken = offer.start;
  let stepIndex = 0;

  while (!stopRun) {
    const t = nowMs();
    if (t >= end) break;

    if (cfg.mode === 'ramp') {
      stepIndex = Math.min(cfg.steps.length - 1, Math.floor((t - offer.start) / (cfg.stepS * 1000)));
      const targetBps = cfg.steps[stepIndex] * 1e6;
      // ~50 ms of burst allowance, since a 1 ms timer is not that precise
      tokens = Math.min(tokens + (targetBps / 8) * ((t - lastToken) / 1000), (targetBps / 8) * 0.05);
      lastToken = t;
    }

    let thisTick = 0;
    while (thisTick < MAX_PER_TICK && !backpressured() && (cfg.mode !== 'ramp' || tokens >= cfg.size)) {
      // encodeData pads to the requested size, so the packet is size bytes.
      const buf = proto.encodeData(proto.FLOW_UP, seq++, nowMs(), cfg.size);
      offer.packets++;
      offer.bytes += cfg.size;
      if (cfg.mode === 'ramp') tokens -= cfg.size;
      try {
        const p = conn.sendProbe(buf);
        if (p && typeof p.then === 'function') {
          offer.pending++;
          p.then(() => { offer.pending--; offer.accepted++; }, () => { offer.pending--; });
        } else {
          offer.accepted++;    // data channel: send() is synchronous
        }
      } catch {
        offer.rejected++;      // e.g. the data channel's send queue is full
      }
      thisTick++;
    }
    if (backpressured()) offer.stalls++;

    if (t - lastSample >= OFFER_SAMPLE_MS) {
      const dt = t - lastSample;
      // bits per ms / 1000 = Mbit/s
      offer.samples.push([(t - offer.start) / 1000, ((offer.bytes - bytesAtSample) * 8) / dt / 1000]);
      bytesAtSample = offer.bytes;
      lastSample = t;
      $('status').textContent = `${((t - offer.start) / 1000).toFixed(0)} s: offered ${fmt.bytes(offer.bytes)}`
        + (cfg.mode === 'ramp' ? `, step ${cfg.steps[stepIndex]} Mbps` : '');
    }
    await tick.next();
  }
  tick.stop();

  const elapsed = Math.max(1, nowMs() - offer.start);
  return {
    mode: cfg.mode,
    offered_packets: offer.packets,
    offered_bytes: offer.bytes,
    accepted_packets: offer.accepted,
    rejected_writes: offer.rejected,
    offered_bps: (offer.bytes * 8 * 1000) / elapsed,
    write_stall_ticks: offer.stalls,
    duration_ms: elapsed,
    packet_size: cfg.size,
    steps: cfg.mode === 'ramp' ? cfg.steps : null,
    step_s: cfg.mode === 'ramp' ? cfg.stepS : null,
  };
}

// ---- run --------------------------------------------------------------------

const progress = [];   // server's live view: [{t_s, bps, packets, srtt}]

async function run(cfg) {
  resetOffer();
  progress.length = 0;
  const conn = cfg.transport === 'webtransport'
    ? await connectWebTransport(cfg, () => {}, { highWaterMark: cfg.pending })
    : await connectWebRTC(cfg, () => {});
  log(`connected: ${conn.label}`);

  if (conn.maxDatagramSize && cfg.size > conn.maxDatagramSize) {
    cfg.size = conn.maxDatagramSize;
    log(`packet size clamped to maxDatagramSize ${cfg.size} B (larger writes are rejected)`);
  }

  // The server needs mode "up" so it records QUIC packet numbers - without them
  // there is no way to tell a datagram the browser dropped from one the network
  // lost, and the whole measurement loses its meaning.
  conn.control.send({
    type: 'start', mode: 'up', duration_s: cfg.durationS, size: cfg.size,
    ack: { mode: 'none' },
    saturate: { progress_ms: 500, mode: cfg.mode },
    client_time_ms: nowMs(), user_agent: navigator.userAgent,
  });
  await conn.control.waitFor('started');

  const live = setInterval(() => renderLive(), 500);
  // The server pushes what it is actually receiving while we send.
  const pump = (async () => {
    for (;;) {
      const msg = await conn.control.waitFor('up_progress', cfg.durationS * 1000 + 30000).catch(() => null);
      if (!msg) return;
      progress.push({ t_s: msg.t_ms / 1000, bps: msg.bps, packets: msg.packets_total,
        srtt: msg.quic?.srtt_ms ?? null });
      if (stopRun) return;
    }
  })();

  log(`sending ${cfg.size} B datagrams ${cfg.mode === 'ramp' ? 'at stepped rates' : 'flat out'} for ${cfg.durationS} s`);
  const sendResult = await blast(conn, cfg);
  clearInterval(live);
  await new Promise((r) => setTimeout(r, 1000));   // let the tail arrive
  conn.control.send({ type: 'finish', client_offered: sendResult });
  const serverReport = await conn.control.waitFor('server_report', 30000).catch(() => ({}));
  pump.catch(() => {});

  const results = {
    experiment: 'capacity',
    transport: cfg.transport,
    config: cfg,
    user_agent: navigator.userAgent,
    client: sendResult,
    progress,
    server: serverReport,
  };
  lastResults = results;
  render(results);
  conn.control.send({ type: 'results', ...results });
  await conn.control.waitFor('saved', 20000).then((m) => log(`server saved ${m.file}`)).catch(() => {});
  conn.close();
  $('download').disabled = false;
}

// A TCP upload over the same path, for comparison: if a plain POST goes much
// faster than the datagram flood, the browser's datagram CC (or its send queue)
// is the limit, not the link.
async function runTcpComparison(cfg) {
  log(`TCP comparison: uploading for ~${cfg.durationS}s`);
  const body = new Uint8Array(8 * 1024 * 1024);
  const controller = new AbortController();
  const stop = setTimeout(() => controller.abort(), cfg.durationS * 1000);
  const t0 = nowMs();
  let bytes = 0;
  try {
    while (!controller.signal.aborted) {
      await fetch(`${cfg.loadUrl}/upload`, { method: 'POST', body, signal: controller.signal });
      bytes += body.byteLength;
    }
  } catch { /* aborted */ }
  clearTimeout(stop);
  const bps = (bytes * 8 * 1000) / Math.max(1, nowMs() - t0);
  log(`TCP upload: ${fmt.mbps(bps)} (${fmt.bytes(bytes)})`);
  $('tcpResult').textContent = `TCP upload ${fmt.mbps(bps)}`;
  return bps;
}

// ---- rendering --------------------------------------------------------------

function tiles(rows) {
  return rows.map(([label, value, flag]) =>
    `<div class="stat${flag ? ' flag' : ''}"><b>${value}</b><span>${label}</span></div>`).join('');
}

function renderLive() {
  const recent = progress.slice(-4);
  const rows = [
    ['offered so far', fmt.bytes(offer.bytes)],
    ['offered rate (last sample)', fmt.mbps((offer.samples.at(-1)?.[1] ?? 0) * 1e6 || null)],
    ['delivered rate @server', fmt.mbps(recent.at(-1)?.bps ?? null)],
    ['packets in flight (writes pending)', fmt.n(offer.pending)],
    ['write-window stalls', fmt.n(offer.stalls), offer.stalls > 0],
    ['server QUIC srtt', fmt.ms(recent.at(-1)?.srtt ?? null)],
  ];
  $('stats').innerHTML = tiles(rows);
  drawRateChart();
}

function drawRateChart() {
  const series = [
    { name: 'offered by the page', color: '#93c5fd', points: offer.samples },
    { name: 'delivered at the server', color: '#2563eb', points: progress.map((p) => [p.t_s, p.bps / 1e6]) },
  ];
  drawChart($('rateChart'), $('rateLegend'), series, 'Mbps');
  drawChart($('rttChart'), $('rttLegend'),
    [{ name: 'server-measured QUIC srtt (browser → server)', color: '#dc2626',
       points: progress.filter((p) => p.srtt != null).map((p) => [p.t_s, p.srtt]) }], 'ms');
}

function render(r) {
  const sat = r.server?.saturation;
  const ana = r.server?.up_analysis;
  const c = r.client;
  const rows = [
    ['steady-state rate @server', fmt.mbps(sat?.steady_rate_bps?.p50)],
    ['peak 100 ms bin', fmt.mbps(sat?.peak_bin_bps)],
    ['offered by the page', fmt.mbps(c?.offered_bps)],
    ['delivered / offered packets', `${fmt.n(sat?.delivered_packets)} / ${fmt.n(c?.offered_packets)}`],
    ['dropped inside the browser', fmt.n(sat?.dropped_in_browser), (sat?.dropped_in_browser ?? 0) > 0],
    ['lost in the network (QUIC pkts)', fmt.n(sat?.quic_packets_missing), (sat?.quic_packets_missing ?? 0) > 0],
    ['ramp to 90% of steady state', sat?.ramp_to_90pct_ms == null ? '–' : `${(sat.ramp_to_90pct_ms / 1000).toFixed(1)} s`],
    ['server QUIC srtt p50', fmt.ms(sat?.quic_srtt_p50_ms)],
    ['implied bytes in flight', sat?.implied_cwnd_bytes == null ? '–' : `${(sat.implied_cwnd_bytes / 1024).toFixed(0)} KB`],
    ['bins flagged browser-queued', fmt.n(ana?.bins_flagged_quic_limited), (ana?.bins_flagged_quic_limited ?? 0) > 0],
    ['write-window stalls', fmt.n(c?.write_stall_ticks)],
    ['data sent', fmt.bytes(c?.offered_bytes)],
  ];
  $('stats').innerHTML = tiles(rows);
  drawRateChart();
  $('verdict').innerHTML = verdict(r);
}

function verdict(r) {
  const sat = r.server?.saturation;
  if (!sat) return '<p class="note">No server-side analysis; the run may have been too short.</p>';
  const v = sat.verdict ?? {};
  const label = {
    'browser-cc': 'The browser\'s congestion control was the limit',
    path: 'The network path was the limit',
    sender: 'This page was the limit',
    unclear: 'Inconclusive',
  }[v.limited_by] ?? 'Inconclusive';
  const parts = [`<p><b>${label}.</b> ${v.explanation ?? ''}</p>`];
  parts.push(`<p class="note">Steady state ${fmt.mbps(sat.steady_rate_bps?.p50)} ` +
    `(p25 ${fmt.mbps(sat.steady_rate_bps?.p25)}, p95 ${fmt.mbps(sat.steady_rate_bps?.p95)}) over ` +
    `${sat.steady_bins} bins of ${sat.bin_ms} ms, after skipping the first ${(sat.ramp_ms / 1000).toFixed(1)} s of ramp. ` +
    `Of ${fmt.n(sat.offered_packets)} packets offered, ${fmt.pct(v.local_drop_fraction)} never left the browser and ` +
    `${fmt.pct(v.network_loss_fraction)} of what left was lost on the way.</p>`);
  if (r.config?.mode === 'ramp') {
    parts.push('<p class="note">Stepped mode: look for the step where the delivered line stops following the offered ' +
      'line on the chart above. That knee is the rate the browser stopped accepting more.</p>');
  }
  parts.push('<p class="note">This measures the rate <em>the browser\'s QUIC stack</em> settled at on this path. An ' +
    'app-level controller of your own can never exceed it, so it is the ceiling your congestion-control experiments ' +
    'run under - compare it with the TCP upload button to see whether the datagram path is more conservative.</p>');
  return parts.join('');
}

// ---- wiring -----------------------------------------------------------------

function readConfig() {
  const f = form.elements;
  const durationS = Math.min(Number(f.durationS.value), MAX_DURATION_S);
  const startMbps = Number(f.startMbps.value);
  const steps = [];
  for (let r = startMbps; r <= Number(f.maxMbps.value); r *= 2) steps.push(r);
  return {
    transport: f.transport.value,
    mode: f.mode.value,
    url: f.url.value,
    signalUrl: f.signalUrl.value,
    loadUrl: f.loadUrl.value.replace(/\/$/, ''),
    size: Number(f.size.value),
    durationS,
    pending: Number(f.pending.value) || DEFAULT_PENDING,
    stepS: Number(f.stepS.value),
    steps: steps.length ? steps : [startMbps],
  };
}

$('stop').addEventListener('click', () => { stopRun = true; $('status').textContent = 'stopping…'; });
$('tcp').addEventListener('click', () => runTcpComparison(readConfig()));

$('download').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(lastResults, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(blob),
    download: `capacity-${lastResults.transport}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
  });
  a.click();
  URL.revokeObjectURL(a.href);
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  stopRun = false;
  $('run').disabled = true;
  $('stop').disabled = false;
  $('download').disabled = true;
  $('status').textContent = 'running…';
  try {
    await run(readConfig());
    $('status').textContent = 'done';
  } catch (err) {
    log(`ERROR: ${err?.stack ?? err}`);
    $('status').textContent = 'failed';
  } finally {
    $('run').disabled = false;
    $('stop').disabled = true;
  }
});
