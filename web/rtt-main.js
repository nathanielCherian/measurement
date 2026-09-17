// RTT monitor: a thin, steady stream of timestamped packets, parsed at both ends.
//
// Every packet carries the sender's clock reading at hand-off; the far side
// turns it around immediately with its own arrival time. That gives RTT without
// clock sync, and - because both ends also parse the timestamps of the stream
// they receive - a one-way delay and arrival-spacing view of each direction
// separately. See web/rtt.js and server/rtt.py.
//
// The load buttons run a saturating HTTP transfer against the same server while
// the stream keeps going: if the one-way delay climbs while the load runs and
// falls when it stops, the bottleneck queue is bufferbloated, and by how much.

import { drawChart } from './chart.js';
import * as proto from './protocol.js';
import { RttMonitor, RttStreamReceiver, percentiles, runRttStream } from './rtt.js';
import { normalizeQuicStats, summarizeQuicStats } from './stats.js';
import { connectWebRTC, connectWebTransport } from './transport.js';

const $ = (id) => document.getElementById(id);
const form = $('form');
const nowMs = () => performance.timeOrigin + performance.now();

// A probe that waits in the browser's own send queue measures that queue, not
// the path, so keep the local datagram queue shallow and let stale ones expire.
const RTT_WT_OPTS = { highWaterMark: 4, maxAgeMs: 2000 };
const LIVE_MS = 500;
const QUIC_STATS_MS = 250;

let lastResults = null;
let stopRun = false;
let queueLabel = 'local queue';
let serverReport = null;   // arrives only at the end of a run; null while one is live

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
  ms: (v) => (v == null ? '–' : `${v.toFixed(2)} ms`),
  mbps: (v) => (v == null ? '–' : `${v.toFixed(1)} Mbps`),
  pct: (v) => (v == null ? '–' : `${v.toFixed(2)} %`),
  n: (v) => (v == null ? '–' : String(v)),
};

// Worker-driven tick source: main-thread timers are throttled in a background
// tab, which would make the "steady" stream anything but.
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

// ---- load generator ---------------------------------------------------------
// A saturating TCP transfer to the same host, started and stopped by hand while
// the probe stream runs. Each sample is (seconds since run start, Mbps), so the
// load chart lines up with the delay chart above it.

const load = {
  controller: null,
  direction: null,
  samples: [],   // [t_s, mbps]
  events: [],    // {t_s, event}
  bytes: 0,
};

let runStart = null;   // nowMs() at the start of the probe stream, for the x axis
const sinceStart = () => (runStart == null ? 0 : (nowMs() - runStart) / 1000);

function noteLoad(event) {
  load.events.push({ t_s: sinceStart(), event });
  log(event);
}

async function startLoad(direction, cfg) {
  if (load.controller) stopLoad();
  const controller = new AbortController();
  load.controller = controller;
  load.direction = direction;
  noteLoad(`load started: ${direction}`);
  $('loadState').textContent = `${direction} load running`;

  try {
    if (direction === 'download') {
      // The sampling window spans requests: on a fast path one 32 MB response
      // is over in well under 250 ms, so per-response windows would never
      // close and the chart would stay empty.
      let windowBytes = 0;
      let windowStart = nowMs();
      while (!controller.signal.aborted) {
        const res = await fetch(`${cfg.loadUrl}/download?bytes=${cfg.loadBytes}&r=${Math.random()}`,
          { signal: controller.signal, cache: 'no-store' });
        const reader = res.body.getReader();
        for (;;) {
          const { value, done } = await reader.read();
          if (done || controller.signal.aborted) break;
          windowBytes += value.byteLength;
          load.bytes += value.byteLength;
          const dt = nowMs() - windowStart;
          if (dt >= 250) {
            load.samples.push([sinceStart(), (windowBytes * 8) / dt / 1000]);
            $('loadState').textContent = `download load ${((windowBytes * 8) / dt / 1000).toFixed(1)} Mbps`;
            windowBytes = 0; windowStart = nowMs();
          }
        }
      }
    } else {
      const body = new Uint8Array(cfg.loadBytes);
      while (!controller.signal.aborted) {
        const t0 = nowMs();
        const res = await fetch(`${cfg.loadUrl}/upload`, { method: 'POST', body, signal: controller.signal });
        await res.json().catch(() => null);
        const dt = nowMs() - t0;
        load.bytes += body.byteLength;
        const mbps = (body.byteLength * 8) / dt / 1000;
        load.samples.push([sinceStart(), mbps]);
        $('loadState').textContent = `upload load ${mbps.toFixed(1)} Mbps`;
      }
    }
  } catch (e) {
    if (!controller.signal.aborted) log(`load error: ${e.message ?? e}`);
  }
  if (load.controller === controller) { load.controller = null; $('loadState').textContent = 'load stopped'; }
}

function stopLoad() {
  if (!load.controller) return;
  load.controller.abort();
  load.controller = null;
  noteLoad('load stopped');
  $('loadState').textContent = 'load stopped';
}

// ---- run --------------------------------------------------------------------

async function run(cfg) {
  const upMonitor = new RttMonitor();       // our stream, our RTTs
  const downReceiver = new RttStreamReceiver();  // the server's stream, parsed here
  let conn = null;

  const onProbe = (pkt, t) => {
    if (!pkt) return;
    if (pkt.type === proto.PING && pkt.flow === proto.FLOW_DOWN) {
      // Echo before anything else: our own processing time lands in the
      // server's RTT sample otherwise.
      conn.sendProbe(proto.encodePong(pkt.flow, pkt.seq, pkt.sendTs, t));
      downReceiver.onPing(pkt, t);
    } else if (pkt.type === proto.PONG && pkt.flow === proto.FLOW_UP) {
      upMonitor.onPong(pkt, t);
    }
  };

  conn = cfg.transport === 'webtransport'
    ? await connectWebTransport(cfg, onProbe, RTT_WT_OPTS)
    : await connectWebRTC(cfg, onProbe);
  log(`connected: ${conn.label}`);
  queueLabel = conn.queueLabel ?? 'local queue';

  // Every hand-off is timed: how long the transport took to accept the write
  // (WebTransport resolves the promise once the datagram is accepted for
  // sending) and how full its own outgoing queue was at that moment. Those two,
  // beside the gap we actually achieved, are the browser-side view of spacing.
  const sendProbe = (bytes, seq) => {
    const t0 = nowMs();
    const queued = conn.queue?.() ?? null;
    const p = conn.sendProbe(bytes);
    if (p && typeof p.then === 'function') p.then(() => upMonitor.onWriteDone(seq, nowMs() - t0, queued));
    else upMonitor.onWriteDone(seq, null, queued);
  };

  conn.control.send({
    type: 'start', mode: 'none', duration_s: 0, size: cfg.size,
    rtt: { direction: cfg.direction, interval_ms: cfg.intervalMs, size: cfg.size, duration_s: cfg.durationS },
    client_time_ms: nowMs(), user_agent: navigator.userAgent,
  });
  await conn.control.waitFor('started');
  runStart = nowMs();
  chartXMax = cfg.durationS;
  serverReport = null;
  load.samples.length = 0; load.events.length = 0; load.bytes = 0;

  // The browser's own view of its QUIC connection, where it exposes one. Its
  // datagram counters say whether the browser dropped or expired packets in its
  // send queue - something no amount of app-level timing can see directly.
  const quicSamples = [];
  const quicTimer = setInterval(async () => {
    const raw = await conn.stats?.();
    if (raw) quicSamples.push(normalizeQuicStats(raw, nowMs(), upMonitor.srtt));
  }, QUIC_STATS_MS);

  const live = setInterval(() => renderLive(upMonitor, downReceiver), LIVE_MS);
  let upResult = null;
  try {
    if (cfg.direction === 'up' || cfg.direction === 'both') {
      const tick = ticker(cfg.intervalMs);
      log(`sending a ${cfg.size} B packet every ${cfg.intervalMs} ms for ${cfg.durationS} s`);
      upResult = await runRttStream({
        send: sendProbe, flow: proto.FLOW_UP, nowMs, monitor: upMonitor,
        intervalMs: cfg.intervalMs, durationS: cfg.durationS, size: cfg.size, tick,
        isOpen: () => !stopRun,
      });
      tick.stop();
    }
    if (cfg.direction === 'down') {
      log('receiving the server stream…');
      await conn.control.waitFor('rtt_done', cfg.durationS * 1000 + 20000).catch((e) => log(`no rtt_done: ${e.message}`));
    }
    // let the last replies land
    await new Promise((r) => setTimeout(r, Math.min(2000, 3 * (upMonitor.srtt ?? 200))));
  } finally {
    clearInterval(live);
    clearInterval(quicTimer);
  }

  conn.control.send({ type: 'finish' });
  serverReport = await conn.control.waitFor('server_report').catch(() => ({}));

  const results = {
    experiment: 'rtt-monitor',
    transport: cfg.transport,
    config: cfg,
    user_agent: navigator.userAgent,
    up_send: upResult,
    up_monitor: upMonitor.summary(true),        // browser -> server -> browser
    down_receiver: downReceiver.summary(true),  // server -> browser, one way
    load: { events: load.events, samples: load.samples, bytes: load.bytes },
    queue_label: queueLabel,
    browser_quic: summarizeQuicStats(quicSamples),
    server: serverReport,
  };
  lastResults = results;
  render(results, upMonitor, downReceiver);
  conn.control.send({ type: 'results', ...results });
  await conn.control.waitFor('saved', 20000).then((m) => log(`server saved ${m.file}`)).catch(() => {});
  conn.close();
  $('download').disabled = false;
}

// ---- rendering --------------------------------------------------------------

function tiles(rows) {
  return rows.map(([label, value, flag]) =>
    `<div class="stat${flag ? ' flag' : ''}"><b>${value}</b><span>${label}</span></div>`).join('');
}

function renderLive(upMonitor, downReceiver) {
  const rows = [];
  if (upMonitor.replies) {
    const rtt = percentiles(upMonitor.rtts);
    rows.push(['packets sent / replied', `${upMonitor.sent} / ${upMonitor.replies}`]);
    rows.push(['min RTT', fmt.ms(upMonitor.minRtt)]);
    rows.push(['srtt', fmt.ms(upMonitor.srtt)]);
    rows.push(['RTT p50 / p95', `${fmt.ms(rtt?.p50)} / ${fmt.ms(rtt?.p95)}`]);
    rows.push(['queue above min p95', fmt.ms(percentiles(upMonitor.rtts.map((r) => r - upMonitor.minRtt))?.p95),
      (upMonitor.srtt ?? 0) - (upMonitor.minRtt ?? 0) > 50]);
  }
  if (upMonitor.sendGaps.length) {
    const gaps = percentiles(upMonitor.sendGaps);
    rows.push(['hand-off gap p50 / p95', `${fmt.ms(gaps?.p50)} / ${fmt.ms(gaps?.p95)}`]);
    const w = percentiles(upMonitor.writeMs);
    if (w) rows.push(['write accepted after p95 / max', `${fmt.ms(w.p95)} / ${fmt.ms(w.max)}`, w.p95 > 1]);
    const q = percentiles(upMonitor.queueDepth);
    if (q) rows.push([queueLabel, q.min === q.max ? `${q.min} (constant: not reported)`
      : `p50 ${q.p50.toFixed(0)} / max ${q.max.toFixed(0)}`]);
  }
  if (downReceiver.received) {
    rows.push(['down packets received', fmt.n(downReceiver.received)]);
    rows.push(['down jitter (RFC 3550)', fmt.ms(downReceiver.jitter)]);
    rows.push(['down one-way delay above min p95',
      fmt.ms(percentiles(downReceiver.owds.map((o) => o - downReceiver.minOwd))?.p95)]);
  }
  if (load.controller) rows.push(['load', `${load.direction}, ${(load.bytes / 1e6).toFixed(1)} MB`]);
  $('stats').innerHTML = tiles(rows);
  drawDelayChart(upMonitor, downReceiver);
  drawSpacingChart(upMonitor, downReceiver, serverReport);
  drawLoadChart();
}

// Both charts share an x axis (seconds since the run started) so a load
// starting at 5 s lines up with the delay it caused.
let chartXMax = null;

function drawDelayChart(upMonitor, downReceiver) {
  const t0 = runStart ?? 0;
  const series = [];
  const up = upMonitor.records ?? [];
  if (up.length) {
    series.push({ name: 'RTT (browser → server → browser)', color: '#2563eb',
      points: up.map((r) => [(r.t - t0) / 1000, r.rtt_ms]) });
    series.push({ name: 'srtt', color: '#93c5fd', points: up.map((r) => [(r.t - t0) / 1000, r.srtt_ms]) });
    series.push({ name: 'up leg above its min', color: '#dc2626',
      points: up.map((r) => [(r.t - t0) / 1000, r.up_excess_ms]) });
    series.push({ name: 'down leg above its min', color: '#16a34a',
      points: up.map((r) => [(r.t - t0) / 1000, r.down_excess_ms]) });
  }
  const dn = downReceiver.records ?? [];
  if (dn.length) {
    series.push({ name: 'server → browser one-way delay above its min', color: '#7c3aed',
      points: dn.map((r) => [(r.t - t0) / 1000, r.owd_excess_ms]) });
  }
  drawChart($('delayChart'), $('delayLegend'), series, 'ms', chartXMax);
}

// What happened to the spacing, step by step: the gap we achieved between
// hand-offs, how long the browser then held each write, and the gap the packets
// actually arrived with at the far end. Divergence between the first and the
// last is the browser (or the path) respacing the stream.
function drawSpacingChart(upMonitor, downReceiver, report) {
  const t0 = runStart ?? 0;
  const series = [];
  const sent = upMonitor.sendRecords ?? [];
  if (sent.length) {
    series.push({ name: 'browser: gap between hand-offs', color: '#2563eb',
      points: sent.map((r) => [(r.t - t0) / 1000, r.gap_ms]) });
    if (upMonitor.writeMs.length) {
      series.push({ name: 'browser: write accepted after', color: '#dc2626',
        points: sent.map((r) => [(r.t - t0) / 1000, r.write_ms]) });
    }
  }
  const dn = downReceiver.records ?? [];
  if (dn.length) {
    series.push({ name: 'browser: arrival gap (server → browser)', color: '#7c3aed',
      points: dn.map((r) => [(r.t - t0) / 1000, r.iat_ms]) });
  }
  // The server's view of our stream only arrives with the final report, as
  // 100 ms bins - enough to see whether it kept the spacing we sent.
  const serverBins = report?.up_rtt?.timeline;
  if (serverBins?.length) {
    series.push({ name: 'server: arrival gap of our stream (100 ms bins)', color: '#16a34a',
      points: serverBins.map((b) => [b.t / 1000, b.iat_ms]) });
  }
  drawChart($('spacingChart'), $('spacingLegend'), series, 'ms', chartXMax);
}

function drawLoadChart() {
  drawChart($('loadChart'), $('loadLegend'),
    [{ name: 'load generator throughput', color: '#b45309', points: load.samples }], 'Mbps', chartXMax);
}

function render(r, upMonitor, downReceiver) {
  renderLive(upMonitor, downReceiver);
  const rows = [];
  const up = r.up_monitor;
  const serverUp = r.server?.up_rtt;      // our stream as the server parsed it
  const serverDown = r.server?.down_rtt;  // the server's own RTT monitor
  if (up) {
    rows.push(['min RTT', fmt.ms(up.min_rtt_ms)]);
    rows.push(['RTT p50 / p95', `${fmt.ms(up.rtt_ms?.p50)} / ${fmt.ms(up.rtt_ms?.p95)}`]);
    rows.push(['queue above min p50 / p95', `${fmt.ms(up.queue_ms?.p50)} / ${fmt.ms(up.queue_ms?.p95)}`]);
    rows.push(['|ΔRTT| p95 (short-term jitter)', fmt.ms(up.ipdv_abs_ms?.p95)]);
    rows.push(['up leg above min p95', fmt.ms(up.up_excess_ms?.p95)]);
    rows.push(['down leg above min p95', fmt.ms(up.down_excess_ms?.p95)]);
    rows.push(['reply loss', fmt.pct(up.loss_pct)]);
  }
  if (up) {
    rows.push(['hand-off gap p50 / p95', `${fmt.ms(up.send_gap_ms?.p50)} / ${fmt.ms(up.send_gap_ms?.p95)}`]);
    if (up.write_ms) rows.push(['write accepted after p95 / max',
      `${fmt.ms(up.write_ms.p95)} / ${fmt.ms(up.write_ms.max)}`, up.write_ms.p95 > 1]);
    if (up.queue_depth) rows.push([queueLabel, up.queue_depth.min === up.queue_depth.max
      ? `${up.queue_depth.min} (constant: not reported)`
      : `p50 ${up.queue_depth.p50.toFixed(0)} / max ${up.queue_depth.max.toFixed(0)}`]);
  }
  if (serverUp) {
    rows.push(['@server: one-way delay above min p95', fmt.ms(serverUp.owd_excess_ms?.p95)]);
    rows.push(['@server: arrival spacing p50 / p95', `${fmt.ms(serverUp.iat_ms?.p50)} / ${fmt.ms(serverUp.iat_ms?.p95)}`]);
    rows.push(['@server: send spacing p50 / p95', `${fmt.ms(serverUp.send_iat_ms?.p50)} / ${fmt.ms(serverUp.send_iat_ms?.p95)}`]);
    rows.push(['@server: spacing change |Δ| p95', fmt.ms(serverUp.ipdv_abs_ms?.p95)]);
    rows.push(['@server: jitter / loss', `${fmt.ms(serverUp.jitter_ms)} / ${fmt.pct(serverUp.loss_pct)}`]);
  }
  if (r.down_receiver) {
    rows.push(['down: one-way delay above min p95', fmt.ms(r.down_receiver.owd_excess_ms?.p95)]);
    rows.push(['down: arrival spacing p50', fmt.ms(r.down_receiver.iat_ms?.p50)]);
    rows.push(['down: jitter / loss', `${fmt.ms(r.down_receiver.jitter_ms)} / ${fmt.pct(r.down_receiver.loss_pct)}`]);
  }
  if (serverDown) rows.push(['server-side RTT p50', fmt.ms(serverDown.rtt_ms?.p50)]);
  const bq = r.browser_quic;
  if (bq?.populated) {
    rows.push(['browser: datagrams expired in its queue', fmt.n(bq.datagramsExpiredOutgoing),
      (bq.datagramsExpiredOutgoing ?? 0) > 0]);
    rows.push(['browser: datagrams lost outgoing', fmt.n(bq.datagramsLostOutgoing)]);
    rows.push(['browser: app srtt − QUIC srtt p95', fmt.ms(bq.appMinusQuicRttMs?.p95),
      (bq.appMinusQuicRttMs?.p95 ?? 0) > 5]);
  } else if (bq?.exposed) {
    rows.push(['browser: getStats()', 'exposed but all zero']);
  } else {
    rows.push(['browser: getStats()', 'not available']);
  }
  if (load.samples.length) {
    rows.push(['load throughput p50', fmt.mbps(percentiles(load.samples.map((s) => s[1]))?.p50)]);
    rows.push(['load bytes', `${(load.bytes / 1e6).toFixed(1)} MB`]);
  }
  $('stats').innerHTML = tiles(rows);
  $('verdict').innerHTML = verdict(r);
}

// Split the delay: which direction did the extra milliseconds appear in, and
// did they appear only while the load was running?
function verdict(r) {
  const up = r.up_monitor;
  if (!up) return '';
  const upx = up.up_excess_ms?.p95 ?? 0;
  const dnx = up.down_excess_ms?.p95 ?? 0;
  const queue = up.queue_ms?.p95 ?? 0;
  const parts = [];
  if (queue < 5) parts.push(`<b>No standing queue:</b> RTT stayed within ${queue.toFixed(1)} ms of its minimum.`);
  else parts.push(`<b>RTT rose ${queue.toFixed(1)} ms above its minimum (p95)</b>, and the excess sat mostly on the ` +
    `${upx > dnx ? 'browser → server' : 'server → browser'} leg (${upx.toFixed(1)} ms up vs ${dnx.toFixed(1)} ms down).`);
  // Three spacings in a row: what we asked for, what we handed over, what
  // arrived. Each step that widens the spread names a different culprit.
  const serverUp = r.server?.up_rtt;
  const asked = r.config?.intervalMs;
  const handoff = up.send_gap_ms;
  const arrived = serverUp?.iat_ms;
  if (handoff) {
    const spread = (p) => (p ? p.p95 - p.p50 : null);
    const bits = [`asked for ${asked} ms spacing; hand-offs came out at p50 ${handoff.p50.toFixed(1)} ms ` +
      `(p95 ${handoff.p95.toFixed(1)} ms, so the page's own timer added ${(spread(handoff) ?? 0).toFixed(1)} ms of spread)`];
    if (up.write_ms && up.write_ms.p95 > 1) {
      bits.push(`<b>the browser held writes</b>: accepting a datagram took up to ${up.write_ms.max.toFixed(1)} ms ` +
        `(p95 ${up.write_ms.p95.toFixed(1)} ms), which is its own send queue, not the network`);
    } else if (up.write_ms) {
      bits.push(`writes were accepted immediately (p95 ${up.write_ms.p95.toFixed(2)} ms), so nothing queued locally`);
    }
    if (arrived) {
      bits.push(`at the server the same packets arrived p50 ${arrived.p50.toFixed(1)} ms apart ` +
        `(p95 ${arrived.p95.toFixed(1)} ms): the spread grew by ${((spread(arrived) ?? 0) - (spread(handoff) ?? 0)).toFixed(1)} ms ` +
        `between hand-off and arrival`);
    }
    parts.push(bits.join('; ') + '.');
  }
  if (r.load?.events?.length) {
    parts.push(`Load generator: ${r.load.events.map((e) => `${e.event} @ ${e.t_s.toFixed(1)} s`).join(', ')}. ` +
      `Compare the delay chart before and after those marks - delay that only appears while the transfer runs is ` +
      `queueing in the bottleneck, not a change of route.`);
  } else {
    parts.push('No load was run. Start an upload or download load and watch whether the one-way delay climbs.');
  }
  return parts.map((p) => `<p class="note">${p}</p>`).join('');
}

// ---- wiring -----------------------------------------------------------------

function readConfig() {
  const f = form.elements;
  return {
    transport: f.transport.value,
    url: f.url.value,
    signalUrl: f.signalUrl.value,
    loadUrl: f.loadUrl.value.replace(/\/$/, ''),
    loadBytes: Number(f.loadBytes.value) * 1024 * 1024,
    direction: f.direction.value,
    intervalMs: Number(f.intervalMs.value),
    size: Number(f.size.value),
    durationS: Number(f.durationS.value),
  };
}

$('startUpload').addEventListener('click', () => startLoad('upload', readConfig()));
$('startDownload').addEventListener('click', () => startLoad('download', readConfig()));
$('stopLoad').addEventListener('click', stopLoad);
$('stop').addEventListener('click', () => { stopRun = true; $('status').textContent = 'stopping…'; });

$('download').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(lastResults, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(blob),
    download: `rtt-${lastResults.transport}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
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
