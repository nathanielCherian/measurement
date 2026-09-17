const $ = (id) => document.getElementById(id);
const form = $('form');
const DEFAULT_PARAMS = {
  fixed: '{"rate_mbps": 2}',
  aimd: '{"start_mbps": 1, "max_mbps": 50, "ai_mbps": 0.5}',
};

// When the page is served from the probe host itself, default to that host.
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(location.hostname)) {
  form.elements.url.value = `https://${location.hostname}:4433/probe`;
}

let lastResults = null;
let live = [];

for (const which of ['up', 'down']) {
  form.elements[`${which}CCName`].addEventListener('change', (e) => {
    form.elements[`${which}CCParams`].value = DEFAULT_PARAMS[e.target.value];
  });
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

// Minimal multi-series line chart. series: [{name, color, points: [[x, y]]}]
function drawChart(canvas, legend, series, yLabel) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  const pad = { l: 56, r: 10, t: 10, b: 24 };
  const all = series.flatMap((s) => s.points).filter(([, y]) => Number.isFinite(y));
  legend.innerHTML = series.map((s) => `<span><i style="background:${s.color}"></i>${s.name}</span>`).join('');
  if (!all.length) return;
  const xMax = Math.max(...all.map((p) => p[0]), 1);
  const yMax = Math.max(...all.map((p) => p[1]), 1e-9) * 1.1;
  const X = (x) => pad.l + (x / xMax) * (w - pad.l - pad.r);
  const Y = (y) => h - pad.b - (y / yMax) * (h - pad.t - pad.b);

  ctx.strokeStyle = '#e3e3e6'; ctx.fillStyle = '#6e6e73'; ctx.font = '11px system-ui'; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = (yMax * i) / 4;
    ctx.beginPath(); ctx.moveTo(pad.l, Y(y)); ctx.lineTo(w - pad.r, Y(y)); ctx.stroke();
    ctx.fillText(y.toFixed(y < 10 ? 2 : 0), 4, Y(y) + 4);
  }
  ctx.fillText(`${yLabel}  /  time (s) →  ${(xMax).toFixed(1)}`, pad.l, h - 6);

  for (const s of series) {
    const pts = s.points.filter(([, y]) => Number.isFinite(y));
    if (!pts.length) continue;
    ctx.strokeStyle = s.color; ctx.lineWidth = 1.5; ctx.beginPath();
    pts.forEach(([x, y], i) => (i ? ctx.lineTo(X(x), Y(y)) : ctx.moveTo(X(x), Y(y))));
    ctx.stroke();
  }
}

function drawLive() {
  drawChart($('rateChart'), $('rateLegend'), [
    { name: 'up CC rate', color: '#2563eb', points: live.filter((p) => p.up).map((p) => [p.t / 1000, p.up.rate_bps / 1e6]) },
    { name: 'down received', color: '#16a34a', points: live.filter((p) => p.downBps != null).map((p) => [p.t / 1000, p.downBps / 1e6]) },
  ], 'Mbps');
  drawChart($('rttChart'), $('rttLegend'), [
    { name: 'up srtt (app)', color: '#2563eb', points: live.filter((p) => p.up?.srtt != null).map((p) => [p.t / 1000, p.up.srtt]) },
    { name: 'browser QUIC smoothedRtt', color: '#9333ea', points: live.filter((p) => p.wt?.smoothedRtt != null).map((p) => [p.t / 1000, p.wt.smoothedRtt]) },
  ], 'ms');
}

function drawFinal(res, server) {
  const bins = (arr, binMs) => (arr ?? []).map((v, i) => [(i * binMs) / 1000, v / 1e6]);
  const tl = (arr, f) => (arr ?? []).map((p) => [p.t / 1000, f(p)]);
  drawChart($('rateChart'), $('rateLegend'), [
    { name: 'up CC rate (browser)', color: '#2563eb', points: tl(res.up?.timeline, (p) => p.cc.rate_bps / 1e6) },
    { name: 'up goodput @server', color: '#60a5fa', points: bins(server.up?.goodput_bps_bins, server.up?.bin_ms) },
    { name: 'down CC rate (server)', color: '#16a34a', points: tl(server.down?.timeline, (p) => p.cc.rate_bps / 1e6) },
    { name: 'down goodput @browser', color: '#86efac', points: bins(res.down?.goodput_bps_bins, res.down?.bin_ms) },
  ], 'Mbps');
  drawChart($('rttChart'), $('rttLegend'), [
    { name: 'up srtt (browser app)', color: '#2563eb', points: tl(res.up?.timeline, (p) => p.srtt) },
    { name: 'down srtt (server app)', color: '#16a34a', points: tl(server.down?.timeline, (p) => p.srtt) },
    { name: 'down srtt (server QUIC)', color: '#f59e0b', points: tl(server.down?.timeline, (p) => p.quic?.srtt_ms) },
  ], 'ms');
}

// ---- QUIC ceiling indicators -------------------------------------------------

const fmtBool = (v) => (v == null ? '–' : v ? 'true' : 'false');
const fmtFrac = (v) => (v == null ? '–' : `${(v * 100).toFixed(0)}%`);

function renderTiles(id, items) {
  $(id).innerHTML = items
    .map(([label, value, flag]) => `<div class="stat${flag ? ' flag' : ''}"><b>${value}</b><span>${label}</span></div>`)
    .join('');
}

function renderQuicLive(m) {
  if (!m.statsExposed || (m.wt && !m.wt.populated)) {
    renderTiles('quicStats', [
      ['browser getStats()', m.statsExposed ? 'exposed but empty (all zeros)' : 'not exposed'],
      ['server analysis', 'after run'],
    ]);
    return;
  }
  const w = m.wt ?? {};
  renderTiles('quicStats', [
    ['browser QUIC smoothedRtt / minRtt', `${fmt.ms(w.smoothedRtt)} / ${fmt.ms(w.minRtt)}`],
    ['atSendCapacity', fmtBool(w.atSendCapacity), w.atSendCapacity],
    ['estimatedSendRate', fmt.mbps(w.estimatedSendRate)],
    ['app srtt − QUIC srtt', fmt.ms(w.appMinusQuicRtt), w.appMinusQuicRtt > 2],
    ['datagrams expired / lost outgoing', `${fmt.n(w.datagramsExpiredOutgoing)} / ${fmt.n(w.datagramsLostOutgoing)}`, w.datagramsExpiredOutgoing > 0],
  ]);
  const series = live.filter((p) => p.wt);
  drawChart($('delayChart'), $('delayLegend'), [
    { name: 'browser app srtt', color: '#2563eb', points: series.map((p) => [p.t / 1000, p.wt.appSrtt]) },
    { name: 'browser QUIC smoothedRtt', color: '#9333ea', points: series.map((p) => [p.t / 1000, p.wt.smoothedRtt]) },
  ], 'ms');
}

function renderQuicFinal(r, s) {
  const bq = r.browser_quic ?? { exposed: false };
  const a = s.up_analysis;
  const ls = a?.loss_split;
  const items = [];
  if (bq.exposed && bq.populated) {
    items.push(
      ['browser: atSendCapacity (of samples)', fmtFrac(bq.atSendCapacityFraction), bq.atSendCapacityFraction > 0],
      ['browser: estimatedSendRate p50', fmt.mbps(bq.estimatedSendRateBps?.p50)],
      ['browser: QUIC smoothedRtt p50', fmt.ms(bq.smoothedRttMs?.p50)],
      ['browser: app − QUIC RTT p50 / p95', `${fmt.ms(bq.appMinusQuicRttMs?.p50)} / ${fmt.ms(bq.appMinusQuicRttMs?.p95)}`, bq.appMinusQuicRttMs?.p50 > 2],
      ['browser: datagrams expired / lost out', `${fmt.n(bq.datagramsExpiredOutgoing)} / ${fmt.n(bq.datagramsLostOutgoing)}`, bq.datagramsExpiredOutgoing > 0],
    );
  } else {
    items.push(['browser getStats()', bq.exposed ? 'exposed but empty (all zeros)' : 'not exposed']);
  }
  if (a) {
    items.push(
      ['server: IAT p50 / p90', `${fmt.ms(a.iat_ms?.p50)} / ${fmt.ms(a.iat_ms?.p90)}`],
      ['server: IAT coefficient of variation', a.iat_ms?.cv == null ? '–' : a.iat_ms.cv.toFixed(2)],
      ['server: back-to-back arrivals (≤0.2 ms)', fmtFrac(a.back_to_back_fraction)],
      ['server: same-tick bursts re-spaced', fmtFrac(a.same_tick_respaced_fraction)],
      ['server: forward-delay excess p50 / p90', `${fmt.ms(a.fwd_excess_ms?.p50)} / ${fmt.ms(a.fwd_excess_ms?.p90)}`],
      ['server: probe loss / QUIC packet loss', ls ? `${ls.app_packets_missing} / ${ls.quic_packets_missing}` : '–'],
      ['server: dropped before send (est.)', ls ? fmt.n(ls.dropped_before_send_estimate) : '–', ls?.dropped_before_send_estimate > 0],
      ['server: bins flagged QUIC-limited', `${a.bins_flagged_quic_limited} / ${a.bins}`, a.bins_flagged_quic_limited > 0],
    );
  } else {
    items.push(['server analysis', 'no up traffic']);
  }
  if (s.netem) items.push(['server: emulated uplink', `${s.netem.config} (dropped ${s.netem.dropped_queue + s.netem.dropped_random})`]);
  renderTiles('quicStats', items);

  const tl = a?.timeline ?? [];
  const bs = (a?.bin_ms ?? 100) / 1000;
  const wt = r.webtransport_stats ?? [];
  drawChart($('delayChart'), $('delayLegend'), [
    { name: 'server: forward-delay excess', color: '#2563eb', points: tl.map((b) => [b.t / 1000 + bs / 2, b.fwd_excess_ms]) },
    { name: 'server: QUIC RTT excess', color: '#16a34a', points: tl.map((b) => [b.t / 1000 + bs / 2, b.quic_rtt_excess_ms]) },
    { name: 'server: local queue (lower bound)', color: '#dc2626', points: tl.map((b) => [b.t / 1000 + bs / 2, b.local_queue_ms_lb]) },
    { name: 'browser: app − QUIC RTT', color: '#9333ea', points: wt.filter((w) => w.populated).map((w) => [w.t / 1000, w.appMinusQuicRtt]) },
  ], 'ms');
  drawChart($('iatChart'), $('iatLegend'), [
    { name: 'server: IAT p50', color: '#0891b2', points: tl.map((b) => [b.t / 1000 + bs / 2, b.iat_p50_ms]) },
    { name: 'server: IAT p90', color: '#f59e0b', points: tl.map((b) => [b.t / 1000 + bs / 2, b.iat_p90_ms]) },
  ], 'ms');
}

$('download').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(lastResults, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(blob),
    download: `probe-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
  });
  a.click();
  URL.revokeObjectURL(a.href);
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = form.elements;
  let certHashB64 = null;
  try {
    certHashB64 = (await (await fetch('cert-hash.json', { cache: 'no-store' })).json()).sha256_b64;
  } catch {
    log('no cert-hash.json; connecting without serverCertificateHashes (needs a CA-trusted cert)');
  }
  const config = {
    url: f.url.value,
    mode: f.mode.value,
    durationS: Number(f.durationS.value),
    size: Number(f.size.value),
    browserCongestionControl: f.browserCongestionControl.value,
    upCC: { name: f.upCCName.value, params: JSON.parse(f.upCCParams.value || '{}') },
    downCC: { name: f.downCCName.value, params: JSON.parse(f.downCCParams.value || '{}') },
    ack: { mode: f.ackMode.value, interval_ms: Number(f.ackIntervalMs.value), every_n: Number(f.ackEveryN.value) },
    outgoingMaxAgeMs: Number(f.outgoingMaxAgeMs.value) || null,
    outgoingHighWaterMark: Number(f.outgoingHighWaterMark.value) || null,
    certHashB64,
  };

  if (!('WebTransport' in window)) { log('WebTransport is not supported in this browser'); return; }

  $('run').disabled = true;
  $('download').disabled = true;
  $('status').textContent = 'running…';
  live = [];
  let prevDown = null;

  const worker = new Worker('worker.js', { type: 'module' });
  const done = () => { $('run').disabled = false; worker.terminate(); };

  worker.onmessage = ({ data: m }) => {
    if (m.type === 'log') log(m.text);
    else if (m.type === 'error') { log(`ERROR: ${m.text}`); $('status').textContent = 'failed'; done(); }
    else if (m.type === 'progress') {
      const downBps = m.down && prevDown ? ((m.down.bytes - prevDown.bytes) * 8 * 1000) / (m.t - prevDown.t) : null;
      if (m.down) prevDown = { bytes: m.down.bytes, t: m.t };
      live.push({ ...m, downBps });
      renderStats([
        ['elapsed', `${(m.t / 1000).toFixed(1)} s`],
        ...(m.up ? [
          ['up CC rate', fmt.mbps(m.up.rate_bps)],
          ['up sent / acked / lost', `${m.up.sent} / ${m.up.acked} / ${m.up.lost}`],
          ['up srtt', fmt.ms(m.up.srtt)],
        ] : []),
        ...(m.down ? [
          ['down received', fmt.mbps(downBps)],
          ['down loss (so far)', fmt.pct(m.down.expected ? 1 - m.down.received / m.down.expected : null)],
          ['down jitter', fmt.ms(m.down.jitter)],
        ] : []),
        ['pending datagram writes', fmt.n(m.pendingWrites)],
      ]);
      renderQuicLive(m);
      drawLive();
    } else if (m.type === 'results') {
      const { results: r, serverReport: s } = m;
      lastResults = { browser: r, server: s };
      renderStats([
        ...(r.up ? [
          ['up sent / acked / lost', `${r.up.sent} / ${r.up.acked} / ${r.up.lost}`],
          ['up loss @server', fmt.pct(s.up?.loss_rate)],
          ['up RTT p50 / p95', `${fmt.ms(r.up.rtt_ms?.p50)} / ${fmt.ms(r.up.rtt_ms?.p95)}`],
          ['up blocked by browser queue', `${r.up.blocked_ticks} ticks`],
        ] : []),
        ...(r.down ? [
          ['down received / expected', `${r.down.received} / ${r.down.expected}`],
          ['down loss @browser', fmt.pct(r.down.loss_rate)],
          ['down RTT p50 / p95 (server)', `${fmt.ms(s.down?.rtt_ms?.p50)} / ${fmt.ms(s.down?.rtt_ms?.p95)}`],
          ['down jitter', fmt.ms(r.down.jitter_ms)],
          ['down reordered', fmt.n(r.down.reordered)],
          ['server blocked by QUIC', `${s.down?.blocked_ticks ?? 0} ticks`],
        ] : []),
        ['ACK mode', r.config.ack?.mode ?? 'packet'],
        ...(r.up ? [['up ACKs received (browser)', fmt.n(r.up.acks_received)]] : []),
        ...(r.down ? [['down ACKs sent (browser)', fmt.n(r.down.acks_sent)]] : []),
        ['max datagram size', fmt.n(r.max_datagram_size)],
      ]);
      drawFinal(r, s);
      renderQuicFinal(r, s);
      log(`done${m.savedFile ? `; server saved ${m.savedFile}` : ''}`);
      $('status').textContent = 'done';
      $('download').disabled = false;
      done();
    }
  };
  worker.postMessage({ type: 'run', config });
});
