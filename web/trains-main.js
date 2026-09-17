// Packet-train experiment over WebTransport or WebRTC DataChannels.
//
// A train is N packets handed to the transport back to back; the receiver
// measures the spacing they arrive with. Because the sender stamps each packet
// as it hands it over, comparing the send-side spread with the arrival spread
// separates "the browser's pacer spread this burst" from "the network did".
//
// Mirrors server/trains.py.

import { drawChart } from './chart.js';
import * as proto from './protocol.js';

const $ = (id) => document.getElementById(id);
const form = $('form');
const nowMs = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Gaps between trains come from a worker: main-thread timers are throttled in
// background tabs, which stretches the gaps (and, in a hidden tab, the run).
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

let lastResults = null;

// When the page is served from the deployed host, point every transport at that
// host: WebTransport on its own UDP port, the WebRTC signaling and the HTTP
// train endpoints behind nginx on the same origin. Local dev keeps the
// per-server ports the form ships with.
if (!['localhost', '127.0.0.1', '::1', '[::1]'].includes(location.hostname)) {
  const f = form.elements;
  f.url.value = `https://${location.hostname}:4433/probe`;
  f.signalUrl.value = `${location.origin}/rtc/offer`;
  f.postUrl.value = `${location.origin}/trains/post`;
}

function log(text) {
  $('log').textContent += `[${new Date().toLocaleTimeString()}] ${text}\n`;
  $('log').scrollTop = $('log').scrollHeight;
}

const fmt = {
  ms: (v) => (v == null ? '–' : `${v.toFixed(2)} ms`),
  mbps: (v) => (v == null ? '–' : `${(v / 1e6).toFixed(1)} Mbps`),
  n: (v) => (v == null ? '–' : String(v)),
};

function percentiles(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const p = (q) => s[Math.floor(q * (s.length - 1))];
  return { min: s[0], p25: p(0.25), p50: p(0.5), p75: p(0.75), p95: p(0.95), max: s[s.length - 1],
    mean: s.reduce((a, b) => a + b, 0) / s.length };
}

// ---- receiver ---------------------------------------------------------------

class TrainReceiver {
  constructor() { this.trains = new Map(); this.expected = new Map(); this.packets = 0; }

  onPacket(pkt, recvTs) {
    this.packets++;
    this.expected.set(pkt.trainId, pkt.trainLen);
    if (!this.trains.has(pkt.trainId)) this.trains.set(pkt.trainId, []);
    this.trains.get(pkt.trainId).push([pkt.index, pkt.sendTs, recvTs, pkt.size]);
  }

  summary() {
    if (!this.trains.size) return null;
    const perTrain = [...this.trains.keys()].sort((a, b) => a - b).map((id) => {
      const rows = [...this.trains.get(id)].sort((a, b) => a[2] - b[2]);
      const recv = rows.map((r) => r[2]);
      const sent = rows.map((r) => r[1]);
      const size = rows[0][3];
      const iat = recv.slice(1).map((t, i) => t - recv[i]);
      const dispersion = recv.length > 1 ? recv[recv.length - 1] - recv[0] : null;
      const sendSpread = sent.length > 1 ? Math.max(...sent) - Math.min(...sent) : null;
      return {
        train_id: id,
        expected: this.expected.get(id),
        received: rows.length,
        reordered: rows.filter((r, i) => i > 0 && r[0] < rows[i - 1][0]).length,
        dispersion_ms: dispersion,
        send_spread_ms: sendSpread,
        iat_ms: iat,
        iat_p50_ms: iat.length ? percentiles(iat).p50 : null,
        implied_rate_bps: dispersion ? ((recv.length - 1) * size * 8 * 1000) / dispersion : null,
        first_recv_ts: recv[0],
      };
    });
    const complete = perTrain.filter((t) => t.received === t.expected && t.dispersion_ms);
    return {
      trains: perTrain.length,
      complete_trains: complete.length,
      packets: this.packets,
      lost_packets: perTrain.reduce((a, t) => a + (t.expected - t.received), 0),
      reordered_packets: perTrain.reduce((a, t) => a + t.reordered, 0),
      dispersion_ms: percentiles(complete.map((t) => t.dispersion_ms)),
      send_spread_ms: percentiles(perTrain.filter((t) => t.send_spread_ms != null).map((t) => t.send_spread_ms)),
      iat_ms: percentiles(perTrain.flatMap((t) => t.iat_ms)),
      implied_rate_bps: percentiles(complete.map((t) => t.implied_rate_bps)),
      per_train: perTrain,
    };
  }
}

async function sendTrains(send, cfg, onTrain) {
  const spreads = [];
  let sent = 0;
  const tick = ticker(cfg.gapMs);
  for (let trainId = 0; trainId < cfg.trains; trainId++) {
    const first = nowMs();
    for (let index = 0; index < cfg.trainLen; index++) {
      send(proto.encodeTrain(proto.FLOW_UP, trainId, index, cfg.trainLen, nowMs(), cfg.size));
      sent++;
    }
    spreads.push(nowMs() - first);
    onTrain?.(trainId + 1, spreads[spreads.length - 1]);
    await tick.next();
  }
  tick.stop();
  return { sent, trains: spreads.length, send_spread_ms: percentiles(spreads) };
}

// ---- transports -------------------------------------------------------------
// Both return { sendProbe, onProbe, sendControl, waitFor, close }.

function ndjsonControl(sendRaw) {
  const waiters = new Map();
  const inbox = [];
  let buf = '';
  return {
    feed(text) {
      buf += text;
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i); buf = buf.slice(i + 1);
        if (!line.trim()) continue;
        const msg = JSON.parse(line);
        const w = waiters.get(msg.type);
        if (w) { waiters.delete(msg.type); w(msg); } else inbox.push(msg);
      }
    },
    send(msg) { sendRaw(JSON.stringify(msg) + '\n'); },
    waitFor(type, timeoutMs = 30000) {
      const idx = inbox.findIndex((m) => m.type === type);
      if (idx >= 0) return Promise.resolve(inbox.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => { waiters.delete(type); reject(new Error(`timeout waiting for ${type}`)); }, timeoutMs);
        waiters.set(type, (m) => { clearTimeout(t); resolve(m); });
      });
    },
  };
}

async function connectWebTransport(cfg, onProbe) {
  const options = {};
  try {
    const hash = (await (await fetch('cert-hash.json', { cache: 'no-store' })).json()).sha256_b64;
    options.serverCertificateHashes = [{ algorithm: 'sha-256', value: Uint8Array.from(atob(hash), (c) => c.charCodeAt(0)) }];
  } catch { /* CA-signed cert */ }
  const transport = new WebTransport(cfg.url, options);
  await transport.ready;
  const dg = transport.datagrams;
  if ('outgoingHighWaterMark' in dg) dg.outgoingHighWaterMark = 1024; // never drop trains locally
  const writer = (typeof dg.createWritable === 'function' ? dg.createWritable() : dg.writable).getWriter();
  const stream = await transport.createBidirectionalStream();
  const streamWriter = stream.writable.getWriter();
  const control = ndjsonControl((text) => streamWriter.write(new TextEncoder().encode(text)));
  (async () => {
    const reader = stream.readable.pipeThrough(new TextDecoderStream()).getReader();
    try { for (;;) { const { value, done } = await reader.read(); if (done) break; control.feed(value); } } catch { /* closed */ }
  })();
  (async () => {
    const reader = dg.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        onProbe(proto.decode(value), nowMs());
      }
    } catch { /* closed */ }
  })();
  return {
    label: `WebTransport (maxDatagramSize ${dg.maxDatagramSize ?? '?'})`,
    sendProbe: (buf) => writer.write(buf).catch(() => {}),
    control,
    close: () => transport.close(),
  };
}

async function connectWebRTC(cfg, onProbe) {
  const pc = new RTCPeerConnection({ iceServers: [] });
  const controlCh = pc.createDataChannel('control', { ordered: true });
  const probe = pc.createDataChannel('probe', { ordered: false, maxRetransmits: 0 });
  probe.binaryType = 'arraybuffer';
  const control = ndjsonControl((text) => controlCh.send(text));
  controlCh.onmessage = (e) => control.feed(typeof e.data === 'string' ? e.data : new TextDecoder().decode(e.data));
  probe.onmessage = (e) => onProbe(proto.decode(new Uint8Array(e.data)), nowMs());

  await pc.setLocalDescription(await pc.createOffer());
  await new Promise((resolve) => {
    if (pc.iceGatheringState === 'complete') return resolve();
    pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && resolve();
    setTimeout(resolve, 3000);
  });
  const answer = await (await fetch(cfg.signalUrl, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sdp: pc.localDescription.sdp, type: pc.localDescription.type }),
  })).json();
  await pc.setRemoteDescription(answer);
  await Promise.all([controlCh, probe].map((ch) => new Promise((resolve, reject) => {
    if (ch.readyState === 'open') return resolve();
    ch.onopen = resolve;
    ch.onerror = () => reject(new Error(`channel ${ch.label} failed`));
  })));
  return {
    label: 'WebRTC DataChannel (unordered, maxRetransmits 0)',
    sendProbe: (buf) => probe.send(buf),
    control,
    close: () => pc.close(),
  };
}

// ---- HTTP POST trains -------------------------------------------------------
// The TCP counterpart: each "packet" is a POST, timestamped at the server when
// its body finishes arriving. See server/http_trains.py for the caveats
// (several sockets, request overhead, TCP repairs loss below us).

async function runPostTrains(cfg) {
  const client = `c${Math.random().toString(36).slice(2, 10)}`;
  const body = new Uint8Array(cfg.size);
  const spreads = [];
  const responseMs = [];
  const tick = ticker(cfg.gapMs);
  let sent = 0;

  for (let trainId = 0; trainId < cfg.trains; trainId++) {
    const first = nowMs();
    const inflight = [];
    for (let index = 0; index < cfg.trainLen; index++) {
      const t = nowMs();
      const url = `${cfg.postUrl}?c=${client}&t=${trainId}&i=${index}&n=${cfg.trainLen}&ts=${t}`;
      inflight.push(fetch(url, { method: 'POST', body, keepalive: false })
        .then(() => responseMs.push(nowMs() - t))
        .catch(() => {}));
      sent++;
    }
    spreads.push(nowMs() - first);
    $('status').textContent = `sent ${trainId + 1}/${cfg.trains} POST trains`;
    await Promise.all(inflight);   // one train at a time, like the datagram version
    await tick.next();
  }
  tick.stop();

  const report = await (await fetch(`${cfg.reportUrl}?c=${client}&save=1`)).json();
  return {
    up_send: { sent, trains: spreads.length, send_spread_ms: percentiles(spreads), response_ms: percentiles(responseMs) },
    server: { up_trains: report.trains, connections: report.connections, saved: report.saved },
  };
}

// One POST per train: a single request whose body is trainLen * size bytes, so
// the bytes go back to back down one connection instead of being spread over
// the browser's connection pool. The server timestamps the body as it arrives.
async function runBulkTrains(cfg) {
  const client = `c${Math.random().toString(36).slice(2, 10)}`;
  const body = new Uint8Array(cfg.trainLen * cfg.size);
  const tick = ticker(cfg.gapMs);
  const responseMs = [];

  for (let trainId = 0; trainId < cfg.trains; trainId++) {
    const t = nowMs();
    const url = `${cfg.bulkUrl}?c=${client}&t=${trainId}&n=${cfg.trainLen}&size=${cfg.size}&ts=${t}`;
    try {
      await fetch(url, { method: 'POST', body });
      responseMs.push(nowMs() - t);
    } catch (e) { log(`train ${trainId} failed: ${e}`); }
    $('status').textContent = `sent ${trainId + 1}/${cfg.trains} bulk trains`;
    await tick.next();
  }
  tick.stop();

  const report = await (await fetch(`${cfg.reportUrl}?c=${client}&save=1`)).json();
  return {
    up_send: { trains: cfg.trains, bytes_per_train: body.length, response_ms: percentiles(responseMs) },
    server: { bulk: report.bulk, connections: report.connections, saved: report.saved },
  };
}

// ---- run --------------------------------------------------------------------

async function run(cfg) {
  if (cfg.transport === 'http-bulk') {
    const r = await runBulkTrains(cfg);
    const results = { transport: 'http-bulk', config: cfg, user_agent: navigator.userAgent, ...r };
    lastResults = results;
    const b = r.server.bulk;
    log(`server saw ${b?.connections ?? '?'} TCP connection(s) for ${b?.trains ?? 0} bulk trains` +
        `${r.server.saved ? `; saved ${r.server.saved}` : ''}`);
    render(results);
    $('download').disabled = false;
    return;
  }

  if (cfg.transport === 'http-post') {
    const r = await runPostTrains(cfg);
    const results = { transport: 'http-post', config: cfg, user_agent: navigator.userAgent, ...r };
    lastResults = results;
    log(`server saw ${Object.keys(r.server.connections ?? {}).length} TCP connection(s)` +
        `${r.server.saved ? `; saved ${r.server.saved}` : ''}`);
    render(results);
    $('download').disabled = false;
    return;
  }

  const down = new TrainReceiver();
  const conn = cfg.transport === 'webtransport'
    ? await connectWebTransport(cfg, (pkt, t) => { if (pkt?.type === proto.TRAIN && pkt.flow === proto.FLOW_DOWN) down.onPacket(pkt, t); })
    : await connectWebRTC(cfg, (pkt, t) => { if (pkt?.type === proto.TRAIN && pkt.flow === proto.FLOW_DOWN) down.onPacket(pkt, t); });
  log(`connected: ${conn.label}`);

  conn.control.send({
    type: 'start', mode: 'none', duration_s: 0, size: cfg.size,
    trains: { direction: cfg.direction, train_len: cfg.trainLen, trains: cfg.trains, gap_ms: cfg.gapMs, size: cfg.size },
    client_time_ms: nowMs(), user_agent: navigator.userAgent,
  });
  await conn.control.waitFor('started');

  let upResult = null;
  if (cfg.direction === 'up' || cfg.direction === 'both') {
    log(`sending ${cfg.trains} trains of ${cfg.trainLen} x ${cfg.size} B, ${cfg.gapMs} ms apart`);
    upResult = await sendTrains(conn.sendProbe, cfg, (n, spread) => {
      if (n % 10 === 0) $('status').textContent = `sent ${n}/${cfg.trains} trains (last burst spread ${spread.toFixed(1)} ms)`;
    });
  }
  if (cfg.direction === 'down' || cfg.direction === 'both') {
    log('waiting for server trains…');
    await conn.control.waitFor('trains_done', cfg.trains * cfg.gapMs + 20000).catch((e) => log(`no trains_done: ${e.message}`));
    await sleep(500); // let the tail arrive
  }

  conn.control.send({ type: 'finish' });
  const serverReport = await conn.control.waitFor('server_report').catch(() => ({}));

  const results = {
    transport: cfg.transport, config: cfg, user_agent: navigator.userAgent,
    up_send: upResult,
    down_trains: down.summary(),
    server: serverReport,
  };
  lastResults = results;
  render(results);
  conn.control.send({ type: 'results', ...results });
  await conn.control.waitFor('saved', 20000).then((m) => log(`server saved ${m.file}`)).catch(() => {});
  conn.close();
  $('download').disabled = false;
}

function render(r) {
  const up = r.server?.up_trains;   // browser -> server, measured at the server
  const dn = r.down_trains;         // server -> browser, measured here
  const tiles = [];
  const add = (label, value) => tiles.push(`<div class="stat"><b>${value}</b><span>${label}</span></div>`);

  if (r.transport === 'http-bulk') {
    const b = r.server?.bulk;
    add('TCP connections used', fmt.n(b?.connections));
    add('bytes per train', fmt.n(r.up_send?.bytes_per_train));
    add('reads per train p50 (server)', fmt.n(b?.chunks_per_train?.p50));
    add('dispersion p50 @server', fmt.ms(b?.dispersion_ms?.p50));
    add('read IAT p50 / p95', `${fmt.ms(b?.iat_ms?.p50)} / ${fmt.ms(b?.iat_ms?.p95)}`);
    add('implied rate p50', fmt.mbps(b?.implied_rate_bps?.p50));
    add('POST response time p50', fmt.ms(r.up_send?.response_ms?.p50));
    $('stats').innerHTML = tiles.join('');
    const per = b?.per_train ?? [];
    drawChart($('dispersionChart'), $('dispersionLegend'), [
      { name: 'bulk: dispersion per train', color: '#2563eb', points: per.map((t, i) => [i, t.dispersion_ms]) },
    ], 'ms (x = train #)');
    drawChart($('rateChart'), $('rateLegend'), [
      { name: 'bulk: implied rate', color: '#2563eb', points: per.map((t, i) => [i, (t.implied_rate_bps ?? 0) / 1e6]) },
    ], 'Mbps (x = train #)');
    drawChart($('iatChart'), $('iatLegend'), [
      { name: 'bulk: IAT between server reads', color: '#16a34a', points: per.flatMap((t) => (t.iat_ms ?? []).map((v, i) => [t.train_id + i / Math.max(1, t.chunks), v])) },
    ], 'ms (x = train #)');
    return;
  }
  if (r.transport === 'http-post') {
    add('TCP connections used', fmt.n(Object.keys(r.server?.connections ?? {}).length));
    add('POST response time p50', fmt.ms(r.up_send?.response_ms?.p50));
  }
  if (up) {
    add('UP trains complete', `${up.complete_trains} / ${up.trains}`);
    add('UP send-side spread p50 (browser)', fmt.ms(up.send_spread_ms?.p50));
    add('UP dispersion p50 @server', fmt.ms(up.dispersion_ms?.p50));
    add('UP IAT p50 / p95', `${fmt.ms(up.iat_ms?.p50)} / ${fmt.ms(up.iat_ms?.p95)}`);
    add('UP implied rate p50', fmt.mbps(up.implied_rate_bps?.p50));
    add('UP lost / reordered', `${up.lost_packets} / ${up.reordered_packets}`);
  }
  if (dn) {
    add('DOWN trains complete', `${dn.complete_trains} / ${dn.trains}`);
    add('DOWN send-side spread p50 (server)', fmt.ms(dn.send_spread_ms?.p50));
    add('DOWN dispersion p50 @browser', fmt.ms(dn.dispersion_ms?.p50));
    add('DOWN IAT p50 / p95', `${fmt.ms(dn.iat_ms?.p50)} / ${fmt.ms(dn.iat_ms?.p95)}`);
    add('DOWN implied rate p50', fmt.mbps(dn.implied_rate_bps?.p50));
    add('DOWN lost / reordered', `${dn.lost_packets} / ${dn.reordered_packets}`);
  }
  $('stats').innerHTML = tiles.join('');

  const series = [];
  if (up) {
    series.push({ name: 'up: dispersion per train', color: '#2563eb', points: up.per_train.map((t, i) => [i, t.dispersion_ms]) });
    series.push({ name: 'up: send-side spread', color: '#93c5fd', points: up.per_train.map((t, i) => [i, t.send_spread_ms]) });
  }
  if (dn) {
    series.push({ name: 'down: dispersion per train', color: '#16a34a', points: dn.per_train.map((t, i) => [i, t.dispersion_ms]) });
    series.push({ name: 'down: send-side spread', color: '#86efac', points: dn.per_train.map((t, i) => [i, t.send_spread_ms]) });
  }
  drawChart($('dispersionChart'), $('dispersionLegend'), series, 'ms (x = train #)');

  const rates = [];
  if (up) rates.push({ name: 'up: implied rate', color: '#2563eb', points: up.per_train.map((t, i) => [i, (t.implied_rate_bps ?? 0) / 1e6]) });
  if (dn) rates.push({ name: 'down: implied rate', color: '#16a34a', points: dn.per_train.map((t, i) => [i, (t.implied_rate_bps ?? 0) / 1e6]) });
  drawChart($('rateChart'), $('rateLegend'), rates, 'Mbps (x = train #)');

  // every IAT of every train, in order: shows the shape inside a burst
  const iatSeries = [];
  if (up) iatSeries.push({ name: 'up: IAT within trains', color: '#2563eb', points: up.per_train.flatMap((t) => (t.iat_ms ?? []).map((v, i) => [t.train_id + i / (t.expected || 1), v])) });
  if (dn) iatSeries.push({ name: 'down: IAT within trains', color: '#16a34a', points: dn.per_train.flatMap((t) => (t.iat_ms ?? []).map((v, i) => [t.train_id + i / (t.expected || 1), v])) });
  drawChart($('iatChart'), $('iatLegend'), iatSeries, 'ms (x = train #)');
}

$('download').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(lastResults, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), {
    href: URL.createObjectURL(blob),
    download: `trains-${lastResults.transport}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
  });
  a.click();
  URL.revokeObjectURL(a.href);
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = form.elements;
  const cfg = {
    transport: f.transport.value,
    url: f.url.value,
    signalUrl: f.signalUrl.value,
    postUrl: f.postUrl.value,
    bulkUrl: f.postUrl.value.replace(/\/post$/, '/bulk'),
    reportUrl: f.postUrl.value.replace(/\/post$/, '/report'),
    direction: f.direction.value,
    trainLen: Number(f.trainLen.value),
    trains: Number(f.trains.value),
    gapMs: Number(f.gapMs.value),
    size: Number(f.size.value),
  };
  $('run').disabled = true;
  $('download').disabled = true;
  $('status').textContent = 'running…';
  try {
    await run(cfg);
    $('status').textContent = 'done';
  } catch (err) {
    log(`ERROR: ${err?.stack ?? err}`);
    $('status').textContent = 'failed';
  } finally {
    $('run').disabled = false;
  }
});
