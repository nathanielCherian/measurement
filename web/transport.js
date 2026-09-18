// Connection setup shared by the packet-train and RTT-monitor pages.
//
// Both transports expose the same shape:
//   { label, sendProbe(bytes), control: {send, waitFor, feed}, stats(), close() }
// so an experiment can be written once and run over QUIC datagrams or an
// unreliable SCTP data channel.

import * as proto from './protocol.js';

const nowMs = () => performance.timeOrigin + performance.now();

// Newline-delimited JSON control messages, with a promise per message type.
export function ndjsonControl(sendRaw) {
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

// opts.highWaterMark / opts.maxAgeMs tune the browser's own datagram queue:
// trains want it deep (never drop a burst locally), an RTT probe wants it
// shallow (a queued probe measures the queue, not the path).
export async function connectWebTransport(cfg, onProbe, opts = {}) {
  const options = {};
  try {
    const hash = (await (await fetch('cert-hash.json', { cache: 'no-store' })).json()).sha256_b64;
    options.serverCertificateHashes = [{ algorithm: 'sha-256', value: Uint8Array.from(atob(hash), (c) => c.charCodeAt(0)) }];
  } catch { /* CA-signed cert */ }
  const transport = new WebTransport(cfg.url, options);
  await transport.ready;
  const dg = transport.datagrams;
  if (opts.highWaterMark != null && 'outgoingHighWaterMark' in dg) dg.outgoingHighWaterMark = opts.highWaterMark;
  if (opts.maxAgeMs != null && 'outgoingMaxAge' in dg) dg.outgoingMaxAge = opts.maxAgeMs;
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
    maxDatagramSize: dg.maxDatagramSize ?? null,
    // write() resolves when the datagram has been accepted for sending, so the
    // caller can time how long the browser held it.
    sendProbe: (buf) => writer.write(buf).catch(() => {}),
    // Room left in the browser's outgoing datagram queue: it falls as the queue
    // fills, so a drop towards 0 means the browser is holding packets back.
    queue: () => (writer.desiredSize == null ? null : writer.desiredSize),
    queueLabel: 'writer.desiredSize (slots free)',
    stats: () => transport.getStats?.().catch(() => null) ?? null,
    control,
    close: () => transport.close(),
  };
}

// POST the SDP offer and insist on getting an SDP answer back.
//
// The failure this exists for: when the signaling URL reaches something that
// is not rtc_server.py - nginx with no /rtc/ location, or a 502 because the
// service is down, or the static server 404ing - the reply is an HTML error
// page, and parsing it as JSON fails with "Unexpected token '<'", which tells
// you nothing about what is actually wrong. So check the status and the
// content type first, and report the URL, the status and what came back.
export async function signal(signalUrl, offer) {
  let res;
  try {
    res = await fetch(signalUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sdp: offer.sdp, type: offer.type }),
    });
  } catch (e) {
    throw new Error(`signaling POST to ${signalUrl} failed to connect (${e.message}). ` +
      `Is rtc_server.py running, and is the URL right?`);
  }

  const body = await res.text();
  const looksJson = (res.headers.get('content-type') || '').includes('json') || body.trim().startsWith('{');
  if (!res.ok || !looksJson) {
    const snippet = body.replace(/\s+/g, ' ').slice(0, 120);
    const hint = [502, 503, 504].includes(res.status)
      ? 'a proxy is there but cannot reach the signaling service behind it ' +
        '(check: systemctl status browser-cc-rtc, journalctl -u browser-cc-rtc)'
      : res.status === 404
        ? 'nothing is serving that path - nginx may have no /rtc/ location (re-run deploy/install.sh)'
        : [405, 501].includes(res.status)
          ? 'that URL is a plain file server, which will not accept a POST - point it at rtc_server.py ' +
            '(http://host:8080/offer, or /rtc/offer behind nginx)'
          : 'that URL is served by something other than rtc_server.py';
    throw new Error(`signaling POST to ${signalUrl} returned ${res.status} ${res.statusText} ` +
      `(${snippet || 'empty body'}) - ${hint}.`);
  }
  try {
    return JSON.parse(body);
  } catch {
    throw new Error(`signaling POST to ${signalUrl} returned invalid JSON: ${body.slice(0, 120)}`);
  }
}

// A data channel caps one message (64 KB in aiortc), and a run's results are
// bigger than that, so the NDJSON stream is cut into chunks; the peer
// reassembles on newlines (see rtc_session.py, which chunks the same way).
const CONTROL_CHUNK = 16000;

export async function connectWebRTC(cfg, onProbe) {
  const pc = new RTCPeerConnection({ iceServers: [] });
  const controlCh = pc.createDataChannel('control', { ordered: true });
  const probe = pc.createDataChannel('probe', { ordered: false, maxRetransmits: 0 });
  probe.binaryType = 'arraybuffer';
  const control = ndjsonControl((text) => {
    for (let i = 0; i < text.length; i += CONTROL_CHUNK) controlCh.send(text.slice(i, i + CONTROL_CHUNK));
  });
  controlCh.onmessage = (e) => control.feed(typeof e.data === 'string' ? e.data : new TextDecoder().decode(e.data));
  probe.onmessage = (e) => onProbe(proto.decode(new Uint8Array(e.data)), nowMs());

  await pc.setLocalDescription(await pc.createOffer());
  await new Promise((resolve) => {
    if (pc.iceGatheringState === 'complete') return resolve();
    pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && resolve();
    setTimeout(resolve, 3000);
  });
  await pc.setRemoteDescription(await signal(cfg.signalUrl, pc.localDescription));
  await Promise.all([controlCh, probe].map((ch) => new Promise((resolve, reject) => {
    if (ch.readyState === 'open') return resolve();
    ch.onopen = resolve;
    ch.onerror = () => reject(new Error(`channel ${ch.label} failed`));
  })));
  return {
    label: 'WebRTC DataChannel (unordered, maxRetransmits 0)',
    maxDatagramSize: null,
    // send() is synchronous and gives no completion signal, so there is no
    // write time to measure here - bufferedAmount is the queue signal instead.
    sendProbe: (buf) => { probe.send(buf); return null; },
    queue: () => probe.bufferedAmount,
    queueLabel: 'bufferedAmount (bytes queued)',
    stats: () => pc.getStats().catch(() => null),
    control,
    close: () => pc.close(),
  };
}
