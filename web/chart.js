// Minimal multi-series line chart shared by the WebTransport and WebRTC pages.
// Minimal multi-series line chart. series: [{name, color, points: [[x, y]]}]
// `xMax` pins the x axis so several charts of the same run line up; omit it to
// scale to the data.
export function drawChart(canvas, legend, series, yLabel, xMax = null) {
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
  xMax = xMax ?? Math.max(...all.map((p) => p[0]), 1);
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
