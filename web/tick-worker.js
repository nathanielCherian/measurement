// Timer source for the WebRTC page: RTCPeerConnection only exists on the main
// thread, but main-thread timers are throttled in background tabs (which
// stalled pacing). Worker timers are not, so the worker ticks and the page
// sends.
let timer = null;
self.onmessage = (e) => {
  if (e.data.type === 'start') {
    clearInterval(timer);
    timer = setInterval(() => self.postMessage({ type: 'tick' }), e.data.intervalMs ?? 1);
  } else if (e.data.type === 'stop') {
    clearInterval(timer);
    timer = null;
  }
};
