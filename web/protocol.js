// Binary datagram format shared with server/protocol.py (big-endian).
// DATA: type u8 | flow u8 | seq u32 | send_ts f64 (ms) | padding
// ACK:  type u8 | flow u8 | seq u32 | echo_send_ts f64 | recv_ts f64
// ACK_BLOCK: type u8 | flow u8 | ack_id u32 | largest u32 | largest_recv_ts f64 | ack_delay_ms f32 |
//            low u32 | n_ranges u8 | (first u32, last u32)* | n_ts u16 | (seq u32, recv_offset_ms f32)*
//   See server/protocol.py for semantics.
// `flow` is the direction of the DATA a packet refers to.

export const DATA = 1;
export const ACK = 2;
export const ACK_BLOCK = 3;
export const MAX_ACK_RANGES = 16;
export const MAX_DATAGRAM_PAYLOAD = 1000;
const BLOCK_HEAD_SIZE = 27;

export function blockTsCapacity(nRanges) {
  return Math.floor((MAX_DATAGRAM_PAYLOAD - BLOCK_HEAD_SIZE - nRanges * 8 - 2) / 8);
}

export function encodeAckBlock(flow, ackId, largest, largestRecvTs, ackDelayMs, low, ranges, timestamps) {
  const buf = new Uint8Array(BLOCK_HEAD_SIZE + ranges.length * 8 + 2 + timestamps.length * 8);
  const v = new DataView(buf.buffer);
  v.setUint8(0, ACK_BLOCK); v.setUint8(1, flow); v.setUint32(2, ackId >>> 0); v.setUint32(6, largest >>> 0);
  v.setFloat64(10, largestRecvTs); v.setFloat32(18, ackDelayMs); v.setUint32(22, low >>> 0); v.setUint8(26, ranges.length);
  let off = BLOCK_HEAD_SIZE;
  for (const [first, last] of ranges) { v.setUint32(off, first); v.setUint32(off + 4, last); off += 8; }
  v.setUint16(off, timestamps.length); off += 2;
  for (const [seq, recvTs] of timestamps) { v.setUint32(off, seq); v.setFloat32(off + 4, recvTs - largestRecvTs); off += 8; }
  return buf;
}
export const FLOW_UP = 0; // browser -> server
export const FLOW_DOWN = 1; // server -> browser
export const DATA_HEADER_SIZE = 14;
export const ACK_SIZE = 22;

export function encodeData(flow, seq, sendTs, size) {
  const buf = new Uint8Array(Math.max(size, DATA_HEADER_SIZE));
  const v = new DataView(buf.buffer);
  v.setUint8(0, DATA);
  v.setUint8(1, flow);
  v.setUint32(2, seq >>> 0);
  v.setFloat64(6, sendTs);
  return buf;
}

export function encodeAck(flow, seq, echoSendTs, recvTs) {
  const buf = new Uint8Array(ACK_SIZE);
  const v = new DataView(buf.buffer);
  v.setUint8(0, ACK);
  v.setUint8(1, flow);
  v.setUint32(2, seq >>> 0);
  v.setFloat64(6, echoSendTs);
  v.setFloat64(14, recvTs);
  return buf;
}

export function decode(bytes) {
  const v = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.byteLength >= DATA_HEADER_SIZE && bytes[0] === DATA) {
    return { type: DATA, flow: v.getUint8(1), seq: v.getUint32(2), sendTs: v.getFloat64(6), size: bytes.byteLength };
  }
  if (bytes.byteLength >= ACK_SIZE && bytes[0] === ACK) {
    return { type: ACK, flow: v.getUint8(1), seq: v.getUint32(2), echoSendTs: v.getFloat64(6), recvTs: v.getFloat64(14) };
  }
  if (bytes.byteLength >= BLOCK_HEAD_SIZE && bytes[0] === ACK_BLOCK) {
    const largestRecvTs = v.getFloat64(10);
    const n = v.getUint8(26);
    let off = BLOCK_HEAD_SIZE;
    const ranges = [];
    for (let i = 0; i < n; i++) { ranges.push([v.getUint32(off), v.getUint32(off + 4)]); off += 8; }
    const nTs = v.getUint16(off); off += 2;
    const timestamps = [];
    for (let i = 0; i < nTs; i++) { timestamps.push([v.getUint32(off), largestRecvTs + v.getFloat32(off + 4)]); off += 8; }
    return {
      type: ACK_BLOCK, flow: v.getUint8(1), ackId: v.getUint32(2), largest: v.getUint32(6), largestRecvTs,
      ackDelayMs: v.getFloat32(18), low: v.getUint32(22), ranges, timestamps,
    };
  }
  return null;
}
