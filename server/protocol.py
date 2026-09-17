"""Binary datagram format shared with web/protocol.js (network byte order).

DATA: type u8 | flow u8 | seq u32 | send_ts f64 (ms, sender clock) | padding
ACK:  type u8 | flow u8 | seq u32 | echo_send_ts f64 | recv_ts f64 (ms, receiver clock)
TRAIN: type u8 | flow u8 | train_id u16 | index u16 | train_len u16 | send_ts f64 | padding
  A burst of `train_len` packets sent back to back; the receiver measures the
  inter-arrival times (see server/trains.py).
PING: type u8 | flow u8 | seq u32 | send_ts f64 (sender clock) | 0 f64 | padding
PONG: type u8 | flow u8 | seq u32 | send_ts f64 (echoed) | echo_recv_ts f64 (responder clock)
  The steady-stream RTT monitor (server/rtt.py): the responder turns a PING
  around immediately, stamping its own arrival time, so one exchange gives the
  round trip *and* each leg separately (up to the clock offset).
ACK_BLOCK (one datagram acknowledging many packets):
      type u8 | flow u8 | ack_id u32 | largest u32 | largest_recv_ts f64 | ack_delay_ms f32 |
      low u32 | n_ranges u8 | n_ranges x (first u32, last u32), descending |
      n_ts u16 | n_ts x (seq u32, recv_offset_ms f32)   # recv_ts - largest_recv_ts
  Ranges list received sequence numbers in [low, largest]; anything in that
  span not covered by a range has not arrived. Timestamps cover packets newly
  received since the previous ACK_BLOCK.

`flow` names the direction of the DATA the packet refers to, so an ACK for a
server->browser packet carries FLOW_DOWN.
"""

import struct
from typing import List, NamedTuple, Optional, Tuple, Union

DATA = 1
ACK = 2
ACK_BLOCK = 3
TRAIN = 4
PING = 5
PONG = 6

FLOW_UP = 0  # browser -> server
FLOW_DOWN = 1  # server -> browser

_DATA = struct.Struct("!BBId")
_ACK = struct.Struct("!BBIdd")
_BLOCK_HEAD = struct.Struct("!BBIIdfIB")
_RANGE = struct.Struct("!II")
_U16 = struct.Struct("!H")
_TS = struct.Struct("!If")
_TRAIN = struct.Struct("!BBHHHd")
_ECHO = struct.Struct("!BBIdd")

MAX_ACK_RANGES = 16
MAX_DATAGRAM_PAYLOAD = 1000  # fits Chrome's 1024-byte maxDatagramSize with the WebTransport prefix

DATA_HEADER_SIZE = _DATA.size
TRAIN_HEADER_SIZE = _TRAIN.size
ACK_SIZE = _ACK.size
ECHO_HEADER_SIZE = _ECHO.size


class Data(NamedTuple):
    flow: int
    seq: int
    send_ts: float
    size: int


class Train(NamedTuple):
    flow: int
    train_id: int
    index: int
    train_len: int
    send_ts: float
    size: int


def encode_train(flow: int, train_id: int, index: int, train_len: int, send_ts: float, size: int) -> bytes:
    header = _TRAIN.pack(TRAIN, flow, train_id & 0xFFFF, index, train_len, send_ts)
    return header + bytes(max(0, size - TRAIN_HEADER_SIZE))


class Ping(NamedTuple):
    flow: int
    seq: int
    send_ts: float  # initiator's clock, when it handed the packet to the transport
    size: int


class Pong(NamedTuple):
    flow: int
    seq: int
    send_ts: float  # echoed unchanged, so RTT needs no state at the initiator
    echo_recv_ts: float  # responder's clock, when the PING arrived
    size: int


def encode_ping(flow: int, seq: int, send_ts: float, size: int = ECHO_HEADER_SIZE) -> bytes:
    header = _ECHO.pack(PING, flow, seq & 0xFFFFFFFF, send_ts, 0.0)
    return header + bytes(max(0, size - ECHO_HEADER_SIZE))


def encode_pong(
    flow: int, seq: int, send_ts: float, echo_recv_ts: float, size: int = ECHO_HEADER_SIZE
) -> bytes:
    header = _ECHO.pack(PONG, flow, seq & 0xFFFFFFFF, send_ts, echo_recv_ts)
    return header + bytes(max(0, size - ECHO_HEADER_SIZE))


class Ack(NamedTuple):
    flow: int
    seq: int
    echo_send_ts: float
    recv_ts: float


class AckBlock(NamedTuple):
    flow: int
    ack_id: int
    largest: int
    largest_recv_ts: float
    ack_delay_ms: float
    low: int
    ranges: List[Tuple[int, int]]  # (first, last), descending
    timestamps: List[Tuple[int, float]]  # (seq, recv_ts)


def block_ts_capacity(n_ranges: int) -> int:
    return (MAX_DATAGRAM_PAYLOAD - _BLOCK_HEAD.size - n_ranges * _RANGE.size - _U16.size) // _TS.size


def encode_ack_block(
    flow: int,
    ack_id: int,
    largest: int,
    largest_recv_ts: float,
    ack_delay_ms: float,
    low: int,
    ranges: List[Tuple[int, int]],
    timestamps: List[Tuple[int, float]],
) -> bytes:
    parts = [_BLOCK_HEAD.pack(ACK_BLOCK, flow, ack_id, largest, largest_recv_ts, ack_delay_ms, low, len(ranges))]
    parts += [_RANGE.pack(first, last) for first, last in ranges]
    parts.append(_U16.pack(len(timestamps)))
    parts += [_TS.pack(seq, recv_ts - largest_recv_ts) for seq, recv_ts in timestamps]
    return b"".join(parts)


def encode_data(flow: int, seq: int, send_ts: float, size: int) -> bytes:
    header = _DATA.pack(DATA, flow, seq & 0xFFFFFFFF, send_ts)
    return header + bytes(max(0, size - DATA_HEADER_SIZE))


def encode_ack(flow: int, seq: int, echo_send_ts: float, recv_ts: float) -> bytes:
    return _ACK.pack(ACK, flow, seq, echo_send_ts, recv_ts)


def decode(buf: bytes) -> Optional[Union[Data, Train, Ack, AckBlock]]:
    if not buf:
        return None
    if buf[0] == DATA and len(buf) >= DATA_HEADER_SIZE:
        _, flow, seq, ts = _DATA.unpack_from(buf)
        return Data(flow, seq, ts, len(buf))
    if buf[0] == TRAIN and len(buf) >= TRAIN_HEADER_SIZE:
        _, flow, train_id, index, train_len, ts = _TRAIN.unpack_from(buf)
        return Train(flow, train_id, index, train_len, ts, len(buf))
    if buf[0] in (PING, PONG) and len(buf) >= ECHO_HEADER_SIZE:
        kind, flow, seq, send_ts, echo_ts = _ECHO.unpack_from(buf)
        if kind == PING:
            return Ping(flow, seq, send_ts, len(buf))
        return Pong(flow, seq, send_ts, echo_ts, len(buf))
    if buf[0] == ACK and len(buf) >= ACK_SIZE:
        _, flow, seq, echo, recv = _ACK.unpack_from(buf)
        return Ack(flow, seq, echo, recv)
    if buf[0] == ACK_BLOCK and len(buf) >= _BLOCK_HEAD.size:
        _, flow, ack_id, largest, largest_ts, delay, low, n_ranges = _BLOCK_HEAD.unpack_from(buf)
        off = _BLOCK_HEAD.size
        ranges = []
        for _ in range(n_ranges):
            ranges.append(_RANGE.unpack_from(buf, off))
            off += _RANGE.size
        (n_ts,) = _U16.unpack_from(buf, off)
        off += _U16.size
        timestamps = []
        for _ in range(n_ts):
            seq, offset = _TS.unpack_from(buf, off)
            timestamps.append((seq, largest_ts + offset))
            off += _TS.size
        return AckBlock(flow, ack_id, largest, largest_ts, delay, low, ranges, timestamps)
    return None
