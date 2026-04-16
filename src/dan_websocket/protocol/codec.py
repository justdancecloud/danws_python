"""DanProtocol v3.5 frame encoding/decoding with DLE byte-stuffing."""

from __future__ import annotations

import struct
from typing import Any

from .types import (
    DLE, STX, ETX, ENQ,
    DataType, FrameType, Frame, DanWSError,
    is_signal_frame, is_key_registration_frame,
)
from .serializer import serialize, deserialize


def dle_encode(data: bytes) -> bytes:
    """DLE-stuff: every 0x10 byte becomes 0x10 0x10."""
    if DLE not in data:
        return data
    result = bytearray()
    for b in data:
        result.append(b)
        if b == DLE:
            result.append(DLE)
    return bytes(result)


def dle_decode(data: bytes) -> bytes:
    """DLE-unstuff: 0x10 0x10 becomes 0x10."""
    if DLE not in data:
        return data
    result = bytearray()
    i = 0
    while i < len(data):
        if data[i] == DLE:
            i += 1  # skip the doubled DLE
        result.append(data[i])
        i += 1
    return bytes(result)


def encode(frame: Frame) -> bytes:
    """Encode a single Frame into bytes with DLE STX/ETX framing."""
    # Serialize payload
    if is_key_registration_frame(frame.frame_type):
        raw_payload = frame.payload.encode("utf-8") if isinstance(frame.payload, str) else b""
    elif is_signal_frame(frame.frame_type):
        raw_payload = b""
    else:
        raw_payload = serialize(frame.data_type, frame.payload)

    # Build raw body: [FrameType:1] [KeyID:4 BE] [DataType:1] [Payload:N]
    raw_body = bytearray(6 + len(raw_payload))
    raw_body[0] = frame.frame_type & 0xFF
    struct.pack_into(">I", raw_body, 1, frame.key_id & 0xFFFFFFFF)
    raw_body[5] = frame.data_type & 0xFF
    raw_body[6:] = raw_payload

    # DLE-escape the entire body
    escaped_body = dle_encode(bytes(raw_body))

    # Wrap with DLE STX ... DLE ETX
    result = bytearray(2 + len(escaped_body) + 2)
    result[0] = DLE
    result[1] = STX
    result[2:2 + len(escaped_body)] = escaped_body
    result[-2] = DLE
    result[-1] = ETX
    return bytes(result)


def encode_batch(frames: list[Frame]) -> bytes:
    """Encode multiple frames and concatenate into a single buffer."""
    parts = [encode(f) for f in frames]
    return b"".join(parts)


def encode_heartbeat() -> bytes:
    """Encode heartbeat: DLE ENQ (2 bytes)."""
    return bytes([DLE, ENQ])


def decode(data: bytes) -> list[Frame]:
    """Decode a byte buffer containing one or more frames."""
    frames: list[Frame] = []
    i = 0

    while i < len(data):
        if i + 1 >= len(data):
            raise DanWSError("FRAME_PARSE_ERROR", "Unexpected end of data")
        if data[i] != DLE or data[i + 1] != STX:
            raise DanWSError(
                "FRAME_PARSE_ERROR",
                f"Expected DLE STX at offset {i}, got 0x{data[i]:02x} 0x{data[i+1]:02x}",
            )
        i += 2  # skip DLE STX

        body_start = i
        body_end = -1

        while i < len(data):
            if data[i] == DLE:
                if i + 1 >= len(data):
                    raise DanWSError("FRAME_PARSE_ERROR", "Unexpected end of data after DLE")
                if data[i + 1] == ETX:
                    body_end = i
                    i += 2
                    break
                elif data[i + 1] == DLE:
                    i += 2
                else:
                    raise DanWSError(
                        "INVALID_DLE_SEQUENCE",
                        f"Invalid DLE sequence: 0x10 0x{data[i+1]:02x}",
                    )
            else:
                i += 1

        if body_end == -1:
            raise DanWSError("FRAME_PARSE_ERROR", "Missing DLE ETX terminator")

        decoded_body = dle_decode(data[body_start:body_end])

        if len(decoded_body) < 6:
            raise DanWSError("FRAME_PARSE_ERROR", f"Frame body too short: {len(decoded_body)} bytes (minimum 6)")

        frame_type = FrameType(decoded_body[0])
        key_id = struct.unpack(">I", decoded_body[1:5])[0]
        data_type = DataType(decoded_body[5])
        raw_payload = decoded_body[6:]

        if is_key_registration_frame(frame_type):
            payload: Any = raw_payload.decode("utf-8")
        elif is_signal_frame(frame_type):
            payload = None
        else:
            payload = deserialize(data_type, raw_payload)

        frames.append(Frame(frame_type=frame_type, key_id=key_id, data_type=data_type, payload=payload))

    return frames
