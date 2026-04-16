"""DLE-based stream parser for incremental frame parsing."""

from __future__ import annotations

from enum import IntEnum
from typing import Callable

from .types import (
    DLE, STX, ETX, ENQ,
    DataType, FrameType, Frame, DanWSError,
    is_signal_frame, is_key_registration_frame,
)
from .serializer import deserialize


class _State(IntEnum):
    Idle = 0
    AfterDLE = 1
    InFrame = 2
    InFrameAfterDLE = 3


class StreamParser:
    """Incremental DLE-based stream parser.

    Feed arbitrary chunks of bytes via `feed()`. Parsed frames and heartbeats
    are delivered through registered callbacks.
    """

    def __init__(self, max_buffer_size: int = 1_048_576):
        self._state = _State.Idle
        self._buffer = bytearray()
        self._max_buffer_size = max_buffer_size
        self._frame_callbacks: list[Callable[[Frame], None]] = []
        self._heartbeat_callbacks: list[Callable[[], None]] = []
        self._error_callbacks: list[Callable[[Exception], None]] = []

    def on_frame(self, callback: Callable[[Frame], None]) -> None:
        self._frame_callbacks.append(callback)

    def on_heartbeat(self, callback: Callable[[], None]) -> None:
        self._heartbeat_callbacks.append(callback)

    def on_error(self, callback: Callable[[Exception], None]) -> None:
        self._error_callbacks.append(callback)

    def reset(self) -> None:
        self._state = _State.Idle
        self._buffer.clear()

    def feed(self, chunk: bytes | bytearray | memoryview) -> None:
        for byte in chunk:
            if self._state == _State.Idle:
                if byte == DLE:
                    self._state = _State.AfterDLE
                else:
                    self._emit_error(DanWSError(
                        "FRAME_PARSE_ERROR",
                        f"Unexpected byte 0x{byte:02x} outside frame",
                    ))

            elif self._state == _State.AfterDLE:
                if byte == STX:
                    self._state = _State.InFrame
                    self._buffer.clear()
                elif byte == ENQ:
                    self._emit_heartbeat()
                    self._state = _State.Idle
                else:
                    self._emit_error(DanWSError(
                        "INVALID_DLE_SEQUENCE",
                        f"Invalid DLE sequence: 0x10 0x{byte:02x}",
                    ))
                    self._state = _State.Idle

            elif self._state == _State.InFrame:
                if byte == DLE:
                    self._state = _State.InFrameAfterDLE
                else:
                    if len(self._buffer) >= self._max_buffer_size:
                        self._emit_error(DanWSError(
                            "FRAME_TOO_LARGE",
                            f"Frame exceeds {self._max_buffer_size} bytes",
                        ))
                        self._buffer.clear()
                        self._state = _State.Idle
                    else:
                        self._buffer.append(byte)

            elif self._state == _State.InFrameAfterDLE:
                if byte == ETX:
                    # Frame complete
                    try:
                        body = bytes(self._buffer)
                        frame = self._parse_frame(body)
                        self._emit_frame(frame)
                    except Exception as err:
                        self._emit_error(err)
                    self._buffer.clear()
                    self._state = _State.Idle
                elif byte == DLE:
                    # Escaped DLE
                    self._buffer.append(DLE)
                    self._state = _State.InFrame
                else:
                    self._emit_error(DanWSError(
                        "INVALID_DLE_SEQUENCE",
                        f"Invalid DLE sequence in frame: 0x10 0x{byte:02x}",
                    ))
                    self._buffer.clear()
                    self._state = _State.Idle

    def _parse_frame(self, body: bytes) -> Frame:
        if len(body) < 6:
            raise DanWSError("FRAME_PARSE_ERROR", f"Frame body too short: {len(body)} bytes")

        frame_type = FrameType(body[0])
        key_id = int.from_bytes(body[1:5], "big")
        data_type = DataType(body[5])
        raw_payload = body[6:]

        if is_key_registration_frame(frame_type):
            payload = raw_payload.decode("utf-8")
        elif is_signal_frame(frame_type):
            payload = None
        else:
            payload = deserialize(data_type, raw_payload)

        return Frame(frame_type=frame_type, key_id=key_id, data_type=data_type, payload=payload)

    def _emit_frame(self, frame: Frame) -> None:
        for cb in self._frame_callbacks:
            cb(frame)

    def _emit_heartbeat(self) -> None:
        for cb in self._heartbeat_callbacks:
            cb()

    def _emit_error(self, err: Exception) -> None:
        for cb in self._error_callbacks:
            cb(err)
