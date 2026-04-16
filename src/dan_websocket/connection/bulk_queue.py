"""BulkQueue batches frames and flushes periodically."""

from __future__ import annotations

import asyncio
from typing import Callable, Awaitable

from ..protocol.types import FrameType, DataType, Frame
from ..protocol.codec import encode_batch

DEFAULT_FLUSH_INTERVAL = 0.1  # 100ms
DEFAULT_MAX_QUEUE_SIZE = 50_000


class BulkQueue:
    def __init__(
        self,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        emit_flush_end: bool = True,
        max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE,
    ):
        self._queue: list[Frame] = []
        self._value_frames: dict[int, Frame] = {}  # keyId -> latest value frame (dedup)
        self._flush_interval = flush_interval
        self._emit_flush_end = emit_flush_end
        self._max_queue_size = max_queue_size
        self._on_flush: Callable[[bytes], Awaitable[None] | None] | None = None
        self._on_overflow: Callable[[], None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._disposed = False

    def on_flush(self, callback: Callable[[bytes], Awaitable[None] | None]) -> None:
        self._on_flush = callback

    def on_overflow(self, callback: Callable[[], None]) -> None:
        self._on_overflow = callback

    def enqueue(self, frame: Frame) -> None:
        if self._disposed:
            return
        total = len(self._queue) + len(self._value_frames)
        if total >= self._max_queue_size:
            self.dispose()
            if self._on_overflow:
                try:
                    self._on_overflow()
                except Exception:
                    pass
            return

        if _is_value_frame(frame.frame_type):
            self._value_frames[frame.key_id] = frame
        else:
            self._queue.append(frame)

        self._start_timer()

    def flush(self) -> None:
        self._stop_timer()

        frames = self._queue + list(self._value_frames.values())
        self._queue = []
        self._value_frames.clear()

        if not frames:
            return

        if self._emit_flush_end:
            frames.append(Frame(
                frame_type=FrameType.ServerFlushEnd,
                key_id=0,
                data_type=DataType.Null,
                payload=None,
            ))

        if self._on_flush:
            data = encode_batch(frames)
            result = self._on_flush(data)
            if asyncio.iscoroutine(result):
                asyncio.ensure_future(result)

    def clear(self) -> None:
        self._stop_timer()
        self._queue = []
        self._value_frames.clear()

    @property
    def pending(self) -> int:
        return len(self._queue) + len(self._value_frames)

    def dispose(self) -> None:
        self.clear()
        self._disposed = True
        self._on_flush = None

    def _start_timer(self) -> None:
        if self._timer is None:
            try:
                loop = asyncio.get_event_loop()
                self._timer = loop.call_later(self._flush_interval, self._timer_fired)
            except RuntimeError:
                pass

    def _stop_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _timer_fired(self) -> None:
        self._timer = None
        self.flush()


def _is_value_frame(frame_type: int) -> bool:
    return frame_type == 0x01 or frame_type == 0x03
