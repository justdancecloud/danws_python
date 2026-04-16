"""Server-side topic handle and payload."""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any, Callable, TYPE_CHECKING

from ..protocol.types import Frame
from .flat_state_manager import FlatStateManager

if TYPE_CHECKING:
    from .session import DanWebSocketSession


class EventType(str, Enum):
    SubscribeEvent = "subscribe"
    ChangedParamsEvent = "changed_params"
    DelayedTaskEvent = "delayed_task"


TopicCallback = Callable[["EventType", "TopicHandle", "DanWebSocketSession"], Any]


class TopicPayload:
    def __init__(self, index: int, allocate_key_id: Callable[[], int], max_value_size: int | None = None):
        self._index = index
        self._allocate_key_id = allocate_key_id
        self._max_value_size = max_value_size
        self._flat_state = FlatStateManager(
            allocate_key_id=allocate_key_id,
            enqueue=lambda f: None,
            on_resync=lambda: None,
            wire_prefix=f"t.{index}.",
            max_value_size=max_value_size,
        )

    def _bind(self, enqueue: Callable[[Frame], None], on_resync: Callable[[], None]) -> None:
        self._flat_state = FlatStateManager(
            allocate_key_id=self._allocate_key_id,
            enqueue=enqueue,
            on_resync=on_resync,
            wire_prefix=f"t.{self._index}.",
            max_value_size=self._max_value_size,
        )

    def set(self, key: str, value: Any) -> None:
        self._flat_state.set(key, value)

    def get(self, key: str) -> Any:
        return self._flat_state.get(key)

    @property
    def keys(self) -> list[str]:
        return self._flat_state.keys

    def clear(self, key: str | None = None) -> None:
        if key is not None:
            self._flat_state.clear(key)
        else:
            self._flat_state.clear()

    def _build_key_frames(self) -> list[Frame]:
        return self._flat_state.build_key_frames()

    def _build_value_frames(self) -> list[Frame]:
        return self._flat_state.build_value_frames()

    @property
    def _size(self) -> int:
        return self._flat_state.size

    @property
    def _idx(self) -> int:
        return self._index


class TopicHandle:
    def __init__(
        self,
        name: str,
        params: dict[str, Any],
        payload: TopicPayload,
        session: DanWebSocketSession,
        log: Callable[[str, Exception | None], None] | None = None,
    ):
        self.name = name
        self.payload = payload
        self._params = dict(params)
        self._callback: TopicCallback | None = None
        self._session = session
        self._delay_ms: float | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._log = log

    @property
    def params(self) -> dict[str, Any]:
        return self._params

    def set_callback(self, fn: TopicCallback) -> None:
        self._callback = fn
        try:
            result = fn(EventType.SubscribeEvent, self, self._session)
            if asyncio.iscoroutine(result):
                asyncio.ensure_future(result)
        except Exception as e:
            if self._log:
                self._log("topic setCallback error", e)

    def set_delayed_task(self, ms: float) -> None:
        self.clear_delayed_task()
        self._delay_ms = ms

        def _fire() -> None:
            if self._callback:
                try:
                    result = self._callback(EventType.DelayedTaskEvent, self, self._session)
                    if asyncio.iscoroutine(result):
                        asyncio.ensure_future(result)
                except Exception as e:
                    if self._log:
                        self._log("delayed task error", e)
            # Re-schedule
            if self._delay_ms is not None:
                try:
                    loop = asyncio.get_event_loop()
                    self._timer = loop.call_later(self._delay_ms / 1000.0, _fire)
                except RuntimeError:
                    pass

        try:
            loop = asyncio.get_event_loop()
            self._timer = loop.call_later(ms / 1000.0, _fire)
        except RuntimeError:
            pass

    def clear_delayed_task(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _update_params(self, new_params: dict[str, Any]) -> None:
        self._params = dict(new_params)
        had_task = self._timer is not None
        saved_ms = self._delay_ms

        self.clear_delayed_task()

        if self._callback:
            try:
                result = self._callback(EventType.ChangedParamsEvent, self, self._session)
                if asyncio.iscoroutine(result):
                    asyncio.ensure_future(result)
            except Exception as e:
                if self._log:
                    self._log("params change callback error", e)

        if had_task and saved_ms is not None:
            self.set_delayed_task(saved_ms)

    def _dispose(self) -> None:
        self.clear_delayed_task()
        self._callback = None
        self._delay_ms = None
