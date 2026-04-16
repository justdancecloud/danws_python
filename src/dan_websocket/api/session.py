"""Server-side session representing one client connection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..protocol.types import DataType, FrameType, Frame, DanWSError
from .flat_state_manager import FlatStateManager
from .topic_handle import TopicHandle, TopicPayload


@dataclass
class TopicInfo:
    name: str
    params: dict[str, Any]


class DanWebSocketSession:
    def __init__(self, client_uuid: str):
        self.id = client_uuid
        self._principal: str | None = None
        self._authorized = False
        self._connected = True
        self._state = "pending"

        self._enqueue_frame: Callable[[Frame], None] | None = None

        # Callbacks
        self._on_ready: list[Callable[[], None]] = []
        self._on_disconnect: list[Callable[[], None]] = []

        # TX providers (from principal shared state)
        self._tx_key_frame_provider: Callable[[], list[Frame]] | None = None
        self._tx_value_frame_provider: Callable[[], list[Frame]] | None = None
        self._server_sync_sent = False

        # Session-level flat TX store (topic modes)
        self._next_key_id = 1
        self._session_enqueue: Callable[[Frame], None] | None = None
        self._session_bound = False
        self._flat_state: FlatStateManager | None = None

        # Topic handles
        self._topic_handles: dict[str, TopicHandle] = {}
        self._topic_index = 0
        self._topics: dict[str, TopicInfo] = {}

        self._max_value_size: int | None = None
        self._debug: bool | Callable[[str, Exception | None], None] = False

    @property
    def principal(self) -> str | None:
        return self._principal

    @property
    def authorized(self) -> bool:
        return self._authorized

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def state(self) -> str:
        return self._state

    # Event registration
    def on_ready(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._on_ready.append(cb)
        def unsub():
            if cb in self._on_ready:
                self._on_ready.remove(cb)
        return unsub

    def on_disconnect(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._on_disconnect.append(cb)
        def unsub():
            if cb in self._on_disconnect:
                self._on_disconnect.remove(cb)
        return unsub

    # Session-level data API (topic modes)
    def set(self, key: str, value: Any) -> None:
        if not self._session_bound or not self._flat_state:
            raise DanWSError("INVALID_MODE", "session.set() is only available in topic modes.")
        self._flat_state.set(key, value)

    def get(self, key: str) -> Any:
        return self._flat_state.get(key) if self._flat_state else None

    @property
    def keys(self) -> list[str]:
        return self._flat_state.keys if self._flat_state else []

    # Topic API
    @property
    def topics(self) -> list[str]:
        return list(self._topics.keys())

    def topic(self, name: str) -> TopicInfo | None:
        return self._topics.get(name)

    def get_topic_handle(self, name: str) -> TopicHandle | None:
        return self._topic_handles.get(name)

    @property
    def topic_handles(self) -> dict[str, TopicHandle]:
        return self._topic_handles

    # Internal methods
    def _set_debug(self, debug: bool | Callable[[str, Exception | None], None]) -> None:
        self._debug = debug

    def _set_max_value_size(self, size: int) -> None:
        self._max_value_size = size

    def _set_enqueue(self, fn: Callable[[Frame], None]) -> None:
        self._enqueue_frame = fn

    def _set_tx_providers(
        self,
        key_frames: Callable[[], list[Frame]],
        value_frames: Callable[[], list[Frame]],
    ) -> None:
        self._tx_key_frame_provider = key_frames
        self._tx_value_frame_provider = value_frames

    def _bind_session_tx(self, enqueue: Callable[[Frame], None]) -> None:
        self._session_enqueue = enqueue
        self._session_bound = True

        def alloc_id():
            kid = self._next_key_id
            self._next_key_id += 1
            return kid

        self._flat_state = FlatStateManager(
            allocate_key_id=alloc_id,
            enqueue=enqueue,
            on_resync=self._trigger_session_resync,
            wire_prefix="",
            max_value_size=self._max_value_size,
        )

    def _authorize(self, principal: str) -> None:
        self._principal = principal
        self._authorized = True
        self._state = "authorized"

    def _start_sync(self) -> None:
        self._state = "synchronizing"
        self._server_sync_sent = False

        if self._tx_key_frame_provider and self._enqueue_frame:
            frames = self._tx_key_frame_provider()
            if frames:
                for f in frames:
                    self._enqueue_frame(f)
                self._server_sync_sent = True
            else:
                self._enqueue_frame(Frame(
                    frame_type=FrameType.ServerSync,
                    key_id=0, data_type=DataType.Null, payload=None,
                ))
                self._server_sync_sent = True
        else:
            self._state = "ready"
            self._emit(self._on_ready)

    def _handle_frame(self, frame: Frame) -> None:
        if frame.frame_type == FrameType.ClientReady:
            if self._state == "ready":
                return
            if self._tx_value_frame_provider and self._enqueue_frame:
                for vf in self._tx_value_frame_provider():
                    self._enqueue_frame(vf)
            if self._server_sync_sent:
                self._state = "ready"
                self._emit(self._on_ready)

        elif frame.frame_type == FrameType.ClientResyncReq:
            if self._tx_key_frame_provider and self._enqueue_frame:
                self._enqueue_frame(Frame(
                    frame_type=FrameType.ServerReset,
                    key_id=0, data_type=DataType.Null, payload=None,
                ))
                for f in self._tx_key_frame_provider():
                    self._enqueue_frame(f)

        elif frame.frame_type == FrameType.ClientKeyRequest:
            self._handle_key_request(frame.key_id)

    def _handle_disconnect(self) -> None:
        self._connected = False
        self._state = "disconnected"
        self._emit(self._on_disconnect)

    def _handle_reconnect(self) -> None:
        self._connected = True
        self._state = "authorized"

    def _add_topic(self, name: str, params: dict[str, Any]) -> None:
        self._topics[name] = TopicInfo(name=name, params=params)

    def _remove_topic(self, name: str) -> bool:
        return self._topics.pop(name, None) is not None

    def _update_topic_params(self, name: str, params: dict[str, Any]) -> None:
        t = self._topics.get(name)
        if t:
            t.params = params

    @property
    def _next_topic_index(self) -> int:
        return self._topic_index

    def _create_topic_handle(
        self, name: str, params: dict[str, Any], wire_index: int | None = None,
    ) -> TopicHandle:
        index = wire_index if wire_index is not None else self._topic_index
        if index >= self._topic_index:
            self._topic_index = index + 1

        def alloc_id():
            kid = self._next_key_id
            self._next_key_id += 1
            return kid

        payload = TopicPayload(index, alloc_id, self._max_value_size)
        if self._session_enqueue:
            payload._bind(self._session_enqueue, self._trigger_session_resync)
        handle = TopicHandle(name, params, payload, self, self._log)
        self._topic_handles[name] = handle
        self._topics[name] = TopicInfo(name=name, params=params)
        return handle

    def _remove_topic_handle(self, name: str) -> None:
        handle = self._topic_handles.get(name)
        if handle:
            handle._dispose()
            del self._topic_handles[name]
            self._topics.pop(name, None)
            self._trigger_session_resync()

    def _dispose_all_topic_handles(self) -> None:
        for handle in self._topic_handles.values():
            handle._dispose()
        self._topic_handles.clear()

    def _trigger_session_resync(self) -> None:
        if not self._session_enqueue:
            return

        self._session_enqueue(Frame(
            frame_type=FrameType.ServerReset,
            key_id=0, data_type=DataType.Null, payload=None,
        ))

        flat_value_frames: list[Frame] | None = None
        if self._flat_state:
            key_frames, value_frames = self._flat_state.build_all_frames()
            for f in key_frames:
                self._session_enqueue(f)
            flat_value_frames = value_frames

        for handle in self._topic_handles.values():
            for f in handle.payload._build_key_frames():
                self._session_enqueue(f)

        self._session_enqueue(Frame(
            frame_type=FrameType.ServerSync,
            key_id=0, data_type=DataType.Null, payload=None,
        ))

        if flat_value_frames:
            for f in flat_value_frames:
                self._session_enqueue(f)

        for handle in self._topic_handles.values():
            for f in handle.payload._build_value_frames():
                self._session_enqueue(f)

    def _handle_key_request(self, key_id: int) -> None:
        if not self._enqueue_frame:
            return
        sync_frame = Frame(frame_type=FrameType.ServerSync, key_id=0, data_type=DataType.Null, payload=None)

        # Search in TX providers
        if self._tx_key_frame_provider and self._tx_value_frame_provider:
            for f in self._tx_key_frame_provider():
                if f.frame_type == FrameType.ServerKeyRegistration and f.key_id == key_id:
                    self._enqueue_frame(f)
                    self._enqueue_frame(sync_frame)
                    for vf in self._tx_value_frame_provider():
                        if vf.key_id == key_id:
                            self._enqueue_frame(vf)
                            break
                    return

        # Search in session flat state
        if self._flat_state:
            found = self._flat_state.get_by_key_id(key_id)
            if found:
                key, entry = found
                self._enqueue_frame(Frame(
                    frame_type=FrameType.ServerKeyRegistration,
                    key_id=entry.key_id, data_type=entry.data_type, payload=key,
                ))
                self._enqueue_frame(sync_frame)
                if entry.value is not None:
                    self._enqueue_frame(Frame(
                        frame_type=FrameType.ServerValue,
                        key_id=entry.key_id, data_type=entry.data_type, payload=entry.value,
                    ))
                return

    def _log(self, msg: str, err: Exception | None = None) -> None:
        if callable(self._debug):
            self._debug(msg, err)
        elif self._debug:
            import sys
            print(f"[dan-ws session] {msg}", err or "", file=sys.stderr)

    def _emit(self, callbacks: list) -> None:
        for cb in callbacks:
            try:
                cb()
            except Exception as e:
                self._log("callback error", e)
