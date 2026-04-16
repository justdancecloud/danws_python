"""DanWebSocketClient - async WebSocket client for DanProtocol v3.5."""

from __future__ import annotations

import asyncio
import os
import struct
import time
import uuid
from typing import Any, Callable, Awaitable

import websockets
from websockets.asyncio.client import ClientConnection

from ..protocol.types import DataType, FrameType, Frame, DanWSError
from ..protocol.codec import encode, encode_heartbeat
from ..protocol.stream_parser import StreamParser
from ..protocol.serializer import detect_data_type
from ..state.key_registry import KeyRegistry
from ..connection.heartbeat_manager import HeartbeatManager
from ..connection.reconnect_engine import ReconnectEngine, ReconnectOptions
from .topic_client_handle import TopicClientHandle

PROTOCOL_MAJOR = 3
PROTOCOL_MINOR = 5


def _generate_uuid_v7() -> str:
    now = int(time.time() * 1000)
    b = bytearray(16)
    b[0] = (now >> 40) & 0xFF
    b[1] = (now >> 32) & 0xFF
    b[2] = (now >> 24) & 0xFF
    b[3] = (now >> 16) & 0xFF
    b[4] = (now >> 8) & 0xFF
    b[5] = now & 0xFF
    random_bytes = os.urandom(10)
    b[6:16] = random_bytes
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _uuid_to_bytes(u: str) -> bytes:
    return bytes.fromhex(u.replace("-", ""))


class DanWebSocketClient:
    def __init__(self, url: str, reconnect: ReconnectOptions | None = None, debug: bool = False):
        self.id = _generate_uuid_v7()
        self._url = url
        self._debug = debug
        self._state = "disconnected"
        self._ws: ClientConnection | None = None
        self._intentional_disconnect = False
        self._recv_task: asyncio.Task | None = None

        # Server->Client key registry and state
        self._registry = KeyRegistry()
        self._store: dict[int, Any] = {}
        self._pending_values: dict[int, Frame] = {}
        self._ready_deferred = False

        # Topic state
        self._subscriptions: dict[str, dict[str, Any]] = {}
        self._topic_dirty = False
        self._topic_client_handles: dict[str, TopicClientHandle] = {}
        self._topic_index_map: dict[str, int] = {}
        self._index_to_topic: dict[int, str] = {}
        self._topic_key_cache: dict[int, tuple[int, str] | None] = {}

        # Connection
        self._heartbeat = HeartbeatManager()
        self._reconnect_engine = ReconnectEngine(reconnect)
        self._parser = StreamParser()

        # Callbacks
        self._on_connect: list[Callable[[], None]] = []
        self._on_disconnect: list[Callable[[], None]] = []
        self._on_ready: list[Callable[[], None]] = []
        self._on_receive: list[Callable[[str, Any], None]] = []
        self._on_update: list[Callable[[dict], None]] = []
        self._on_error: list[Callable[[DanWSError], None]] = []

        self._parser.on_frame(self._handle_frame)
        self._parser.on_heartbeat(lambda: self._heartbeat.received())
        self._parser.on_error(lambda err: self._log("Parser error", err))
        self._setup_internals()

    @property
    def state(self) -> str:
        return self._state

    def get(self, key: str) -> Any:
        entry = self._registry.get_by_path(key)
        if not entry:
            return None
        return self._store.get(entry.key_id)

    @property
    def keys(self) -> list[str]:
        return self._registry.paths

    async def connect(self) -> None:
        if self._state not in ("disconnected", "reconnecting"):
            return
        self._intentional_disconnect = False
        self._state = "connecting"
        try:
            self._ws = await websockets.connect(self._url)
            self._handle_open()
            self._recv_task = asyncio.create_task(self._receive_loop())
        except Exception as e:
            self._log("connect failed", e)
            self._handle_close()

    def disconnect(self) -> None:
        self._intentional_disconnect = True
        self._reconnect_engine.stop()
        self._cleanup()
        self._state = "disconnected"
        self._emit(self._on_disconnect)

    def authorize(self, token: str) -> None:
        if not self._ws:
            return
        frame = Frame(
            frame_type=FrameType.Auth,
            key_id=0,
            data_type=DataType.String,
            payload=token,
        )
        self._send_frame(frame)
        self._state = "authorizing"

    # Topic API
    def subscribe(self, topic_name: str, params: dict[str, Any] | None = None) -> None:
        self._subscriptions[topic_name] = params or {}
        self._send_topic_sync()

    def unsubscribe(self, topic_name: str) -> None:
        if self._subscriptions.pop(topic_name, None) is not None:
            self._topic_client_handles.pop(topic_name, None)
            self._send_topic_sync()

    def topic(self, name: str) -> TopicClientHandle:
        handle = self._topic_client_handles.get(name)
        if not handle:
            idx = self._topic_index_map.get(name, -1)
            handle = TopicClientHandle(name, idx, self._registry, lambda kid: self._store.get(kid), self._log)
            self._topic_client_handles[name] = handle
        return handle

    @property
    def topics(self) -> list[str]:
        return list(self._subscriptions.keys())

    # Event registration
    def on_connect(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._on_connect.append(cb)
        def unsub():
            if cb in self._on_connect:
                self._on_connect.remove(cb)
        return unsub

    def on_disconnect(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._on_disconnect.append(cb)
        def unsub():
            if cb in self._on_disconnect:
                self._on_disconnect.remove(cb)
        return unsub

    def on_ready(self, cb: Callable[[], None]) -> Callable[[], None]:
        self._on_ready.append(cb)
        def unsub():
            if cb in self._on_ready:
                self._on_ready.remove(cb)
        return unsub

    def on_receive(self, cb: Callable[[str, Any], None]) -> Callable[[], None]:
        self._on_receive.append(cb)
        def unsub():
            if cb in self._on_receive:
                self._on_receive.remove(cb)
        return unsub

    def on_update(self, cb: Callable[[dict], None]) -> Callable[[], None]:
        self._on_update.append(cb)
        def unsub():
            if cb in self._on_update:
                self._on_update.remove(cb)
        return unsub

    def on_error(self, cb: Callable[[DanWSError], None]) -> Callable[[], None]:
        self._on_error.append(cb)
        def unsub():
            if cb in self._on_error:
                self._on_error.remove(cb)
        return unsub

    # Internals

    def _setup_internals(self) -> None:
        self._heartbeat.on_send(lambda data: self._send_raw(data))
        self._heartbeat.on_timeout(lambda: self._handle_close())

        self._reconnect_engine.on_attempt(lambda: asyncio.ensure_future(self.connect()))
        self._reconnect_engine.on_exhausted(lambda: self._on_reconnect_exhausted())

    def _on_reconnect_exhausted(self) -> None:
        self._state = "disconnected"
        self._emit_error(DanWSError("RECONNECT_EXHAUSTED", "All reconnection attempts exhausted"))

    def _handle_open(self) -> None:
        self._state = "identifying"
        self._heartbeat.start()

        # Send IDENTIFY
        uuid_bytes = _uuid_to_bytes(self.id)
        payload = bytearray(18)
        payload[:16] = uuid_bytes
        payload[16] = PROTOCOL_MAJOR
        payload[17] = PROTOCOL_MINOR
        identify_frame = Frame(
            frame_type=FrameType.Identify,
            key_id=0,
            data_type=DataType.Binary,
            payload=bytes(payload),
        )
        self._send_frame(identify_frame)
        self._emit(self._on_connect)

        if self._topic_dirty and self._subscriptions:
            self._send_topic_sync()

    def _handle_close(self) -> None:
        self._heartbeat.stop()

        if self._ws:
            try:
                asyncio.ensure_future(self._ws.close())
            except Exception:
                pass
            self._ws = None

        if self._intentional_disconnect:
            return

        self._emit(self._on_disconnect)

        if self._reconnect_engine.is_active:
            self._state = "reconnecting"
            self._reconnect_engine.retry()
        else:
            self._state = "reconnecting"
            self._reconnect_engine.start()

    async def _receive_loop(self) -> None:
        try:
            assert self._ws is not None
            async for message in self._ws:
                if isinstance(message, (bytes, bytearray)):
                    self._parser.feed(message)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            self._log("receive error", e)
        finally:
            self._handle_close()

    def _handle_frame(self, frame: Frame) -> None:
        ft = frame.frame_type

        if ft == FrameType.AuthOk:
            self._state = "synchronizing"

        elif ft == FrameType.AuthFail:
            self._intentional_disconnect = True
            self._emit_error(DanWSError("AUTH_REJECTED", str(frame.payload)))
            self._cleanup()
            self._state = "disconnected"
            self._emit(self._on_disconnect)

        elif ft == FrameType.ServerKeyRegistration:
            if self._state == "identifying":
                self._state = "synchronizing"
            key_path = frame.payload
            self._registry.register_one(frame.key_id, key_path, frame.data_type)
            pending = self._pending_values.pop(frame.key_id, None)
            if pending:
                self._store[frame.key_id] = pending.payload
                topic_info = self._get_topic_info(frame.key_id, key_path)
                if topic_info:
                    topic_idx, user_key = topic_info
                    topic_name = self._index_to_topic.get(topic_idx)
                    if topic_name:
                        handle = self._topic_client_handles.get(topic_name)
                        if handle:
                            handle._notify(user_key, pending.payload)
                else:
                    for cb in self._on_receive:
                        try:
                            cb(key_path, pending.payload)
                        except Exception as e:
                            self._log("onReceive error", e)

        elif ft == FrameType.ServerSync:
            if self._state == "identifying":
                self._state = "synchronizing"
            if self._state != "ready":
                ready_frame = Frame(
                    frame_type=FrameType.ClientReady,
                    key_id=0, data_type=DataType.Null, payload=None,
                )
                self._send_frame(ready_frame)

            if self._registry.size == 0:
                self._state = "ready"
                self._emit(self._on_ready)
                if self._reconnect_engine.is_active:
                    self._reconnect_engine.stop()
                if self._subscriptions:
                    self._send_topic_sync()

        elif ft == FrameType.ServerValue:
            if not self._registry.has_key_id(frame.key_id):
                self._send_frame(Frame(
                    frame_type=FrameType.ClientKeyRequest,
                    key_id=frame.key_id, data_type=DataType.Null, payload=None,
                ))
                self._pending_values[frame.key_id] = frame
                return

            self._store[frame.key_id] = frame.payload
            entry = self._registry.get_by_key_id(frame.key_id)
            if entry:
                topic_info = self._get_topic_info(frame.key_id, entry.path)
                if topic_info:
                    topic_idx, user_key = topic_info
                    topic_name = self._index_to_topic.get(topic_idx)
                    if topic_name:
                        handle = self._topic_client_handles.get(topic_name)
                        if handle:
                            handle._notify(user_key, frame.payload)
                else:
                    for cb in self._on_receive:
                        try:
                            cb(entry.path, frame.payload)
                        except Exception as e:
                            self._log("onReceive error", e)

            if self._state == "synchronizing" and not self._ready_deferred:
                self._ready_deferred = True

                async def _defer_ready():
                    await asyncio.sleep(0)  # yield to process remaining frames
                    self._ready_deferred = False
                    if self._state != "synchronizing":
                        return
                    self._state = "ready"
                    self._emit(self._on_ready)
                    if self._reconnect_engine.is_active:
                        self._reconnect_engine.stop()
                    if self._subscriptions:
                        self._send_topic_sync()

                asyncio.ensure_future(_defer_ready())

        elif ft == FrameType.ServerFlushEnd:
            if self._on_update:
                view = {k: self.get(k) for k in self.keys}
                for cb in self._on_update:
                    try:
                        cb(view)
                    except Exception as e:
                        self._log("onUpdate error", e)
            for handle in self._topic_client_handles.values():
                handle._flush_update()

        elif ft == FrameType.ServerKeyDelete:
            deleted_entry = self._registry.get_by_key_id(frame.key_id)
            deleted_topic_info = self._get_topic_info(frame.key_id, deleted_entry.path) if deleted_entry else None
            self._registry.remove_by_key_id(frame.key_id)
            self._store.pop(frame.key_id, None)
            self._topic_key_cache.pop(frame.key_id, None)
            if deleted_entry:
                if deleted_topic_info:
                    topic_idx, user_key = deleted_topic_info
                    topic_name = self._index_to_topic.get(topic_idx)
                    if topic_name:
                        handle = self._topic_client_handles.get(topic_name)
                        if handle:
                            handle._notify(user_key, None)
                else:
                    for cb in self._on_receive:
                        try:
                            cb(deleted_entry.path, None)
                        except Exception as e:
                            self._log("onReceive error", e)

        elif ft == FrameType.ServerReset:
            self._registry.clear()
            self._store.clear()
            self._pending_values.clear()
            self._topic_key_cache.clear()
            self._ready_deferred = False
            self._state = "synchronizing"

        elif ft == FrameType.ArrayShiftLeft:
            self._handle_array_shift_left(frame)

        elif ft == FrameType.ArrayShiftRight:
            self._handle_array_shift_right(frame)

        elif ft == FrameType.Error:
            self._emit_error(DanWSError("REMOTE_ERROR", str(frame.payload)))

    def _handle_array_shift_left(self, frame: Frame) -> None:
        length_entry = self._registry.get_by_key_id(frame.key_id)
        if not length_entry:
            return
        length_path = length_entry.path
        prefix = length_path[:len(length_path) - len(".length")]
        current_length = self._store.get(frame.key_id, 0)
        if not isinstance(current_length, (int, float)):
            current_length = 0
        current_length = int(current_length)
        shift_count = max(0, min(int(frame.payload), current_length))

        for i in range(current_length - shift_count):
            src = self._registry.get_by_path(f"{prefix}.{i + shift_count}")
            dst = self._registry.get_by_path(f"{prefix}.{i}")
            if src and dst:
                self._store[dst.key_id] = self._store.get(src.key_id)

        new_length = current_length - shift_count
        self._store[frame.key_id] = new_length

        topic_info = self._get_topic_info(frame.key_id, length_path)
        if topic_info:
            topic_idx, user_key = topic_info
            user_prefix = user_key[:len(user_key) - len(".length")]
            topic_name = self._index_to_topic.get(topic_idx)
            if topic_name:
                handle = self._topic_client_handles.get(topic_name)
                if handle:
                    handle._notify(user_prefix + ".length", new_length)
        else:
            for cb in self._on_receive:
                try:
                    cb(prefix + ".length", new_length)
                except Exception as e:
                    self._log("onReceive error", e)

    def _handle_array_shift_right(self, frame: Frame) -> None:
        length_entry = self._registry.get_by_key_id(frame.key_id)
        if not length_entry:
            return
        length_path = length_entry.path
        prefix = length_path[:len(length_path) - len(".length")]
        current_length = self._store.get(frame.key_id, 0)
        if not isinstance(current_length, (int, float)):
            current_length = 0
        current_length = int(current_length)
        shift_count = max(0, min(int(frame.payload), current_length))

        for i in range(current_length - 1, -1, -1):
            src = self._registry.get_by_path(f"{prefix}.{i}")
            dst = self._registry.get_by_path(f"{prefix}.{i + shift_count}")
            if src and dst:
                self._store[dst.key_id] = self._store.get(src.key_id)

        topic_info = self._get_topic_info(frame.key_id, length_path)
        if topic_info:
            topic_idx, user_key = topic_info
            user_prefix = user_key[:len(user_key) - len(".length")]
            topic_name = self._index_to_topic.get(topic_idx)
            if topic_name:
                handle = self._topic_client_handles.get(topic_name)
                if handle:
                    handle._notify(user_prefix + ".length", current_length)
        else:
            for cb in self._on_receive:
                try:
                    cb(prefix + ".length", current_length)
                except Exception as e:
                    self._log("onReceive error", e)

    def _get_topic_info(self, key_id: int, path: str) -> tuple[int, str] | None:
        cached = self._topic_key_cache.get(key_id, -1)
        if cached != -1:
            return cached
        if len(path) > 2 and path[0] == "t" and path[1] == ".":
            second_dot = path.find(".", 2)
            if second_dot != -1:
                try:
                    idx = int(path[2:second_dot])
                    info = (idx, path[second_dot + 1:])
                    self._topic_key_cache[key_id] = info
                    return info
                except ValueError:
                    pass
        self._topic_key_cache[key_id] = None
        return None

    def _send_topic_sync(self) -> None:
        if not self._ws:
            self._topic_dirty = True
            return

        entries: list[tuple[str, Any]] = []
        self._topic_index_map.clear()
        self._index_to_topic.clear()
        idx = 0
        for topic_name, params in self._subscriptions.items():
            self._topic_index_map[topic_name] = idx
            self._index_to_topic[idx] = topic_name
            handle = self._topic_client_handles.get(topic_name)
            if not handle:
                handle = TopicClientHandle(topic_name, idx, self._registry, lambda kid: self._store.get(kid), self._log)
                self._topic_client_handles[topic_name] = handle
            else:
                handle._set_index(idx)
            entries.append((f"topic.{idx}.name", topic_name))
            for param_key, param_value in params.items():
                entries.append((f"topic.{idx}.param.{param_key}", param_value))
            idx += 1

        # ClientReset
        self._send_frame(Frame(
            frame_type=FrameType.ClientReset,
            key_id=0, data_type=DataType.Null, payload=None,
        ))

        # ClientKeyRegistration + ClientValue
        key_id = 1
        key_entries: list[tuple[int, Any, DataType]] = []
        for path, value in entries:
            dt = detect_data_type(value)
            self._send_frame(Frame(
                frame_type=FrameType.ClientKeyRegistration,
                key_id=key_id, data_type=dt, payload=path,
            ))
            key_entries.append((key_id, value, dt))
            key_id += 1

        for kid, value, dt in key_entries:
            self._send_frame(Frame(
                frame_type=FrameType.ClientValue,
                key_id=kid, data_type=dt, payload=value,
            ))

        # ClientSync
        self._send_frame(Frame(
            frame_type=FrameType.ClientSync,
            key_id=0, data_type=DataType.Null, payload=None,
        ))

        self._topic_dirty = False

    def _send_frame(self, frame: Frame) -> None:
        data = encode(frame)
        self._send_raw(data)

    def _send_raw(self, data: bytes) -> None:
        if self._ws:
            try:
                asyncio.ensure_future(self._ws.send(data))
            except Exception:
                pass

    def _cleanup(self) -> None:
        self._heartbeat.stop()
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
        if self._ws:
            try:
                asyncio.ensure_future(self._ws.close())
            except Exception:
                pass
            self._ws = None

    def _log(self, msg: str, err: Exception | None = None) -> None:
        if self._debug:
            import sys
            print(f"[dan-ws client] {msg}", err or "", file=sys.stderr)

    def _emit(self, callbacks: list) -> None:
        for cb in callbacks:
            try:
                cb()
            except Exception as e:
                self._log("callback error", e)

    def _emit_error(self, err: DanWSError) -> None:
        if not self._on_error:
            self._log(f"Unhandled DanWSError: {err.code} {err.message}", err)
            return
        for cb in self._on_error:
            try:
                cb(err)
            except Exception as e:
                self._log("onError callback error", e)
