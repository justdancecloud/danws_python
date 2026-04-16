"""DanWebSocketServer - async WebSocket server for DanProtocol v3.5."""

from __future__ import annotations

import asyncio
import re
import sys
import uuid
from typing import Any, Callable, Awaitable

import websockets
from websockets.asyncio.server import Server as WSServer, ServerConnection

from ..protocol.types import DataType, FrameType, Frame, DanWSError
from ..protocol.codec import encode
from ..protocol.stream_parser import StreamParser
from ..state.key_registry import KeyRegistry
from ..connection.heartbeat_manager import HeartbeatManager
from ..connection.bulk_queue import BulkQueue
from .session import DanWebSocketSession, TopicInfo
from .principal_store import PrincipalTX, PrincipalManager
from .topic_handle import TopicHandle

ServerMode = str  # "broadcast" | "principal" | "session_topic" | "session_principal_topic"
BROADCAST_PRINCIPAL = "__broadcast__"
PROTOCOL_MAJOR = 3
PROTOCOL_MINOR = 5


def _bytes_to_uuid(data: bytes) -> str:
    h = data.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _uuid_to_bytes(u: str) -> bytes:
    h = u.replace("-", "")
    return bytes.fromhex(h)


class _InternalSession:
    __slots__ = (
        "session", "ws", "bulk_queue", "heartbeat",
        "auth_timeout_handle", "ttl_timer",
        "client_registry", "client_values",
        "auth_token",
    )

    def __init__(
        self,
        session: DanWebSocketSession,
        ws: ServerConnection | None,
        bulk_queue: BulkQueue,
        heartbeat: HeartbeatManager,
    ):
        self.session = session
        self.ws = ws
        self.bulk_queue = bulk_queue
        self.heartbeat = heartbeat
        self.auth_timeout_handle: asyncio.TimerHandle | None = None
        self.ttl_timer: asyncio.TimerHandle | None = None
        self.client_registry: KeyRegistry | None = None
        self.client_values: dict[int, Any] | None = None
        self.auth_token: str | None = None


class DanWebSocketServer:
    PROTOCOL_MAJOR = PROTOCOL_MAJOR
    PROTOCOL_MINOR = PROTOCOL_MINOR

    def __init__(
        self,
        port: int,
        path: str = "/",
        mode: ServerMode = "principal",
        ttl: int = 600_000,
        flush_interval_ms: int = 100,
        max_connections: int = 0,
        max_frames_per_sec: int = 0,
        debug: bool = False,
    ):
        self._port = port
        self._path = path
        self.mode = mode
        self._ttl = ttl / 1000.0  # convert to seconds
        self._flush_interval = flush_interval_ms / 1000.0
        self._debug = debug
        self._auth_enabled = False
        self._auth_timeout = 5.0
        self._max_connections = max_connections
        self._max_frames_per_sec = max_frames_per_sec

        self._principals = PrincipalManager()
        if not self._is_topic_mode:
            self._principals._set_on_new_principal(self._bind_principal_tx)

        self._sessions: dict[str, _InternalSession] = {}
        self._tmp_sessions: dict[str, _InternalSession] = {}
        self._principal_index: dict[str, set[_InternalSession]] = {}

        # Callbacks
        self._on_connection: list[Callable[[DanWebSocketSession], None]] = []
        self._on_authorize: list[Callable[[str, str], None]] = []
        self._on_session_expired: list[Callable[[DanWebSocketSession], None]] = []

        # Topic namespace
        self._topic_on_subscribe: list[Callable[[DanWebSocketSession, TopicHandle], None]] = []
        self._topic_on_unsubscribe: list[Callable[[DanWebSocketSession, TopicHandle], None]] = []

        # Metrics
        self._frames_in = 0
        self._frames_out = 0
        self._frame_counters: dict[str, tuple[int, float]] = {}

        self._server: WSServer | None = None

    @property
    def _is_topic_mode(self) -> bool:
        return self.mode in ("session_topic", "session_principal_topic")

    @property
    def topic(self) -> "_TopicNamespace":
        return _TopicNamespace(self)

    # -- Broadcast mode API --

    def set(self, key: str, value: Any) -> None:
        self._assert_mode("broadcast", "set")
        self._principals.principal(BROADCAST_PRINCIPAL).set(key, value)

    def get(self, key: str) -> Any:
        self._assert_mode("broadcast", "get")
        return self._principals.principal(BROADCAST_PRINCIPAL).get(key)

    @property
    def keys(self) -> list[str]:
        if self.mode != "broadcast":
            return []
        return self._principals.principal(BROADCAST_PRINCIPAL).keys

    def clear(self, key: str | None = None) -> None:
        self._assert_mode("broadcast", "clear")
        self._principals.principal(BROADCAST_PRINCIPAL).clear(key)

    # -- Principal mode API --

    def principal(self, name: str) -> PrincipalTX:
        if self.mode not in ("principal", "session_principal_topic"):
            raise DanWSError("INVALID_MODE", "server.principal() is only available in principal/session_principal_topic mode.")
        return self._principals.principal(name)

    # -- Common API --

    def set_max_connections(self, n: int) -> None:
        self._max_connections = n

    def set_max_frames_per_sec(self, n: int) -> None:
        self._max_frames_per_sec = n

    def metrics(self) -> dict:
        return {
            "activeSessions": len(self._sessions),
            "pendingSessions": len(self._tmp_sessions),
            "principalCount": self._principals.size,
            "framesIn": self._frames_in,
            "framesOut": self._frames_out,
        }

    def enable_authorization(self, enabled: bool, timeout_ms: int = 5000) -> None:
        self._auth_enabled = enabled
        self._auth_timeout = timeout_ms / 1000.0

    def authorize(self, client_uuid: str, token: str, principal: str) -> None:
        if principal is None:
            raise DanWSError("INVALID_PRINCIPAL", "authorize(): principal must not be None.")
        internal = self._tmp_sessions.get(client_uuid)
        if not internal:
            return
        del self._tmp_sessions[client_uuid]
        if internal.auth_timeout_handle:
            internal.auth_timeout_handle.cancel()
            internal.auth_timeout_handle = None
        internal.session._authorize(principal)
        self._send_frame(internal, Frame(
            frame_type=FrameType.AuthOk,
            key_id=0, data_type=DataType.Null, payload=None,
        ))
        self._sessions[client_uuid] = internal
        self._activate_session(internal, principal)

    def reject(self, client_uuid: str, reason: str = "Rejected") -> None:
        internal = self._tmp_sessions.get(client_uuid)
        if not internal:
            return
        del self._tmp_sessions[client_uuid]
        if internal.auth_timeout_handle:
            internal.auth_timeout_handle.cancel()
        self._send_frame(internal, Frame(
            frame_type=FrameType.AuthFail,
            key_id=0, data_type=DataType.String, payload=reason,
        ))
        if internal.ws:
            ws = internal.ws
            try:
                loop = asyncio.get_event_loop()
                loop.call_later(0.05, lambda: asyncio.ensure_future(self._close_ws(ws)))
            except RuntimeError:
                pass

    def get_session(self, uuid_str: str) -> DanWebSocketSession | None:
        internal = self._sessions.get(uuid_str)
        return internal.session if internal else None

    # -- Event registration --

    def on_connection(self, cb: Callable[[DanWebSocketSession], None]) -> Callable[[], None]:
        self._on_connection.append(cb)
        def unsub():
            if cb in self._on_connection:
                self._on_connection.remove(cb)
        return unsub

    def on_authorize(self, cb: Callable[[str, str], None]) -> Callable[[], None]:
        self._on_authorize.append(cb)
        def unsub():
            if cb in self._on_authorize:
                self._on_authorize.remove(cb)
        return unsub

    def on_session_expired(self, cb: Callable[[DanWebSocketSession], None]) -> Callable[[], None]:
        self._on_session_expired.append(cb)
        def unsub():
            if cb in self._on_session_expired:
                self._on_session_expired.remove(cb)
        return unsub

    # -- Start/Stop --

    async def start(self) -> None:
        self._server = await websockets.serve(
            self._handle_connection,
            "0.0.0.0",
            self._port,
        )

    def close(self) -> None:
        for internal in self._sessions.values():
            internal.session._dispose_all_topic_handles()
            internal.session._handle_disconnect()
            internal.heartbeat.stop()
            internal.bulk_queue.dispose()
            if internal.ttl_timer:
                internal.ttl_timer.cancel()
            if internal.ws:
                try:
                    asyncio.ensure_future(internal.ws.close())
                except Exception:
                    pass
        for internal in self._tmp_sessions.values():
            if internal.auth_timeout_handle:
                internal.auth_timeout_handle.cancel()
            if internal.ws:
                try:
                    asyncio.ensure_future(internal.ws.close())
                except Exception:
                    pass
        self._sessions.clear()
        self._tmp_sessions.clear()
        self._principal_index.clear()
        if self._server:
            self._server.close()

    # -- Internal --

    def _assert_mode(self, expected: str, method: str) -> None:
        if self.mode != expected:
            raise DanWSError("INVALID_MODE", f"server.{method}() is only available in {expected} mode.")

    def _log(self, msg: str, err: Exception | None = None) -> None:
        if self._debug:
            print(f"[dan-ws] {msg}", err or "", file=sys.stderr)

    def _bind_principal_tx(self, ptx: PrincipalTX) -> None:
        ptx._on_value(lambda frame: self._broadcast_to_principal(ptx.name, frame))
        ptx._on_incremental(lambda kf, sf, vf: self._broadcast_incremental(ptx.name, kf, sf, vf))
        ptx._on_resync_cb(lambda: self._broadcast_resync(ptx.name, ptx))

    def _broadcast_to_principal(self, principal_name: str, frame: Frame) -> None:
        for internal in self._get_sessions_for_principal(principal_name):
            if internal.session.state == "ready" and internal.ws:
                internal.bulk_queue.enqueue(frame)

    def _broadcast_incremental(self, principal_name: str, kf: Frame, sf: Frame, vf: Frame) -> None:
        for internal in self._get_sessions_for_principal(principal_name):
            if internal.session.state == "ready" and internal.ws:
                internal.bulk_queue.enqueue(kf)
                internal.bulk_queue.enqueue(sf)
                internal.bulk_queue.enqueue(vf)

    def _broadcast_resync(self, principal_name: str, ptx: PrincipalTX) -> None:
        key_frames = ptx._build_key_frames()
        for internal in self._get_sessions_for_principal(principal_name):
            if internal.session.connected and internal.ws:
                internal.bulk_queue.enqueue(Frame(
                    frame_type=FrameType.ServerReset,
                    key_id=0, data_type=DataType.Null, payload=None,
                ))
                for f in key_frames:
                    internal.bulk_queue.enqueue(f)

    def _get_sessions_for_principal(self, name: str) -> set[_InternalSession]:
        return self._principal_index.get(name, set())

    def _index_add_session(self, principal: str, internal: _InternalSession) -> None:
        if principal not in self._principal_index:
            self._principal_index[principal] = set()
        self._principal_index[principal].add(internal)

    def _index_remove_session(self, principal: str, internal: _InternalSession) -> None:
        s = self._principal_index.get(principal)
        if s:
            s.discard(internal)
            if not s:
                del self._principal_index[principal]

    async def _handle_connection(self, websocket: ServerConnection) -> None:
        parser = StreamParser()
        identified = False
        client_uuid = ""
        internal_ref: _InternalSession | None = None

        def on_heartbeat():
            nonlocal client_uuid
            internal = self._sessions.get(client_uuid) or self._tmp_sessions.get(client_uuid)
            if internal:
                internal.heartbeat.received()

        def on_frame(frame: Frame):
            nonlocal identified, client_uuid, internal_ref
            self._frames_in += 1

            if not identified:
                if frame.frame_type != FrameType.Identify:
                    asyncio.ensure_future(websocket.close())
                    return
                payload = frame.payload
                if isinstance(payload, (bytes, bytearray)) and len(payload) in (16, 18):
                    if len(payload) == 18:
                        client_major = payload[16]
                        if client_major != PROTOCOL_MAJOR:
                            self._log(f"Rejecting client with incompatible protocol: {client_major}")
                            asyncio.ensure_future(websocket.close())
                            return
                    client_uuid = _bytes_to_uuid(payload[:16])
                else:
                    asyncio.ensure_future(websocket.close())
                    return
                identified = True
                internal_ref = self._handle_identified(websocket, client_uuid)
                return

            # Auth frame
            if frame.frame_type == FrameType.Auth:
                internal = self._tmp_sessions.get(client_uuid)
                if internal and self._auth_enabled:
                    token = frame.payload
                    internal.auth_token = token
                    for cb in self._on_authorize:
                        try:
                            cb(client_uuid, token)
                        except Exception as e:
                            self._log("onAuthorize callback error", e)
                return

            # Client topic frames
            if self._is_topic_mode:
                internal = self._sessions.get(client_uuid)
                if internal:
                    if frame.frame_type in (
                        FrameType.ClientReset, FrameType.ClientKeyRegistration,
                        FrameType.ClientValue, FrameType.ClientSync,
                    ):
                        self._handle_client_topic_frame(internal, frame)
                        return

            # Route to session handler
            internal = self._sessions.get(client_uuid)
            if internal:
                internal.session._handle_frame(frame)

        def on_error(err: Exception):
            self._log("Stream parser error", err)

        parser.on_heartbeat(on_heartbeat)
        parser.on_frame(on_frame)
        parser.on_error(on_error)

        try:
            async for message in websocket:
                if isinstance(message, (bytes, bytearray)):
                    parser.feed(message)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if client_uuid:
                self._handle_session_disconnect(client_uuid)

    def _handle_identified(self, ws: ServerConnection, client_uuid: str) -> _InternalSession | None:
        total = len(self._sessions) + len(self._tmp_sessions)
        if self._max_connections > 0 and client_uuid not in self._sessions and total >= self._max_connections:
            self._log(f"Max connections reached ({self._max_connections})")
            asyncio.ensure_future(ws.close())
            return None

        existing = self._sessions.get(client_uuid)
        if existing:
            if existing.ws:
                try:
                    asyncio.ensure_future(existing.ws.close())
                except Exception:
                    pass
            if existing.ttl_timer:
                existing.ttl_timer.cancel()
                existing.ttl_timer = None

            existing.ws = ws
            existing.session._handle_reconnect()
            existing.heartbeat.start()
            existing.bulk_queue.on_flush(lambda data: self._send_raw(existing, data))

            if self._auth_enabled:
                self._tmp_sessions[client_uuid] = existing
                self._sessions.pop(client_uuid, None)
                self._start_auth_timeout(existing, client_uuid, ws)
            else:
                principal = existing.session.principal or BROADCAST_PRINCIPAL
                existing.session._authorize(principal)
                self._activate_session(existing, principal)
            return existing

        session = DanWebSocketSession(client_uuid)
        session._set_debug(self._debug)
        bulk_queue = BulkQueue(self._flush_interval)
        heartbeat = HeartbeatManager()

        internal = _InternalSession(session=session, ws=ws, bulk_queue=bulk_queue, heartbeat=heartbeat)

        session._set_enqueue(lambda f: bulk_queue.enqueue(f))
        bulk_queue.on_flush(lambda data: self._send_raw(internal, data))
        heartbeat.on_send(lambda data: self._send_raw_bytes(internal, data))
        heartbeat.on_timeout(lambda: self._handle_session_disconnect(client_uuid))
        heartbeat.start()

        if self._auth_enabled:
            self._tmp_sessions[client_uuid] = internal
            self._start_auth_timeout(internal, client_uuid, ws)
        else:
            default_principal = BROADCAST_PRINCIPAL if self.mode == "broadcast" else "default"
            session._authorize(default_principal)
            self._sessions[client_uuid] = internal
            self._activate_session(internal, default_principal)

        return internal

    def _activate_session(self, internal: _InternalSession, principal: str) -> None:
        if self._is_topic_mode:
            internal.session._bind_session_tx(lambda f: internal.bulk_queue.enqueue(f))
            for cb in self._on_connection:
                try:
                    cb(internal.session)
                except Exception as e:
                    self._log("onConnection callback error", e)
            internal.bulk_queue.enqueue(Frame(
                frame_type=FrameType.ServerSync,
                key_id=0, data_type=DataType.Null, payload=None,
            ))
        else:
            effective = BROADCAST_PRINCIPAL if self.mode == "broadcast" else principal
            ptx = self._principals.principal(effective)
            self._principals._add_session(effective)
            self._index_add_session(effective, internal)

            internal.session._set_tx_providers(
                lambda: ptx._build_key_frames(),
                lambda: ptx._build_value_frames(),
            )

            for cb in self._on_connection:
                try:
                    cb(internal.session)
                except Exception as e:
                    self._log("onConnection callback error", e)
            internal.session._start_sync()

    def _handle_client_topic_frame(self, internal: _InternalSession, frame: Frame) -> None:
        if frame.frame_type == FrameType.ClientReset:
            if internal.client_registry:
                internal.client_registry.clear()
            else:
                internal.client_registry = KeyRegistry()
            if internal.client_values is not None:
                internal.client_values.clear()
            else:
                internal.client_values = {}

        elif frame.frame_type == FrameType.ClientKeyRegistration:
            if not internal.client_registry:
                internal.client_registry = KeyRegistry()
            internal.client_registry.register_one(frame.key_id, frame.payload, frame.data_type)

        elif frame.frame_type == FrameType.ClientValue:
            if internal.client_values is None:
                internal.client_values = {}
            internal.client_values[frame.key_id] = frame.payload

        elif frame.frame_type == FrameType.ClientSync:
            self._process_topic_sync(internal)

    _TOPIC_NAME_VALID = re.compile(r"^[a-zA-Z0-9_.\-]{1,128}$")
    _MAX_TOPICS_PER_SESSION = 100

    def _process_topic_sync(self, internal: _InternalSession) -> None:
        session = internal.session
        new_topics: dict[str, dict[str, Any]] = {}
        name_to_index: dict[str, int] = {}

        if internal.client_registry and internal.client_values is not None:
            index_to_name: dict[str, str] = {}

            for path in internal.client_registry.paths:
                entry = internal.client_registry.get_by_path(path)
                if not entry:
                    continue

                if path.endswith(".name") and path.startswith("topic."):
                    idx_str = path[6:len(path) - 5]
                    topic_name = internal.client_values.get(entry.key_id)
                    if (
                        topic_name
                        and isinstance(topic_name, str)
                        and self._TOPIC_NAME_VALID.match(topic_name)
                        and topic_name != BROADCAST_PRINCIPAL
                        and len(new_topics) < self._MAX_TOPICS_PER_SESSION
                    ):
                        index_to_name[idx_str] = topic_name
                        name_to_index[topic_name] = int(idx_str)
                        if topic_name not in new_topics:
                            new_topics[topic_name] = {}
                else:
                    param_idx = path.find(".param.")
                    if param_idx != -1 and path.startswith("topic."):
                        idx_str = path[6:param_idx]
                        param_key = path[param_idx + 7:]
                        topic_name = index_to_name.get(idx_str)
                        if topic_name:
                            value = internal.client_values.get(entry.key_id)
                            if value is not None:
                                new_topics[topic_name][param_key] = value

        # Diff: unsubscribed
        old_topics = set(session.topics)
        for old_name in old_topics:
            if old_name not in new_topics:
                handle = session.get_topic_handle(old_name)
                if handle:
                    for cb in self._topic_on_unsubscribe:
                        try:
                            cb(session, handle)
                        except Exception as e:
                            self._log("topic.onUnsubscribe error", e)
                session._remove_topic_handle(old_name)
                session._remove_topic(old_name)

        # Diff: new / changed
        for name, params in new_topics.items():
            existing_handle = session.get_topic_handle(name)
            existing_info = session.topic(name)

            if not existing_handle and not existing_info:
                client_idx = name_to_index.get(name, session._next_topic_index)
                handle = session._create_topic_handle(name, params, client_idx)
                for cb in self._topic_on_subscribe:
                    try:
                        cb(session, handle)
                    except Exception as e:
                        self._log("topic.onSubscribe error", e)
            else:
                old_params = existing_handle.params if existing_handle else (existing_info.params if existing_info else {})
                if old_params != params:
                    if existing_handle:
                        existing_handle._update_params(params)
                    session._update_topic_params(name, params)

    def _handle_session_disconnect(self, uuid_str: str) -> None:
        self._frame_counters.pop(uuid_str, None)
        internal = self._sessions.get(uuid_str)
        if not internal:
            tmp = self._tmp_sessions.get(uuid_str)
            if tmp:
                del self._tmp_sessions[uuid_str]
                tmp.heartbeat.stop()
            return

        if not internal.session.connected:
            return

        internal.session._dispose_all_topic_handles()
        internal.session._handle_disconnect()
        internal.heartbeat.stop()
        internal.bulk_queue.clear()
        internal.ws = None

        def expire():
            self._sessions.pop(uuid_str, None)
            principal = internal.session.principal
            if principal and not self._is_topic_mode:
                effective = BROADCAST_PRINCIPAL if self.mode == "broadcast" else principal
                no_sessions = self._principals._remove_session(effective)
                self._index_remove_session(effective, internal)
            for cb in self._on_session_expired:
                try:
                    cb(internal.session)
                except Exception as e:
                    self._log("onSessionExpired error", e)

        try:
            loop = asyncio.get_event_loop()
            internal.ttl_timer = loop.call_later(self._ttl, expire)
        except RuntimeError:
            expire()

    def _send_frame(self, internal: _InternalSession, frame: Frame) -> None:
        if internal.ws:
            self._frames_out += 1
            data = encode(frame)
            try:
                asyncio.ensure_future(internal.ws.send(data))
            except Exception:
                pass

    def _send_raw(self, internal: _InternalSession, data: bytes) -> None:
        if internal.ws:
            try:
                asyncio.ensure_future(internal.ws.send(data))
            except Exception:
                pass

    def _send_raw_bytes(self, internal: _InternalSession, data: bytes) -> None:
        self._send_raw(internal, data)

    def _start_auth_timeout(self, internal: _InternalSession, client_uuid: str, ws: ServerConnection) -> None:
        def timeout():
            self._tmp_sessions.pop(client_uuid, None)
            asyncio.ensure_future(self._close_ws(ws))

        try:
            loop = asyncio.get_event_loop()
            internal.auth_timeout_handle = loop.call_later(self._auth_timeout, timeout)
        except RuntimeError:
            pass

    @staticmethod
    async def _close_ws(ws: ServerConnection) -> None:
        try:
            await ws.close()
        except Exception:
            pass


class _TopicNamespace:
    def __init__(self, server: DanWebSocketServer):
        self._server = server

    def on_subscribe(self, cb: Callable[[DanWebSocketSession, TopicHandle], None]) -> Callable[[], None]:
        self._server._topic_on_subscribe.append(cb)
        def unsub():
            if cb in self._server._topic_on_subscribe:
                self._server._topic_on_subscribe.remove(cb)
        return unsub

    def on_unsubscribe(self, cb: Callable[[DanWebSocketSession, TopicHandle], None]) -> Callable[[], None]:
        self._server._topic_on_unsubscribe.append(cb)
        def unsub():
            if cb in self._server._topic_on_unsubscribe:
                self._server._topic_on_unsubscribe.remove(cb)
        return unsub
