"""Integration tests: Python server <-> Python client."""

import asyncio
import pytest
import pytest_asyncio

from dan_websocket.api.server import DanWebSocketServer
from dan_websocket.api.client import DanWebSocketClient
from dan_websocket.api.topic_handle import EventType
from dan_websocket.connection.reconnect_engine import ReconnectOptions


def _find_free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


async def _wait_for(condition, timeout=5.0, interval=0.05):
    """Wait until condition() is truthy."""
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return condition()


class TestBroadcastMode:
    @pytest.mark.asyncio
    async def test_basic_broadcast(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            server.set("counter", 42)
            server.set("name", "hello")

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            received = {}

            def on_receive(key, value):
                received[key] = value

            ready = asyncio.Event()
            client.on_receive(on_receive)
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            assert client.get("counter") == 42
            assert client.get("name") == "hello"

            # Live update
            server.set("counter", 100)
            await _wait_for(lambda: received.get("counter") == 100)
            assert received["counter"] == 100

            client.disconnect()
        finally:
            server.close()

    @pytest.mark.asyncio
    async def test_broadcast_object_flatten(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            server.set("user", {"name": "Alice", "age": 30})

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            assert client.get("user.name") == "Alice"
            assert client.get("user.age") == 30

            client.disconnect()
        finally:
            server.close()

    @pytest.mark.asyncio
    async def test_broadcast_array_flatten(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            server.set("scores", [10, 20, 30])

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            assert client.get("scores.length") == 3
            assert client.get("scores.0") == 10
            assert client.get("scores.1") == 20
            assert client.get("scores.2") == 30

            client.disconnect()
        finally:
            server.close()

    @pytest.mark.asyncio
    async def test_broadcast_keys_and_clear(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            server.set("a", 1)
            server.set("b", 2)
            assert "a" in server.keys
            assert "b" in server.keys

            server.clear("a")
            assert "a" not in server.keys
            assert "b" in server.keys
        finally:
            server.close()


class TestPrincipalMode:
    @pytest.mark.asyncio
    async def test_principal_basic(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="principal")
        server.enable_authorization(True)
        await server.start()

        try:
            # Pre-set data for the principal BEFORE auth happens
            server.principal("user1").set("balance", 100)

            def on_auth(client_uuid, token):
                if token == "valid-token":
                    server.authorize(client_uuid, token, "user1")
                else:
                    server.reject(client_uuid, "Invalid token")

            server.on_authorize(on_auth)

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())
            client.on_connect(lambda: client.authorize("valid-token"))

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            # Wait for the value to arrive
            await _wait_for(lambda: client.get("balance") == 100)
            assert client.get("balance") == 100

            # Live update
            server.principal("user1").set("balance", 200)
            await _wait_for(lambda: client.get("balance") == 200)
            assert client.get("balance") == 200

            client.disconnect()
        finally:
            server.close()

    @pytest.mark.asyncio
    async def test_principal_rejection(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="principal")
        server.enable_authorization(True)
        await server.start()

        try:
            def on_auth(client_uuid, token):
                server.reject(client_uuid, "Bad token")

            server.on_authorize(on_auth)

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            errors = []
            client.on_error(lambda e: errors.append(e))
            client.on_connect(lambda: client.authorize("bad-token"))

            await client.connect()
            await _wait_for(lambda: len(errors) > 0)
            assert any(e.code == "AUTH_REJECTED" for e in errors)

            client.disconnect()
        finally:
            server.close()


class TestSessionTopicMode:
    @pytest.mark.asyncio
    async def test_topic_subscribe(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="session_topic")
        await server.start()

        try:
            def on_subscribe(session, topic):
                topic.payload.set("title", f"Board for {topic.name}")
                topic.payload.set("count", 42)

            server.topic.on_subscribe(on_subscribe)

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            # Subscribe to a topic
            topic_handle = client.topic("board")
            received = {}
            topic_handle.on_receive(lambda k, v: received.update({k: v}))

            client.subscribe("board", {"roomId": "abc"})

            await _wait_for(lambda: received.get("title") is not None)
            assert received["title"] == "Board for board"
            assert received["count"] == 42

            client.disconnect()
        finally:
            server.close()

    @pytest.mark.asyncio
    async def test_topic_unsubscribe(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="session_topic")
        await server.start()

        try:
            subscribed = []
            unsubscribed = []

            def on_sub(session, topic):
                subscribed.append(topic.name)
                topic.payload.set("data", "yes")

            def on_unsub(session, topic):
                unsubscribed.append(topic.name)

            server.topic.on_subscribe(on_sub)
            server.topic.on_unsubscribe(on_unsub)

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            client.subscribe("topic1")
            await _wait_for(lambda: "topic1" in subscribed)

            client.unsubscribe("topic1")
            await _wait_for(lambda: "topic1" in unsubscribed)

            client.disconnect()
        finally:
            server.close()


class TestSessionPrincipalTopicMode:
    @pytest.mark.asyncio
    async def test_principal_topic(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="session_principal_topic")
        server.enable_authorization(True)
        await server.start()

        try:
            def on_auth(client_uuid, token):
                server.authorize(client_uuid, token, "user1")

            def on_sub(session, topic):
                topic.payload.set("msg", f"Hello {session.principal}")

            server.on_authorize(on_auth)
            server.topic.on_subscribe(on_sub)

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())
            client.on_connect(lambda: client.authorize("token"))

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            topic_handle = client.topic("chat")
            received = {}
            topic_handle.on_receive(lambda k, v: received.update({k: v}))

            client.subscribe("chat")
            await _wait_for(lambda: received.get("msg") is not None)
            assert received["msg"] == "Hello user1"

            client.disconnect()
        finally:
            server.close()


class TestMetrics:
    @pytest.mark.asyncio
    async def test_metrics(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            m = server.metrics()
            assert m["activeSessions"] == 0

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            m = server.metrics()
            assert m["activeSessions"] == 1

            client.disconnect()
        finally:
            server.close()


class TestOnConnection:
    @pytest.mark.asyncio
    async def test_on_connection_callback(self):
        port = _find_free_port()
        server = DanWebSocketServer(port=port, mode="broadcast")
        await server.start()

        try:
            sessions = []
            server.on_connection(lambda s: sessions.append(s))

            client = DanWebSocketClient(
                f"ws://localhost:{port}",
                reconnect=ReconnectOptions(enabled=False),
            )
            ready = asyncio.Event()
            client.on_ready(lambda: ready.set())

            await client.connect()
            await asyncio.wait_for(ready.wait(), timeout=5.0)

            assert len(sessions) == 1
            assert sessions[0].id == client.id

            client.disconnect()
        finally:
            server.close()
