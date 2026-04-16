# dan-websocket (Python)

[![PyPI](https://img.shields.io/pypi/v/dan-websocket)](https://pypi.org/project/dan-websocket/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

Python server and client library for the **DanProtocol v3.5** real-time state synchronization protocol. Wire-compatible with the [TypeScript](https://www.npmjs.com/package/dan-websocket) and [Java](https://central.sonatype.com/artifact/io.github.justdancecloud/dan-websocket) implementations.

## Why?

DanProtocol is designed for pushing real-time state from server to clients with minimal bandwidth. A boolean update is ~13 bytes total vs ~30+ bytes for JSON. It uses binary framing, auto-flatten for objects/arrays, VarNumber encoding, and principal-based state isolation.

This Python package brings the same protocol to the Python ecosystem with full async/await support via `asyncio` and `websockets`.

## Use Cases

- Real-time dashboards and monitoring
- IoT sensor data streaming
- Multiplayer game state synchronization
- Live collaboration tools
- Any scenario requiring efficient server-to-client state push

## Installation

```bash
pip install dan-websocket
```

Requires Python 3.10+ and `websockets>=12.0`.

## Quick Start

### Server (Broadcast Mode)

```python
import asyncio
from dan_websocket import DanWebSocketServer

async def main():
    server = DanWebSocketServer(port=9010, mode="broadcast")
    await server.start()

    # Set state - automatically pushed to all connected clients
    server.set("temperature", 23.5)
    server.set("status", "online")

    # Objects and arrays are auto-flattened
    server.set("sensor", {"id": 1, "name": "Main", "readings": [10, 20, 30]})
    # Clients receive: sensor.id=1, sensor.name="Main",
    #   sensor.readings.length=3, sensor.readings.0=10, ...

    # Keep running
    await asyncio.Event().wait()

asyncio.run(main())
```

### Client

```python
import asyncio
from dan_websocket import DanWebSocketClient

async def main():
    client = DanWebSocketClient("ws://localhost:9010")

    @client.on_ready
    def on_ready():
        print(f"Connected! Keys: {client.keys}")
        print(f"Temperature: {client.get('temperature')}")

    @client.on_receive
    def on_receive(key, value):
        print(f"Update: {key} = {value}")

    await client.connect()
    await asyncio.Event().wait()

asyncio.run(main())
```

### Server (Principal Mode - Per-User State)

```python
import asyncio
from dan_websocket import DanWebSocketServer

async def main():
    server = DanWebSocketServer(port=9010, mode="principal")
    server.enable_authorization(True)

    def on_auth(client_uuid, token):
        # Validate token and assign principal
        user = validate_token(token)
        if user:
            server.authorize(client_uuid, token, user.name)
            server.principal(user.name).set("balance", user.balance)
        else:
            server.reject(client_uuid, "Invalid token")

    server.on_authorize(on_auth)
    await server.start()
    await asyncio.Event().wait()

asyncio.run(main())
```

### Server (Topic Mode - Subscription-Based)

```python
import asyncio
from dan_websocket import DanWebSocketServer, EventType

async def main():
    server = DanWebSocketServer(port=9010, mode="session_topic")

    def on_subscribe(session, topic):
        # topic.name = subscription name
        # topic.params = subscription parameters
        topic.payload.set("title", f"Data for {topic.name}")
        topic.payload.set("items", [1, 2, 3])

        # Set a delayed task for periodic updates
        topic.set_delayed_task(1000)  # ms

        # Handle callback events
        topic.set_callback(lambda event, t, s: (
            t.payload.set("updated", True)
            if event == EventType.DelayedTaskEvent
            else None
        ))

    server.topic.on_subscribe(on_subscribe)
    await server.start()
    await asyncio.Event().wait()

asyncio.run(main())
```

### Client with Topics

```python
import asyncio
from dan_websocket import DanWebSocketClient

async def main():
    client = DanWebSocketClient("ws://localhost:9010")

    @client.on_ready
    def on_ready():
        # Subscribe to topics
        client.subscribe("board", {"roomId": "abc"})

        handle = client.topic("board")
        handle.on_receive(lambda key, value: print(f"Topic update: {key}={value}"))

    await client.connect()
    await asyncio.Event().wait()

asyncio.run(main())
```

## Server Modes

| Mode | Description |
|------|-------------|
| `broadcast` | Single shared state pushed to all clients |
| `principal` | Per-authenticated-user state (multiple devices share one state) |
| `session_topic` | Clients subscribe to named topics with parameters |
| `session_principal_topic` | Topics + principal-based authentication |

## API Reference

### DanWebSocketServer

```python
server = DanWebSocketServer(
    port=9010,
    mode="broadcast",       # broadcast | principal | session_topic | session_principal_topic
    ttl=600_000,            # session TTL in ms (default: 10 min)
    flush_interval_ms=100,  # batch flush interval (default: 100ms)
    max_connections=0,       # 0 = unlimited
    max_frames_per_sec=0,   # 0 = unlimited
)
```

### DanWebSocketClient

```python
client = DanWebSocketClient(
    "ws://localhost:9010",
    reconnect=ReconnectOptions(
        enabled=True,
        max_retries=10,
        base_delay=1.0,
        max_delay=30.0,
    ),
)
```

## Cross-Language Compatibility

This implementation is wire-compatible with:
- **TypeScript**: `npm install dan-websocket`
- **Java**: `io.github.justdancecloud:dan-websocket` (Maven Central)

A Python client can connect to a Java/TypeScript server and vice versa.

## Running Tests

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

## License

MIT
