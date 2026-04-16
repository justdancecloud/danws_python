"""Heartbeat manager for DanProtocol connections."""

from __future__ import annotations

import asyncio
import time
from typing import Callable, Awaitable

from ..protocol.codec import encode_heartbeat

SEND_INTERVAL = 10.0  # seconds
TIMEOUT_THRESHOLD = 15.0  # seconds
CHECK_INTERVAL = 5.0  # seconds


class HeartbeatManager:
    def __init__(self) -> None:
        self._send_task: asyncio.Task | None = None
        self._check_task: asyncio.Task | None = None
        self._last_received: float = 0.0
        self._on_send: Callable[[bytes], Awaitable[None] | None] | None = None
        self._on_timeout: Callable[[], Awaitable[None] | None] | None = None
        self._running = False

    def on_send(self, callback: Callable[[bytes], Awaitable[None] | None]) -> None:
        self._on_send = callback

    def on_timeout(self, callback: Callable[[], Awaitable[None] | None]) -> None:
        self._on_timeout = callback

    def start(self) -> None:
        self.stop()
        self._last_received = time.monotonic()
        self._running = True

        loop = asyncio.get_event_loop()
        self._send_task = loop.create_task(self._send_loop())
        self._check_task = loop.create_task(self._check_loop())

    def received(self) -> None:
        self._last_received = time.monotonic()

    def stop(self) -> None:
        self._running = False
        if self._send_task and not self._send_task.done():
            self._send_task.cancel()
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
        self._send_task = None
        self._check_task = None

    @property
    def is_running(self) -> bool:
        return self._running

    async def _send_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(SEND_INTERVAL)
                if self._running and self._on_send:
                    result = self._on_send(encode_heartbeat())
                    if asyncio.iscoroutine(result):
                        await result
        except asyncio.CancelledError:
            pass

    async def _check_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(CHECK_INTERVAL)
                if self._running and time.monotonic() - self._last_received > TIMEOUT_THRESHOLD:
                    self.stop()
                    if self._on_timeout:
                        result = self._on_timeout()
                        if asyncio.iscoroutine(result):
                            await result
                    break
        except asyncio.CancelledError:
            pass
