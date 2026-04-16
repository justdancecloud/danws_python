"""Reconnection engine with exponential backoff."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Callable, Awaitable


@dataclass
class ReconnectOptions:
    enabled: bool = True
    max_retries: int = 10  # 0 = unlimited
    base_delay: float = 1.0  # seconds
    max_delay: float = 30.0
    backoff_multiplier: float = 2.0
    jitter: bool = True


class ReconnectEngine:
    def __init__(self, options: ReconnectOptions | None = None):
        self._options = options or ReconnectOptions()
        self._attempt = 0
        self._active = False
        self._timer: asyncio.TimerHandle | None = None
        self._on_reconnect: Callable[[int, float], None] | None = None
        self._on_exhausted: Callable[[], None] | None = None
        self._on_attempt: Callable[[], Awaitable[None] | None] | None = None

    def on_reconnect(self, callback: Callable[[int, float], None]) -> None:
        self._on_reconnect = callback

    def on_exhausted(self, callback: Callable[[], None]) -> None:
        self._on_exhausted = callback

    def on_attempt(self, callback: Callable[[], Awaitable[None] | None]) -> None:
        self._on_attempt = callback

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def is_active(self) -> bool:
        return self._active

    def start(self) -> None:
        if not self._options.enabled or self._active:
            return
        self._active = True
        self._attempt = 0
        self._schedule_next()

    def stop(self) -> None:
        self._active = False
        self._attempt = 0
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def calculate_delay(self, attempt: int) -> float:
        raw = self._options.base_delay * (self._options.backoff_multiplier ** (attempt - 1))
        capped = min(raw, self._options.max_delay)
        if self._options.jitter:
            return capped * (0.5 + random.random())
        return capped

    def retry(self) -> None:
        if self._active:
            self._schedule_next()

    def _schedule_next(self) -> None:
        self._attempt += 1

        if self._options.max_retries > 0 and self._attempt > self._options.max_retries:
            self._active = False
            if self._on_exhausted:
                self._on_exhausted()
            return

        delay = self.calculate_delay(self._attempt)

        if self._on_reconnect:
            self._on_reconnect(self._attempt, delay)

        try:
            loop = asyncio.get_event_loop()
            self._timer = loop.call_later(delay, self._fire_attempt)
        except RuntimeError:
            pass

    def _fire_attempt(self) -> None:
        self._timer = None
        if self._on_attempt:
            result = self._on_attempt()
            if asyncio.iscoroutine(result):
                asyncio.ensure_future(result)
