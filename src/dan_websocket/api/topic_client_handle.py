"""Client-side topic handle."""

from __future__ import annotations

from typing import Any, Callable

from ..state.key_registry import KeyRegistry


class TopicClientHandle:
    def __init__(
        self,
        name: str,
        index: int,
        registry: KeyRegistry,
        store_get: Callable[[int], Any],
        log: Callable[[str, Exception | None], None] | None = None,
    ):
        self.name = name
        self._index = index
        self._registry = registry
        self._store_get = store_get
        self._on_receive: list[Callable[[str, Any], None]] = []
        self._on_update: list[Callable[[dict], None]] = []
        self._log = log
        self._dirty = False

    def get(self, key: str) -> Any:
        wire_path = f"t.{self._index}.{key}"
        entry = self._registry.get_by_path(wire_path)
        if not entry:
            return None
        return self._store_get(entry.key_id)

    @property
    def keys(self) -> list[str]:
        prefix = f"t.{self._index}."
        result: list[str] = []
        for path in self._registry.paths:
            if path.startswith(prefix):
                result.append(path[len(prefix):])
        return result

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

    def _notify(self, user_key: str, value: Any) -> None:
        for cb in self._on_receive:
            try:
                cb(user_key, value)
            except Exception as e:
                if self._log:
                    self._log("topic onReceive error", e)
        self._dirty = True

    def _flush_update(self) -> None:
        if not self._dirty or not self._on_update:
            return
        self._dirty = False
        view = {k: self.get(k) for k in self.keys}
        for cb in self._on_update:
            try:
                cb(view)
            except Exception as e:
                if self._log:
                    self._log("topic onUpdate error", e)

    def _set_index(self, index: int) -> None:
        self._index = index
