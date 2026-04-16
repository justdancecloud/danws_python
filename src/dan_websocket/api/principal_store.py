"""Principal-based state management."""

from __future__ import annotations

from typing import Any, Callable

from ..protocol.types import DataType, FrameType, Frame
from .flat_state_manager import FlatStateManager


class PrincipalTX:
    """Shared TX state for one principal."""

    def __init__(self, name: str, max_value_size: int | None = None):
        self.name = name
        self._next_key_id = 1
        self._on_value_set: Callable[[Frame], None] | None = None
        self._on_keys_changed: Callable[[], None] | None = None
        self._on_incremental_key: Callable[[Frame, Frame, Frame], None] | None = None
        self._cached_key_frames: list[Frame] | None = None
        self._flat_state = FlatStateManager(
            allocate_key_id=self._alloc_id,
            enqueue=lambda f: self._on_value_set(f) if self._on_value_set else None,
            on_resync=self._trigger_resync,
            wire_prefix="",
            on_incremental_key=lambda kf, sf, vf: (
                self._on_incremental_key(kf, sf, vf) if self._on_incremental_key
                else self._trigger_resync()
            ),
            on_key_structure_change=lambda: setattr(self, "_cached_key_frames", None),
            max_value_size=max_value_size,
        )

    def _alloc_id(self) -> int:
        kid = self._next_key_id
        self._next_key_id += 1
        return kid

    def _on_value(self, fn: Callable[[Frame], None]) -> None:
        self._on_value_set = fn

    def _on_resync_cb(self, fn: Callable[[], None]) -> None:
        self._on_keys_changed = fn

    def _on_incremental(self, fn: Callable[[Frame, Frame, Frame], None]) -> None:
        self._on_incremental_key = fn

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
            self._next_key_id = 1

    def _build_key_frames(self) -> list[Frame]:
        if self._cached_key_frames is not None:
            return self._cached_key_frames
        frames = self._flat_state.build_key_frames()
        frames.append(Frame(
            frame_type=FrameType.ServerSync,
            key_id=0,
            data_type=DataType.Null,
            payload=None,
        ))
        self._cached_key_frames = frames
        return frames

    def _build_value_frames(self) -> list[Frame]:
        return self._flat_state.build_value_frames()

    def _trigger_resync(self) -> None:
        self._cached_key_frames = None
        if self._on_keys_changed:
            self._on_keys_changed()


class PrincipalManager:
    """Manages all principals."""

    def __init__(self) -> None:
        self._principals: dict[str, PrincipalTX] = {}
        self._session_counts: dict[str, int] = {}
        self._on_new_principal: Callable[[PrincipalTX], None] | None = None
        self._max_value_size: int | None = None

    def _set_on_new_principal(self, fn: Callable[[PrincipalTX], None]) -> None:
        self._on_new_principal = fn

    def _set_max_value_size(self, size: int) -> None:
        self._max_value_size = size

    @property
    def size(self) -> int:
        return len(self._principals)

    def principal(self, name: str) -> PrincipalTX:
        ptx = self._principals.get(name)
        if ptx is None:
            ptx = PrincipalTX(name, self._max_value_size)
            self._principals[name] = ptx
            if self._on_new_principal:
                self._on_new_principal(ptx)
        return ptx

    @property
    def principals(self) -> list[str]:
        return [name for name, count in self._session_counts.items() if count > 0]

    def has(self, name: str) -> bool:
        return name in self._principals

    def delete(self, name: str) -> None:
        self._principals.pop(name, None)
        self._session_counts.pop(name, None)

    def _add_session(self, principal: str) -> None:
        self._session_counts[principal] = self._session_counts.get(principal, 0) + 1

    def _remove_session(self, principal: str) -> bool:
        count = self._session_counts.get(principal, 1) - 1
        if count <= 0:
            self._session_counts.pop(principal, None)
            return True
        self._session_counts[principal] = count
        return False

    def _has_active_sessions(self, principal: str) -> bool:
        return self._session_counts.get(principal, 0) > 0
