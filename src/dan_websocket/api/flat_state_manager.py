"""Shared flatten + diff + setLeaf logic for state management."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ..protocol.types import DataType, FrameType, Frame, DanWSError
from ..protocol.serializer import serialize, detect_data_type
from ..state.key_registry import validate_key_path
from .flatten import flatten_value, should_flatten


@dataclass
class FlatEntry:
    key_id: int
    data_type: DataType
    value: Any


class FlatStateManager:
    def __init__(
        self,
        allocate_key_id: Callable[[], int],
        enqueue: Callable[[Frame], None],
        on_resync: Callable[[], None],
        wire_prefix: str = "",
        on_incremental_key: Callable[[Frame, Frame, Frame], None] | None = None,
        on_key_structure_change: Callable[[], None] | None = None,
        max_value_size: int | None = None,
    ):
        self._entries: dict[str, FlatEntry] = {}
        self._by_key_id: dict[int, tuple[str, FlatEntry]] = {}
        self._flattened_keys: dict[str, set[str]] = {}
        self._previous_arrays: dict[str, list] = {}
        self._freed_key_ids: list[int] = []

        self._allocate_key_id_fn = allocate_key_id
        self._enqueue = enqueue
        self._on_resync = on_resync
        self._wire_prefix = wire_prefix
        self._on_incremental_key = on_incremental_key
        self._on_key_structure_change = on_key_structure_change
        self._max_value_size = max_value_size

    def _allocate_key_id(self) -> int:
        if self._freed_key_ids:
            return self._freed_key_ids.pop()
        return self._allocate_key_id_fn()

    def _free_key_id(self, key_id: int) -> None:
        if len(self._freed_key_ids) < 10_000:
            self._freed_key_ids.append(key_id)

    def set(self, key: str, value: Any) -> None:
        if should_flatten(value):
            if isinstance(value, list):
                self._previous_arrays[key] = list(value)

            flattened = flatten_value(key, value)
            new_keys = set(flattened.keys())
            old_keys = self._flattened_keys.get(key)
            structure_changed = False
            if old_keys:
                for old_path in old_keys:
                    if old_path not in new_keys:
                        entry = self._entries.get(old_path)
                        if entry:
                            self._enqueue(Frame(
                                frame_type=FrameType.ServerKeyDelete,
                                key_id=entry.key_id,
                                data_type=DataType.Null,
                                payload=None,
                            ))
                            self._free_key_id(entry.key_id)
                            self._by_key_id.pop(entry.key_id, None)
                            del self._entries[old_path]
                            structure_changed = True
            self._flattened_keys[key] = new_keys
            for path, leaf in flattened.items():
                self._set_leaf(path, leaf)
            if structure_changed and self._on_key_structure_change:
                self._on_key_structure_change()
            return

        if self._set_leaf(key, value):
            self._on_resync()

    def get(self, key: str) -> Any:
        entry = self._entries.get(key)
        return entry.value if entry else None

    @property
    def keys(self) -> list[str]:
        return list(self._entries.keys())

    @property
    def size(self) -> int:
        return len(self._entries)

    def clear(self, key: str | None = None) -> None:
        if key is not None:
            flat_keys = self._flattened_keys.get(key)
            if flat_keys:
                for path in flat_keys:
                    entry = self._entries.get(path)
                    if entry:
                        self._enqueue(Frame(
                            frame_type=FrameType.ServerKeyDelete,
                            key_id=entry.key_id,
                            data_type=DataType.Null,
                            payload=None,
                        ))
                        self._free_key_id(entry.key_id)
                        self._by_key_id.pop(entry.key_id, None)
                        del self._entries[path]
                del self._flattened_keys[key]
                self._previous_arrays.pop(key, None)
                if self._on_key_structure_change:
                    self._on_key_structure_change()
            else:
                entry = self._entries.get(key)
                if entry:
                    self._enqueue(Frame(
                        frame_type=FrameType.ServerKeyDelete,
                        key_id=entry.key_id,
                        data_type=DataType.Null,
                        payload=None,
                    ))
                    self._free_key_id(entry.key_id)
                    self._by_key_id.pop(entry.key_id, None)
                    del self._entries[key]
                    self._previous_arrays.pop(key, None)
                    if self._on_key_structure_change:
                        self._on_key_structure_change()
        else:
            if self._entries:
                for entry in self._entries.values():
                    self._freed_key_ids.append(entry.key_id)
                self._entries.clear()
                self._by_key_id.clear()
                self._flattened_keys.clear()
                self._previous_arrays.clear()
                if self._on_key_structure_change:
                    self._on_key_structure_change()
                self._on_resync()

    def build_key_frames(self) -> list[Frame]:
        frames: list[Frame] = []
        for key, entry in self._entries.items():
            wire_path = f"{self._wire_prefix}{key}" if self._wire_prefix else key
            frames.append(Frame(
                frame_type=FrameType.ServerKeyRegistration,
                key_id=entry.key_id,
                data_type=entry.data_type,
                payload=wire_path,
            ))
        return frames

    def build_value_frames(self) -> list[Frame]:
        frames: list[Frame] = []
        for entry in self._entries.values():
            if entry.value is not None:
                frames.append(Frame(
                    frame_type=FrameType.ServerValue,
                    key_id=entry.key_id,
                    data_type=entry.data_type,
                    payload=entry.value,
                ))
        return frames

    def build_all_frames(self) -> tuple[list[Frame], list[Frame]]:
        key_frames: list[Frame] = []
        value_frames: list[Frame] = []
        for key, entry in self._entries.items():
            wire_path = f"{self._wire_prefix}{key}" if self._wire_prefix else key
            key_frames.append(Frame(
                frame_type=FrameType.ServerKeyRegistration,
                key_id=entry.key_id,
                data_type=entry.data_type,
                payload=wire_path,
            ))
            if entry.value is not None:
                value_frames.append(Frame(
                    frame_type=FrameType.ServerValue,
                    key_id=entry.key_id,
                    data_type=entry.data_type,
                    payload=entry.value,
                ))
        return key_frames, value_frames

    def get_by_key_id(self, key_id: int) -> tuple[str, FlatEntry] | None:
        return self._by_key_id.get(key_id)

    def _set_leaf(self, key: str, value: Any) -> bool:
        """Returns True if resync is needed (currently always False)."""
        validate_key_path(key)
        new_type = detect_data_type(value)

        existing = self._entries.get(key)

        if existing:
            if existing.value is value:
                return False

            if existing.data_type != new_type:
                # Type changed - delete old, register new
                self._enqueue(Frame(
                    frame_type=FrameType.ServerKeyDelete,
                    key_id=existing.key_id,
                    data_type=DataType.Null,
                    payload=None,
                ))
                self._free_key_id(existing.key_id)
                self._by_key_id.pop(existing.key_id, None)
                del self._entries[key]
                if self._on_key_structure_change:
                    self._on_key_structure_change()

                new_key_id = self._allocate_key_id()
                new_entry = FlatEntry(key_id=new_key_id, data_type=new_type, value=value)
                self._entries[key] = new_entry
                self._by_key_id[new_key_id] = (key, new_entry)
                wire_path = f"{self._wire_prefix}{key}" if self._wire_prefix else key
                self._enqueue(Frame(
                    frame_type=FrameType.ServerKeyRegistration,
                    key_id=new_key_id, data_type=new_type, payload=wire_path,
                ))
                self._enqueue(Frame(
                    frame_type=FrameType.ServerSync,
                    key_id=0, data_type=DataType.Null, payload=None,
                ))
                self._enqueue(Frame(
                    frame_type=FrameType.ServerValue,
                    key_id=new_key_id, data_type=new_type, payload=value,
                ))
                return False

            existing.value = value
            self._enqueue(Frame(
                frame_type=FrameType.ServerValue,
                key_id=existing.key_id,
                data_type=existing.data_type,
                payload=value,
            ))
            return False

        # New key
        key_id = self._allocate_key_id()
        new_entry = FlatEntry(key_id=key_id, data_type=new_type, value=value)
        self._entries[key] = new_entry
        self._by_key_id[key_id] = (key, new_entry)
        if self._on_key_structure_change:
            self._on_key_structure_change()
        wire_path = f"{self._wire_prefix}{key}" if self._wire_prefix else key
        key_frame = Frame(
            frame_type=FrameType.ServerKeyRegistration,
            key_id=key_id, data_type=new_type, payload=wire_path,
        )
        sync_frame = Frame(
            frame_type=FrameType.ServerSync,
            key_id=0, data_type=DataType.Null, payload=None,
        )
        value_frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=key_id, data_type=new_type, payload=value,
        )
        if self._on_incremental_key:
            self._on_incremental_key(key_frame, sync_frame, value_frame)
        else:
            self._enqueue(key_frame)
            self._enqueue(sync_frame)
            self._enqueue(value_frame)
        return False
