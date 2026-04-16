"""Key registry for mapping keyId <-> keyPath."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

from ..protocol.types import DataType, DanWSError

KEY_PATH_REGEX = re.compile(r"^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*$")
MAX_KEY_PATH_BYTES = 200
DEFAULT_MAX_KEYS = 10_000

_validated_paths: set[str] = set()
_MAX_VALIDATED_CACHE = 10_000


def validate_key_path(path: str) -> None:
    if path in _validated_paths:
        return
    if not path:
        raise DanWSError("INVALID_KEY_PATH", "Key path must not be empty")
    if not KEY_PATH_REGEX.match(path):
        raise DanWSError("INVALID_KEY_PATH", f'Invalid key path: "{path}"')
    if len(path.encode("utf-8")) > MAX_KEY_PATH_BYTES:
        raise DanWSError("INVALID_KEY_PATH", f'Key path exceeds 200 bytes: "{path}"')
    if len(_validated_paths) >= _MAX_VALIDATED_CACHE:
        _validated_paths.clear()
    _validated_paths.add(path)


@dataclass
class KeyEntry:
    path: str
    data_type: DataType
    key_id: int


class KeyRegistry:
    def __init__(self, max_keys: int = DEFAULT_MAX_KEYS):
        self._by_id: dict[int, KeyEntry] = {}
        self._by_path: dict[str, KeyEntry] = {}
        self._next_id = 1
        self._cached_paths: list[str] | None = None
        self._max_keys = max_keys

    def _enforce_limit(self) -> None:
        if len(self._by_id) >= self._max_keys:
            raise DanWSError(
                "KEY_LIMIT_EXCEEDED",
                f"Key registry limit reached ({self._max_keys}). Too many unique keys.",
            )

    def register_one(self, key_id: int, path: str, data_type: DataType) -> None:
        validate_key_path(path)
        if path not in self._by_path:
            self._enforce_limit()
        entry = KeyEntry(path=path, data_type=data_type, key_id=key_id)
        self._by_id[key_id] = entry
        self._by_path[path] = entry
        if key_id >= self._next_id:
            self._next_id = key_id + 1
        self._cached_paths = None

    def get_by_key_id(self, key_id: int) -> KeyEntry | None:
        return self._by_id.get(key_id)

    def get_by_path(self, path: str) -> KeyEntry | None:
        return self._by_path.get(path)

    def has_key_id(self, key_id: int) -> bool:
        return key_id in self._by_id

    def has_path(self, path: str) -> bool:
        return path in self._by_path

    def remove_by_key_id(self, key_id: int) -> bool:
        entry = self._by_id.get(key_id)
        if not entry:
            return False
        del self._by_id[key_id]
        self._by_path.pop(entry.path, None)
        self._cached_paths = None
        return True

    @property
    def size(self) -> int:
        return len(self._by_id)

    @property
    def paths(self) -> list[str]:
        if self._cached_paths is None:
            self._cached_paths = list(self._by_path.keys())
        return self._cached_paths

    def entries(self) -> Iterator[KeyEntry]:
        return iter(self._by_id.values())

    def clear(self) -> None:
        self._by_id.clear()
        self._by_path.clear()
        self._next_id = 1
        self._cached_paths = None
