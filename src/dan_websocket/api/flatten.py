"""Auto-flatten objects/arrays into dot-path leaf keys."""

from __future__ import annotations

from datetime import datetime
from typing import Any

MAX_DEPTH = 10


def is_plain_object(v: Any) -> bool:
    """Check if a value is a plain dict (not a special type)."""
    return isinstance(v, dict)


def should_flatten(value: Any) -> bool:
    """Check if a value should be auto-flattened."""
    return isinstance(value, (dict, list))


def flatten_value(
    prefix: str,
    value: Any,
    depth: int = 0,
    seen: set | None = None,
) -> dict[str, Any]:
    """Flatten an object/array into a dict of dot-path -> leaf value."""
    if seen is None:
        seen = set()

    result: dict[str, Any] = {}

    if depth > MAX_DEPTH:
        raise ValueError(f'Auto-flatten depth limit exceeded (max {MAX_DEPTH}) at path "{prefix}"')

    if isinstance(value, list):
        obj_id = id(value)
        if obj_id in seen:
            raise ValueError(f'Circular reference detected at path "{prefix}"')
        seen.add(obj_id)
        prefix_dot = prefix + "."
        result[prefix_dot + "length"] = len(value)
        for i, child in enumerate(value):
            child_path = prefix_dot + str(i)
            if isinstance(child, (dict, list)):
                result.update(flatten_value(child_path, child, depth + 1, seen))
            else:
                result[child_path] = child
        seen.discard(obj_id)
        return result

    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen:
            raise ValueError(f'Circular reference detected at path "{prefix}"')
        seen.add(obj_id)
        prefix_dot = prefix + "."
        for key, child in value.items():
            child_path = prefix_dot + str(key)
            if isinstance(child, (dict, list)):
                result.update(flatten_value(child_path, child, depth + 1, seen))
            else:
                result[child_path] = child
        seen.discard(obj_id)
        return result

    # Leaf value
    result[prefix] = value
    return result
