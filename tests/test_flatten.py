"""Tests for auto-flatten logic."""

import pytest

from dan_websocket.api.flatten import flatten_value, should_flatten


class TestShouldFlatten:
    def test_dict(self):
        assert should_flatten({"a": 1})

    def test_list(self):
        assert should_flatten([1, 2, 3])

    def test_primitives(self):
        assert not should_flatten(42)
        assert not should_flatten("hello")
        assert not should_flatten(None)
        assert not should_flatten(True)


class TestFlattenValue:
    def test_simple_dict(self):
        result = flatten_value("user", {"name": "Alice", "age": 30})
        assert result == {"user.name": "Alice", "user.age": 30}

    def test_array(self):
        result = flatten_value("scores", [10, 20, 30])
        assert result == {
            "scores.length": 3,
            "scores.0": 10,
            "scores.1": 20,
            "scores.2": 30,
        }

    def test_nested(self):
        result = flatten_value("data", {"items": [{"id": 1}]})
        assert result == {
            "data.items.length": 1,
            "data.items.0.id": 1,
        }

    def test_empty_array(self):
        result = flatten_value("arr", [])
        assert result == {"arr.length": 0}

    def test_empty_dict(self):
        result = flatten_value("obj", {})
        assert result == {}

    def test_leaf_value(self):
        result = flatten_value("key", 42)
        assert result == {"key": 42}

    def test_depth_limit(self):
        # Create deeply nested structure
        value: dict = {"a": 42}
        for _ in range(15):
            value = {"nested": value}
        with pytest.raises(ValueError, match="depth limit"):
            flatten_value("root", value)
