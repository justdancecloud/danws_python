"""Tests for the key registry."""

import pytest

from dan_websocket.protocol.types import DataType, DanWSError
from dan_websocket.state.key_registry import KeyRegistry, validate_key_path


class TestValidateKeyPath:
    def test_valid_paths(self):
        for path in ["key", "a.b", "sensor_1.value", "root.users.0.name"]:
            validate_key_path(path)  # should not raise

    def test_empty_path(self):
        with pytest.raises(DanWSError, match="must not be empty"):
            validate_key_path("")

    def test_invalid_chars(self):
        with pytest.raises(DanWSError, match="Invalid key path"):
            validate_key_path("key with space")

    def test_leading_dot(self):
        with pytest.raises(DanWSError, match="Invalid key path"):
            validate_key_path(".key")

    def test_trailing_dot(self):
        with pytest.raises(DanWSError, match="Invalid key path"):
            validate_key_path("key.")

    def test_consecutive_dots(self):
        with pytest.raises(DanWSError, match="Invalid key path"):
            validate_key_path("key..value")


class TestKeyRegistry:
    def test_register_and_lookup(self):
        reg = KeyRegistry()
        reg.register_one(1, "sensor.temp", DataType.VarInteger)

        entry = reg.get_by_key_id(1)
        assert entry is not None
        assert entry.path == "sensor.temp"
        assert entry.data_type == DataType.VarInteger

        entry2 = reg.get_by_path("sensor.temp")
        assert entry2 is not None
        assert entry2.key_id == 1

    def test_has_key_id(self):
        reg = KeyRegistry()
        assert not reg.has_key_id(1)
        reg.register_one(1, "key", DataType.String)
        assert reg.has_key_id(1)

    def test_has_path(self):
        reg = KeyRegistry()
        assert not reg.has_path("key")
        reg.register_one(1, "key", DataType.String)
        assert reg.has_path("key")

    def test_remove_by_key_id(self):
        reg = KeyRegistry()
        reg.register_one(1, "key", DataType.String)
        assert reg.remove_by_key_id(1)
        assert not reg.has_key_id(1)
        assert not reg.has_path("key")
        assert not reg.remove_by_key_id(1)

    def test_size(self):
        reg = KeyRegistry()
        assert reg.size == 0
        reg.register_one(1, "a", DataType.Null)
        reg.register_one(2, "b", DataType.Null)
        assert reg.size == 2

    def test_paths(self):
        reg = KeyRegistry()
        reg.register_one(1, "a", DataType.Null)
        reg.register_one(2, "b", DataType.Null)
        assert set(reg.paths) == {"a", "b"}

    def test_clear(self):
        reg = KeyRegistry()
        reg.register_one(1, "a", DataType.Null)
        reg.clear()
        assert reg.size == 0
        assert not reg.has_key_id(1)

    def test_limit(self):
        reg = KeyRegistry(max_keys=2)
        reg.register_one(1, "a", DataType.Null)
        reg.register_one(2, "b", DataType.Null)
        with pytest.raises(DanWSError, match="Key registry limit"):
            reg.register_one(3, "c", DataType.Null)
