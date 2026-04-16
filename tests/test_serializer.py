"""Tests for serializer: roundtrip for all DataTypes."""

import math
import struct
from datetime import datetime, timezone

import pytest

from dan_websocket.protocol.serializer import serialize, deserialize, detect_data_type
from dan_websocket.protocol.types import DataType, DanWSError


class TestSerializerRoundtrip:
    def test_null(self):
        data = serialize(DataType.Null, None)
        assert data == b""
        assert deserialize(DataType.Null, data) is None

    def test_bool_true(self):
        data = serialize(DataType.Bool, True)
        assert data == b"\x01"
        assert deserialize(DataType.Bool, data) is True

    def test_bool_false(self):
        data = serialize(DataType.Bool, False)
        assert data == b"\x00"
        assert deserialize(DataType.Bool, data) is False

    def test_uint8(self):
        for v in [0, 1, 127, 255]:
            data = serialize(DataType.Uint8, v)
            assert deserialize(DataType.Uint8, data) == v

    def test_uint16(self):
        for v in [0, 1, 256, 65535]:
            data = serialize(DataType.Uint16, v)
            assert deserialize(DataType.Uint16, data) == v

    def test_uint32(self):
        for v in [0, 1, 65536, 0xFFFFFFFF]:
            data = serialize(DataType.Uint32, v)
            assert deserialize(DataType.Uint32, data) == v

    def test_uint64(self):
        for v in [0, 1, 2**32, 2**63]:
            data = serialize(DataType.Uint64, v)
            assert deserialize(DataType.Uint64, data) == v

    def test_int32(self):
        for v in [-2147483648, -1, 0, 1, 2147483647]:
            data = serialize(DataType.Int32, v)
            assert deserialize(DataType.Int32, data) == v

    def test_int64(self):
        for v in [-(2**63), -1, 0, 1, 2**63 - 1]:
            data = serialize(DataType.Int64, v)
            assert deserialize(DataType.Int64, data) == v

    def test_float32(self):
        for v in [0.0, 1.0, -1.0, 3.14]:
            data = serialize(DataType.Float32, v)
            result = deserialize(DataType.Float32, data)
            assert abs(result - v) < 0.001

    def test_float64(self):
        for v in [0.0, 1.0, -1.0, 3.141592653589793, 1e100]:
            data = serialize(DataType.Float64, v)
            assert deserialize(DataType.Float64, data) == v

    def test_string(self):
        for v in ["", "hello", "Alice", "unicode: \u4e16\u754c"]:
            data = serialize(DataType.String, v)
            assert deserialize(DataType.String, data) == v

    def test_binary(self):
        for v in [b"", b"\x00\x01\x02", b"\xff" * 100]:
            data = serialize(DataType.Binary, v)
            assert deserialize(DataType.Binary, data) == v

    def test_timestamp(self):
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        data = serialize(DataType.Timestamp, dt)
        result = deserialize(DataType.Timestamp, data)
        assert isinstance(result, datetime)
        assert abs((result - dt).total_seconds()) < 1

    def test_var_integer(self):
        for v in [0, 1, -1, 42, -42, 63, 64, -64, 300, -300, 100000, -100000]:
            data = serialize(DataType.VarInteger, v)
            assert deserialize(DataType.VarInteger, data) == v

    def test_var_integer_known_bytes(self):
        # 42 -> zigzag 84 -> VarInt 0x54
        data = serialize(DataType.VarInteger, 42)
        assert data == bytes([0x54])

        # 0 -> zigzag 0 -> VarInt 0x00
        data = serialize(DataType.VarInteger, 0)
        assert data == bytes([0x00])

        # -1 -> zigzag 1 -> VarInt 0x01
        data = serialize(DataType.VarInteger, -1)
        assert data == bytes([0x01])

    def test_var_double(self):
        for v in [3.14, -7.5, 0.001, 99.99, 0.5, 1.5]:
            data = serialize(DataType.VarDouble, v)
            result = deserialize(DataType.VarDouble, data)
            assert abs(result - v) < 1e-10, f"Expected {v}, got {result}"

    def test_var_double_known_bytes(self):
        # 3.14 -> scale=2, mantissa=314, positive -> first_byte=2
        data = serialize(DataType.VarDouble, 3.14)
        assert data[0] == 2  # scale=2, positive

    def test_var_double_fallback(self):
        # NaN
        data = serialize(DataType.VarDouble, float("nan"))
        assert data[0] == 0x80
        result = deserialize(DataType.VarDouble, data)
        assert math.isnan(result)

        # Infinity
        data = serialize(DataType.VarDouble, float("inf"))
        assert data[0] == 0x80
        result = deserialize(DataType.VarDouble, data)
        assert math.isinf(result) and result > 0

        # -Infinity
        data = serialize(DataType.VarDouble, float("-inf"))
        assert data[0] == 0x80
        result = deserialize(DataType.VarDouble, data)
        assert math.isinf(result) and result < 0

    def test_var_float(self):
        for v in [3.14, -7.5, 0.001]:
            data = serialize(DataType.VarFloat, v)
            result = deserialize(DataType.VarFloat, data)
            # VarFloat uses same scale+mantissa as VarDouble for non-fallback
            assert abs(result - v) < 1e-6, f"Expected {v}, got {result}"

    def test_var_float_fallback(self):
        data = serialize(DataType.VarFloat, float("nan"))
        assert data[0] == 0x80
        assert len(data) == 5  # 1 + 4 bytes for float32


class TestAutoDetect:
    def test_none(self):
        assert detect_data_type(None) == DataType.Null

    def test_bool(self):
        assert detect_data_type(True) == DataType.Bool
        assert detect_data_type(False) == DataType.Bool

    def test_int(self):
        assert detect_data_type(42) == DataType.VarInteger
        assert detect_data_type(-1) == DataType.VarInteger

    def test_float(self):
        assert detect_data_type(3.14) == DataType.VarDouble

    def test_string(self):
        assert detect_data_type("hello") == DataType.String

    def test_binary(self):
        assert detect_data_type(b"\x00\x01") == DataType.Binary

    def test_datetime(self):
        assert detect_data_type(datetime.now()) == DataType.Timestamp
