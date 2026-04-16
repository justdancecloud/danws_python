"""Serialization and deserialization for DanProtocol data types."""

from __future__ import annotations

import math
import struct
from datetime import datetime, timezone
from typing import Any

from .types import DataType, DanWSError, DATA_TYPE_SIZES


def serialize(data_type: DataType, value: Any) -> bytes:
    if data_type == DataType.Null:
        return b""

    if data_type == DataType.Bool:
        return b"\x01" if value else b"\x00"

    if data_type == DataType.Uint8:
        return bytes([value & 0xFF])

    if data_type == DataType.Uint16:
        return struct.pack(">H", value)

    if data_type == DataType.Uint32:
        return struct.pack(">I", value)

    if data_type == DataType.Uint64:
        return struct.pack(">Q", value)

    if data_type == DataType.Int32:
        return struct.pack(">i", value)

    if data_type == DataType.Int64:
        return struct.pack(">q", value)

    if data_type == DataType.Float32:
        return struct.pack(">f", value)

    if data_type == DataType.Float64:
        return struct.pack(">d", value)

    if data_type == DataType.String:
        if isinstance(value, str):
            return value.encode("utf-8")
        raise DanWSError("INVALID_VALUE_TYPE", f"String requires str, got {type(value).__name__}")

    if data_type == DataType.Binary:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        raise DanWSError("INVALID_VALUE_TYPE", f"Binary requires bytes, got {type(value).__name__}")

    if data_type == DataType.Timestamp:
        if isinstance(value, datetime):
            ms = int(value.timestamp() * 1000)
        elif isinstance(value, (int, float)):
            ms = int(value)
        else:
            raise DanWSError("INVALID_VALUE_TYPE", f"Timestamp requires datetime or number, got {type(value).__name__}")
        return struct.pack(">q", ms)

    if data_type == DataType.VarInteger:
        return _serialize_var_integer(value)

    if data_type == DataType.VarDouble:
        return _serialize_var_double(value)

    if data_type == DataType.VarFloat:
        return _serialize_var_float(value)

    raise DanWSError("UNKNOWN_DATA_TYPE", f"Unknown data type: 0x{data_type:02x}")


def deserialize(data_type: DataType, payload: bytes) -> Any:
    expected_size = DATA_TYPE_SIZES.get(data_type, -1)
    if expected_size >= 0 and len(payload) != expected_size:
        raise DanWSError(
            "PAYLOAD_SIZE_MISMATCH",
            f"{data_type.name} expects {expected_size} bytes, got {len(payload)}",
        )

    if data_type == DataType.Null:
        return None

    if data_type == DataType.Bool:
        if payload[0] == 0x01:
            return True
        if payload[0] == 0x00:
            return False
        raise DanWSError("INVALID_VALUE_TYPE", f"Bool payload must be 0x00 or 0x01, got 0x{payload[0]:02x}")

    if data_type == DataType.Uint8:
        return payload[0]

    if data_type == DataType.Uint16:
        return struct.unpack(">H", payload)[0]

    if data_type == DataType.Uint32:
        return struct.unpack(">I", payload)[0]

    if data_type == DataType.Uint64:
        return struct.unpack(">Q", payload)[0]

    if data_type == DataType.Int32:
        return struct.unpack(">i", payload)[0]

    if data_type == DataType.Int64:
        return struct.unpack(">q", payload)[0]

    if data_type == DataType.Float32:
        return struct.unpack(">f", payload)[0]

    if data_type == DataType.Float64:
        return struct.unpack(">d", payload)[0]

    if data_type == DataType.String:
        return payload.decode("utf-8")

    if data_type == DataType.Binary:
        return bytes(payload)

    if data_type == DataType.Timestamp:
        ms = struct.unpack(">q", payload)[0]
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

    if data_type == DataType.VarInteger:
        return _deserialize_var_integer(payload)

    if data_type == DataType.VarDouble:
        return _deserialize_var_double(payload)

    if data_type == DataType.VarFloat:
        return _deserialize_var_float(payload)

    raise DanWSError("UNKNOWN_DATA_TYPE", f"Unknown data type: 0x{data_type:02x}")


# --- VarInt helpers ---

def _encode_var_int(value: int) -> bytes:
    if value == 0:
        return b"\x00"
    result = bytearray()
    while value > 0:
        byte = value & 0x7F
        value >>= 7
        if value > 0:
            byte |= 0x80
        result.append(byte)
    return bytes(result)


def _decode_var_int(payload: bytes, offset: int = 0) -> int:
    value = 0
    multiplier = 1
    i = offset
    while i < len(payload):
        byte = payload[i]
        value += (byte & 0x7F) * multiplier
        multiplier *= 128
        i += 1
        if (byte & 0x80) == 0:
            break
    return value


# --- VarInteger (0x0D) ---

def _serialize_var_integer(value: int) -> bytes:
    # Zigzag encode
    zigzag = value * 2 if value >= 0 else (-value) * 2 - 1
    return _encode_var_int(zigzag)


def _deserialize_var_integer(payload: bytes) -> int:
    if len(payload) == 0:
        raise DanWSError("PAYLOAD_SIZE_MISMATCH", "VarInteger requires at least 1 byte")
    zigzag = _decode_var_int(payload)
    if zigzag & 1:
        return -(zigzag // 2) - 1
    return zigzag // 2


# --- VarDouble (0x0E) ---

def _fallback_float64(value: float) -> bytes:
    return b"\x80" + struct.pack(">d", value)


def _serialize_var_double(value: float) -> bytes:
    if not math.isfinite(value):
        return _fallback_float64(value)
    # Check for -0.0
    if value == 0.0 and math.copysign(1.0, value) < 0:
        return _fallback_float64(value)

    abs_val = abs(value)
    s = _float_to_str(abs_val)

    if "e" in s or "E" in s:
        return _fallback_float64(value)

    dot_idx = s.find(".")
    if dot_idx == -1:
        scale = 0
        mantissa = int(s)
    else:
        scale = len(s) - dot_idx - 1
        if scale > 63:
            return _fallback_float64(value)
        mantissa = int(s.replace(".", ""))

    if mantissa > 9007199254740991:  # Number.MAX_SAFE_INTEGER
        return _fallback_float64(value)

    negative = value < 0
    first_byte = (scale + 64) if negative else scale
    return bytes([first_byte]) + _encode_var_int(mantissa)


def _deserialize_var_double(payload: bytes) -> float:
    if len(payload) == 0:
        raise DanWSError("PAYLOAD_SIZE_MISMATCH", "VarDouble requires at least 1 byte")

    first_byte = payload[0]
    if first_byte == 0x80:
        if len(payload) < 9:
            raise DanWSError("PAYLOAD_SIZE_MISMATCH", "VarDouble fallback requires 9 bytes")
        return struct.unpack(">d", payload[1:9])[0]

    negative = first_byte >= 64
    scale = (first_byte - 64) if negative else first_byte
    mantissa = _decode_var_int(payload, 1)
    result = mantissa / (10 ** scale)
    if negative:
        result = -result
    return result


# --- VarFloat (0x0F) ---

def _fallback_float32(value: float) -> bytes:
    return b"\x80" + struct.pack(">f", value)


def _serialize_var_float(value: float) -> bytes:
    if not math.isfinite(value):
        return _fallback_float32(value)
    if value == 0.0 and math.copysign(1.0, value) < 0:
        return _fallback_float32(value)

    abs_val = abs(value)
    s = _float_to_str(abs_val)

    if "e" in s or "E" in s:
        return _fallback_float32(value)

    dot_idx = s.find(".")
    if dot_idx == -1:
        scale = 0
        mantissa = int(s)
    else:
        scale = len(s) - dot_idx - 1
        if scale > 63:
            return _fallback_float32(value)
        mantissa = int(s.replace(".", ""))

    if mantissa > 9007199254740991:
        return _fallback_float32(value)

    negative = value < 0
    first_byte = (scale + 64) if negative else scale
    return bytes([first_byte]) + _encode_var_int(mantissa)


def _deserialize_var_float(payload: bytes) -> float:
    if len(payload) == 0:
        raise DanWSError("PAYLOAD_SIZE_MISMATCH", "VarFloat requires at least 1 byte")

    first_byte = payload[0]
    if first_byte == 0x80:
        if len(payload) < 5:
            raise DanWSError("PAYLOAD_SIZE_MISMATCH", "VarFloat fallback requires 5 bytes")
        return struct.unpack(">f", payload[1:5])[0]

    negative = first_byte >= 64
    scale = (first_byte - 64) if negative else first_byte
    mantissa = _decode_var_int(payload, 1)
    result = mantissa / (10 ** scale)
    if negative:
        result = -result
    return result


def _float_to_str(value: float) -> str:
    """Convert float to string representation matching JS Number.toString() behavior.

    We need to produce the shortest decimal representation that roundtrips.
    Python's repr() does this correctly since Python 3.1 (uses David Gay's dtoa).
    """
    if value == 0.0:
        return "0"
    s = repr(value)
    # repr may produce e.g. "3.14" or "1e-07"
    if "e" in s or "E" in s:
        return s
    return s


# --- Auto-type detection ---

def detect_data_type(value: Any) -> DataType:
    """Auto-detect the DataType for a Python value."""
    if value is None:
        return DataType.Null
    if isinstance(value, bool):
        return DataType.Bool
    if isinstance(value, int) and not isinstance(value, bool):
        return DataType.VarInteger
    if isinstance(value, float):
        if value != value:  # NaN
            return DataType.VarDouble
        if math.isinf(value):
            return DataType.VarDouble
        # Check if it's an integer value stored as float
        if value == int(value) and abs(value) < 2**53:
            return DataType.VarInteger
        return DataType.VarDouble
    if isinstance(value, str):
        return DataType.String
    if isinstance(value, (bytes, bytearray, memoryview)):
        return DataType.Binary
    if isinstance(value, datetime):
        return DataType.Timestamp
    raise DanWSError("UNKNOWN_VALUE_TYPE", f"Cannot detect DataType for value: {value!r}")
