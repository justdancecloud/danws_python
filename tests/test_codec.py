"""Tests for codec: encode/decode roundtrip."""

import pytest

from dan_websocket.protocol.types import DataType, FrameType, Frame, DanWSError
from dan_websocket.protocol.codec import encode, decode, encode_batch, encode_heartbeat, dle_encode, dle_decode


class TestDLE:
    def test_dle_encode_no_dle(self):
        data = bytes([0x01, 0x02, 0x03])
        assert dle_encode(data) is data  # no copy needed

    def test_dle_encode_with_dle(self):
        data = bytes([0x01, 0x10, 0x03])
        assert dle_encode(data) == bytes([0x01, 0x10, 0x10, 0x03])

    def test_dle_decode_no_dle(self):
        data = bytes([0x01, 0x02, 0x03])
        assert dle_decode(data) is data

    def test_dle_decode_with_dle(self):
        data = bytes([0x01, 0x10, 0x10, 0x03])
        assert dle_decode(data) == bytes([0x01, 0x10, 0x03])

    def test_dle_roundtrip(self):
        original = bytes([0x10, 0x10, 0x00, 0x10, 0xFF])
        encoded = dle_encode(original)
        assert dle_decode(encoded) == original


class TestEncodeDecode:
    def test_signal_frame(self):
        frame = Frame(
            frame_type=FrameType.ServerSync,
            key_id=0,
            data_type=DataType.Null,
            payload=None,
        )
        data = encode(frame)
        frames = decode(data)
        assert len(frames) == 1
        assert frames[0].frame_type == FrameType.ServerSync
        assert frames[0].key_id == 0
        assert frames[0].data_type == DataType.Null
        assert frames[0].payload is None

    def test_bool_value(self):
        frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=1,
            data_type=DataType.Bool,
            payload=True,
        )
        data = encode(frame)
        frames = decode(data)
        assert len(frames) == 1
        assert frames[0].payload is True

    def test_string_value(self):
        frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=2,
            data_type=DataType.String,
            payload="Alice",
        )
        data = encode(frame)
        frames = decode(data)
        assert len(frames) == 1
        assert frames[0].payload == "Alice"

    def test_key_registration(self):
        frame = Frame(
            frame_type=FrameType.ServerKeyRegistration,
            key_id=1,
            data_type=DataType.String,
            payload="sensor.temperature",
        )
        data = encode(frame)
        frames = decode(data)
        assert len(frames) == 1
        assert frames[0].payload == "sensor.temperature"

    def test_var_integer(self):
        frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=3,
            data_type=DataType.VarInteger,
            payload=42,
        )
        data = encode(frame)
        frames = decode(data)
        assert frames[0].payload == 42

    def test_var_double(self):
        frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=4,
            data_type=DataType.VarDouble,
            payload=3.14,
        )
        data = encode(frame)
        frames = decode(data)
        assert abs(frames[0].payload - 3.14) < 1e-10

    def test_key_id_with_dle_byte(self):
        # KeyID 0x00000010 contains DLE byte
        frame = Frame(
            frame_type=FrameType.ServerValue,
            key_id=0x10,
            data_type=DataType.VarInteger,
            payload=1,
        )
        data = encode(frame)
        frames = decode(data)
        assert frames[0].key_id == 0x10
        assert frames[0].payload == 1

    def test_batch_encode_decode(self):
        frames = [
            Frame(FrameType.ServerKeyRegistration, 1, DataType.String, "key1"),
            Frame(FrameType.ServerValue, 1, DataType.VarInteger, 42),
            Frame(FrameType.ServerSync, 0, DataType.Null, None),
        ]
        data = encode_batch(frames)
        decoded = decode(data)
        assert len(decoded) == 3
        assert decoded[0].payload == "key1"
        assert decoded[1].payload == 42
        assert decoded[2].frame_type == FrameType.ServerSync

    def test_heartbeat(self):
        hb = encode_heartbeat()
        assert hb == bytes([0x10, 0x05])

    def test_known_wire_format(self):
        """Test known wire format from protocol spec."""
        # ServerSync signal frame: 10 02 04 00 00 00 00 00 10 03
        expected = bytes([0x10, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x03])
        frame = Frame(FrameType.ServerSync, 0, DataType.Null, None)
        assert encode(frame) == expected

    def test_known_bool_wire(self):
        # Bool true for KeyID 1: 10 02 01 00 00 00 01 01 01 10 03
        expected = bytes([0x10, 0x02, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x10, 0x03])
        frame = Frame(FrameType.ServerValue, 1, DataType.Bool, True)
        assert encode(frame) == expected
