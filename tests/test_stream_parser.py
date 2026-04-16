"""Tests for the stream parser."""

import pytest

from dan_websocket.protocol.types import DataType, FrameType, Frame
from dan_websocket.protocol.codec import encode, encode_heartbeat, encode_batch
from dan_websocket.protocol.stream_parser import StreamParser


class TestStreamParser:
    def test_single_frame(self):
        parser = StreamParser()
        frames = []
        parser.on_frame(lambda f: frames.append(f))

        frame = Frame(FrameType.ServerSync, 0, DataType.Null, None)
        parser.feed(encode(frame))

        assert len(frames) == 1
        assert frames[0].frame_type == FrameType.ServerSync

    def test_heartbeat(self):
        parser = StreamParser()
        heartbeats = []
        parser.on_heartbeat(lambda: heartbeats.append(True))

        parser.feed(encode_heartbeat())
        assert len(heartbeats) == 1

    def test_multiple_frames(self):
        parser = StreamParser()
        frames = []
        parser.on_frame(lambda f: frames.append(f))

        batch = encode_batch([
            Frame(FrameType.ServerKeyRegistration, 1, DataType.String, "key1"),
            Frame(FrameType.ServerValue, 1, DataType.VarInteger, 42),
            Frame(FrameType.ServerSync, 0, DataType.Null, None),
        ])
        parser.feed(batch)

        assert len(frames) == 3
        assert frames[0].payload == "key1"
        assert frames[1].payload == 42
        assert frames[2].frame_type == FrameType.ServerSync

    def test_chunked_feed(self):
        """Feed data one byte at a time."""
        parser = StreamParser()
        frames = []
        parser.on_frame(lambda f: frames.append(f))

        data = encode(Frame(FrameType.ServerValue, 1, DataType.VarInteger, 42))
        for byte in data:
            parser.feed(bytes([byte]))

        assert len(frames) == 1
        assert frames[0].payload == 42

    def test_mixed_heartbeat_and_frames(self):
        parser = StreamParser()
        frames = []
        heartbeats = []
        parser.on_frame(lambda f: frames.append(f))
        parser.on_heartbeat(lambda: heartbeats.append(True))

        data = encode_heartbeat() + encode(Frame(FrameType.ServerSync, 0, DataType.Null, None)) + encode_heartbeat()
        parser.feed(data)

        assert len(heartbeats) == 2
        assert len(frames) == 1

    def test_dle_escaped_payload(self):
        """Frame with keyId containing 0x10 should be handled correctly."""
        parser = StreamParser()
        frames = []
        parser.on_frame(lambda f: frames.append(f))

        frame = Frame(FrameType.ServerValue, 0x10, DataType.VarInteger, 5)
        parser.feed(encode(frame))

        assert len(frames) == 1
        assert frames[0].key_id == 0x10
        assert frames[0].payload == 5

    def test_error_callback(self):
        parser = StreamParser()
        errors = []
        parser.on_error(lambda e: errors.append(e))

        # Feed invalid data
        parser.feed(bytes([0xFF]))
        assert len(errors) == 1

    def test_reset(self):
        parser = StreamParser()
        frames = []
        parser.on_frame(lambda f: frames.append(f))

        # Feed partial frame then reset
        data = encode(Frame(FrameType.ServerSync, 0, DataType.Null, None))
        parser.feed(data[:5])
        parser.reset()

        # Feed complete frame
        parser.feed(data)
        assert len(frames) == 1
