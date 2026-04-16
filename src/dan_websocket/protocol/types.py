"""DanProtocol v3.5 types and constants."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

# Control characters
DLE = 0x10
STX = 0x02
ETX = 0x03
ENQ = 0x05


class DataType(IntEnum):
    Null = 0x00
    Bool = 0x01
    Uint8 = 0x02
    Uint16 = 0x03
    Uint32 = 0x04
    Uint64 = 0x05
    Int32 = 0x06
    Int64 = 0x07
    Float32 = 0x08
    Float64 = 0x09
    String = 0x0A
    Binary = 0x0B
    Timestamp = 0x0C
    VarInteger = 0x0D
    VarDouble = 0x0E
    VarFloat = 0x0F


class FrameType(IntEnum):
    ServerKeyRegistration = 0x00
    ServerValue = 0x01
    ClientKeyRegistration = 0x02
    ClientValue = 0x03
    ServerSync = 0x04
    ClientReady = 0x05
    ClientSync = 0x06
    ServerReady = 0x07
    Error = 0x08
    ServerReset = 0x09
    ClientResyncReq = 0x0A
    ClientReset = 0x0B
    ServerResyncReq = 0x0C
    Identify = 0x0D
    Auth = 0x0E
    AuthOk = 0x0F
    AuthFail = 0x11
    ArrayShiftLeft = 0x20
    ArrayShiftRight = 0x21
    ServerKeyDelete = 0x22
    ClientKeyRequest = 0x23
    ServerFlushEnd = 0xFF


@dataclass
class Frame:
    frame_type: FrameType
    key_id: int
    data_type: DataType
    payload: Any


class DanWSError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


SIGNAL_FRAME_TYPES = frozenset({
    FrameType.ServerSync,
    FrameType.ClientReady,
    FrameType.ClientSync,
    FrameType.ServerReady,
    FrameType.ServerReset,
    FrameType.ClientResyncReq,
    FrameType.ClientReset,
    FrameType.ServerResyncReq,
    FrameType.AuthOk,
    FrameType.ServerFlushEnd,
    FrameType.ServerKeyDelete,
    FrameType.ClientKeyRequest,
})

KEY_REGISTRATION_FRAME_TYPES = frozenset({
    FrameType.ServerKeyRegistration,
    FrameType.ClientKeyRegistration,
})


def is_signal_frame(ft: FrameType) -> bool:
    return ft in SIGNAL_FRAME_TYPES


def is_key_registration_frame(ft: FrameType) -> bool:
    return ft in KEY_REGISTRATION_FRAME_TYPES


# Fixed byte sizes for each data type. -1 means variable length.
DATA_TYPE_SIZES: dict[DataType, int] = {
    DataType.Null: 0,
    DataType.Bool: 1,
    DataType.Uint8: 1,
    DataType.Uint16: 2,
    DataType.Uint32: 4,
    DataType.Uint64: 8,
    DataType.Int32: 4,
    DataType.Int64: 8,
    DataType.Float32: 4,
    DataType.Float64: 8,
    DataType.String: -1,
    DataType.Binary: -1,
    DataType.Timestamp: -1,
    DataType.VarInteger: -1,
    DataType.VarDouble: -1,
    DataType.VarFloat: -1,
}
