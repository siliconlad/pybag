from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from . import types as t
from .builtin_interfaces import *

@dataclass
class Bool:
    """Class for std_msgs/msg/Bool."""

    data: t.bool
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Bool'

@dataclass
class Byte:
    """Class for std_msgs/msg/Byte."""

    data: t.int8
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Byte'

@dataclass
class Char:
    """Class for std_msgs/msg/Char."""

    data: t.uint8
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Char'

@dataclass
class ColorRGBA:
    """Class for std_msgs/msg/ColorRGBA."""

    r: t.float32
    g: t.float32
    b: t.float32
    a: t.float32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/ColorRGBA'

@dataclass
class Empty:
    """Class for std_msgs/msg/Empty."""

    structure_needs_at_least_one_member: t.uint8
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Empty'

@dataclass
class Float32:
    """Class for std_msgs/msg/Float32."""

    data: t.float32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Float32'

@dataclass
class Float64:
    """Class for std_msgs/msg/Float64."""

    data: t.float64
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Float64'

@dataclass
class Header:
    """Class for std_msgs/msg/Header."""

    stamp: t.Complex(Time)
    frame_id: t.string
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Header'

@dataclass
class Int16:
    """Class for std_msgs/msg/Int16."""

    data: t.int16
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int16'

@dataclass
class Int32:
    """Class for std_msgs/msg/Int32."""

    data: t.int32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int32'

@dataclass
class Int64:
    """Class for std_msgs/msg/Int64."""

    data: t.int64
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int64'

@dataclass
class Int8:
    """Class for std_msgs/msg/Int8."""

    data: t.int8
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int8'

@dataclass
class MultiArrayDimension:
    """Class for std_msgs/msg/MultiArrayDimension."""

    label: t.string
    size: t.uint32
    stride: t.uint32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/MultiArrayDimension'

@dataclass
class String:
    """Class for std_msgs/msg/String."""

    data: t.string
    __msgtype__: ClassVar[str] = 'std_msgs/msg/String'

@dataclass
class UInt16:
    """Class for std_msgs/msg/UInt16."""

    data: t.uint16
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt16'

@dataclass
class UInt32:
    """Class for std_msgs/msg/UInt32."""

    data: t.uint32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt32'

@dataclass
class UInt64:
    """Class for std_msgs/msg/UInt64."""

    data: t.uint64
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt64'

@dataclass
class UInt8:
    """Class for std_msgs/msg/UInt8."""

    data: t.uint8
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt8'

@dataclass
class MultiArrayLayout:
    """Class for std_msgs/msg/MultiArrayLayout."""

    dim: t.Array(t.Complex(MultiArrayDimension))
    data_offset: t.uint32
    __msgtype__: ClassVar[str] = 'std_msgs/msg/MultiArrayLayout'

@dataclass
class ByteMultiArray:
    """Class for std_msgs/msg/ByteMultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int8)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/ByteMultiArray'

@dataclass
class Float32MultiArray:
    """Class for std_msgs/msg/Float32MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.float32)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Float32MultiArray'

@dataclass
class Float64MultiArray:
    """Class for std_msgs/msg/Float64MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.float64)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Float64MultiArray'

@dataclass
class Int16MultiArray:
    """Class for std_msgs/msg/Int16MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int16)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int16MultiArray'

@dataclass
class Int32MultiArray:
    """Class for std_msgs/msg/Int32MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int32)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int32MultiArray'

@dataclass
class Int64MultiArray:
    """Class for std_msgs/msg/Int64MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int64)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int64MultiArray'

@dataclass
class Int8MultiArray:
    """Class for std_msgs/msg/Int8MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int8)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/Int8MultiArray'

@dataclass
class UInt16MultiArray:
    """Class for std_msgs/msg/UInt16MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint16)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt16MultiArray'

@dataclass
class UInt32MultiArray:
    """Class for std_msgs/msg/UInt32MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint32)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt32MultiArray'

@dataclass
class UInt64MultiArray:
    """Class for std_msgs/msg/UInt64MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint64)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt64MultiArray'

@dataclass
class UInt8MultiArray:
    """Class for std_msgs/msg/UInt8MultiArray."""

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint8)
    __msgtype__: ClassVar[str] = 'std_msgs/msg/UInt8MultiArray'
