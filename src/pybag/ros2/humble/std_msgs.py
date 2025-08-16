from dataclasses import dataclass

import pybag.types as t
import pybag.ros2.humble.builtin_interfaces as builtin_interfaces


@dataclass
class Bool:
    __msg_name__ = 'std_msgs/msg/Bool'

    data: t.bool


@dataclass
class Byte:
    __msg_name__ = 'std_msgs/msg/Byte'

    data: t.int8


@dataclass
class Char:
    __msg_name__ = 'std_msgs/msg/Char'

    data: t.uint8


@dataclass
class ColorRGBA:
    __msg_name__ = 'std_msgs/msg/ColorRGBA'

    r: t.float32
    g: t.float32
    b: t.float32
    a: t.float32


@dataclass
class Empty:
    __msg_name__ = 'std_msgs/msg/Empty'

    structure_needs_at_least_one_member: t.uint8


@dataclass
class Float32:
    __msg_name__ = 'std_msgs/msg/Float32'

    data: t.float32


@dataclass
class Float64:
    __msg_name__ = 'std_msgs/msg/Float64'

    data: t.float64


@dataclass
class Header:
    __msg_name__ = 'std_msgs/msg/Header'

    stamp: t.Complex(builtin_interfaces.Time)
    frame_id: t.string


@dataclass
class Int16:
    __msg_name__ = 'std_msgs/msg/Int16'

    data: t.int16


@dataclass
class Int32:
    __msg_name__ = 'std_msgs/msg/Int32'

    data: t.int32


@dataclass
class Int64:
    __msg_name__ = 'std_msgs/msg/Int64'

    data: t.int64


@dataclass
class Int8:
    __msg_name__ = 'std_msgs/msg/Int8'

    data: t.int8


@dataclass
class MultiArrayDimension:
    __msg_name__ = 'std_msgs/msg/MultiArrayDimension'

    label: t.string
    size: t.uint32
    stride: t.uint32


@dataclass
class String:
    __msg_name__ = 'std_msgs/msg/String'

    data: t.string


@dataclass
class UInt16:
    __msg_name__ = 'std_msgs/msg/UInt16'

    data: t.uint16


@dataclass
class UInt32:
    __msg_name__ = 'std_msgs/msg/UInt32'

    data: t.uint32


@dataclass
class UInt64:
    __msg_name__ = 'std_msgs/msg/UInt64'

    data: t.uint64


@dataclass
class UInt8:
    __msg_name__ = 'std_msgs/msg/UInt8'

    data: t.uint8


@dataclass
class MultiArrayLayout:
    __msg_name__ = 'std_msgs/msg/MultiArrayLayout'

    dim: t.Array(t.Complex(MultiArrayDimension))
    data_offset: t.uint32


@dataclass
class ByteMultiArray:
    __msg_name__ = 'std_msgs/msg/ByteMultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int8)


@dataclass
class Float32MultiArray:
    __msg_name__ = 'std_msgs/msg/Float32MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.float32)


@dataclass
class Float64MultiArray:
    __msg_name__ = 'std_msgs/msg/Float64MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.float64)


@dataclass
class Int16MultiArray:
    __msg_name__ = 'std_msgs/msg/Int16MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int16)


@dataclass
class Int32MultiArray:
    __msg_name__ = 'std_msgs/msg/Int32MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int32)


@dataclass
class Int64MultiArray:
    __msg_name__ = 'std_msgs/msg/Int64MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int64)


@dataclass
class Int8MultiArray:
    __msg_name__ = 'std_msgs/msg/Int8MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.int8)


@dataclass
class UInt16MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt16MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint16)


@dataclass
class UInt32MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt32MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint32)


@dataclass
class UInt64MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt64MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint64)


@dataclass
class UInt8MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt8MultiArray'

    layout: t.Complex(MultiArrayLayout)
    data: t.Array(t.uint8)
