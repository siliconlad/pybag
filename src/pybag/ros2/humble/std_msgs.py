"""ROS2 std_msgs message types for humble.

Auto-generated with embedded encode/decode methods.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pybag.types as t

from pybag.ros2.humble import builtin_interfaces


@dataclass(kw_only=True)
class Bool:
    __msg_name__ = 'std_msgs/msg/Bool'

    data: t.bool

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        _fields['data'] = struct.unpack(fmt_prefix + '?', _data.read(1))[0]
        return Bool(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + '?', self.data))


@dataclass(kw_only=True)
class Byte:
    __msg_name__ = 'std_msgs/msg/Byte'

    data: t.byte

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['data'] = decoder.byte()
        return Byte(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _value = self.data
        if isinstance(_value, str):
            _value = ord(_value)
        elif isinstance(_value, (bytes, bytearray)):
            _value = _value[0]
        _payload.write(struct.pack(fmt_prefix + 'B', _value))


@dataclass(kw_only=True)
class MultiArrayDimension:
    __msg_name__ = 'std_msgs/msg/MultiArrayDimension'

    label: t.string
    size: t.uint32
    stride: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['label'] = decoder.string()
        _data.align(4)
        size, stride = struct.unpack(fmt_prefix + 'II', _data.read(8))
        _fields['size'] = size
        _fields['stride'] = stride
        return MultiArrayDimension(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.string(self.label)
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.size))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.stride))


@dataclass(kw_only=True)
class MultiArrayLayout:
    __msg_name__ = 'std_msgs/msg/MultiArrayLayout'

    dim: t.Sequence[t.Complex[MultiArrayDimension]]
    data_offset: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _len = decoder.uint32()
        _fields['dim'] = [MultiArrayDimension.decode(decoder) for _ in range(_len)]
        _data.align(4)
        _fields['data_offset'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        return MultiArrayLayout(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.uint32(len(self.dim))
        for _item in self.dim:
            _item.encode(encoder)
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.data_offset))


@dataclass(kw_only=True)
class ByteMultiArray:
    __msg_name__ = 'std_msgs/msg/ByteMultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.byte]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _fields['data'] = decoder.sequence('byte')
        return ByteMultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(1)
        for _item in self.data:
            if isinstance(_item, str):
                _payload.write(struct.pack(fmt_prefix + 'B', ord(_item)))
            elif isinstance(_item, (bytes, bytearray)):
                _payload.write(struct.pack(fmt_prefix + 'B', _item[0]))
            else:
                _payload.write(struct.pack(fmt_prefix + 'B', _item))


@dataclass(kw_only=True)
class Char:
    __msg_name__ = 'std_msgs/msg/Char'

    data: t.char

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['data'] = decoder.char()
        return Char(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _value = self.data
        if isinstance(_value, str):
            _value = ord(_value)
        elif isinstance(_value, (bytes, bytearray)):
            _value = _value[0]
        _payload.write(struct.pack(fmt_prefix + 'B', _value))


@dataclass(kw_only=True)
class ColorRGBA:
    __msg_name__ = 'std_msgs/msg/ColorRGBA'

    r: t.float32
    g: t.float32
    b: t.float32
    a: t.float32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        r, g, b, a = struct.unpack(fmt_prefix + 'ffff', _data.read(16))
        _fields['r'] = r
        _fields['g'] = g
        _fields['b'] = b
        _fields['a'] = a
        return ColorRGBA(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.r))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.g))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.b))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.a))


@dataclass(kw_only=True)
class Empty:
    __msg_name__ = 'std_msgs/msg/Empty'

    structure_needs_at_least_one_member: t.uint8 = 0

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        _fields['structure_needs_at_least_one_member'] = struct.unpack(fmt_prefix + 'B', _data.read(1))[0]
        return Empty(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'B', self.structure_needs_at_least_one_member))


@dataclass(kw_only=True)
class Float32:
    __msg_name__ = 'std_msgs/msg/Float32'

    data: t.float32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        _fields['data'] = struct.unpack(fmt_prefix + 'f', _data.read(4))[0]
        return Float32(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.data))


@dataclass(kw_only=True)
class Float32MultiArray:
    __msg_name__ = 'std_msgs/msg/Float32MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.float32]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(4)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'f' * _len, _data.read(4 * _len)))
        return Float32MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(4)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'f', _item))


@dataclass(kw_only=True)
class Float64:
    __msg_name__ = 'std_msgs/msg/Float64'

    data: t.float64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        _fields['data'] = struct.unpack(fmt_prefix + 'd', _data.read(8))[0]
        return Float64(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.data))


@dataclass(kw_only=True)
class Float64MultiArray:
    __msg_name__ = 'std_msgs/msg/Float64MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.float64]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(8)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'd' * _len, _data.read(8 * _len)))
        return Float64MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(8)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'd', _item))


@dataclass(kw_only=True)
class Header:
    __msg_name__ = 'std_msgs/msg/Header'

    stamp: t.Complex[builtin_interfaces.Time]
    frame_id: t.string

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['stamp'] = builtin_interfaces.Time.decode(decoder)
        _fields['frame_id'] = decoder.string()
        return Header(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.stamp.encode(encoder)
        encoder.string(self.frame_id)


@dataclass(kw_only=True)
class Int16:
    __msg_name__ = 'std_msgs/msg/Int16'

    data: t.int16

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(2)
        _fields['data'] = struct.unpack(fmt_prefix + 'h', _data.read(2))[0]
        return Int16(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(2)
        _payload.write(struct.pack(fmt_prefix + 'h', self.data))


@dataclass(kw_only=True)
class Int16MultiArray:
    __msg_name__ = 'std_msgs/msg/Int16MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.int16]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(2)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'h' * _len, _data.read(2 * _len)))
        return Int16MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(2)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'h', _item))


@dataclass(kw_only=True)
class Int32:
    __msg_name__ = 'std_msgs/msg/Int32'

    data: t.int32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        _fields['data'] = struct.unpack(fmt_prefix + 'i', _data.read(4))[0]
        return Int32(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'i', self.data))


@dataclass(kw_only=True)
class Int32MultiArray:
    __msg_name__ = 'std_msgs/msg/Int32MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.int32]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(4)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'i' * _len, _data.read(4 * _len)))
        return Int32MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(4)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'i', _item))


@dataclass(kw_only=True)
class Int64:
    __msg_name__ = 'std_msgs/msg/Int64'

    data: t.int64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        _fields['data'] = struct.unpack(fmt_prefix + 'q', _data.read(8))[0]
        return Int64(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'q', self.data))


@dataclass(kw_only=True)
class Int64MultiArray:
    __msg_name__ = 'std_msgs/msg/Int64MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.int64]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(8)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'q' * _len, _data.read(8 * _len)))
        return Int64MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(8)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'q', _item))


@dataclass(kw_only=True)
class Int8:
    __msg_name__ = 'std_msgs/msg/Int8'

    data: t.int8

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        _fields['data'] = struct.unpack(fmt_prefix + 'b', _data.read(1))[0]
        return Int8(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'b', self.data))


@dataclass(kw_only=True)
class Int8MultiArray:
    __msg_name__ = 'std_msgs/msg/Int8MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.int8]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(1)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'b' * _len, _data.read(1 * _len)))
        return Int8MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(1)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'b', _item))


@dataclass(kw_only=True)
class String:
    __msg_name__ = 'std_msgs/msg/String'

    data: t.string

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['data'] = decoder.string()
        return String(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.string(self.data)


@dataclass(kw_only=True)
class UInt16:
    __msg_name__ = 'std_msgs/msg/UInt16'

    data: t.uint16

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(2)
        _fields['data'] = struct.unpack(fmt_prefix + 'H', _data.read(2))[0]
        return UInt16(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(2)
        _payload.write(struct.pack(fmt_prefix + 'H', self.data))


@dataclass(kw_only=True)
class UInt16MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt16MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.uint16]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(2)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'H' * _len, _data.read(2 * _len)))
        return UInt16MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(2)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'H', _item))


@dataclass(kw_only=True)
class UInt32:
    __msg_name__ = 'std_msgs/msg/UInt32'

    data: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        _fields['data'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        return UInt32(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.data))


@dataclass(kw_only=True)
class UInt32MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt32MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.uint32]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(4)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'I' * _len, _data.read(4 * _len)))
        return UInt32MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(4)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'I', _item))


@dataclass(kw_only=True)
class UInt64:
    __msg_name__ = 'std_msgs/msg/UInt64'

    data: t.uint64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        _fields['data'] = struct.unpack(fmt_prefix + 'Q', _data.read(8))[0]
        return UInt64(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'Q', self.data))


@dataclass(kw_only=True)
class UInt64MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt64MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.uint64]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(8)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'Q' * _len, _data.read(8 * _len)))
        return UInt64MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(8)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'Q', _item))


@dataclass(kw_only=True)
class UInt8:
    __msg_name__ = 'std_msgs/msg/UInt8'

    data: t.uint8

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        _fields['data'] = struct.unpack(fmt_prefix + 'B', _data.read(1))[0]
        return UInt8(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'B', self.data))


@dataclass(kw_only=True)
class UInt8MultiArray:
    __msg_name__ = 'std_msgs/msg/UInt8MultiArray'

    layout: t.Complex[MultiArrayLayout]
    data: t.Sequence[t.uint8]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['layout'] = MultiArrayLayout.decode(decoder)
        _len = decoder.uint32()
        _data.align(1)
        _fields['data'] = list(struct.unpack(fmt_prefix + 'B' * _len, _data.read(1 * _len)))
        return UInt8MultiArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.layout.encode(encoder)
        encoder.uint32(len(self.data))
        _payload.align(1)
        for _item in self.data:
            _payload.write(struct.pack(fmt_prefix + 'B', _item))


