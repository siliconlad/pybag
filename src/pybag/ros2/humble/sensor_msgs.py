"""ROS2 sensor_msgs message types for humble.

Auto-generated with embedded encode/decode methods.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pybag.types as t

from pybag.ros2.humble import builtin_interfaces
from pybag.ros2.humble import geometry_msgs
from pybag.ros2.humble import std_msgs


@dataclass(kw_only=True)
class RegionOfInterest:
    __msg_name__ = 'sensor_msgs/msg/RegionOfInterest'

    x_offset: t.uint32
    y_offset: t.uint32
    height: t.uint32
    width: t.uint32
    do_rectify: t.bool

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        x_offset, y_offset, height, width = struct.unpack(fmt_prefix + 'IIII', _data.read(16))
        _fields['x_offset'] = x_offset
        _fields['y_offset'] = y_offset
        _fields['height'] = height
        _fields['width'] = width
        _data.align(1)
        _fields['do_rectify'] = struct.unpack(fmt_prefix + '?', _data.read(1))[0]
        return RegionOfInterest(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.x_offset))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.y_offset))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.height))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.width))
        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + '?', self.do_rectify))


@dataclass(kw_only=True)
class ChannelFloat32:
    __msg_name__ = 'sensor_msgs/msg/ChannelFloat32'

    name: t.string
    values: t.Sequence[t.float32]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['name'] = decoder.string()
        _len = decoder.uint32()
        _data.align(4)
        _fields['values'] = list(struct.unpack(fmt_prefix + 'f' * _len, _data.read(4 * _len)))
        return ChannelFloat32(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.string(self.name)
        encoder.uint32(len(self.values))
        _payload.align(4)
        for _item in self.values:
            _payload.write(struct.pack(fmt_prefix + 'f', _item))


@dataclass(kw_only=True)
class JoyFeedback:
    __msg_name__ = 'sensor_msgs/msg/JoyFeedback'

    TYPE_LED: t.uint8 = 0
    TYPE_RUMBLE: t.uint8 = 1
    TYPE_BUZZER: t.uint8 = 2
    type: t.uint8
    id: t.uint8
    intensity: t.float32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        type, id = struct.unpack(fmt_prefix + 'BB', _data.read(2))
        _fields['type'] = type
        _fields['id'] = id
        _data.align(4)
        _fields['intensity'] = struct.unpack(fmt_prefix + 'f', _data.read(4))[0]
        return JoyFeedback(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'B', self.type))
        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'B', self.id))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.intensity))


@dataclass(kw_only=True)
class JoyFeedbackArray:
    __msg_name__ = 'sensor_msgs/msg/JoyFeedbackArray'

    array: t.Sequence[t.Complex[JoyFeedback]]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _len = decoder.uint32()
        _fields['array'] = [JoyFeedback.decode(decoder) for _ in range(_len)]
        return JoyFeedbackArray(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.uint32(len(self.array))
        for _item in self.array:
            _item.encode(encoder)


@dataclass(kw_only=True)
class LaserEcho:
    __msg_name__ = 'sensor_msgs/msg/LaserEcho'

    echoes: t.Sequence[t.float32]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _len = decoder.uint32()
        _data.align(4)
        _fields['echoes'] = list(struct.unpack(fmt_prefix + 'f' * _len, _data.read(4 * _len)))
        return LaserEcho(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.uint32(len(self.echoes))
        _payload.align(4)
        for _item in self.echoes:
            _payload.write(struct.pack(fmt_prefix + 'f', _item))


@dataclass(kw_only=True)
class NavSatStatus:
    __msg_name__ = 'sensor_msgs/msg/NavSatStatus'

    STATUS_NO_FIX: t.int8 = -1
    STATUS_FIX: t.int8 = 0
    STATUS_SBAS_FIX: t.int8 = 1
    STATUS_GBAS_FIX: t.int8 = 2
    SERVICE_GPS: t.uint16 = 1
    SERVICE_GLONASS: t.uint16 = 2
    SERVICE_COMPASS: t.uint16 = 4
    SERVICE_GALILEO: t.uint16 = 8
    status: t.int8
    service: t.uint16

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(1)
        _fields['status'] = struct.unpack(fmt_prefix + 'b', _data.read(1))[0]
        _data.align(2)
        _fields['service'] = struct.unpack(fmt_prefix + 'H', _data.read(2))[0]
        return NavSatStatus(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'b', self.status))
        _payload.align(2)
        _payload.write(struct.pack(fmt_prefix + 'H', self.service))


@dataclass(kw_only=True)
class PointField:
    __msg_name__ = 'sensor_msgs/msg/PointField'

    INT8: t.uint8 = 1
    UINT8: t.uint8 = 2
    INT16: t.uint8 = 3
    UINT16: t.uint8 = 4
    INT32: t.uint8 = 5
    UINT32: t.uint8 = 6
    FLOAT32: t.uint8 = 7
    FLOAT64: t.uint8 = 8
    name: t.string
    offset: t.uint32
    datatype: t.uint8
    count: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['name'] = decoder.string()
        _data.align(4)
        _fields['offset'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        _data.align(1)
        _fields['datatype'] = struct.unpack(fmt_prefix + 'B', _data.read(1))[0]
        _data.align(4)
        _fields['count'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        return PointField(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.string(self.name)
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.offset))
        _payload.align(1)
        _payload.write(struct.pack(fmt_prefix + 'B', self.datatype))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.count))


