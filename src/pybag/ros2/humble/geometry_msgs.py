"""ROS2 geometry_msgs message types for humble.

Auto-generated with embedded encode/decode methods.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pybag.types as t

from pybag.ros2.humble import std_msgs


@dataclass(kw_only=True)
class Vector3:
    __msg_name__ = 'geometry_msgs/msg/Vector3'

    x: t.float64
    y: t.float64
    z: t.float64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        x, y, z = struct.unpack(fmt_prefix + 'ddd', _data.read(24))
        _fields['x'] = x
        _fields['y'] = y
        _fields['z'] = z
        return Vector3(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.x))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.y))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.z))


@dataclass(kw_only=True)
class Accel:
    __msg_name__ = 'geometry_msgs/msg/Accel'

    linear: t.Complex[Vector3]
    angular: t.Complex[Vector3]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['linear'] = Vector3.decode(decoder)
        _fields['angular'] = Vector3.decode(decoder)
        return Accel(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.linear.encode(encoder)
        self.angular.encode(encoder)


@dataclass(kw_only=True)
class AccelWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/AccelWithCovariance'

    accel: t.Complex[Accel]
    covariance: t.Array[t.float64, 36]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['accel'] = Accel.decode(decoder)
        _data.align(8)
        _fields['covariance'] = list(struct.unpack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', _data.read(288)))
        return AccelWithCovariance(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.accel.encode(encoder)
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', *self.covariance))


@dataclass(kw_only=True)
class Inertia:
    __msg_name__ = 'geometry_msgs/msg/Inertia'

    m: t.float64
    com: t.Complex[Vector3]
    ixx: t.float64
    ixy: t.float64
    ixz: t.float64
    iyy: t.float64
    iyz: t.float64
    izz: t.float64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        _fields['m'] = struct.unpack(fmt_prefix + 'd', _data.read(8))[0]
        _fields['com'] = Vector3.decode(decoder)
        _data.align(8)
        ixx, ixy, ixz, iyy, iyz, izz = struct.unpack(fmt_prefix + 'dddddd', _data.read(48))
        _fields['ixx'] = ixx
        _fields['ixy'] = ixy
        _fields['ixz'] = ixz
        _fields['iyy'] = iyy
        _fields['iyz'] = iyz
        _fields['izz'] = izz
        return Inertia(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.m))
        self.com.encode(encoder)
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.ixx))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.ixy))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.ixz))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.iyy))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.iyz))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.izz))


@dataclass(kw_only=True)
class Point:
    __msg_name__ = 'geometry_msgs/msg/Point'

    x: t.float64
    y: t.float64
    z: t.float64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        x, y, z = struct.unpack(fmt_prefix + 'ddd', _data.read(24))
        _fields['x'] = x
        _fields['y'] = y
        _fields['z'] = z
        return Point(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.x))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.y))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.z))


@dataclass(kw_only=True)
class Point32:
    __msg_name__ = 'geometry_msgs/msg/Point32'

    x: t.float32
    y: t.float32
    z: t.float32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        x, y, z = struct.unpack(fmt_prefix + 'fff', _data.read(12))
        _fields['x'] = x
        _fields['y'] = y
        _fields['z'] = z
        return Point32(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.x))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.y))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'f', self.z))


@dataclass(kw_only=True)
class Polygon:
    __msg_name__ = 'geometry_msgs/msg/Polygon'

    points: t.Sequence[t.Complex[Point32]]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _len = decoder.uint32()
        _fields['points'] = [Point32.decode(decoder) for _ in range(_len)]
        return Polygon(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        encoder.uint32(len(self.points))
        for _item in self.points:
            _item.encode(encoder)


@dataclass(kw_only=True)
class PolygonInstance:
    __msg_name__ = 'geometry_msgs/msg/PolygonInstance'

    polygon: t.Complex[Polygon]
    id: t.int64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['polygon'] = Polygon.decode(decoder)
        _data.align(8)
        _fields['id'] = struct.unpack(fmt_prefix + 'q', _data.read(8))[0]
        return PolygonInstance(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.polygon.encode(encoder)
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'q', self.id))


@dataclass(kw_only=True)
class Quaternion:
    __msg_name__ = 'geometry_msgs/msg/Quaternion'

    x: t.float64 = 0
    y: t.float64 = 0
    z: t.float64 = 0
    w: t.float64 = 1

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        x, y, z, w = struct.unpack(fmt_prefix + 'dddd', _data.read(32))
        _fields['x'] = x
        _fields['y'] = y
        _fields['z'] = z
        _fields['w'] = w
        return Quaternion(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.x))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.y))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.z))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.w))


@dataclass(kw_only=True)
class Pose:
    __msg_name__ = 'geometry_msgs/msg/Pose'

    position: t.Complex[Point]
    orientation: t.Complex[Quaternion]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['position'] = Point.decode(decoder)
        _fields['orientation'] = Quaternion.decode(decoder)
        return Pose(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.position.encode(encoder)
        self.orientation.encode(encoder)


@dataclass(kw_only=True)
class Pose2D:
    __msg_name__ = 'geometry_msgs/msg/Pose2D'

    x: t.float64
    y: t.float64
    theta: t.float64

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(8)
        x, y, theta = struct.unpack(fmt_prefix + 'ddd', _data.read(24))
        _fields['x'] = x
        _fields['y'] = y
        _fields['theta'] = theta
        return Pose2D(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.x))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.y))
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'd', self.theta))


@dataclass(kw_only=True)
class PoseWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/PoseWithCovariance'

    pose: t.Complex[Pose]
    covariance: t.Array[t.float64, 36]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['pose'] = Pose.decode(decoder)
        _data.align(8)
        _fields['covariance'] = list(struct.unpack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', _data.read(288)))
        return PoseWithCovariance(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.pose.encode(encoder)
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', *self.covariance))


@dataclass(kw_only=True)
class Transform:
    __msg_name__ = 'geometry_msgs/msg/Transform'

    translation: t.Complex[Vector3]
    rotation: t.Complex[Quaternion]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['translation'] = Vector3.decode(decoder)
        _fields['rotation'] = Quaternion.decode(decoder)
        return Transform(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.translation.encode(encoder)
        self.rotation.encode(encoder)


@dataclass(kw_only=True)
class Twist:
    __msg_name__ = 'geometry_msgs/msg/Twist'

    linear: t.Complex[Vector3]
    angular: t.Complex[Vector3]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['linear'] = Vector3.decode(decoder)
        _fields['angular'] = Vector3.decode(decoder)
        return Twist(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.linear.encode(encoder)
        self.angular.encode(encoder)


@dataclass(kw_only=True)
class TwistWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/TwistWithCovariance'

    twist: t.Complex[Twist]
    covariance: t.Array[t.float64, 36]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['twist'] = Twist.decode(decoder)
        _data.align(8)
        _fields['covariance'] = list(struct.unpack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', _data.read(288)))
        return TwistWithCovariance(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.twist.encode(encoder)
        _payload.align(8)
        _payload.write(struct.pack(fmt_prefix + 'dddddddddddddddddddddddddddddddddddd', *self.covariance))


@dataclass(kw_only=True)
class Wrench:
    __msg_name__ = 'geometry_msgs/msg/Wrench'

    force: t.Complex[Vector3]
    torque: t.Complex[Vector3]

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _fields['force'] = Vector3.decode(decoder)
        _fields['torque'] = Vector3.decode(decoder)
        return Wrench(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        self.force.encode(encoder)
        self.torque.encode(encoder)


