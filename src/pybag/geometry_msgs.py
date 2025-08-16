from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from . import types as t
from .std_msgs import *

@dataclass
class Point:
    """Class for geometry_msgs/msg/Point."""

    x: t.float64
    y: t.float64
    z: t.float64
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Point'

@dataclass
class Point32:
    """Class for geometry_msgs/msg/Point32."""

    x: t.float32
    y: t.float32
    z: t.float32
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Point32'

@dataclass
class Pose2D:
    """Class for geometry_msgs/msg/Pose2D."""

    x: t.float64
    y: t.float64
    theta: t.float64
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Pose2D'

@dataclass
class Quaternion:
    """Class for geometry_msgs/msg/Quaternion."""

    x: t.float64
    y: t.float64
    z: t.float64
    w: t.float64
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Quaternion'

@dataclass
class Vector3:
    """Class for geometry_msgs/msg/Vector3."""

    x: t.float64
    y: t.float64
    z: t.float64
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Vector3'

@dataclass
class PointStamped:
    """Class for geometry_msgs/msg/PointStamped."""

    header: t.Complex(Header)
    point: t.Complex(Point)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PointStamped'

@dataclass
class Polygon:
    """Class for geometry_msgs/msg/Polygon."""

    points: t.Array(t.Complex(Point32))
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Polygon'

@dataclass
class Pose:
    """Class for geometry_msgs/msg/Pose."""

    position: t.Complex(Point)
    orientation: t.Complex(Quaternion)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Pose'

@dataclass
class QuaternionStamped:
    """Class for geometry_msgs/msg/QuaternionStamped."""

    header: t.Complex(Header)
    quaternion: t.Complex(Quaternion)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/QuaternionStamped'

@dataclass
class Accel:
    """Class for geometry_msgs/msg/Accel."""

    linear: t.Complex(Vector3)
    angular: t.Complex(Vector3)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Accel'

@dataclass
class Inertia:
    """Class for geometry_msgs/msg/Inertia."""

    m: t.float64
    com: t.Complex(Vector3)
    ixx: t.float64
    ixy: t.float64
    ixz: t.float64
    iyy: t.float64
    iyz: t.float64
    izz: t.float64
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Inertia'

@dataclass
class Transform:
    """Class for geometry_msgs/msg/Transform."""

    translation: t.Complex(Vector3)
    rotation: t.Complex(Quaternion)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Transform'

@dataclass
class Twist:
    """Class for geometry_msgs/msg/Twist."""

    linear: t.Complex(Vector3)
    angular: t.Complex(Vector3)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Twist'

@dataclass
class Vector3Stamped:
    """Class for geometry_msgs/msg/Vector3Stamped."""

    header: t.Complex(Header)
    vector: t.Complex(Vector3)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Vector3Stamped'

@dataclass
class Wrench:
    """Class for geometry_msgs/msg/Wrench."""

    force: t.Complex(Vector3)
    torque: t.Complex(Vector3)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/Wrench'

@dataclass
class PolygonStamped:
    """Class for geometry_msgs/msg/PolygonStamped."""

    header: t.Complex(Header)
    polygon: t.Complex(Polygon)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PolygonStamped'

@dataclass
class PoseArray:
    """Class for geometry_msgs/msg/PoseArray."""

    header: t.Complex(Header)
    poses: t.Array(t.Complex(Pose))
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PoseArray'

@dataclass
class PoseStamped:
    """Class for geometry_msgs/msg/PoseStamped."""

    header: t.Complex(Header)
    pose: t.Complex(Pose)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PoseStamped'

@dataclass
class PoseWithCovariance:
    """Class for geometry_msgs/msg/PoseWithCovariance."""

    pose: t.Complex(Pose)
    covariance: t.Array(t.float64, 36)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PoseWithCovariance'

@dataclass
class AccelStamped:
    """Class for geometry_msgs/msg/AccelStamped."""

    header: t.Complex(Header)
    accel: t.Complex(Accel)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/AccelStamped'

@dataclass
class AccelWithCovariance:
    """Class for geometry_msgs/msg/AccelWithCovariance."""

    accel: t.Complex(Accel)
    covariance: t.Array(t.float64, 36)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/AccelWithCovariance'

@dataclass
class InertiaStamped:
    """Class for geometry_msgs/msg/InertiaStamped."""

    header: t.Complex(Header)
    inertia: t.Complex(Inertia)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/InertiaStamped'

@dataclass
class TransformStamped:
    """Class for geometry_msgs/msg/TransformStamped."""

    header: t.Complex(Header)
    child_frame_id: t.string
    transform: t.Complex(Transform)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/TransformStamped'

@dataclass
class TwistStamped:
    """Class for geometry_msgs/msg/TwistStamped."""

    header: t.Complex(Header)
    twist: t.Complex(Twist)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/TwistStamped'

@dataclass
class TwistWithCovariance:
    """Class for geometry_msgs/msg/TwistWithCovariance."""

    twist: t.Complex(Twist)
    covariance: t.Array(t.float64, 36)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/TwistWithCovariance'

@dataclass
class WrenchStamped:
    """Class for geometry_msgs/msg/WrenchStamped."""

    header: t.Complex(Header)
    wrench: t.Complex(Wrench)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/WrenchStamped'

@dataclass
class PoseWithCovarianceStamped:
    """Class for geometry_msgs/msg/PoseWithCovarianceStamped."""

    header: t.Complex(Header)
    pose: t.Complex(PoseWithCovariance)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/PoseWithCovarianceStamped'

@dataclass
class AccelWithCovarianceStamped:
    """Class for geometry_msgs/msg/AccelWithCovarianceStamped."""

    header: t.Complex(Header)
    accel: t.Complex(AccelWithCovariance)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/AccelWithCovarianceStamped'

@dataclass
class TwistWithCovarianceStamped:
    """Class for geometry_msgs/msg/TwistWithCovarianceStamped."""

    header: t.Complex(Header)
    twist: t.Complex(TwistWithCovariance)
    __msgtype__: ClassVar[str] = 'geometry_msgs/msg/TwistWithCovarianceStamped'
