from dataclasses import dataclass

import pybag.types as t
from .std_msgs import *


@dataclass
class Point:
    __msg_name__ = 'geometry_msgs/msg/Point'

    x: t.float64
    y: t.float64
    z: t.float64


@dataclass
class Point32:
    __msg_name__ = 'geometry_msgs/msg/Point32'

    x: t.float32
    y: t.float32
    z: t.float32


@dataclass
class Pose2D:
    __msg_name__ = 'geometry_msgs/msg/Pose2D'

    x: t.float64
    y: t.float64
    theta: t.float64


@dataclass
class Quaternion:
    __msg_name__ = 'geometry_msgs/msg/Quaternion'

    x: t.float64
    y: t.float64
    z: t.float64
    w: t.float64


@dataclass
class Vector3:
    __msg_name__ = 'geometry_msgs/msg/Vector3'

    x: t.float64
    y: t.float64
    z: t.float64


@dataclass
class PointStamped:
    __msg_name__ = 'geometry_msgs/msg/PointStamped'

    header: t.Complex(Header)
    point: t.Complex(Point)


@dataclass
class Polygon:
    __msg_name__ = 'geometry_msgs/msg/Polygon'

    points: t.Array(t.Complex(Point32))


@dataclass
class Pose:
    __msg_name__ = 'geometry_msgs/msg/Pose'

    position: t.Complex(Point)
    orientation: t.Complex(Quaternion)


@dataclass
class QuaternionStamped:
    __msg_name__ = 'geometry_msgs/msg/QuaternionStamped'

    header: t.Complex(Header)
    quaternion: t.Complex(Quaternion)


@dataclass
class Accel:
    __msg_name__ = 'geometry_msgs/msg/Accel'

    linear: t.Complex(Vector3)
    angular: t.Complex(Vector3)


@dataclass
class Inertia:
    __msg_name__ = 'geometry_msgs/msg/Inertia'

    m: t.float64
    com: t.Complex(Vector3)
    ixx: t.float64
    ixy: t.float64
    ixz: t.float64
    iyy: t.float64
    iyz: t.float64
    izz: t.float64


@dataclass
class Transform:
    __msg_name__ = 'geometry_msgs/msg/Transform'

    translation: t.Complex(Vector3)
    rotation: t.Complex(Quaternion)


@dataclass
class Twist:
    __msg_name__ = 'geometry_msgs/msg/Twist'

    linear: t.Complex(Vector3)
    angular: t.Complex(Vector3)


@dataclass
class Vector3Stamped:
    __msg_name__ = 'geometry_msgs/msg/Vector3Stamped'

    header: t.Complex(Header)
    vector: t.Complex(Vector3)


@dataclass
class Wrench:
    __msg_name__ = 'geometry_msgs/msg/Wrench'

    force: t.Complex(Vector3)
    torque: t.Complex(Vector3)


@dataclass
class PolygonStamped:
    __msg_name__ = 'geometry_msgs/msg/PolygonStamped'

    header: t.Complex(Header)
    polygon: t.Complex(Polygon)


@dataclass
class PoseArray:
    __msg_name__ = 'geometry_msgs/msg/PoseArray'

    header: t.Complex(Header)
    poses: t.Array(t.Complex(Pose))


@dataclass
class PoseStamped:
    __msg_name__ = 'geometry_msgs/msg/PoseStamped'

    header: t.Complex(Header)
    pose: t.Complex(Pose)


@dataclass
class PoseWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/PoseWithCovariance'

    pose: t.Complex(Pose)
    covariance: t.Array(t.float64, 36)


@dataclass
class AccelStamped:
    __msg_name__ = 'geometry_msgs/msg/AccelStamped'

    header: t.Complex(Header)
    accel: t.Complex(Accel)


@dataclass
class AccelWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/AccelWithCovariance'

    accel: t.Complex(Accel)
    covariance: t.Array(t.float64, 36)


@dataclass
class InertiaStamped:
    __msg_name__ = 'geometry_msgs/msg/InertiaStamped'

    header: t.Complex(Header)
    inertia: t.Complex(Inertia)


@dataclass
class TransformStamped:
    __msg_name__ = 'geometry_msgs/msg/TransformStamped'

    header: t.Complex(Header)
    child_frame_id: t.string
    transform: t.Complex(Transform)


@dataclass
class TwistStamped:
    __msg_name__ = 'geometry_msgs/msg/TwistStamped'

    header: t.Complex(Header)
    twist: t.Complex(Twist)


@dataclass
class TwistWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/TwistWithCovariance'

    twist: t.Complex(Twist)
    covariance: t.Array(t.float64, 36)


@dataclass
class WrenchStamped:
    __msg_name__ = 'geometry_msgs/msg/WrenchStamped'

    header: t.Complex(Header)
    wrench: t.Complex(Wrench)


@dataclass
class PoseWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/PoseWithCovarianceStamped'

    header: t.Complex(Header)
    pose: t.Complex(PoseWithCovariance)


@dataclass
class AccelWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/AccelWithCovarianceStamped'

    header: t.Complex(Header)
    accel: t.Complex(AccelWithCovariance)


@dataclass
class TwistWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/TwistWithCovarianceStamped'

    header: t.Complex(Header)
    twist: t.Complex(TwistWithCovariance)
