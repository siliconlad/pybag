from dataclasses import dataclass
from typing import Literal

import pybag.ros2.humble.std_msgs as std_msgs
import pybag.types as t


@dataclass(kw_only=True)
class Point:
    __msg_name__ = 'geometry_msgs/msg/Point'

    x: t.float64
    y: t.float64
    z: t.float64


@dataclass(kw_only=True)
class Point32:
    __msg_name__ = 'geometry_msgs/msg/Point32'

    x: t.float32
    y: t.float32
    z: t.float32


@dataclass(kw_only=True)
class PointStamped:
    __msg_name__ = 'geometry_msgs/msg/PointStamped'

    header: t.Complex[std_msgs.Header]
    point: t.Complex[Point]


@dataclass(kw_only=True)
class Polygon:
    __msg_name__ = 'geometry_msgs/msg/Polygon'

    points: t.Array[t.Complex[Point32]]


@dataclass(kw_only=True)
class PolygonInstance:
    __msg_name__ = 'geometry_msgs/msg/PolygonInstance'

    polygon: t.Complex[Polygon]
    id: t.int64


@dataclass(kw_only=True)
class PolygonInstanceStamped:
    __msg_name__ = 'geometry_msgs/msg/PolygonInstanceStamped'

    header: t.Complex[std_msgs.Header]
    polygon: t.Complex[PolygonInstance]


@dataclass(kw_only=True)
class PolygonStamped:
    __msg_name__ = 'geometry_msgs/msg/PolygonStamped'

    header: t.Complex[std_msgs.Header]
    polygon: t.Complex[Polygon]


@dataclass(kw_only=True)
class Quaternion:
    __msg_name__ = 'geometry_msgs/msg/Quaternion'

    x: t.float64 = 0
    y: t.float64 = 0
    z: t.float64 = 0
    w: t.float64 = 1


@dataclass(kw_only=True)
class QuaternionStamped:
    __msg_name__ = 'geometry_msgs/msg/QuaternionStamped'

    header: t.Complex[std_msgs.Header]
    quaternion: t.Complex[Quaternion]


@dataclass(kw_only=True)
class Pose:
    __msg_name__ = 'geometry_msgs/msg/Pose'

    position: t.Complex[Point]
    orientation: t.Complex[Quaternion]


@dataclass(kw_only=True)
class Pose2D:
    __msg_name__ = 'geometry_msgs/msg/Pose2D'

    x: t.float64
    y: t.float64
    theta: t.float64


@dataclass(kw_only=True)
class PoseArray:
    __msg_name__ = 'geometry_msgs/msg/PoseArray'

    header: t.Complex[std_msgs.Header]
    poses: t.Array[t.Complex[Pose]]


@dataclass(kw_only=True)
class PoseStamped:
    __msg_name__ = 'geometry_msgs/msg/PoseStamped'

    header: t.Complex[std_msgs.Header]
    pose: t.Complex[Pose]


@dataclass(kw_only=True)
class PoseWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/PoseWithCovariance'

    pose: t.Complex[Pose]
    covariance: t.Array[t.float64, Literal[36]]


@dataclass(kw_only=True)
class PoseWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/PoseWithCovarianceStamped'

    header: t.Complex[std_msgs.Header]
    pose: t.Complex[PoseWithCovariance]


@dataclass(kw_only=True)
class Vector3:
    __msg_name__ = 'geometry_msgs/msg/Vector3'

    x: t.float64
    y: t.float64
    z: t.float64


@dataclass(kw_only=True)
class Vector3Stamped:
    __msg_name__ = 'geometry_msgs/msg/Vector3Stamped'

    header: t.Complex[std_msgs.Header]
    vector: t.Complex[Vector3]


@dataclass(kw_only=True)
class Accel:
    __msg_name__ = 'geometry_msgs/msg/Accel'

    linear: t.Complex[Vector3]
    angular: t.Complex[Vector3]


@dataclass(kw_only=True)
class AccelStamped:
    __msg_name__ = 'geometry_msgs/msg/AccelStamped'

    header: t.Complex[std_msgs.Header]
    accel: t.Complex[Accel]


@dataclass(kw_only=True)
class AccelWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/AccelWithCovariance'

    accel: t.Complex[Accel]
    covariance: t.Array[t.float64, Literal[36]]


@dataclass(kw_only=True)
class AccelWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/AccelWithCovarianceStamped'

    header: t.Complex[std_msgs.Header]
    accel: t.Complex[AccelWithCovariance]


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


@dataclass(kw_only=True)
class InertiaStamped:
    __msg_name__ = 'geometry_msgs/msg/InertiaStamped'

    header: t.Complex[std_msgs.Header]
    inertia: t.Complex[Inertia]


@dataclass(kw_only=True)
class Transform:
    __msg_name__ = 'geometry_msgs/msg/Transform'

    translation: t.Complex[Vector3]
    rotation: t.Complex[Quaternion]


@dataclass(kw_only=True)
class TransformStamped:
    __msg_name__ = 'geometry_msgs/msg/TransformStamped'

    header: t.Complex[std_msgs.Header]
    child_frame_id: t.string
    transform: t.Complex[Transform]


@dataclass(kw_only=True)
class Twist:
    __msg_name__ = 'geometry_msgs/msg/Twist'

    linear: t.Complex[Vector3]
    angular: t.Complex[Vector3]


@dataclass(kw_only=True)
class TwistStamped:
    __msg_name__ = 'geometry_msgs/msg/TwistStamped'

    header: t.Complex[std_msgs.Header]
    twist: t.Complex[Twist]


@dataclass(kw_only=True)
class TwistWithCovariance:
    __msg_name__ = 'geometry_msgs/msg/TwistWithCovariance'

    twist: t.Complex[Twist]
    covariance: t.Array[t.float64, Literal[36]]


@dataclass(kw_only=True)
class TwistWithCovarianceStamped:
    __msg_name__ = 'geometry_msgs/msg/TwistWithCovarianceStamped'

    header: t.Complex[std_msgs.Header]
    twist: t.Complex[TwistWithCovariance]


@dataclass(kw_only=True)
class VelocityStamped:
    __msg_name__ = 'geometry_msgs/msg/VelocityStamped'

    header: t.Complex[std_msgs.Header]
    body_frame_id: t.string
    reference_frame_id: t.string
    velocity: t.Complex[Twist]


@dataclass(kw_only=True)
class Wrench:
    __msg_name__ = 'geometry_msgs/msg/Wrench'

    force: t.Complex[Vector3]
    torque: t.Complex[Vector3]


@dataclass(kw_only=True)
class WrenchStamped:
    __msg_name__ = 'geometry_msgs/msg/WrenchStamped'

    header: t.Complex[std_msgs.Header]
    wrench: t.Complex[Wrench]
