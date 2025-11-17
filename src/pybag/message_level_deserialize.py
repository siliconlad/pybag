"""Message-level deserialization with Rust parsing."""
from pybag.pybag_rust import deserialize_odometry as _rust_deserialize_odometry
from pybag.ros2.humble.builtin_interfaces import Time
from pybag.ros2.humble.geometry_msgs import (
    Point, Pose, PoseWithCovariance, Quaternion, Twist, TwistWithCovariance, Vector3
)
from pybag.ros2.humble.nav_msgs import Odometry
from pybag.ros2.humble.std_msgs import Header
from pybag.deserialize import MessageDeserializer as PythonMessageDeserializer
from pybag.mcap.records import MessageRecord, SchemaRecord
from pybag.schema import SchemaDecoder
from pybag.encoding.cdr_rust import CdrDecoder
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder
from typing import Callable


def construct_odometry_from_rust(
    header_sec: int,
    header_nanosec: int,
    frame_id: str,
    child_frame_id: str,
    point_x: float, point_y: float, point_z: float,
    quat_x: float, quat_y: float, quat_z: float, quat_w: float,
    pose_cov: list[float],
    linear_x: float, linear_y: float, linear_z: float,
    angular_x: float, angular_y: float, angular_z: float,
    twist_cov: list[float]
) -> Odometry:
    """Construct an Odometry message from Rust-parsed values."""
    return Odometry(
        header=Header(
            stamp=Time(sec=header_sec, nanosec=header_nanosec),
            frame_id=frame_id
        ),
        child_frame_id=child_frame_id,
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(x=point_x, y=point_y, z=point_z),
                orientation=Quaternion(x=quat_x, y=quat_y, z=quat_z, w=quat_w)
            ),
            covariance=pose_cov
        ),
        twist=TwistWithCovariance(
            twist=Twist(
                linear=Vector3(x=linear_x, y=linear_y, z=linear_z),
                angular=Vector3(x=angular_x, y=angular_y, z=angular_z)
            ),
            covariance=twist_cov
        )
    )


class MessageLevelDeserializer:
    """Deserializer using message-level Rust parsing."""

    def __init__(
        self,
        schema_decoder: SchemaDecoder,
        message_decoder: type,
    ):
        self._schema_decoder = schema_decoder
        self._message_decoder = message_decoder
        # Fallback to Python for unsupported types
        self._python_deserializer = PythonMessageDeserializer(schema_decoder, CdrDecoder)
        self._compiled: dict[int, Callable[[MessageRecord], type]] = {}

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> type:
        """Deserialize a message using message-level Rust parsing when available."""
        # Check if we have a Rust deserializer for this message type
        if schema.name == "nav_msgs/msg/Odometry":
            # Use Rust message-level deserializer
            try:
                return _rust_deserialize_odometry(message.data)
            except Exception:
                # Fall back to Python if Rust fails
                return self._python_deserializer.deserialize_message(message, schema)
        else:
            # Fall back to Python for unsupported types
            return self._python_deserializer.deserialize_message(message, schema)


class MessageLevelDeserializerFactory:
    """Factory for creating message-level deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageLevelDeserializer | None:
        if profile == "ros2":
            return MessageLevelDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        return None
