"""
Benchmarks for message deserialization comparing pybag, rosbags, and mcap-ros2.

This module tests the performance of deserializing CDR-encoded ROS2 messages
across different libraries and message types.
"""

import random
from typing import Any

import numpy as np
import pytest
from mcap.records import Schema as McapSchema
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.typesys import Stores, get_typestore

from pybag.deserialize import MessageDeserializer
from pybag.encoding.cdr import CdrDecoder
from pybag.mcap.records import MessageRecord, SchemaRecord
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder

# pybag message types
from pybag.ros2.humble.builtin_interfaces import Time as PybagTime
from pybag.ros2.humble.geometry_msgs import (
    Point as PybagPoint,
    Pose as PybagPose,
    PoseWithCovariance as PybagPoseWithCovariance,
    Quaternion as PybagQuaternion,
    Twist as PybagTwist,
    TwistWithCovariance as PybagTwistWithCovariance,
    Vector3 as PybagVector3,
)
from pybag.ros2.humble.nav_msgs import Odometry as PybagOdometry
from pybag.ros2.humble.sensor_msgs import (
    Imu as PybagImu,
    LaserScan as PybagLaserScan,
    PointCloud2 as PybagPointCloud2,
    PointField as PybagPointField,
)
from pybag.ros2.humble.std_msgs import Header as PybagHeader, String as PybagString
from pybag.serialize import MessageSerializerFactory

# rosbags types
TYPESTORE = get_typestore(Stores.LATEST)
RosbagsOdometry = TYPESTORE.types["nav_msgs/msg/Odometry"]
RosbagsHeader = TYPESTORE.types["std_msgs/msg/Header"]
RosbagsTime = TYPESTORE.types["builtin_interfaces/msg/Time"]
RosbagsPoseWithCovariance = TYPESTORE.types["geometry_msgs/msg/PoseWithCovariance"]
RosbagsPose = TYPESTORE.types["geometry_msgs/msg/Pose"]
RosbagsPoint = TYPESTORE.types["geometry_msgs/msg/Point"]
RosbagsQuaternion = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
RosbagsTwistWithCovariance = TYPESTORE.types["geometry_msgs/msg/TwistWithCovariance"]
RosbagsTwist = TYPESTORE.types["geometry_msgs/msg/Twist"]
RosbagsVector3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]
RosbagsImu = TYPESTORE.types["sensor_msgs/msg/Imu"]
RosbagsLaserScan = TYPESTORE.types["sensor_msgs/msg/LaserScan"]
RosbagsPointCloud2 = TYPESTORE.types["sensor_msgs/msg/PointCloud2"]
RosbagsPointField = TYPESTORE.types["sensor_msgs/msg/PointField"]
RosbagsString = TYPESTORE.types["std_msgs/msg/String"]


# Schema definitions for mcap-ros2 decoder
ODOMETRY_SCHEMA = """
std_msgs/Header header
string child_frame_id
geometry_msgs/PoseWithCovariance pose
geometry_msgs/TwistWithCovariance twist
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/PoseWithCovariance
geometry_msgs/Pose pose
float64[36] covariance
================================================================================
MSG: geometry_msgs/Pose
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
================================================================================
MSG: geometry_msgs/TwistWithCovariance
geometry_msgs/Twist twist
float64[36] covariance
================================================================================
MSG: geometry_msgs/Twist
geometry_msgs/Vector3 linear
geometry_msgs/Vector3 angular
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
"""

IMU_SCHEMA = """
std_msgs/Header header
geometry_msgs/Quaternion orientation
float64[9] orientation_covariance
geometry_msgs/Vector3 angular_velocity
float64[9] angular_velocity_covariance
geometry_msgs/Vector3 linear_acceleration
float64[9] linear_acceleration_covariance
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
================================================================================
MSG: geometry_msgs/Quaternion
float64 x
float64 y
float64 z
float64 w
================================================================================
MSG: geometry_msgs/Vector3
float64 x
float64 y
float64 z
"""

LASER_SCAN_SCHEMA = """
std_msgs/Header header
float32 angle_min
float32 angle_max
float32 angle_increment
float32 time_increment
float32 scan_time
float32 range_min
float32 range_max
float32[] ranges
float32[] intensities
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"""

STRING_SCHEMA = """
string data
"""


# ============================================================================
# Message Generators for pybag (for serialization)
# ============================================================================


def generate_pybag_odometry(rng: random.Random, timestamp: int) -> PybagOdometry:
    """Generate an Odometry message using pybag types."""
    return PybagOdometry(
        header=PybagHeader(
            stamp=PybagTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="odom",
        ),
        child_frame_id="base_link",
        pose=PybagPoseWithCovariance(
            pose=PybagPose(
                position=PybagPoint(
                    x=rng.random() * 10.0,
                    y=rng.random() * 10.0,
                    z=rng.random(),
                ),
                orientation=PybagQuaternion(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                    w=rng.random(),
                ),
            ),
            covariance=[rng.random() for _ in range(36)],
        ),
        twist=PybagTwistWithCovariance(
            twist=PybagTwist(
                linear=PybagVector3(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                ),
                angular=PybagVector3(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                ),
            ),
            covariance=[rng.random() for _ in range(36)],
        ),
    )


def generate_pybag_imu(rng: random.Random, timestamp: int) -> PybagImu:
    """Generate an IMU message using pybag types."""
    return PybagImu(
        header=PybagHeader(
            stamp=PybagTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="imu_link",
        ),
        orientation=PybagQuaternion(
            x=rng.random(),
            y=rng.random(),
            z=rng.random(),
            w=rng.random(),
        ),
        orientation_covariance=[rng.random() for _ in range(9)],
        angular_velocity=PybagVector3(
            x=rng.random() * 0.1,
            y=rng.random() * 0.1,
            z=rng.random() * 0.1,
        ),
        angular_velocity_covariance=[rng.random() for _ in range(9)],
        linear_acceleration=PybagVector3(
            x=rng.random() * 9.8,
            y=rng.random() * 9.8,
            z=9.8 + rng.random(),
        ),
        linear_acceleration_covariance=[rng.random() for _ in range(9)],
    )


def generate_pybag_laser_scan(
    rng: random.Random, timestamp: int, num_ranges: int = 360
) -> PybagLaserScan:
    """Generate a LaserScan message using pybag types."""
    return PybagLaserScan(
        header=PybagHeader(
            stamp=PybagTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="laser_link",
        ),
        angle_min=-3.14159,
        angle_max=3.14159,
        angle_increment=6.28318 / num_ranges,
        time_increment=0.0001,
        scan_time=0.1,
        range_min=0.1,
        range_max=30.0,
        ranges=[rng.random() * 10.0 for _ in range(num_ranges)],
        intensities=[rng.random() * 100.0 for _ in range(num_ranges)],
    )


def generate_pybag_string(rng: random.Random, timestamp: int) -> PybagString:
    """Generate a String message using pybag types."""
    return PybagString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


# ============================================================================
# Helper to create serialized messages using rosbags
# ============================================================================


def generate_rosbags_odometry(rng: random.Random, timestamp: int) -> Any:
    """Generate an Odometry message using rosbags types."""
    return RosbagsOdometry(
        header=RosbagsHeader(
            stamp=RosbagsTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="odom",
        ),
        child_frame_id="base_link",
        pose=RosbagsPoseWithCovariance(
            pose=RosbagsPose(
                position=RosbagsPoint(
                    x=rng.random() * 10.0,
                    y=rng.random() * 10.0,
                    z=rng.random(),
                ),
                orientation=RosbagsQuaternion(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                    w=rng.random(),
                ),
            ),
            covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
        ),
        twist=RosbagsTwistWithCovariance(
            twist=RosbagsTwist(
                linear=RosbagsVector3(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                ),
                angular=RosbagsVector3(
                    x=rng.random(),
                    y=rng.random(),
                    z=rng.random(),
                ),
            ),
            covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
        ),
    )


def generate_rosbags_imu(rng: random.Random, timestamp: int) -> Any:
    """Generate an IMU message using rosbags types."""
    return RosbagsImu(
        header=RosbagsHeader(
            stamp=RosbagsTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="imu_link",
        ),
        orientation=RosbagsQuaternion(
            x=rng.random(),
            y=rng.random(),
            z=rng.random(),
            w=rng.random(),
        ),
        orientation_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
        angular_velocity=RosbagsVector3(
            x=rng.random() * 0.1,
            y=rng.random() * 0.1,
            z=rng.random() * 0.1,
        ),
        angular_velocity_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
        linear_acceleration=RosbagsVector3(
            x=rng.random() * 9.8,
            y=rng.random() * 9.8,
            z=9.8 + rng.random(),
        ),
        linear_acceleration_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
    )


def generate_rosbags_laser_scan(rng: random.Random, timestamp: int, num_ranges: int = 360) -> Any:
    """Generate a LaserScan message using rosbags types."""
    return RosbagsLaserScan(
        header=RosbagsHeader(
            stamp=RosbagsTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="laser_link",
        ),
        angle_min=-3.14159,
        angle_max=3.14159,
        angle_increment=6.28318 / num_ranges,
        time_increment=0.0001,
        scan_time=0.1,
        range_min=0.1,
        range_max=30.0,
        ranges=np.array([rng.random() * 10.0 for _ in range(num_ranges)], dtype=np.float32),
        intensities=np.array([rng.random() * 100.0 for _ in range(num_ranges)], dtype=np.float32),
    )


def generate_rosbags_string(rng: random.Random, timestamp: int) -> Any:
    """Generate a String message using rosbags types."""
    return RosbagsString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


# ============================================================================
# Deserialization Benchmarks - Odometry
# ============================================================================


class TestDeserializeOdometry:
    """Benchmark deserialization of Odometry messages."""

    @pytest.fixture
    def serialized_messages(self) -> list[bytes]:
        """Serialize messages using rosbags for consistent test data."""
        rng = random.Random(42)
        messages = [generate_rosbags_odometry(rng, i * 1_000_000) for i in range(1000)]
        # Convert memoryview to bytes for compatibility with pybag
        return [bytes(TYPESTORE.serialize_cdr(msg, RosbagsOdometry.__msgtype__)) for msg in messages]

    @pytest.fixture
    def pybag_schema_record(self) -> SchemaRecord:
        """Create a schema record for pybag deserializer."""
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None
        return SchemaRecord(
            id=1,
            name="nav_msgs/msg/Odometry",
            encoding="ros2msg",
            data=serializer.serialize_schema(PybagOdometry),
        )

    def test_pybag_deserialize_odometry(
        self,
        benchmark: BenchmarkFixture,
        serialized_messages: list[bytes],
        pybag_schema_record: SchemaRecord,
    ) -> None:
        deserializer = MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

        # Warm up deserializer cache
        msg_record = MessageRecord(
            channel_id=1,
            sequence=0,
            log_time=0,
            publish_time=0,
            data=serialized_messages[0],
        )
        deserializer.deserialize_message(msg_record, pybag_schema_record)

        def deserialize_all() -> None:
            for i, data in enumerate(serialized_messages):
                msg_record = MessageRecord(
                    channel_id=1,
                    sequence=i,
                    log_time=i * 1_000_000,
                    publish_time=i * 1_000_000,
                    data=data,
                )
                deserializer.deserialize_message(msg_record, pybag_schema_record)

        benchmark(deserialize_all)

    def test_rosbags_deserialize_odometry(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        def deserialize_all() -> None:
            for data in serialized_messages:
                TYPESTORE.deserialize_cdr(data, RosbagsOdometry.__msgtype__)

        benchmark(deserialize_all)

    def test_mcap_ros2_deserialize_odometry(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        decoder_factory = DecoderFactory()
        schema = McapSchema(
            id=1,
            name="nav_msgs/msg/Odometry",
            encoding="ros2msg",
            data=ODOMETRY_SCHEMA.encode(),
        )
        decoder = decoder_factory.decoder_for("cdr", schema)
        assert decoder is not None

        def deserialize_all() -> None:
            for data in serialized_messages:
                decoder(data)

        benchmark(deserialize_all)


# ============================================================================
# Deserialization Benchmarks - IMU
# ============================================================================


class TestDeserializeImu:
    """Benchmark deserialization of IMU messages."""

    @pytest.fixture
    def serialized_messages(self) -> list[bytes]:
        rng = random.Random(42)
        messages = [generate_rosbags_imu(rng, i * 1_000_000) for i in range(1000)]
        # Convert memoryview to bytes for compatibility with pybag
        return [bytes(TYPESTORE.serialize_cdr(msg, RosbagsImu.__msgtype__)) for msg in messages]

    @pytest.fixture
    def pybag_schema_record(self) -> SchemaRecord:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None
        return SchemaRecord(
            id=1,
            name="sensor_msgs/msg/Imu",
            encoding="ros2msg",
            data=serializer.serialize_schema(PybagImu),
        )

    def test_pybag_deserialize_imu(
        self,
        benchmark: BenchmarkFixture,
        serialized_messages: list[bytes],
        pybag_schema_record: SchemaRecord,
    ) -> None:
        deserializer = MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

        # Warm up
        msg_record = MessageRecord(
            channel_id=1,
            sequence=0,
            log_time=0,
            publish_time=0,
            data=serialized_messages[0],
        )
        deserializer.deserialize_message(msg_record, pybag_schema_record)

        def deserialize_all() -> None:
            for i, data in enumerate(serialized_messages):
                msg_record = MessageRecord(
                    channel_id=1,
                    sequence=i,
                    log_time=i * 1_000_000,
                    publish_time=i * 1_000_000,
                    data=data,
                )
                deserializer.deserialize_message(msg_record, pybag_schema_record)

        benchmark(deserialize_all)

    def test_rosbags_deserialize_imu(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        def deserialize_all() -> None:
            for data in serialized_messages:
                TYPESTORE.deserialize_cdr(data, RosbagsImu.__msgtype__)

        benchmark(deserialize_all)

    def test_mcap_ros2_deserialize_imu(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        decoder_factory = DecoderFactory()
        schema = McapSchema(
            id=1,
            name="sensor_msgs/msg/Imu",
            encoding="ros2msg",
            data=IMU_SCHEMA.encode(),
        )
        decoder = decoder_factory.decoder_for("cdr", schema)
        assert decoder is not None

        def deserialize_all() -> None:
            for data in serialized_messages:
                decoder(data)

        benchmark(deserialize_all)


# ============================================================================
# Deserialization Benchmarks - LaserScan
# ============================================================================


class TestDeserializeLaserScan:
    """Benchmark deserialization of LaserScan messages (variable-length arrays)."""

    @pytest.fixture
    def serialized_messages(self) -> list[bytes]:
        rng = random.Random(42)
        messages = [
            generate_rosbags_laser_scan(rng, i * 1_000_000, num_ranges=360) for i in range(100)
        ]
        # Convert memoryview to bytes for compatibility with pybag
        return [bytes(TYPESTORE.serialize_cdr(msg, RosbagsLaserScan.__msgtype__)) for msg in messages]

    @pytest.fixture
    def pybag_schema_record(self) -> SchemaRecord:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None
        return SchemaRecord(
            id=1,
            name="sensor_msgs/msg/LaserScan",
            encoding="ros2msg",
            data=serializer.serialize_schema(PybagLaserScan),
        )

    def test_pybag_deserialize_laser_scan(
        self,
        benchmark: BenchmarkFixture,
        serialized_messages: list[bytes],
        pybag_schema_record: SchemaRecord,
    ) -> None:
        deserializer = MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

        # Warm up
        msg_record = MessageRecord(
            channel_id=1,
            sequence=0,
            log_time=0,
            publish_time=0,
            data=serialized_messages[0],
        )
        deserializer.deserialize_message(msg_record, pybag_schema_record)

        def deserialize_all() -> None:
            for i, data in enumerate(serialized_messages):
                msg_record = MessageRecord(
                    channel_id=1,
                    sequence=i,
                    log_time=i * 1_000_000,
                    publish_time=i * 1_000_000,
                    data=data,
                )
                deserializer.deserialize_message(msg_record, pybag_schema_record)

        benchmark(deserialize_all)

    def test_rosbags_deserialize_laser_scan(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        def deserialize_all() -> None:
            for data in serialized_messages:
                TYPESTORE.deserialize_cdr(data, RosbagsLaserScan.__msgtype__)

        benchmark(deserialize_all)

    def test_mcap_ros2_deserialize_laser_scan(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        decoder_factory = DecoderFactory()
        schema = McapSchema(
            id=1,
            name="sensor_msgs/msg/LaserScan",
            encoding="ros2msg",
            data=LASER_SCAN_SCHEMA.encode(),
        )
        decoder = decoder_factory.decoder_for("cdr", schema)
        assert decoder is not None

        def deserialize_all() -> None:
            for data in serialized_messages:
                decoder(data)

        benchmark(deserialize_all)


# ============================================================================
# Deserialization Benchmarks - String
# ============================================================================


class TestDeserializeString:
    """Benchmark deserialization of String messages (simple message type)."""

    @pytest.fixture
    def serialized_messages(self) -> list[bytes]:
        rng = random.Random(42)
        messages = [generate_rosbags_string(rng, i * 1_000_000) for i in range(1000)]
        # Convert memoryview to bytes for compatibility with pybag
        return [bytes(TYPESTORE.serialize_cdr(msg, RosbagsString.__msgtype__)) for msg in messages]

    @pytest.fixture
    def pybag_schema_record(self) -> SchemaRecord:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None
        return SchemaRecord(
            id=1,
            name="std_msgs/msg/String",
            encoding="ros2msg",
            data=serializer.serialize_schema(PybagString),
        )

    def test_pybag_deserialize_string(
        self,
        benchmark: BenchmarkFixture,
        serialized_messages: list[bytes],
        pybag_schema_record: SchemaRecord,
    ) -> None:
        deserializer = MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

        # Warm up
        msg_record = MessageRecord(
            channel_id=1,
            sequence=0,
            log_time=0,
            publish_time=0,
            data=serialized_messages[0],
        )
        deserializer.deserialize_message(msg_record, pybag_schema_record)

        def deserialize_all() -> None:
            for i, data in enumerate(serialized_messages):
                msg_record = MessageRecord(
                    channel_id=1,
                    sequence=i,
                    log_time=i * 1_000_000,
                    publish_time=i * 1_000_000,
                    data=data,
                )
                deserializer.deserialize_message(msg_record, pybag_schema_record)

        benchmark(deserialize_all)

    def test_rosbags_deserialize_string(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        def deserialize_all() -> None:
            for data in serialized_messages:
                TYPESTORE.deserialize_cdr(data, RosbagsString.__msgtype__)

        benchmark(deserialize_all)

    def test_mcap_ros2_deserialize_string(
        self, benchmark: BenchmarkFixture, serialized_messages: list[bytes]
    ) -> None:
        decoder_factory = DecoderFactory()
        schema = McapSchema(
            id=1,
            name="std_msgs/msg/String",
            encoding="ros2msg",
            data=STRING_SCHEMA.encode(),
        )
        decoder = decoder_factory.decoder_for("cdr", schema)
        assert decoder is not None

        def deserialize_all() -> None:
            for data in serialized_messages:
                decoder(data)

        benchmark(deserialize_all)
