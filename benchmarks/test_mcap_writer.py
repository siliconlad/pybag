"""
Benchmarks for MCAP writing comparing pybag, rosbags, and official mcap-ros2.

This module tests writing messages to MCAP files across different libraries,
message counts, and topic configurations.
"""

import random
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, Iterable

import numpy as np
import pytest
from mcap_ros2.writer import Writer as McapWriter
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag2 import StoragePlugin
from rosbags.rosbag2 import Writer as RosbagsWriter
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_writer import McapFileWriter

# pybag message types
from pybag.ros2.humble.builtin_interfaces import Time as PybagTime
from pybag.ros2.humble.geometry_msgs import (
    Point as PybagPoint,
    Pose as PybagPose,
    PoseWithCovariance as PybagPoseWithCovariance,
    Quaternion as PybagQuaternion,
    Transform as PybagTransform,
    Twist as PybagTwist,
    TwistWithCovariance as PybagTwistWithCovariance,
    Vector3 as PybagVector3,
)
from pybag.ros2.humble.nav_msgs import Odometry as PybagOdometry
from pybag.ros2.humble.sensor_msgs import Imu as PybagImu
from pybag.ros2.humble.std_msgs import Header as PybagHeader, String as PybagString

# rosbags types
TYPESTORE = get_typestore(Stores.ROS2_HUMBLE)
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
RosbagsString = TYPESTORE.types["std_msgs/msg/String"]


# Schema definitions for official mcap-ros2
ODOMETRY_SCHEMA = dedent(
    """
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
)

IMU_SCHEMA = dedent(
    """
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
)

STRING_SCHEMA = dedent(
    """
    string data
"""
)


# ============================================================================
# Message Generators
# ============================================================================


def generate_pybag_odometry(rng: random.Random, timestamp: int) -> PybagOdometry:
    """Generate an Odometry message using pybag types."""
    return PybagOdometry(
        header=PybagHeader(
            stamp=PybagTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="map",
        ),
        child_frame_id="base_link",
        pose=PybagPoseWithCovariance(
            pose=PybagPose(
                position=PybagPoint(
                    x=rng.random(),
                    y=rng.random(),
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


def generate_pybag_string(rng: random.Random, timestamp: int) -> PybagString:
    """Generate a String message using pybag types."""
    return PybagString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


def generate_rosbags_odometry(rng: random.Random, timestamp: int) -> Any:
    """Generate an Odometry message using rosbags types."""
    return RosbagsOdometry(
        header=RosbagsHeader(
            stamp=RosbagsTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="map",
        ),
        child_frame_id="base_link",
        pose=RosbagsPoseWithCovariance(
            pose=RosbagsPose(
                position=RosbagsPoint(
                    x=rng.random(),
                    y=rng.random(),
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


def generate_rosbags_string(rng: random.Random, timestamp: int) -> Any:
    """Generate a String message using rosbags types."""
    return RosbagsString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


def generate_official_odometry(rng: random.Random, timestamp: int) -> dict:
    """Generate an Odometry message as dict for official mcap-ros2."""
    return {
        "header": {
            "stamp": {
                "sec": timestamp // 1_000_000_000,
                "nanosec": timestamp % 1_000_000_000,
            },
            "frame_id": "map",
        },
        "child_frame_id": "base_link",
        "pose": {
            "pose": {
                "position": {
                    "x": rng.random(),
                    "y": rng.random(),
                    "z": rng.random(),
                },
                "orientation": {
                    "x": rng.random(),
                    "y": rng.random(),
                    "z": rng.random(),
                    "w": rng.random(),
                },
            },
            "covariance": [rng.random() for _ in range(36)],
        },
        "twist": {
            "twist": {
                "linear": {
                    "x": rng.random(),
                    "y": rng.random(),
                    "z": rng.random(),
                },
                "angular": {
                    "x": rng.random(),
                    "y": rng.random(),
                    "z": rng.random(),
                },
            },
            "covariance": [rng.random() for _ in range(36)],
        },
    }


def generate_official_imu(rng: random.Random, timestamp: int) -> dict:
    """Generate an IMU message as dict for official mcap-ros2."""
    return {
        "header": {
            "stamp": {
                "sec": timestamp // 1_000_000_000,
                "nanosec": timestamp % 1_000_000_000,
            },
            "frame_id": "imu_link",
        },
        "orientation": {
            "x": rng.random(),
            "y": rng.random(),
            "z": rng.random(),
            "w": rng.random(),
        },
        "orientation_covariance": [rng.random() for _ in range(9)],
        "angular_velocity": {
            "x": rng.random() * 0.1,
            "y": rng.random() * 0.1,
            "z": rng.random() * 0.1,
        },
        "angular_velocity_covariance": [rng.random() for _ in range(9)],
        "linear_acceleration": {
            "x": rng.random() * 9.8,
            "y": rng.random() * 9.8,
            "z": 9.8 + rng.random(),
        },
        "linear_acceleration_covariance": [rng.random() for _ in range(9)],
    }


def generate_official_string(rng: random.Random, timestamp: int) -> dict:
    """Generate a String message as dict for official mcap-ros2."""
    return {"data": f"message_{timestamp}_{rng.randint(0, 1000000)}"}


# ============================================================================
# Standard Write Benchmarks (1000 messages, single topic)
# ============================================================================


class TestWriteStandard:
    """Write benchmarks with standard 1000 Odometry messages."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagOdometry]:
        rng = random.Random(42)
        return [generate_pybag_odometry(rng, i * 1_500_000_000) for i in range(1000)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_odometry(rng, i * 1_500_000_000) for i in range(1000)]

    @pytest.fixture
    def official_messages(self) -> list[dict]:
        rng = random.Random(42)
        return [generate_official_odometry(rng, i * 1_500_000_000) for i in range(1000)]

    def test_pybag_write(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagOdometry]
    ) -> None:
        def write_with_pybag(path: Path) -> None:
            writer = McapFileWriter.open(path)
            writer.add_channel("/odom", PybagOdometry)
            for i, msg in enumerate(pybag_messages):
                timestamp = int(i * 1_500_000_000)
                writer.write_message("/odom", timestamp, msg)
            writer.close()

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "pybag.mcap"
            benchmark(lambda: write_with_pybag(path))

    def test_rosbags_write(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def write_with_rosbags(path: Path) -> None:
            with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
                conn = writer.add_connection(
                    "/odom", RosbagsOdometry.__msgtype__, typestore=TYPESTORE
                )
                for i, msg in enumerate(rosbags_messages):
                    timestamp = int(i * 1_500_000_000)
                    serialized = TYPESTORE.serialize_cdr(msg, RosbagsOdometry.__msgtype__)
                    writer.write(conn, timestamp, serialized)

        with TemporaryDirectory() as tmpdir:
            counter = count()
            benchmark(lambda: write_with_rosbags(Path(tmpdir) / str(next(counter))))

    def test_official_write(
        self, benchmark: BenchmarkFixture, official_messages: list[dict]
    ) -> None:
        def write_with_official(path: Path) -> None:
            with open(path, "wb") as f:
                writer = McapWriter(f)
                schema = writer.register_msgdef("nav_msgs/msg/Odometry", ODOMETRY_SCHEMA)
                for i, msg in enumerate(official_messages):
                    timestamp = int(i * 1_500_000_000)
                    writer.write_message(
                        topic="/odom",
                        schema=schema,
                        message=msg,
                        log_time=timestamp,
                        publish_time=timestamp,
                    )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "official.mcap"
            benchmark(lambda: write_with_official(path))


# ============================================================================
# Small Write Benchmarks (10 messages)
# ============================================================================


class TestWriteSmall:
    """Write benchmarks with small 10 message count."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagOdometry]:
        rng = random.Random(42)
        return [generate_pybag_odometry(rng, i * 1_500_000_000) for i in range(10)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_odometry(rng, i * 1_500_000_000) for i in range(10)]

    @pytest.fixture
    def official_messages(self) -> list[dict]:
        rng = random.Random(42)
        return [generate_official_odometry(rng, i * 1_500_000_000) for i in range(10)]

    def test_pybag_write_small(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagOdometry]
    ) -> None:
        def write_with_pybag(path: Path) -> None:
            writer = McapFileWriter.open(path)
            writer.add_channel("/odom", PybagOdometry)
            for i, msg in enumerate(pybag_messages):
                timestamp = int(i * 1_500_000_000)
                writer.write_message("/odom", timestamp, msg)
            writer.close()

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "pybag.mcap"
            benchmark(lambda: write_with_pybag(path))

    def test_rosbags_write_small(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def write_with_rosbags(path: Path) -> None:
            with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
                conn = writer.add_connection(
                    "/odom", RosbagsOdometry.__msgtype__, typestore=TYPESTORE
                )
                for i, msg in enumerate(rosbags_messages):
                    timestamp = int(i * 1_500_000_000)
                    serialized = TYPESTORE.serialize_cdr(msg, RosbagsOdometry.__msgtype__)
                    writer.write(conn, timestamp, serialized)

        with TemporaryDirectory() as tmpdir:
            counter = count()
            benchmark(lambda: write_with_rosbags(Path(tmpdir) / str(next(counter))))

    def test_official_write_small(
        self, benchmark: BenchmarkFixture, official_messages: list[dict]
    ) -> None:
        def write_with_official(path: Path) -> None:
            with open(path, "wb") as f:
                writer = McapWriter(f)
                schema = writer.register_msgdef("nav_msgs/msg/Odometry", ODOMETRY_SCHEMA)
                for i, msg in enumerate(official_messages):
                    timestamp = int(i * 1_500_000_000)
                    writer.write_message(
                        topic="/odom",
                        schema=schema,
                        message=msg,
                        log_time=timestamp,
                        publish_time=timestamp,
                    )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "official.mcap"
            benchmark(lambda: write_with_official(path))


# ============================================================================
# Multi-Topic Write Benchmarks (3 topics: Odometry, IMU, String)
# ============================================================================


class TestWriteMultiTopic:
    """Write benchmarks with multiple topics."""

    @pytest.fixture
    def pybag_messages(
        self,
    ) -> tuple[list[PybagOdometry], list[PybagImu], list[PybagString]]:
        rng = random.Random(42)
        odom_msgs = [generate_pybag_odometry(rng, i * 10_000_000) for i in range(100)]
        imu_msgs = [generate_pybag_imu(rng, i * 5_000_000) for i in range(200)]
        string_msgs = [generate_pybag_string(rng, i * 1_000_000_000) for i in range(10)]
        return odom_msgs, imu_msgs, string_msgs

    @pytest.fixture
    def rosbags_messages(self) -> tuple[list[Any], list[Any], list[Any]]:
        rng = random.Random(42)
        odom_msgs = [generate_rosbags_odometry(rng, i * 10_000_000) for i in range(100)]
        imu_msgs = [generate_rosbags_imu(rng, i * 5_000_000) for i in range(200)]
        string_msgs = [generate_rosbags_string(rng, i * 1_000_000_000) for i in range(10)]
        return odom_msgs, imu_msgs, string_msgs

    @pytest.fixture
    def official_messages(self) -> tuple[list[dict], list[dict], list[dict]]:
        rng = random.Random(42)
        odom_msgs = [generate_official_odometry(rng, i * 10_000_000) for i in range(100)]
        imu_msgs = [generate_official_imu(rng, i * 5_000_000) for i in range(200)]
        string_msgs = [generate_official_string(rng, i * 1_000_000_000) for i in range(10)]
        return odom_msgs, imu_msgs, string_msgs

    def test_pybag_write_multi_topic(
        self,
        benchmark: BenchmarkFixture,
        pybag_messages: tuple[list[PybagOdometry], list[PybagImu], list[PybagString]],
    ) -> None:
        odom_msgs, imu_msgs, string_msgs = pybag_messages

        def write_with_pybag(path: Path) -> None:
            writer = McapFileWriter.open(path)
            writer.add_channel("/odom", PybagOdometry)
            writer.add_channel("/imu", PybagImu)
            writer.add_channel("/status", PybagString)

            # Interleave messages by timestamp
            all_msgs: list[tuple[int, str, Any]] = []
            for i, msg in enumerate(odom_msgs):
                all_msgs.append((i * 10_000_000, "/odom", msg))
            for i, msg in enumerate(imu_msgs):
                all_msgs.append((i * 5_000_000, "/imu", msg))
            for i, msg in enumerate(string_msgs):
                all_msgs.append((i * 1_000_000_000, "/status", msg))

            all_msgs.sort(key=lambda x: x[0])
            for timestamp, topic, msg in all_msgs:
                writer.write_message(topic, timestamp, msg)
            writer.close()

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "pybag.mcap"
            benchmark(lambda: write_with_pybag(path))

    def test_rosbags_write_multi_topic(
        self,
        benchmark: BenchmarkFixture,
        rosbags_messages: tuple[list[Any], list[Any], list[Any]],
    ) -> None:
        odom_msgs, imu_msgs, string_msgs = rosbags_messages

        def write_with_rosbags(path: Path) -> None:
            with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
                odom_conn = writer.add_connection(
                    "/odom", RosbagsOdometry.__msgtype__, typestore=TYPESTORE
                )
                imu_conn = writer.add_connection(
                    "/imu", RosbagsImu.__msgtype__, typestore=TYPESTORE
                )
                string_conn = writer.add_connection(
                    "/status", RosbagsString.__msgtype__, typestore=TYPESTORE
                )

                # Interleave messages by timestamp
                all_msgs: list[tuple[int, Any, Any, str]] = []
                for i, msg in enumerate(odom_msgs):
                    all_msgs.append(
                        (i * 10_000_000, odom_conn, msg, RosbagsOdometry.__msgtype__)
                    )
                for i, msg in enumerate(imu_msgs):
                    all_msgs.append((i * 5_000_000, imu_conn, msg, RosbagsImu.__msgtype__))
                for i, msg in enumerate(string_msgs):
                    all_msgs.append(
                        (i * 1_000_000_000, string_conn, msg, RosbagsString.__msgtype__)
                    )

                all_msgs.sort(key=lambda x: x[0])
                for timestamp, conn, msg, msgtype in all_msgs:
                    serialized = TYPESTORE.serialize_cdr(msg, msgtype)
                    writer.write(conn, timestamp, serialized)

        with TemporaryDirectory() as tmpdir:
            counter = count()
            benchmark(lambda: write_with_rosbags(Path(tmpdir) / str(next(counter))))

    def test_official_write_multi_topic(
        self,
        benchmark: BenchmarkFixture,
        official_messages: tuple[list[dict], list[dict], list[dict]],
    ) -> None:
        odom_msgs, imu_msgs, string_msgs = official_messages

        def write_with_official(path: Path) -> None:
            with open(path, "wb") as f:
                writer = McapWriter(f)
                odom_schema = writer.register_msgdef("nav_msgs/msg/Odometry", ODOMETRY_SCHEMA)
                imu_schema = writer.register_msgdef("sensor_msgs/msg/Imu", IMU_SCHEMA)
                string_schema = writer.register_msgdef("std_msgs/msg/String", STRING_SCHEMA)

                # Interleave messages by timestamp
                all_msgs: list[tuple[int, str, Any, Any]] = []
                for i, msg in enumerate(odom_msgs):
                    all_msgs.append((i * 10_000_000, "/odom", odom_schema, msg))
                for i, msg in enumerate(imu_msgs):
                    all_msgs.append((i * 5_000_000, "/imu", imu_schema, msg))
                for i, msg in enumerate(string_msgs):
                    all_msgs.append((i * 1_000_000_000, "/status", string_schema, msg))

                all_msgs.sort(key=lambda x: x[0])
                for timestamp, topic, schema, msg in all_msgs:
                    writer.write_message(
                        topic=topic,
                        schema=schema,
                        message=msg,
                        log_time=timestamp,
                        publish_time=timestamp,
                    )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "official.mcap"
            benchmark(lambda: write_with_official(path))


# ============================================================================
# Large Write Benchmarks (10000 messages)
# ============================================================================


class TestWriteLarge:
    """Write benchmarks with large 10000 message count."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagOdometry]:
        rng = random.Random(42)
        return [generate_pybag_odometry(rng, i * 1_000_000) for i in range(10000)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_odometry(rng, i * 1_000_000) for i in range(10000)]

    @pytest.fixture
    def official_messages(self) -> list[dict]:
        rng = random.Random(42)
        return [generate_official_odometry(rng, i * 1_000_000) for i in range(10000)]

    def test_pybag_write_large(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagOdometry]
    ) -> None:
        def write_with_pybag(path: Path) -> None:
            writer = McapFileWriter.open(path)
            writer.add_channel("/odom", PybagOdometry)
            for i, msg in enumerate(pybag_messages):
                timestamp = int(i * 1_000_000)
                writer.write_message("/odom", timestamp, msg)
            writer.close()

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "pybag.mcap"
            benchmark(lambda: write_with_pybag(path))

    def test_rosbags_write_large(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def write_with_rosbags(path: Path) -> None:
            with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
                conn = writer.add_connection(
                    "/odom", RosbagsOdometry.__msgtype__, typestore=TYPESTORE
                )
                for i, msg in enumerate(rosbags_messages):
                    timestamp = int(i * 1_000_000)
                    serialized = TYPESTORE.serialize_cdr(msg, RosbagsOdometry.__msgtype__)
                    writer.write(conn, timestamp, serialized)

        with TemporaryDirectory() as tmpdir:
            counter = count()
            benchmark(lambda: write_with_rosbags(Path(tmpdir) / str(next(counter))))

    def test_official_write_large(
        self, benchmark: BenchmarkFixture, official_messages: list[dict]
    ) -> None:
        def write_with_official(path: Path) -> None:
            with open(path, "wb") as f:
                writer = McapWriter(f)
                schema = writer.register_msgdef("nav_msgs/msg/Odometry", ODOMETRY_SCHEMA)
                for i, msg in enumerate(official_messages):
                    timestamp = int(i * 1_000_000)
                    writer.write_message(
                        topic="/odom",
                        schema=schema,
                        message=msg,
                        log_time=timestamp,
                        publish_time=timestamp,
                    )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "official.mcap"
            benchmark(lambda: write_with_official(path))
