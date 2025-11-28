"""
Benchmarks for message serialization comparing pybag, rosbags, and mcap-ros2.

This module tests the performance of serializing ROS2 messages to CDR format
across different libraries and message types.
"""

import random
from typing import Any

import numpy as np
import pytest
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.typesys import Stores, get_typestore

from pybag.serialize import MessageSerializerFactory

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
from pybag.ros2.humble.sensor_msgs import (
    Imu as PybagImu,
    LaserScan as PybagLaserScan,
    PointCloud2 as PybagPointCloud2,
    PointField as PybagPointField,
)
from pybag.ros2.humble.std_msgs import Header as PybagHeader, String as PybagString

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


# ============================================================================
# Message Generators for pybag
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


def generate_pybag_point_cloud2(
    rng: random.Random, timestamp: int, num_points: int = 1000
) -> PybagPointCloud2:
    """Generate a PointCloud2 message using pybag types."""
    point_step = 16  # 4 floats * 4 bytes
    row_step = point_step * num_points
    data: list[int] = []

    for _ in range(num_points):
        # Pack x, y, z, intensity as float32 bytes
        import struct

        x = rng.random() * 10.0 - 5.0
        y = rng.random() * 10.0 - 5.0
        z = rng.random() * 3.0
        intensity = rng.random() * 100.0
        data.extend(struct.pack("<ffff", x, y, z, intensity))

    return PybagPointCloud2(
        header=PybagHeader(
            stamp=PybagTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="lidar_link",
        ),
        height=1,
        width=num_points,
        fields=[
            PybagPointField(name="x", offset=0, datatype=7, count=1),
            PybagPointField(name="y", offset=4, datatype=7, count=1),
            PybagPointField(name="z", offset=8, datatype=7, count=1),
            PybagPointField(name="intensity", offset=12, datatype=7, count=1),
        ],
        is_bigendian=False,
        point_step=point_step,
        row_step=row_step,
        data=list(data),
        is_dense=True,
    )


def generate_pybag_string(rng: random.Random, timestamp: int) -> PybagString:
    """Generate a String message using pybag types."""
    return PybagString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


# ============================================================================
# Message Generators for rosbags
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


def generate_rosbags_point_cloud2(rng: random.Random, timestamp: int, num_points: int = 1000) -> Any:
    """Generate a PointCloud2 message using rosbags types."""
    point_step = 16
    row_step = point_step * num_points
    data = np.zeros(num_points * 4, dtype=np.float32)
    for i in range(num_points):
        data[i * 4] = rng.random() * 10.0 - 5.0
        data[i * 4 + 1] = rng.random() * 10.0 - 5.0
        data[i * 4 + 2] = rng.random() * 3.0
        data[i * 4 + 3] = rng.random() * 100.0

    return RosbagsPointCloud2(
        header=RosbagsHeader(
            stamp=RosbagsTime(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="lidar_link",
        ),
        height=1,
        width=num_points,
        fields=[
            RosbagsPointField(name="x", offset=0, datatype=7, count=1),
            RosbagsPointField(name="y", offset=4, datatype=7, count=1),
            RosbagsPointField(name="z", offset=8, datatype=7, count=1),
            RosbagsPointField(name="intensity", offset=12, datatype=7, count=1),
        ],
        is_bigendian=False,
        point_step=point_step,
        row_step=row_step,
        data=data.view(np.uint8),
        is_dense=True,
    )


def generate_rosbags_string(rng: random.Random, timestamp: int) -> Any:
    """Generate a String message using rosbags types."""
    return RosbagsString(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


# ============================================================================
# Serialization Benchmarks - Simple Message (Odometry)
# ============================================================================


class TestSerializeOdometry:
    """Benchmark serialization of Odometry messages."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagOdometry]:
        rng = random.Random(42)
        return [generate_pybag_odometry(rng, i * 1_000_000) for i in range(1000)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_odometry(rng, i * 1_000_000) for i in range(1000)]

    def test_pybag_serialize_odometry(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagOdometry]
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up serializer cache
        serializer.serialize_message(pybag_messages[0])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_odometry(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def serialize_all() -> None:
            for msg in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, RosbagsOdometry.__msgtype__)

        benchmark(serialize_all)


# ============================================================================
# Serialization Benchmarks - IMU Message (Moderate complexity)
# ============================================================================


class TestSerializeImu:
    """Benchmark serialization of IMU messages."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagImu]:
        rng = random.Random(42)
        return [generate_pybag_imu(rng, i * 1_000_000) for i in range(1000)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_imu(rng, i * 1_000_000) for i in range(1000)]

    def test_pybag_serialize_imu(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagImu]
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up
        serializer.serialize_message(pybag_messages[0])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_imu(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def serialize_all() -> None:
            for msg in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, RosbagsImu.__msgtype__)

        benchmark(serialize_all)


# ============================================================================
# Serialization Benchmarks - LaserScan (Variable-length arrays)
# ============================================================================


class TestSerializeLaserScan:
    """Benchmark serialization of LaserScan messages (variable-length arrays)."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagLaserScan]:
        rng = random.Random(42)
        return [generate_pybag_laser_scan(rng, i * 1_000_000, num_ranges=360) for i in range(100)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_laser_scan(rng, i * 1_000_000, num_ranges=360) for i in range(100)]

    def test_pybag_serialize_laser_scan(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagLaserScan]
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up
        serializer.serialize_message(pybag_messages[0])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_laser_scan(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def serialize_all() -> None:
            for msg in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, RosbagsLaserScan.__msgtype__)

        benchmark(serialize_all)


# ============================================================================
# Serialization Benchmarks - PointCloud2 (Large binary data)
# ============================================================================


class TestSerializePointCloud2:
    """Benchmark serialization of PointCloud2 messages (large binary data)."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagPointCloud2]:
        rng = random.Random(42)
        return [
            generate_pybag_point_cloud2(rng, i * 1_000_000, num_points=10000) for i in range(10)
        ]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [
            generate_rosbags_point_cloud2(rng, i * 1_000_000, num_points=10000) for i in range(10)
        ]

    def test_pybag_serialize_point_cloud2(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagPointCloud2]
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up
        serializer.serialize_message(pybag_messages[0])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_point_cloud2(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def serialize_all() -> None:
            for msg in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, RosbagsPointCloud2.__msgtype__)

        benchmark(serialize_all)


# ============================================================================
# Serialization Benchmarks - String (Simple message)
# ============================================================================


class TestSerializeString:
    """Benchmark serialization of String messages (simple message type)."""

    @pytest.fixture
    def pybag_messages(self) -> list[PybagString]:
        rng = random.Random(42)
        return [generate_pybag_string(rng, i * 1_000_000) for i in range(1000)]

    @pytest.fixture
    def rosbags_messages(self) -> list[Any]:
        rng = random.Random(42)
        return [generate_rosbags_string(rng, i * 1_000_000) for i in range(1000)]

    def test_pybag_serialize_string(
        self, benchmark: BenchmarkFixture, pybag_messages: list[PybagString]
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up
        serializer.serialize_message(pybag_messages[0])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_string(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[Any]
    ) -> None:
        def serialize_all() -> None:
            for msg in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, RosbagsString.__msgtype__)

        benchmark(serialize_all)


# ============================================================================
# Batch Serialization - Mixed Message Types
# ============================================================================


class TestSerializeMixed:
    """Benchmark serialization of mixed message types."""

    @pytest.fixture
    def pybag_messages(self) -> list:
        rng = random.Random(42)
        messages = []
        for i in range(200):
            messages.extend(
                [
                    generate_pybag_odometry(rng, i * 1_000_000),
                    generate_pybag_imu(rng, i * 1_000_000),
                    generate_pybag_string(rng, i * 1_000_000),
                ]
            )
        return messages

    @pytest.fixture
    def rosbags_messages(self) -> list[tuple[Any, str]]:
        rng = random.Random(42)
        messages = []
        for i in range(200):
            messages.extend(
                [
                    (generate_rosbags_odometry(rng, i * 1_000_000), RosbagsOdometry.__msgtype__),
                    (generate_rosbags_imu(rng, i * 1_000_000), RosbagsImu.__msgtype__),
                    (generate_rosbags_string(rng, i * 1_000_000), RosbagsString.__msgtype__),
                ]
            )
        return messages

    def test_pybag_serialize_mixed(
        self, benchmark: BenchmarkFixture, pybag_messages: list
    ) -> None:
        serializer = MessageSerializerFactory.from_profile("ros2")
        assert serializer is not None

        # Warm up all message types
        serializer.serialize_message(pybag_messages[0])
        serializer.serialize_message(pybag_messages[1])
        serializer.serialize_message(pybag_messages[2])

        def serialize_all() -> None:
            for msg in pybag_messages:
                serializer.serialize_message(msg)

        benchmark(serialize_all)

    def test_rosbags_serialize_mixed(
        self, benchmark: BenchmarkFixture, rosbags_messages: list[tuple[Any, str]]
    ) -> None:
        def serialize_all() -> None:
            for msg, msgtype in rosbags_messages:
                TYPESTORE.serialize_cdr(msg, msgtype)

        benchmark(serialize_all)
