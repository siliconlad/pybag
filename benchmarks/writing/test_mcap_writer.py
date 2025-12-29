import random
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Iterable

import numpy as np
from mcap_ros2.writer import Writer as McapWriter
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag2 import StoragePlugin
from rosbags.rosbag2 import Writer as RosbagsWriter
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_writer import McapFileWriter


def test_pybag_write(benchmark: BenchmarkFixture) -> None:
    from pybag.ros2.humble.builtin_interfaces import Time
    from pybag.ros2.humble.geometry_msgs import (
        Point,
        Pose,
        PoseWithCovariance,
        Quaternion,
        Twist,
        TwistWithCovariance,
        Vector3
    )
    from pybag.ros2.humble.nav_msgs import Odometry
    from pybag.ros2.humble.std_msgs import Header

    def _generate_pybag_odometries(count: int = 1000, seed: int = 0) -> list[Odometry]:
        # Set random number generator
        rng = random.Random(seed)

        # Generate messages
        messages: list[Odometry] = []
        for i in range(count):
            timestamp = int(i * 1_500_000_000)
            msg = Odometry(
                header=Header(
                    stamp=Time(
                        sec=timestamp // 1_000_000_000,
                        nanosec=timestamp % 1_000_000_000,
                    ),
                    frame_id="map",
                ),
                child_frame_id="base_link",
                pose=PoseWithCovariance(
                    pose=Pose(
                        position=Point(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        orientation=Quaternion(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                            w=rng.random(),
                        ),
                    ),
                    covariance=[rng.random() for _ in range(36)],
                ),
                twist=TwistWithCovariance(
                    twist=Twist(
                        linear=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        angular=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                    ),
                    covariance=[rng.random() for _ in range(36)],
                ),
            )
            messages.append(msg)
        return messages

    def _write_with_pybag(path: Path, messages: Iterable) -> None:
        writer = McapFileWriter.open(path)
        writer.add_channel("/odom", schema=Odometry)
        for i, msg in enumerate(messages):
            timestamp = int(i * 1_500_000_000)
            writer.write_message("/odom", timestamp, msg)
        writer.close()

    messages = _generate_pybag_odometries()
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "pybag.mcap"
        benchmark(lambda: _write_with_pybag(path, messages))


def test_official_write(benchmark: BenchmarkFixture) -> None:
    schema_name = "nav_msgs/msg/Odometry"
    schema_text = dedent("""
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
    """)

    def _generate_official_odometries(count: int = 1000, seed: int = 0) -> list:
        # Set random number generator
        rng = random.Random(seed)
        # Generate messages
        messages: list[dict] = []
        for i in range(count):
            timestamp = int(i * 1_500_000_000)
            msg = {
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
            messages.append(msg)
        return messages

    def _write_with_official(path: Path, messages: Iterable) -> Path:
        with open(path, "wb") as f:
            writer = McapWriter(f)
            schema = writer.register_msgdef(schema_name, schema_text)
            for i, msg in enumerate(messages):
                timestamp = int(i * 1_500_000_000)
                writer.write_message(
                    topic="/odom",
                    schema=schema,
                    message=msg,
                    log_time=timestamp,
                    publish_time=timestamp,
                )
        return path

    messages = _generate_official_odometries()
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "official.mcap"
        benchmark(lambda: _write_with_official(path, messages))


def test_rosbags_write(benchmark: BenchmarkFixture) -> None:
    # Get releveant message types
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    Odometry = typestore.types["nav_msgs/msg/Odometry"]
    Header = typestore.types["std_msgs/msg/Header"]
    Time = typestore.types["builtin_interfaces/msg/Time"]
    PoseWithCovariance = typestore.types["geometry_msgs/msg/PoseWithCovariance"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    TwistWithCovariance = typestore.types["geometry_msgs/msg/TwistWithCovariance"]
    Twist = typestore.types["geometry_msgs/msg/Twist"]
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]

    def _generate_rosbags_odometries(count: int = 1000, seed: int = 0) -> list:
        # Set random number generator
        rng = random.Random(seed)

        # Generate messages
        messages: list = []
        for i in range(count):
            timestamp = int(i * 1_500_000_000)
            msg = Odometry(
                header=Header(
                    stamp=Time(
                        sec=timestamp // 1_000_000_000,
                        nanosec=timestamp % 1_000_000_000,
                    ),
                    frame_id="map",
                ),
                child_frame_id="base_link",
                pose=PoseWithCovariance(
                    pose=Pose(
                        position=Point(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        orientation=Quaternion(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                            w=rng.random(),
                        ),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
                ),
                twist=TwistWithCovariance(
                    twist=Twist(
                        linear=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        angular=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
                ),
            )
            messages.append(msg)
        return messages

    def _write_with_rosbags(path: Path, messages: Iterable) -> None:
        with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=typestore)
            for i, ros_msg in enumerate(messages):
                timestamp = int(i * 1_500_000_000)
                serialized_msg = typestore.serialize_cdr(ros_msg, Odometry.__msgtype__)
                writer.write(conn, timestamp, serialized_msg)

    messages = _generate_rosbags_odometries()
    with TemporaryDirectory() as tmpdir:
        counter = count()
        benchmark(lambda: _write_with_rosbags(Path(tmpdir) / str(next(counter)), messages))
