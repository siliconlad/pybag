from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

from mcap.writer import Writer as McapWriter
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag1 import Writer as Rosbag1Writer
from rosbags.typesys import Stores, get_typestore

from .benchmark_utils import generate_ros1_odometries


def test_rosbags_write(benchmark: BenchmarkFixture) -> None:
    typestore = get_typestore(Stores.ROS1_NOETIC)
    msgtype = "nav_msgs/msg/Odometry"

    def _write_with_rosbags(path: Path, messages: Iterable) -> None:
        with Rosbag1Writer(path) as writer:
            connection = writer.add_connection("/odom", msgtype, typestore=typestore)
            for i, ros_msg in enumerate(messages):
                timestamp = int(i * 1_500_000_000)
                serialized = typestore.serialize_ros1(ros_msg, msgtype)
                writer.write(connection, timestamp, serialized)

    messages = generate_ros1_odometries()
    with TemporaryDirectory() as tmpdir:
        counter = count()
        benchmark(
            lambda: _write_with_rosbags(
                Path(tmpdir) / f"rosbags_{next(counter)}.bag",
                messages,
            )
        )


def test_official_write(benchmark: BenchmarkFixture) -> None:
    typestore = get_typestore(Stores.ROS1_NOETIC)
    msgtype = "nav_msgs/msg/Odometry"
    ros1_msgtype = msgtype.replace("/msg/", "/")
    msgdef, _ = typestore.generate_msgdef(msgtype)

    def _write_with_official(path: Path, messages: Iterable) -> None:
        with open(path, "wb") as stream:
            writer = McapWriter(stream)
            writer.start(profile="ros1", library="pybag-benchmarks")
            schema_id = writer.register_schema(
                name=ros1_msgtype,
                data=msgdef.encode(),
                encoding="ros1msg",
            )
            channel_id = writer.register_channel(
                topic="/odom",
                schema_id=schema_id,
                message_encoding="ros1",
            )
            for i, ros_msg in enumerate(messages):
                timestamp = int(i * 1_500_000_000)
                serialized = typestore.serialize_ros1(ros_msg, msgtype)
                writer.add_message(
                    channel_id=channel_id,
                    log_time=timestamp,
                    publish_time=timestamp,
                    data=serialized,
                )
            writer.finish()

    messages = generate_ros1_odometries()
    with TemporaryDirectory() as tmpdir:
        counter = count()
        benchmark(
            lambda: _write_with_official(
                Path(tmpdir) / f"official_{next(counter)}.mcap",
                messages,
            )
        )
