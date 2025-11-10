"""Benchmark to demonstrate chunk cache benefits."""
import random
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader

# Setup
TYPESTORE = get_typestore(Stores.LATEST)
Odometry = TYPESTORE.types["nav_msgs/msg/Odometry"]
Header = TYPESTORE.types["std_msgs/msg/Header"]
Time = TYPESTORE.types["builtin_interfaces/msg/Time"]
PoseWithCovariance = TYPESTORE.types["geometry_msgs/msg/PoseWithCovariance"]
Pose = TYPESTORE.types["geometry_msgs/msg/Pose"]
Point = TYPESTORE.types["geometry_msgs/msg/Point"]
Quaternion = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
TwistWithCovariance = TYPESTORE.types["geometry_msgs/msg/TwistWithCovariance"]
Twist = TYPESTORE.types["geometry_msgs/msg/Twist"]
Vector3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]


def create_multi_channel_mcap(path: Path, message_count: int = 1000, channel_count: int = 5) -> Path:
    """Create an MCAP file with multiple channels sharing chunks."""
    rng = random.Random(0)

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        # Create multiple channels
        connections = []
        for i in range(channel_count):
            conn = writer.add_connection(f"/odom{i}", Odometry.__msgtype__, typestore=TYPESTORE)
            connections.append(conn)

        # Write messages interleaved across channels (so they share chunks)
        for i in range(message_count):
            timestamp = int(i * 1_500_000_000)
            odom = Odometry(
                header=Header(
                    stamp=Time(
                        sec=timestamp // 1_000_000_000,
                        nanosec=timestamp % 1_000_000_000,
                    ),
                    frame_id="map"
                ),
                child_frame_id="base_link",
                pose=PoseWithCovariance(
                    pose=Pose(
                        position=Point(x=rng.random(), y=rng.random(), z=rng.random()),
                        orientation=Quaternion(x=rng.random(), y=rng.random(), z=rng.random(), w=rng.random()),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)]),
                ),
                twist=TwistWithCovariance(
                    twist=Twist(
                        linear=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
                        angular=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)]),
                ),
            )
            # Write to a random channel to ensure interleaving
            conn = connections[i % channel_count]
            writer.write(conn, timestamp, TYPESTORE.serialize_cdr(odom, Odometry.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def test_pybag_read_multi_channel_with_cache(benchmark: BenchmarkFixture) -> None:
    """Test reading multiple channels (benefits from chunk cache)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_channel_mcap(Path(tmpdir) / "test", message_count=1000, channel_count=5)

        def read_all_channels():
            with McapFileReader.from_file(mcap, chunk_cache_size=8) as reader:
                for topic in reader.get_topics():
                    msg_count = 0
                    for _ in reader.messages(topic):
                        msg_count += 1

        benchmark(read_all_channels)


def test_pybag_read_multi_channel_no_cache(benchmark: BenchmarkFixture) -> None:
    """Test reading multiple channels WITHOUT chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_channel_mcap(Path(tmpdir) / "test", message_count=1000, channel_count=5)

        def read_all_channels():
            with McapFileReader.from_file(mcap, chunk_cache_size=0) as reader:
                for topic in reader.get_topics():
                    msg_count = 0
                    for _ in reader.messages(topic):
                        msg_count += 1

        benchmark(read_all_channels)
