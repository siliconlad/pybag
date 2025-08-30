import random
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

import numpy as np
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def create_test_mcap(path: Path, message_count: int = 1000, seed: int = 0) -> Path:
    """Create an MCAP file with a single `/odom` topic of Odometry messages."""
    rng = random.Random(seed)

    typestore = get_typestore(Stores.LATEST)
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

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=typestore)
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
                        position=Point(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random()
                        ),
                        orientation=Quaternion(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                            w=rng.random(),
                        ),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)]),
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
                    covariance=np.array([rng.random() for _ in range(36)]),
                ),
            )
            writer.write(odom_conn, timestamp, typestore.serialize_cdr(odom, Odometry.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def read_with_pybag(mcap: Path) -> Iterator[Any]:
    reader = McapFileReader.from_file(mcap)
    for topic in reader.get_topics():
        for message in reader.messages(topic):
            yield message.data


def read_with_rosbags(mcap: Path) -> Iterator[Any]:
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([mcap.parent]) as reader:
        for conn, _, data in reader.messages():
            yield typestore.deserialize_cdr(data, conn.msgtype)


def read_with_official(mcap: Path) -> Iterator[Any]:
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, ros_msg in reader.iter_decoded_messages(log_time_order=False):
            yield ros_msg


def test_official_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_official(mcap), maxlen=0))


def test_rosbags_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_rosbags(mcap), maxlen=0))


def test_pybag_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_pybag(mcap), maxlen=0))
