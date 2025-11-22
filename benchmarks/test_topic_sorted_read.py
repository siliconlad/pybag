"""Benchmark comparing read performance of topic-sorted vs unsorted MCAP files."""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator
import random

from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
import numpy as np

from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.cli.mcap_sort_by_topic import sort_by_topic

from .benchmark_utils import TYPESTORE, Odometry, Header, Time, PoseWithCovariance, Pose, Point, Quaternion, TwistWithCovariance, Twist, Vector3


def create_multi_topic_mcap(path: Path, topics: int = 5, messages_per_topic: int = 200, seed: int = 0) -> Path:
    """Create an MCAP file with multiple topics, messages interleaved.

    Args:
        path: Directory to create the MCAP in.
        topics: Number of topics to create.
        messages_per_topic: Number of messages per topic.
        seed: Random seed for reproducibility.

    Returns:
        Path to the created MCAP file.
    """
    rng = random.Random(seed)

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        # Create connections for multiple topics
        connections = []
        for i in range(topics):
            conn = writer.add_connection(f"/odom_{i}", Odometry.__msgtype__, typestore=TYPESTORE)
            connections.append(conn)

        # Generate messages and interleave them
        all_messages = []
        for topic_idx in range(topics):
            for msg_idx in range(messages_per_topic):
                timestamp = int((topic_idx * messages_per_topic + msg_idx) * 1_000_000_000)
                odom = Odometry(
                    header=Header(
                        stamp=Time(
                            sec=timestamp // 1_000_000_000,
                            nanosec=timestamp % 1_000_000_000,
                        ),
                        frame_id=f"map_{topic_idx}"
                    ),
                    child_frame_id=f"base_link_{topic_idx}",
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
                all_messages.append((connections[topic_idx], timestamp, TYPESTORE.serialize_cdr(odom, Odometry.__msgtype__)))

        # Shuffle messages to simulate interleaved recording
        rng.shuffle(all_messages)

        # Write shuffled messages
        for conn, timestamp, data in all_messages:
            writer.write(conn, timestamp, data)

    return next(Path(path).rglob("*.mcap"))


def read_single_topic(mcap: Path, topic_pattern: str = "/odom_0") -> Iterator[bytes]:
    """Read messages from a single topic."""
    with McapRecordReaderFactory.from_file(mcap) as reader:
        # Get channels matching the topic
        channels = reader.get_channels()
        channel_ids = [cid for cid, ch in channels.items() if ch.topic == topic_pattern]

        for message in reader.get_messages(channel_id=channel_ids):
            yield message.data


def read_all_topics(mcap: Path) -> Iterator[bytes]:
    """Read all messages from all topics."""
    with McapRecordReaderFactory.from_file(mcap) as reader:
        for message in reader.get_messages():
            yield message.data


def test_read_single_topic_unsorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading a single topic from unsorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test", topics=5, messages_per_topic=200)
        benchmark(lambda: deque(read_single_topic(mcap, "/odom_0"), maxlen=0))


def test_read_single_topic_sorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading a single topic from topic-sorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test", topics=5, messages_per_topic=200)
        sorted_mcap = sort_by_topic(mcap, chunk_size=1024 * 256, chunk_compression="lz4", overwrite=True)
        benchmark(lambda: deque(read_single_topic(sorted_mcap, "/odom_0"), maxlen=0))


def test_read_all_topics_unsorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading all topics from unsorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test", topics=5, messages_per_topic=200)
        benchmark(lambda: deque(read_all_topics(mcap), maxlen=0))


def test_read_all_topics_sorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading all topics from topic-sorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test", topics=5, messages_per_topic=200)
        sorted_mcap = sort_by_topic(mcap, chunk_size=1024 * 256, chunk_compression="lz4", overwrite=True)
        benchmark(lambda: deque(read_all_topics(sorted_mcap), maxlen=0))
