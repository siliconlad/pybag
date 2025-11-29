"""Benchmark comparing read performance of topic-sorted vs unsorted MCAP files."""
import random
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from pytest_benchmark.fixture import BenchmarkFixture

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.nav_msgs as nav_msgs
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.cli.mcap_sort import sort_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap_writer import McapFileWriter


def create_multi_topic_mcap(
    path: Path,
    topics: int = 5,
    messages_per_topic: int = 200,
    seed: int = 0,
    chunk_size: int = 1024 * 256,
) -> Path:
    """Create an MCAP file with multiple topics, messages interleaved.

    Args:
        path: Path to the output MCAP file.
        topics: Number of topics to create.
        messages_per_topic: Number of messages per topic.
        seed: Random seed for reproducibility.
        chunk_size: Chunk size in bytes for consistent behavior.

    Returns:
        Path to the created MCAP file.
    """
    rng = random.Random(seed)

    # Generate all messages first, then shuffle to simulate interleaved recording
    all_messages: list[tuple[str, int, nav_msgs.Odometry]] = []

    for topic_idx in range(topics):
        for msg_idx in range(messages_per_topic):
            timestamp = int((topic_idx * messages_per_topic + msg_idx) * 1_000_000_000)
            odom = nav_msgs.Odometry(
                header=std_msgs.Header(
                    stamp=builtin_interfaces.Time(
                        sec=timestamp // 1_000_000_000,
                        nanosec=timestamp % 1_000_000_000,
                    ),
                    frame_id=f"map_{topic_idx}",
                ),
                child_frame_id=f"base_link_{topic_idx}",
                pose=geometry_msgs.PoseWithCovariance(
                    pose=geometry_msgs.Pose(
                        position=geometry_msgs.Point(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        orientation=geometry_msgs.Quaternion(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                            w=rng.random(),
                        ),
                    ),
                    covariance=[rng.random() for _ in range(36)],
                ),
                twist=geometry_msgs.TwistWithCovariance(
                    twist=geometry_msgs.Twist(
                        linear=geometry_msgs.Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        angular=geometry_msgs.Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                    ),
                    covariance=[rng.random() for _ in range(36)],
                ),
            )
            all_messages.append((f"/odom_{topic_idx}", timestamp, odom))

    # Shuffle messages to simulate interleaved recording
    rng.shuffle(all_messages)

    # Write shuffled messages
    with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression="lz4") as writer:
        for topic, timestamp, odom in all_messages:
            writer.write_message(topic, timestamp, odom)

    return path


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
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test.mcap", topics=5, messages_per_topic=200)
        benchmark(lambda: deque(read_single_topic(mcap, "/odom_0"), maxlen=0))


def test_read_single_topic_sorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading a single topic from topic-sorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test.mcap", topics=5, messages_per_topic=200)
        sorted_mcap = sort_mcap(mcap, chunk_size=1024 * 256, chunk_compression="lz4", sort_by_topic=True)
        benchmark(lambda: deque(read_single_topic(sorted_mcap, "/odom_0"), maxlen=0))


def test_read_all_topics_unsorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading all topics from unsorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test.mcap", topics=5, messages_per_topic=200)
        benchmark(lambda: deque(read_all_topics(mcap), maxlen=0))


def test_read_all_topics_sorted(benchmark: BenchmarkFixture) -> None:
    """Benchmark reading all topics from topic-sorted MCAP."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_multi_topic_mcap(Path(tmpdir) / "test.mcap", topics=5, messages_per_topic=200)
        sorted_mcap = sort_mcap(mcap, chunk_size=1024 * 256, chunk_compression="lz4", sort_by_topic=True)
        benchmark(lambda: deque(read_all_topics(sorted_mcap), maxlen=0))
