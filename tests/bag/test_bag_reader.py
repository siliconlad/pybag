"""Tests for the ROS 1 bag file reader message ordering and filtering."""

import logging
import random
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import pytest
from rosbags.rosbag1 import Reader as RosbagsReader
from rosbags.rosbag1 import Writer as RosbagsWriter
from rosbags.typesys import Stores, get_typestore

import pybag
import pybag.types as t
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter


@dataclass(kw_only=True)
class SimpleMessage:
    """A simple test message."""

    __msg_name__ = "test_msgs/SimpleMessage"
    value: t.int32
    name: t.string


@pytest.fixture
def typestore():
    """Get the ROS1 Noetic typestore from rosbags."""
    return get_typestore(Stores.ROS1_NOETIC)

#################
#  Basic Tests  #
#################


@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_messages_filter(in_log_time_order: bool, in_reverse: bool):
    """Test that the filter parameter correctly filters messages."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "filter_test.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/test", 0, SimpleMessage(value=1, name="positive"))
            writer.write_message("/test", 1, SimpleMessage(value=-1, name="negative"))

        with BagFileReader.from_file(path) as reader:
            all_messages = list(reader.messages("/test", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            assert len(all_messages) == 2
            if in_reverse:
                assert all_messages[0].data.value == -1
                assert all_messages[1].data.value == 1
            else:
                assert all_messages[0].data.value == 1
                assert all_messages[1].data.value == -1

            positive = list(
                reader.messages(
                    "/test",
                    filter=lambda msg: msg.data.value > 0,
                    in_log_time_order=in_log_time_order,
                    in_reverse=in_reverse,
                )
            )
            assert len(positive) == 1
            assert positive[0].data.value == 1

            negative = list(
                reader.messages(
                    "/test",
                    filter=lambda msg: msg.data.value < 0,
                    in_log_time_order=in_log_time_order,
                    in_reverse=in_reverse,
                )
            )
            assert len(negative) == 1
            assert negative[0].data.value == -1


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_multiple_existing_topics(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages from multiple existing topics.

    Note: When in_log_time_order=False with a single chunk, messages are still
    returned in timestamp order within that chunk. The "write order" only affects
    the order of chunks, not messages within a chunk.
    """
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multiple_topics.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            # Write messages to /topic1 with timestamps 10, 20, 30
            writer.write_message("/topic1", 10, SimpleMessage(value=10, name="topic1_0"))
            writer.write_message("/topic1", 20, SimpleMessage(value=20, name="topic1_1"))
            writer.write_message("/topic1", 30, SimpleMessage(value=30, name="topic1_2"))

            # Write messages to /topic2 with timestamps 15, 25, 35
            writer.write_message("/topic2", 15, SimpleMessage(value=15, name="topic2_0"))
            writer.write_message("/topic2", 25, SimpleMessage(value=25, name="topic2_1"))
            writer.write_message("/topic2", 35, SimpleMessage(value=35, name="topic2_2"))

            # Write messages to /topic3 with timestamps 5, 45, 50
            writer.write_message("/topic3", 5, SimpleMessage(value=5, name="topic3_0"))
            writer.write_message("/topic3", 45, SimpleMessage(value=45, name="topic3_1"))
            writer.write_message("/topic3", 50, SimpleMessage(value=50, name="topic3_2"))

        with BagFileReader.from_file(path) as reader:
            messages = list(
                reader.messages(
                    ["/topic1", "/topic2"],
                    in_log_time_order=in_log_time_order,
                    in_reverse=in_reverse,
                )
            )
            logging.info(f"pybag: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            # Assert that 6 messages are returned (3 from each topic)
            assert len(messages) == 6, f"Expected 6 messages, got {len(messages)}"

            # With single chunk or in_log_time_order=True, messages are sorted by timestamp
            # With multiple chunks and in_log_time_order=False, chunks are in file order
            # but messages within each chunk are still sorted by timestamp
            if in_log_time_order:
                expected_log_times = [10, 15, 20, 25, 30, 35]
                expected_data = [
                    "topic1_0",
                    "topic2_0",
                    "topic1_1",
                    "topic2_1",
                    "topic1_2",
                    "topic2_2",
                ]
            else:
                # Multiple chunks, not in log time order - chunks processed in file order
                expected_log_times = [10, 20, 30, 15, 25, 35]
                expected_data = [
                    "topic1_0",
                    "topic1_1",
                    "topic1_2",
                    "topic2_0",
                    "topic2_1",
                    "topic2_2",
                ]

            if in_reverse:
                expected_log_times = expected_log_times[::-1]
                expected_data = expected_data[::-1]

            assert [msg.log_time for msg in messages] == expected_log_times, (
                f"Expected log times {expected_log_times}, "
                f"got {[msg.log_time for msg in messages]}"
            )
            assert [msg.data.name for msg in messages] == expected_data, (
                f"Expected data {expected_data}, "
                f"got {[msg.data.name for msg in messages]}"
            )


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_multiple_topics_with_nonexistent(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages from multiple topics where some don't exist."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "partial_topics.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            # Write 10 messages to /existing to ensure multiple chunks are created
            for i in range(10):
                writer.write_message(
                    "/existing", i * 10, SimpleMessage(value=i, name=f"msg_{i}")
                )

        with BagFileReader.from_file(path) as reader:
            logging.info(f"Number of chunks: {len(reader._chunk_infos)}")
            if chunk_size < 100:
                assert len(reader._chunk_infos) > 1, "Expected multiple chunks"

            # Call with both an existing and non-existent topic
            messages = list(
                reader.messages(
                    ["/existing", "/nonexistent"],
                    in_log_time_order=in_log_time_order,
                    in_reverse=in_reverse,
                )
            )
            logging.info(f"pybag: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            # Assert that only 10 messages are returned (from /existing only)
            assert len(messages) == 10, (
                f"Expected 10 messages from /existing, got {len(messages)}"
            )

            # Assert the log times and data are correct
            expected_log_times = list(range(0, 100, 10))
            expected_data = [f"msg_{i}" for i in range(10)]
            if in_reverse:
                expected_log_times = expected_log_times[::-1]
                expected_data = expected_data[::-1]
            assert [msg.log_time for msg in messages] == expected_log_times, (
                f"Expected log times {expected_log_times}, "
                f"got {[msg.log_time for msg in messages]}"
            )
            assert [msg.data.name for msg in messages] == expected_data, (
                f"Expected data {expected_data}, "
                f"got {[msg.data.name for msg in messages]}"
            )


@pytest.mark.parametrize("chunk_size", [pytest.param(1024 * 1024, id="single_chunk"), pytest.param(50, id="multiple_chunks")])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_glob_pattern_matching(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages using glob patterns to match multiple topics.

    This test verifies that glob patterns like '/sensor/*' can be used to match
    multiple topics that share a common prefix. The test creates topics under
    two different namespaces (/sensor and /control) and validates:
    1. Glob patterns correctly match all topics under a namespace
    2. Messages are returned in the correct order (log_time or write order)
    3. Glob patterns with no matches return an empty list without errors

    Note: When in_log_time_order=False with a single chunk, messages are still
    returned in timestamp order within that chunk. The "write order" only affects
    the order of chunks, not messages within a chunk.
    """
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "glob_pattern.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            # /sensor/camera
            writer.write_message("/sensor/camera", 10, SimpleMessage(value=10, name="camera_0"))
            writer.write_message("/sensor/camera", 20, SimpleMessage(value=20, name="camera_1"))
            # /sensor/lidar
            writer.write_message("/sensor/lidar", 15, SimpleMessage(value=15, name="lidar_0"))
            writer.write_message("/sensor/lidar", 25, SimpleMessage(value=25, name="lidar_1"))
            # /sensor/imu
            writer.write_message("/sensor/imu", 12, SimpleMessage(value=12, name="imu_0"))
            writer.write_message("/sensor/imu", 22, SimpleMessage(value=22, name="imu_1"))
            # /control
            writer.write_message("/control/speed", 8, SimpleMessage(value=8, name="speed_0"))
            writer.write_message("/control/steering", 30, SimpleMessage(value=30, name="steering_0"))
            # Write second speed message AFTER steering (out of timestamp order)
            writer.write_message("/control/speed", 18, SimpleMessage(value=18, name="speed_1"))

        with BagFileReader.from_file(path) as reader:
            # Test case 1: Glob pattern matching /sensor/* topics
            messages = list(reader.messages("/sensor/*", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag /sensor/*: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag /sensor/*: {[msg.data.name for msg in messages]}")
            assert len(messages) == 6, f"Expected 6 messages from /sensor/*, got {len(messages)}"

            # With single chunk or in_log_time_order=True, messages are sorted by timestamp
            if in_log_time_order:
                expected_log_times = [10, 12, 15, 20, 22, 25]
                expected_data = [
                    "camera_0",
                    "imu_0",
                    "lidar_0",
                    "camera_1",
                    "imu_1",
                    "lidar_1",
                ]
            else:
                # Multiple chunks, not in log time order
                expected_log_times = [10, 20, 15, 25, 12, 22]
                expected_data = [
                    "camera_0",
                    "camera_1",
                    "lidar_0",
                    "lidar_1",
                    "imu_0",
                    "imu_1",
                ]

            if in_reverse:
                expected_log_times = expected_log_times[::-1]
                expected_data = expected_data[::-1]

            assert [msg.log_time for msg in messages] == expected_log_times, (
                f"Expected log times {expected_log_times}, "
                f"got {[msg.log_time for msg in messages]}"
            )
            assert [msg.data.name for msg in messages] == expected_data, (
                f"Expected data {expected_data}, "
                f"got {[msg.data.name for msg in messages]}"
            )

            # Test case 2: Glob pattern matching /control/* topics
            messages = list(reader.messages("/control/*", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag /control/*: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag /control/*: {[msg.data.name for msg in messages]}")
            assert len(messages) == 3, f"Expected 3 messages from /control/*, got {len(messages)}"

            if in_log_time_order:
                expected_log_times = [8, 18, 30]
                expected_data = ["speed_0", "speed_1", "steering_0"]
            else:
                # Multiple chunks, not in log time order
                expected_log_times = [8, 30, 18]
                expected_data = ["speed_0", "steering_0", "speed_1"]

            if in_reverse:
                expected_log_times = expected_log_times[::-1]
                expected_data = expected_data[::-1]

            assert [msg.log_time for msg in messages] == expected_log_times, (
                f"Expected log times {expected_log_times}, "
                f"got {[msg.log_time for msg in messages]}"
            )
            assert [msg.data.name for msg in messages] == expected_data, (
                f"Expected data {expected_data}, "
                f"got {[msg.data.name for msg in messages]}"
            )

            # Test case 3: Glob pattern with no matches
            messages = list(reader.messages("/nonexistent/*", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag /nonexistent/*: number of messages: {len(messages)}")
            assert len(messages) == 0, f"Expected 0 messages from /nonexistent/*, got {len(messages)}"


#####################
#  Ordering Tests   #
#####################


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_ordered_messages(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages that are written in timestamp order."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "ordered.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for i in range(8):
                writer.write_message("/ordered", i, SimpleMessage(value=i, name=f"msg_{i}"))

        with BagFileReader.from_file(path) as reader:
            messages = list(reader.messages("/ordered", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag: {[message.log_time for message in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            expected_log_times = list(range(8))
            expected_log_times = expected_log_times[::-1] if in_reverse else expected_log_times
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.name for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_reverse_ordered_messages(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages that are written in reverse timestamp order.

    Note: When in_log_time_order=False with a single chunk, messages are still
    returned in timestamp order within that chunk. The "write order" only affects
    the order of chunks, not messages within a chunk.
    """
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "reverse.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for i in range(7, -1, -1):
                writer.write_message("/unordered", i, SimpleMessage(value=i, name=f"msg_{i}"))

        with BagFileReader.from_file(path) as reader:
            messages = list(reader.messages("/unordered", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag: {[message.log_time for message in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            expected_log_times = list(range(8)) if in_log_time_order else list(reversed(range(8)))
            expected_log_times = expected_log_times[::-1] if in_reverse else expected_log_times
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.name for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_random_ordered_messages(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test reading messages written with shuffled timestamps.

    Note: When in_log_time_order=False with a single chunk, messages are still
    returned in timestamp order within that chunk. The "write order" only affects
    the order of chunks, not messages within a chunk.
    """
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "random.bag"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f"Shuffled timestamps: {shuffled_timestamps}")

        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for time in shuffled_timestamps:
                writer.write_message(
                    "/overlapping", time, SimpleMessage(value=time, name=f"msg_{time}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(reader.messages("/overlapping", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            expected_log_times = sorted_timestamps if in_log_time_order else shuffled_timestamps
            expected_log_times = expected_log_times[::-1] if in_reverse else expected_log_times
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.name for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_duplicate_timestamps(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test that multiple messages with the same log time are all returned.

    Note: When messages have identical timestamps, their relative order within
    the same chunk is preserved, but across chunks with overlapping time ranges,
    the heap-based merge may not guarantee perfect reversal.
    """
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "duplicate_timestamps.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            timestamp = 1000
            writer.write_message("/test", timestamp, SimpleMessage(value=0, name="msg_0"))
            writer.write_message("/test", timestamp, SimpleMessage(value=1, name="msg_1"))
            writer.write_message("/test", timestamp, SimpleMessage(value=2, name="msg_2"))

        with BagFileReader.from_file(path) as reader:
            logging.info(f"Number of chunks: {len(reader._chunk_infos)}")

            messages = list(reader.messages("/test", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            logging.info(f"pybag: {[msg.log_time for msg in messages]}")
            logging.info(f"pybag: {[msg.data.name for msg in messages]}")

            # All messages should have the same timestamp
            assert [msg.log_time for msg in messages] == [timestamp] * 3
            expected_data = ["msg_2", "msg_1", "msg_0"] if in_reverse else ["msg_0", "msg_1", "msg_2"]
            assert [msg.data.name for msg in messages] == expected_data


########################
#  Time Filter Tests   #
########################


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_time_filter_start_time(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test filtering messages by start_time."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "time_filter.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for i in range(10):
                writer.write_message(
                    "/test", i * 10, SimpleMessage(value=i, name=f"msg_{i}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(
                reader.messages(
                    "/test", start_time=50, in_log_time_order=in_log_time_order, in_reverse=in_reverse
                )
            )
            expected_times = [90, 80, 70, 60, 50] if in_reverse else [50, 60, 70, 80, 90]
            assert [msg.log_time for msg in messages] == expected_times


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(10, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_time_filter_end_time(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test filtering messages by end_time."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "time_filter.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for i in range(10):
                writer.write_message(
                    "/test", i * 10, SimpleMessage(value=i, name=f"msg_{i}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(
                reader.messages(
                    "/test", end_time=40, in_log_time_order=in_log_time_order, in_reverse=in_reverse,
                )
            )
            expected_times = [40, 30, 20, 10, 0] if in_reverse else [0, 10, 20, 30, 40]
            assert [msg.log_time for msg in messages] == expected_times


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(50, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_time_filter_range(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test filtering messages by both start_time and end_time."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "time_filter.bag"
        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for i in range(10):
                writer.write_message(
                    "/test", i * 10, SimpleMessage(value=i, name=f"msg_{i}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(
                reader.messages(
                    "/test",
                    start_time=20,
                    end_time=60,
                    in_log_time_order=in_log_time_order,
                    in_reverse=in_reverse,
                )
            )
            expected_times = [60, 50, 40, 30, 20] if in_reverse else [20, 30, 40, 50, 60]
            assert [msg.log_time for msg in messages] == expected_times


@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(50, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_time_filter_with_random_order(chunk_size: int, in_log_time_order: bool, in_reverse: bool):
    """Test time filtering with messages written out of order."""
    random.seed(123)

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "time_filter_random.bag"

        sorted_timestamps = list(range(0, 100, 10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))

        with BagFileWriter.open(path, chunk_size=chunk_size) as writer:
            for time in shuffled_timestamps:
                writer.write_message(
                    "/test", time, SimpleMessage(value=time, name=f"msg_{time}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(
                reader.messages(
                    "/test", start_time=30, end_time=70, in_log_time_order=in_log_time_order, in_reverse=in_reverse,
                )
            )
            expected_times = [30, 40, 50, 60, 70] if in_log_time_order else [i for i in shuffled_timestamps if 30 <= i <= 70]
            expected_times = expected_times[::-1] if in_reverse else expected_times
            assert [msg.log_time for msg in messages] == expected_times


########################
#  Compression Tests   #
########################


@pytest.mark.parametrize("compression", ["none", "bz2"])
@pytest.mark.parametrize("chunk_size", [
    pytest.param(1024 * 1024, id="single_chunk"),
    pytest.param(50, id="multiple_chunks")
])
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("in_reverse", [True, False])
def test_compression_message_ordering(
    compression: Literal["none", "bz2"], chunk_size: int, in_log_time_order: bool, in_reverse: bool,
):
    """Test that message ordering is correct with different compression algorithms."""
    random.seed(456)

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / f"compressed_{compression}.bag"

        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))

        with BagFileWriter.open(
            path, compression=compression, chunk_size=chunk_size
        ) as writer:
            for time in shuffled_timestamps:
                writer.write_message(
                    "/test", time, SimpleMessage(value=time, name=f"msg_{time}")
                )

        with BagFileReader.from_file(path) as reader:
            messages = list(reader.messages("/test", in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            expected_times = sorted_timestamps if in_log_time_order else shuffled_timestamps
            expected_times = expected_times[::-1] if in_reverse else expected_times
            assert [msg.log_time for msg in messages] == expected_times


########################
#  Edge Cases          #
########################


def test_empty_topic_list():
    """Test reading with an empty topic list returns no messages."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "empty_topic.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/test", 0, SimpleMessage(value=0, name="msg_0"))

        with BagFileReader.from_file(path) as reader:
            messages = list(reader.messages([]))
            assert len(messages) == 0


###############################
# Test rosbags compatibility  #
###############################


def test_rosbags_write_pybag_read_string(typestore):
    """Test that pybag can read a String message written by rosbags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags (timestamps in seconds)
        String = typestore.types["std_msgs/msg/String"]
        with RosbagsWriter(bag_path) as writer:
            conn = writer.add_connection("/test", String.__msgtype__, typestore=typestore)
            writer.write(conn, 1, typestore.serialize_ros1(String(data="hello"), String.__msgtype__))
            writer.write(conn, 2, typestore.serialize_ros1(String(data="world"), String.__msgtype__))

        # Read with pybag
        with BagFileReader.from_file(bag_path) as reader:
            # Check start/end time
            reader.start_time == 1
            reader.end_time == 2

            # Check messages
            messages = list(reader.messages("/test"))
            assert len(messages) == 2
            assert messages[0].topic == "/test"
            assert messages[0].data.data == "hello"
            assert messages[1].topic == "/test"
            assert messages[1].data.data == "world"


def test_rosbags_write_pybag_read_int32(typestore):
    """Test that pybag can read an Int32 message written by rosbags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags
        Int32 = typestore.types["std_msgs/msg/Int32"]
        with RosbagsWriter(bag_path) as writer:
            conn = writer.add_connection("/numbers", Int32.__msgtype__, typestore=typestore)
            writer.write(conn, 1, typestore.serialize_ros1(Int32(data=42), Int32.__msgtype__))
            writer.write(conn, 2, typestore.serialize_ros1(Int32(data=-100), Int32.__msgtype__))

        # Read with pybag
        with BagFileReader.from_file(bag_path) as reader:
            messages = list(reader.messages("/numbers"))

            assert len(messages) == 2
            assert messages[0].data.data == 42
            assert messages[1].data.data == -100


def test_rosbags_write_pybag_read_float64(typestore):
    """Test that pybag can read a Float64 message written by rosbags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags
        Float64 = typestore.types["std_msgs/msg/Float64"]
        with RosbagsWriter(bag_path) as writer:
            conn = writer.add_connection("/floats", Float64.__msgtype__, typestore=typestore)
            writer.write(conn, 1, typestore.serialize_ros1(Float64(data=3.14159), Float64.__msgtype__))
            writer.write(conn, 2, typestore.serialize_ros1(Float64(data=-2.71828), Float64.__msgtype__))

        # Read with pybag
        with BagFileReader.from_file(bag_path) as reader:
            messages = list(reader.messages("/floats"))

            assert len(messages) == 2
            assert abs(messages[0].data.data - 3.14159) < 1e-10
            assert abs(messages[1].data.data - (-2.71828)) < 1e-10


def test_rosbags_write_pybag_read_multiple_topics(typestore):
    """Test that pybag can read multiple topics written by rosbags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags
        String = typestore.types["std_msgs/msg/String"]
        Int32 = typestore.types["std_msgs/msg/Int32"]
        with RosbagsWriter(bag_path) as writer:
            str_conn = writer.add_connection("/strings", String.__msgtype__, typestore=typestore)
            int_conn = writer.add_connection("/numbers", Int32.__msgtype__, typestore=typestore)

            writer.write(str_conn, 1, typestore.serialize_ros1(String(data="hello"), String.__msgtype__))
            writer.write(int_conn, 2, typestore.serialize_ros1(Int32(data=42), Int32.__msgtype__))
            writer.write(str_conn, 3, typestore.serialize_ros1(String(data="world"), String.__msgtype__))

        # Read with pybag
        with BagFileReader.from_file(bag_path) as reader:
            # Check topics
            topics = reader.get_topics()
            assert "/strings" in topics
            assert "/numbers" in topics

            # Read string messages
            str_messages = list(reader.messages("/strings"))
            assert len(str_messages) == 2
            assert str_messages[0].data.data == "hello"
            assert str_messages[1].data.data == "world"

            # Read int messages
            int_messages = list(reader.messages("/numbers"))
            assert len(int_messages) == 1
            assert int_messages[0].data.data == 42


def test_rosbags_write_pybag_read_message_order(typestore):
    """Test that pybag reads messages in correct timestamp order from rosbags bags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags in non-sequential order
        String = typestore.types["std_msgs/msg/String"]
        with RosbagsWriter(bag_path) as writer:
            conn = writer.add_connection("/test", String.__msgtype__, typestore=typestore)
            # Write out of order
            writer.write(conn, 3, typestore.serialize_ros1(String(data="third"), String.__msgtype__))
            writer.write(conn, 1, typestore.serialize_ros1(String(data="first"), String.__msgtype__))
            writer.write(conn, 2, typestore.serialize_ros1(String(data="second"), String.__msgtype__))

        # Read with pybag in log time order
        with BagFileReader.from_file(bag_path) as reader:
            messages = list(reader.messages("/test", in_log_time_order=True))
            assert len(messages) == 3
            assert messages[0].data.data == "first"
            assert messages[1].data.data == "second"
            assert messages[2].data.data == "third"

        with BagFileReader.from_file(bag_path) as reader:
            messages = list(reader.messages("/test", in_log_time_order=False))
            assert len(messages) == 3
            assert messages[0].data.data == "third"
            assert messages[1].data.data == "first"
            assert messages[2].data.data == "second"


def test_rosbags_write_pybag_read_glob_pattern(typestore):
    """Test that pybag glob patterns work on rosbags-written bags."""
    with TemporaryDirectory() as temp_dir:
        bag_path = Path(temp_dir) / "test.bag"

        # Write with rosbags
        String = typestore.types["std_msgs/msg/String"]
        with RosbagsWriter(bag_path) as writer:
            cam_conn = writer.add_connection("/sensor/camera", String.__msgtype__, typestore=typestore)
            lidar_conn = writer.add_connection("/sensor/lidar", String.__msgtype__, typestore=typestore)
            ctrl_conn = writer.add_connection("/control/speed", String.__msgtype__, typestore=typestore)

            writer.write(cam_conn, 1, typestore.serialize_ros1(String(data="camera"), String.__msgtype__))
            writer.write(lidar_conn, 2, typestore.serialize_ros1(String(data="lidar"), String.__msgtype__))
            writer.write(ctrl_conn, 3, typestore.serialize_ros1(String(data="speed"), String.__msgtype__))

        # Read with pybag using glob
        with BagFileReader.from_file(bag_path) as reader:
            sensor_messages = list(reader.messages("/sensor/*"))
            assert len(sensor_messages) == 2
            assert set(m.data.data for m in sensor_messages) == {"camera", "lidar"}

            control_messages = list(reader.messages("/control/*"))
            assert len(control_messages) == 1
            assert control_messages[0].data.data == "speed"
