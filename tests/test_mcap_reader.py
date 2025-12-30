"""Tests for the MCAP reader."""
import logging
import random
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import pytest
from mcap.reader import make_reader
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory
from mcap_ros2.writer import Writer as Ros2McapWriter
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

import pybag.ros1.noetic.std_msgs as ros1_std_msgs
import pybag.ros2.humble.std_msgs as ros2_std_msgs
import pybag.types as t
from pybag.mcap_reader import McapFileReader, McapMultipleFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.schema.ros2msg import Ros2MsgError


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob("*.mcap"))


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request):
    return get_typestore(request.param)

#################
#  Pybag tests  #
#################

@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
def test_messages_filter(typestore: Typestore, in_log_time_order: bool, enable_crc_check: bool):
    # Write a temporary mcap file
    Int32 = typestore.types["std_msgs/msg/Int32"]
    with TemporaryDirectory() as temp_dir:
        with Writer(
            Path(temp_dir) / "rosbags",
            version=9,
            storage_plugin=StoragePlugin.MCAP,
        ) as writer:
            conn = writer.add_connection("/rosbags", Int32.__msgtype__, typestore=typestore)
            writer.write(conn, 0, typestore.serialize_cdr(Int32(data=1), Int32.__msgtype__))
            writer.write(conn, 1, typestore.serialize_cdr(Int32(data=-1), Int32.__msgtype__))

        mcap_file = _find_mcap_file(temp_dir)
        with McapFileReader.from_file(mcap_file, enable_crc_check=enable_crc_check) as reader:
            all_messages = list(reader.messages("/rosbags", in_log_time_order=in_log_time_order))
            assert len(all_messages) == 2
            assert all_messages[0].data.data == 1
            assert all_messages[1].data.data == -1

            positive = list(reader.messages(
                "/rosbags",
                filter=lambda msg: msg.data.data > 0,
                in_log_time_order=in_log_time_order
            ))
            assert len(positive) == 1
            assert positive[0].data.data == 1

            negative = list(reader.messages(
                "/rosbags",
                filter=lambda msg: msg.data.data < 0,
                in_log_time_order=in_log_time_order
            ))
            assert len(negative) == 1
            assert negative[0].data.data == -1


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_multiple_existing_topics(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reading messages from multiple existing topics."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multiple_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # Write messages to /topic1 with timestamps 10, 20, 30
            writer.write_message("/topic1", 10, String(data="topic1_0"))
            writer.write_message("/topic1", 20, String(data="topic1_1"))
            writer.write_message("/topic1", 30, String(data="topic1_2"))

            # Write messages to /topic2 with timestamps 15, 25, 35
            writer.write_message("/topic2", 15, String(data="topic2_0"))
            writer.write_message("/topic2", 25, String(data="topic2_1"))
            writer.write_message("/topic2", 35, String(data="topic2_2"))

            # Write messages to /topic3 with timestamps 5, 45, 50
            writer.write_message("/topic3", 5, String(data="topic3_0"))
            writer.write_message("/topic3", 45, String(data="topic3_1"))
            writer.write_message("/topic3", 50, String(data="topic3_2"))

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages(["/topic1", "/topic2"], in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            # Assert that 6 messages are returned (3 from each topic)
            assert len(messages) == 6, f"Expected 6 messages, got {len(messages)}"

            if in_log_time_order:
                expected_log_times = [10, 15, 20, 25, 30, 35]
                expected_data = ["topic1_0", "topic2_0", "topic1_1", "topic2_1", "topic1_2", "topic2_2"]
            else:
                expected_log_times = [10, 20, 30, 15, 25, 35]
                expected_data = ["topic1_0", "topic1_1", "topic1_2", "topic2_0", "topic2_1", "topic2_2"]

            assert [msg.log_time for msg in messages] == expected_log_times, \
                f"Expected log times {expected_log_times}, got {[msg.log_time for msg in messages]}"
            assert [msg.data.data for msg in messages] == expected_data, \
                f"Expected data {expected_data}, got {[msg.data.data for msg in messages]}"

@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_multiple_topics_with_nonexistent(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reading messages from multiple topics where some don't exist."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "partial_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # Write 10 messages to /existing to ensure multiple chunks are created
            for i in range(10):
                writer.write_message("/existing", i * 10, String(data=f"msg_{i}"))

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            # Call with both an existing and non-existent topic
            messages = list(reader.messages(["/existing", "/nonexistent"], in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            # Assert that only 10 messages are returned (from /existing only)
            assert len(messages) == 10, f"Expected 10 messages from /existing, got {len(messages)}"

            # Assert the log times and data are correct
            expected_log_times = list(range(0, 100, 10))
            expected_data = [f"msg_{i}" for i in range(10)]
            assert [msg.log_time for msg in messages] == expected_log_times, \
                f"Expected log times {expected_log_times}, got {[msg.log_time for msg in messages]}"
            assert [msg.data.data for msg in messages] == expected_data, \
                f"Expected data {expected_data}, got {[msg.data.data for msg in messages]}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_multiple_nonexistent_topics(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reading messages from multiple topics that don't exist."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "no_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # Write enough messages to /real_topic to create multiple chunks
            for i in range(10):
                writer.write_message("/real_topic", i * 10, String(data=f"msg_{i}"))

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            # Call with non-existent topics
            messages = list(reader.messages(["/fake1", "/fake2"], in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: number of messages: {len(messages)}')

            # Assert that 0 messages are returned (empty list)
            assert len(messages) == 0, f"Expected 0 messages from non-existent topics, got {len(messages)}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_glob_pattern_matching(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reading messages using glob patterns to match multiple topics.

    This test verifies that glob patterns like '/sensor/*' can be used to match
    multiple topics that share a common prefix. The test creates topics under
    two different namespaces (/sensor and /control) and validates:
    1. Glob patterns correctly match all topics under a namespace
    2. Messages are returned in the correct order (log_time or write order)
    3. Glob patterns with no matches return an empty list without errors
    """
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "glob_pattern.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # /sensor/camera
            writer.write_message("/sensor/camera", 10, String(data="camera_0"))
            writer.write_message("/sensor/camera", 20, String(data="camera_1"))
            # /sensor/lidar
            writer.write_message("/sensor/lidar", 15, String(data="lidar_0"))
            writer.write_message("/sensor/lidar", 25, String(data="lidar_1"))
            # /sensor/imu
            writer.write_message("/sensor/imu", 12, String(data="imu_0"))
            writer.write_message("/sensor/imu", 22, String(data="imu_1"))
            # /control
            writer.write_message("/control/speed", 8, String(data="speed_0"))
            writer.write_message("/control/steering", 30, String(data="steering_0"))
            # Write second speed message AFTER steering (out of timestamp order)
            writer.write_message("/control/speed", 18, String(data="speed_1"))

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            # Test case 1: Glob pattern matching /sensor/* topics
            messages = list(reader.messages("/sensor/*", in_log_time_order=in_log_time_order))
            logging.info(f'pybag /sensor/*: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag /sensor/*: {[msg.data.data for msg in messages]}')
            assert len(messages) == 6, f"Expected 6 messages from /sensor/*, got {len(messages)}"

            if in_log_time_order:
                expected_log_times = [10, 12, 15, 20, 22, 25]
                expected_data = ["camera_0", "imu_0", "lidar_0", "camera_1", "imu_1", "lidar_1"]
            else:
                expected_log_times = [10, 20, 15, 25, 12, 22]
                expected_data = ["camera_0", "camera_1", "lidar_0", "lidar_1", "imu_0", "imu_1"]

            assert [msg.log_time for msg in messages] == expected_log_times, \
                f"Expected log times {expected_log_times}, got {[msg.log_time for msg in messages]}"
            assert [msg.data.data for msg in messages] == expected_data, \
                f"Expected data {expected_data}, got {[msg.data.data for msg in messages]}"

            # Test case 2: Glob pattern matching /control/* topics
            messages = list(reader.messages("/control/*", in_log_time_order=in_log_time_order))
            logging.info(f'pybag /control/*: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag /control/*: {[msg.data.data for msg in messages]}')
            assert len(messages) == 3, f"Expected 3 messages from /control/*, got {len(messages)}"

            if in_log_time_order:
                expected_log_times = [8, 18, 30]
                expected_data = ["speed_0", "speed_1", "steering_0"]
            else:
                expected_log_times = [8, 30, 18]
                expected_data = ["speed_0", "steering_0", "speed_1"]

            assert [msg.log_time for msg in messages] == expected_log_times, \
                f"Expected log times {expected_log_times}, got {[msg.log_time for msg in messages]}"
            assert [msg.data.data for msg in messages] == expected_data, \
                f"Expected data {expected_data}, got {[msg.data.data for msg in messages]}"

            # Test case 3: Glob pattern with no matches
            messages = list(reader.messages("/nonexistent/*", in_log_time_order=in_log_time_order))
            logging.info(f'pybag /nonexistent/*: number of messages: {len(messages)}')
            assert len(messages) == 0, f"Expected 0 messages from /nonexistent/*, got {len(messages)}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_ordered_messages(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            writer.write_message("/unordered", 0, String(data=f"msg_{0}"))
            writer.write_message("/unordered", 1, String(data=f"msg_{1}"))
            writer.write_message("/unordered", 2, String(data=f"msg_{2}"))
            writer.write_message("/unordered", 3, String(data=f"msg_{3}"))
            writer.write_message("/unordered", 4, String(data=f"msg_{4}"))
            writer.write_message("/unordered", 5, String(data=f"msg_{5}"))
            writer.write_message("/unordered", 6, String(data=f"msg_{6}"))
            writer.write_message("/unordered", 7, String(data=f"msg_{7}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            decoder_factory = Ros2DecoderFactory() if profile == 'ros2' else Ros1DecoderFactory()
            reader = make_reader(f, decoder_factories=[decoder_factory])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/unordered", in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[message.log_time for message in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            expected_log_times = list(range(8))
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.data for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_ordered_messages(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            writer.write_message("/unordered", 7, String(data=f"msg_{7}"))
            writer.write_message("/unordered", 6, String(data=f"msg_{6}"))
            writer.write_message("/unordered", 5, String(data=f"msg_{5}"))
            writer.write_message("/unordered", 4, String(data=f"msg_{4}"))
            writer.write_message("/unordered", 3, String(data=f"msg_{3}"))
            writer.write_message("/unordered", 2, String(data=f"msg_{2}"))
            writer.write_message("/unordered", 1, String(data=f"msg_{1}"))
            writer.write_message("/unordered", 0, String(data=f"msg_{0}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            decoder_factory = Ros2DecoderFactory() if profile == 'ros2' else Ros1DecoderFactory()
            reader = make_reader(f, decoder_factories=[decoder_factory])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/unordered", in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[message.log_time for message in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            expected_log_times = list(range(8)) if in_log_time_order else list(reversed(range(8)))
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.data for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_random_ordered_messages(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    random.seed(42)  # Make tests reproducible

    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "overlapping.mcap"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f'Shuffled timestamps: {shuffled_timestamps}')

        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            for time in shuffled_timestamps:
                writer.write_message("/overlapping", time, String(data=f"msg_{time}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            decoder_factory = Ros2DecoderFactory() if profile == 'ros2' else Ros1DecoderFactory()
            reader = make_reader(f, decoder_factories=[decoder_factory])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/overlapping", in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            expected_log_times = sorted_timestamps if in_log_time_order else shuffled_timestamps
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.data for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_duplicate_timestamps(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test that multiple messages with the same log time are all returned."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "duplicate_timestamps.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # Ensure we have messages split across different chunks if enabled
            timestamp = 1000
            writer.write_message("/test", timestamp, String(data="msg_0"))
            writer.write_message("/test", timestamp, String(data="msg_1"))
            writer.write_message("/test", timestamp, String(data="msg_2"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            decoder_factory = Ros2DecoderFactory() if profile == 'ros2' else Ros1DecoderFactory()
            reader = make_reader(f, decoder_factories=[decoder_factory])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/test", in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            assert [msg.log_time for msg in messages] == [timestamp] * 3
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_multi_topic_out_of_order(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]
    expected_per_topic = {"/ahead": ahead_messages, "/behind": behind_messages}

    # Write messages to a bag file
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multi_topic_pybag.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            for log_time, data in ahead_messages:
                writer.write_message("/ahead", log_time, String(data=data))

            for log_time, data in behind_messages:
                writer.write_message("/behind", log_time, String(data=data))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            decoder_factory = Ros2DecoderFactory() if profile == 'ros2' else Ros1DecoderFactory()
            reader = make_reader(f, decoder_factories=[decoder_factory])
            for topic in expected_per_topic:
                official_mcap_messages = list(reader.iter_decoded_messages([topic], log_time_order=True))
                logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
                logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read each topic from the bag file
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            for topic, expected in expected_per_topic.items():
                messages = list(reader.messages(topic, in_log_time_order=in_log_time_order))
                assert [msg.log_time for msg in messages] == [log_time for log_time, _ in expected]
                assert [msg.data.data for msg in messages] == [data for _, data in expected]

######################
# Multi MCAP Reader  #
######################

@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_read_multiple_files_as_one(enable_crc_check: bool, profile) -> None:
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        file1 = temp_path / "one.mcap"
        file2 = str(temp_path / "two.mcap")  # str is also Iterable

        with McapFileWriter.open(file1, chunk_size=1, profile=profile) as writer:
            writer.write_message("/chatter", 1, String(data="hello"))
            writer.write_message("/chatter", 3, String(data="again"))
        with McapFileWriter.open(file2, chunk_size=1, profile=profile) as writer:
            writer.write_message("/chatter", 2, String(data="world"))
            writer.write_message("/chatter", 4, String(data="!!"))

        reader = McapMultipleFileReader.from_files([file1, file2], enable_crc_check=enable_crc_check)
        assert reader.start_time == 1 and reader.end_time == 4
        assert reader.get_message_count("/chatter") == 4

        messages = list(reader.messages("/chatter"))
        assert [m.data.data for m in messages] == ["hello", "world", "again", "!!"]

#############################################
# Compatibility with Official MCAP Library  #
#############################################

@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
def test_random_ordered_messages_from_official_mcap(
    chunk_size,
    in_log_time_order: bool,
    enable_crc_check: bool,
):
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "overlapping.mcap"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f"Shuffled timestamps: {shuffled_timestamps}")

        # Write messages with shuffled timestamps using official mcap library
        with open(path, 'wb') as f:
            writer = Ros2McapWriter(f) if chunk_size is None else Ros2McapWriter(f, chunk_size=chunk_size)
            try:
                schema_id = writer.register_msgdef('std_msgs/String', 'string data\n')
                for timestamp in shuffled_timestamps:
                    writer.write_message(
                        topic='/overlapping',
                        schema=schema_id,
                        message={'data': f'msg_{timestamp}'},
                        log_time=timestamp,
                        publish_time=timestamp,
                    )
            finally:
                writer.finish()

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[Ros2DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
            logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/overlapping", in_log_time_order=in_log_time_order))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')

            expected_log_times = sorted_timestamps if in_log_time_order else shuffled_timestamps
            assert [msg.log_time for msg in messages] == expected_log_times
            assert [msg.data.data for msg in messages] == [f"msg_{t}" for t in expected_log_times]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(1, id="tiny_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
def test_multi_topic_out_of_order_from_official_mcap(
    chunk_size,
    in_log_time_order: bool,
    enable_crc_check: bool,
):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]
    expected_per_topic = {"/ahead": ahead_messages, "/behind": behind_messages}

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multi_topic_official.mcap"
        with open(path, "wb") as handle:
            writer = Ros2McapWriter(handle) if chunk_size is None else Ros2McapWriter(handle, chunk_size=chunk_size)
            try:
                schema = writer.register_msgdef("std_msgs/String", "string data\n")

                for log_time, data in ahead_messages:
                    writer.write_message(
                        topic="/ahead",
                        schema=schema,
                        message={"data": data},
                        log_time=log_time,
                        publish_time=log_time,
                    )

                for log_time, data in behind_messages:
                    writer.write_message(
                        topic="/behind",
                        schema=schema,
                        message={"data": data},
                        log_time=log_time,
                        publish_time=log_time,
                    )
            finally:
                writer.finish()

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[Ros2DecoderFactory()])
            for topic in expected_per_topic:
                official_mcap_messages = list(reader.iter_decoded_messages([topic], log_time_order=True))
                logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
                logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read each topic from the bag file
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            for topic, expected in expected_per_topic.items():
                messages = list(reader.messages(topic, in_log_time_order=in_log_time_order))
                assert [msg.log_time for msg in messages] == [log_time for log_time, _ in expected]
                assert [msg.data.data for msg in messages] == [data for _, data in expected]

#########################
#  Reverse Iteration    #
#########################

@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_iteration_multiple_topics(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reverse iteration with multiple topics interleaved correctly."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "reverse_multi.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            # Write in specific interleaved order
            writer.write_message("/topic1", 10, String(data="t1_10"))
            writer.write_message("/topic2", 5, String(data="t2_5"))
            writer.write_message("/topic1", 3, String(data="t1_3"))
            writer.write_message("/topic2", 15, String(data="t2_15"))

        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            # Reverse iteration
            reverse_messages = list(reader.messages(["/topic1", "/topic2"], in_log_time_order=in_log_time_order, in_reverse=True))
            if in_log_time_order:
                assert [msg.log_time for msg in reverse_messages] == [15, 10, 5, 3]
                assert [msg.data.data for msg in reverse_messages] == ["t2_15", "t1_10", "t2_5", "t1_3"]
            else:
                assert [msg.log_time for msg in reverse_messages] == [15, 3, 5, 10]
                assert [msg.data.data for msg in reverse_messages] == ["t2_15", "t1_3", "t2_5", "t1_10"]

            # Reverse iteration (one topic)
            reverse_messages = list(reader.messages("/topic1", in_log_time_order=in_log_time_order, in_reverse=True))
            if in_log_time_order:
                assert [msg.log_time for msg in reverse_messages] == [10, 3]
                assert [msg.data.data for msg in reverse_messages] == ["t1_10", "t1_3"]
            else:
                assert [msg.log_time for msg in reverse_messages] == [3, 10]
                assert [msg.data.data for msg in reverse_messages] == ["t1_3", "t1_10"]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("in_log_time_order", [True, False])
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_iteration_with_time_filter(chunk_size, in_log_time_order: bool, enable_crc_check: bool, profile):
    """Test reverse iteration respects start_time and end_time filters."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "reverse_filter.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            for i in range(10):
                writer.write_message("/test", i * 10, String(data=f"msg_{i}"))

        # Reverse iteration with time range [20, 60]
        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            messages = list(reader.messages("/test", start_time=20, end_time=60, in_log_time_order=in_log_time_order, in_reverse=True))
            expected_times = [60, 50, 40, 30, 20]
            assert [msg.log_time for msg in messages] == expected_times


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_iteration_duplicate_timestamps(chunk_size, enable_crc_check: bool, profile):
    """Test reverse iteration handles duplicate timestamps correctly."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "reverse_dup.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            timestamp = 1000
            writer.write_message("/test", timestamp, String(data="msg_0"))
            writer.write_message("/test", timestamp, String(data="msg_1"))
            writer.write_message("/test", timestamp, String(data="msg_2"))

        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            messages = list(reader.messages("/test", in_reverse=True))
            assert [message.data.data for message in messages] == ["msg_2", "msg_1", "msg_0"]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_iteration_with_filter(chunk_size, enable_crc_check: bool, profile):
    """Test reverse iteration works with message filter callback."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "reverse_filter_cb.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None, profile=profile) as writer:
            for i in range(10):
                writer.write_message("/test", i, String(data=f"msg_{i}"))

        with McapFileReader.from_file(path, enable_crc_check=enable_crc_check) as reader:
            messages = list(reader.messages(
                "/test",
                filter=lambda msg: msg.log_time % 2 == 0,
                in_reverse=True
            ))
            expected_times = [8, 6, 4, 2, 0]
            assert [msg.log_time for msg in messages] == expected_times


@pytest.mark.parametrize("enable_crc_check", [True, False])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_reverse_iteration_multiple_files(enable_crc_check: bool, profile):
    """Test reverse iteration across multiple MCAP files."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        file1 = temp_path / "one.mcap"
        file2 = temp_path / "two.mcap"

        with McapFileWriter.open(file1, chunk_size=1, profile=profile) as writer:
            writer.write_message("/chatter", 1, String(data="hello"))
            writer.write_message("/chatter", 3, String(data="again"))
        with McapFileWriter.open(file2, chunk_size=1, profile=profile) as writer:
            writer.write_message("/chatter", 2, String(data="world"))
            writer.write_message("/chatter", 4, String(data="!!"))

        reader = McapMultipleFileReader.from_files([file1, file2], enable_crc_check=enable_crc_check)

        # Forward iteration
        forward_messages = list(reader.messages("/chatter", in_reverse=False))
        assert [m.data.data for m in forward_messages] == ["hello", "world", "again", "!!"]
        assert [m.log_time for m in forward_messages] == [1, 2, 3, 4]

        # Reverse iteration
        reverse_messages = list(reader.messages("/chatter", in_reverse=True))
        assert [m.data.data for m in reverse_messages] == ["!!", "again", "world", "hello"]
        assert [m.log_time for m in reverse_messages] == [4, 3, 2, 1]


####################################
#  ROS 2 Char Type Roundtrip Test  #
####################################

def test_mcap_ros2_char_roundtrip():
    """Test that ROS 2 char type is correctly written and read as a string."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "char_test.mcap"

        # Write with pybag
        with McapFileWriter.open(path, profile="ros2") as writer:
            msg = ros2_std_msgs.Char(data='A')
            writer.write_message("/char", 1000, msg)

        # Read with pybag
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/char"))
            assert len(messages) == 1
            assert messages[0].data.data == 'A'  # Should be a string, not an integer


def test_mcap_ros2_char_multiple_values():
    """Test that ROS 2 char type handles various character values."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "char_multi_test.mcap"

        # Write with pybag - test various characters
        with McapFileWriter.open(path, profile="ros2") as writer:
            writer.write_message("/char", 1000, ros2_std_msgs.Char(data='A'))
            writer.write_message("/char", 2000, ros2_std_msgs.Char(data='z'))
            writer.write_message("/char", 3000, ros2_std_msgs.Char(data='0'))
            writer.write_message("/char", 4000, ros2_std_msgs.Char(data=' '))

        # Read with pybag
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/char"))
            assert len(messages) == 4
            assert messages[0].data.data == 'A'
            assert messages[1].data.data == 'z'
            assert messages[2].data.data == '0'
            assert messages[3].data.data == ' '


#########################################
#  ROS 1 Types with ROS 2 Profile Tests #
#########################################


@dataclass(kw_only=True)
class Ros1TimeMessage:
    """A message with ROS 1 time type (not valid for ROS 2 profile)."""
    __msg_name__ = 'test_msgs/Ros1TimeMessage'
    stamp: t.ros1.time


@dataclass(kw_only=True)
class Ros1DurationMessage:
    """A message with ROS 1 duration type (not valid for ROS 2 profile)."""
    __msg_name__ = 'test_msgs/Ros1DurationMessage'
    elapsed: t.ros1.duration


def test_mcap_writer_ros2_profile_rejects_ros1_time_type():
    """Test that MCAP writer with ros2 profile rejects ROS 1 time type with helpful error."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "ros1_time.mcap"
        with pytest.raises(Ros2MsgError, match="ROS 1 'time' type cannot be used with ROS 2/MCAP"):
            with McapFileWriter.open(path, profile="ros2") as writer:
                msg = Ros1TimeMessage(stamp=t.ros1.Time(secs=1234567890, nsecs=123456789))
                writer.write_message("/time", 1000, msg)


def test_mcap_writer_ros2_profile_rejects_ros1_duration_type():
    """Test that MCAP writer with ros2 profile rejects ROS 1 duration type with helpful error."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "ros1_duration.mcap"
        with pytest.raises(Ros2MsgError, match="ROS 1 'duration' type cannot be used with ROS 2/MCAP"):
            with McapFileWriter.open(path, profile="ros2") as writer:
                msg = Ros1DurationMessage(elapsed=t.ros1.Duration(secs=100, nsecs=500000000))
                writer.write_message("/duration", 1000, msg)
