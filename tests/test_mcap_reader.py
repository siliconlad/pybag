"""Tests for the MCAP reader."""
import logging
import random
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from mcap_ros2.writer import Writer as McapWriter
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob("*.mcap"))


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request):
    return get_typestore(request.param)

#################
#  Pybag tests  #
#################

@pytest.mark.parametrize("in_log_time_order", [True, False])
def test_messages_filter(typestore: Typestore, in_log_time_order: bool):
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
        with McapFileReader.from_file(mcap_file) as reader:
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
def test_multiple_existing_topics(chunk_size, in_log_time_order: bool):
    """Test reading messages from multiple existing topics."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multiple_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            # Write messages to /topic1 with timestamps 10, 20, 30
            writer.write_message("/topic1", 10, std_msgs.String(data="topic1_0"))
            writer.write_message("/topic1", 20, std_msgs.String(data="topic1_1"))
            writer.write_message("/topic1", 30, std_msgs.String(data="topic1_2"))

            # Write messages to /topic2 with timestamps 15, 25, 35
            writer.write_message("/topic2", 15, std_msgs.String(data="topic2_0"))
            writer.write_message("/topic2", 25, std_msgs.String(data="topic2_1"))
            writer.write_message("/topic2", 35, std_msgs.String(data="topic2_2"))

            # Write messages to /topic3 with timestamps 5, 45, 50
            writer.write_message("/topic3", 5, std_msgs.String(data="topic3_0"))
            writer.write_message("/topic3", 45, std_msgs.String(data="topic3_1"))
            writer.write_message("/topic3", 50, std_msgs.String(data="topic3_2"))

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_multiple_topics_with_nonexistent(chunk_size, in_log_time_order: bool):
    """Test reading messages from multiple topics where some don't exist."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "partial_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            # Write 10 messages to /existing to ensure multiple chunks are created
            for i in range(10):
                writer.write_message("/existing", i * 10, std_msgs.String(data=f"msg_{i}"))

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_multiple_nonexistent_topics(chunk_size, in_log_time_order: bool):
    """Test reading messages from multiple topics that don't exist."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "no_topics.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            # Write enough messages to /real_topic to create multiple chunks
            for i in range(10):
                writer.write_message("/real_topic", i * 10, std_msgs.String(data=f"msg_{i}"))

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_glob_pattern_matching(chunk_size, in_log_time_order: bool):
    """Test reading messages using glob patterns to match multiple topics.

    This test verifies that glob patterns like '/sensor/*' can be used to match
    multiple topics that share a common prefix. The test creates topics under
    two different namespaces (/sensor and /control) and validates:
    1. Glob patterns correctly match all topics under a namespace
    2. Messages are returned in the correct order (log_time or write order)
    3. Glob patterns with no matches return an empty list without errors
    """
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "glob_pattern.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            # /sensor/camera
            writer.write_message("/sensor/camera", 10, std_msgs.String(data="camera_0"))
            writer.write_message("/sensor/camera", 20, std_msgs.String(data="camera_1"))
            # /sensor/lidar
            writer.write_message("/sensor/lidar", 15, std_msgs.String(data="lidar_0"))
            writer.write_message("/sensor/lidar", 25, std_msgs.String(data="lidar_1"))
            # /sensor/imu
            writer.write_message("/sensor/imu", 12, std_msgs.String(data="imu_0"))
            writer.write_message("/sensor/imu", 22, std_msgs.String(data="imu_1"))
            # /control
            writer.write_message("/control/speed", 8, std_msgs.String(data="speed_0"))
            writer.write_message("/control/steering", 30, std_msgs.String(data="steering_0"))
            # Write second speed message AFTER steering (out of timestamp order)
            writer.write_message("/control/speed", 18, std_msgs.String(data="speed_1"))

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_ordered_messages(chunk_size, in_log_time_order: bool):
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            writer.write_message("/unordered", 0, std_msgs.String(data=f"msg_{0}"))
            writer.write_message("/unordered", 1, std_msgs.String(data=f"msg_{1}"))
            writer.write_message("/unordered", 2, std_msgs.String(data=f"msg_{2}"))
            writer.write_message("/unordered", 3, std_msgs.String(data=f"msg_{3}"))
            writer.write_message("/unordered", 4, std_msgs.String(data=f"msg_{4}"))
            writer.write_message("/unordered", 5, std_msgs.String(data=f"msg_{5}"))
            writer.write_message("/unordered", 6, std_msgs.String(data=f"msg_{6}"))
            writer.write_message("/unordered", 7, std_msgs.String(data=f"msg_{7}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_reverse_ordered_messages(chunk_size, in_log_time_order: bool):
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            writer.write_message("/unordered", 7, std_msgs.String(data=f"msg_{7}"))
            writer.write_message("/unordered", 6, std_msgs.String(data=f"msg_{6}"))
            writer.write_message("/unordered", 5, std_msgs.String(data=f"msg_{5}"))
            writer.write_message("/unordered", 4, std_msgs.String(data=f"msg_{4}"))
            writer.write_message("/unordered", 3, std_msgs.String(data=f"msg_{3}"))
            writer.write_message("/unordered", 2, std_msgs.String(data=f"msg_{2}"))
            writer.write_message("/unordered", 1, std_msgs.String(data=f"msg_{1}"))
            writer.write_message("/unordered", 0, std_msgs.String(data=f"msg_{0}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_random_ordered_messages(chunk_size, in_log_time_order: bool):
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "overlapping.mcap"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f'Shuffled timestamps: {shuffled_timestamps}')

        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for time in shuffled_timestamps:
                writer.write_message("/overlapping", time, std_msgs.String(data=f"msg_{time}"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_duplicate_timestamps(chunk_size, in_log_time_order: bool):
    """Test that multiple messages with the same log time are all returned."""
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "duplicate_timestamps.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            # Ensure we have messages split across different chunks if enabled
            timestamp = 1000
            writer.write_message("/test", timestamp, std_msgs.String(data="msg_0"))
            writer.write_message("/test", timestamp, std_msgs.String(data="msg_1"))
            writer.write_message("/test", timestamp, std_msgs.String(data="msg_2"))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_multi_topic_out_of_order(chunk_size, in_log_time_order: bool):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]
    expected_per_topic = {"/ahead": ahead_messages, "/behind": behind_messages}

    # Write messages to a bag file
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multi_topic_pybag.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for log_time, data in ahead_messages:
                writer.write_message("/ahead", log_time, std_msgs.String(data=data))

            for log_time, data in behind_messages:
                writer.write_message("/behind", log_time, std_msgs.String(data=data))

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            for topic in expected_per_topic:
                official_mcap_messages = list(reader.iter_decoded_messages([topic], log_time_order=True))
                logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
                logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read each topic from the bag file
        with McapFileReader.from_file(path) as reader:
            for topic, expected in expected_per_topic.items():
                messages = list(reader.messages(topic, in_log_time_order=in_log_time_order))
                assert [msg.log_time for msg in messages] == [log_time for log_time, _ in expected]
                assert [msg.data.data for msg in messages] == [data for _, data in expected]

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
def test_random_ordered_messages_from_official_mcap(
    chunk_size,
    in_log_time_order: bool,
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
            writer = McapWriter(f) if chunk_size is None else McapWriter(f, chunk_size=chunk_size)
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
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
            logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
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
def test_multi_topic_out_of_order_from_official_mcap(
    chunk_size,
    in_log_time_order: bool,
):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]
    expected_per_topic = {"/ahead": ahead_messages, "/behind": behind_messages}

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multi_topic_official.mcap"
        with open(path, "wb") as handle:
            writer = McapWriter(handle) if chunk_size is None else McapWriter(handle, chunk_size=chunk_size)
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
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            for topic in expected_per_topic:
                official_mcap_messages = list(reader.iter_decoded_messages([topic], log_time_order=True))
                logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')
                logging.info(f'mcap: {[msg[-1].data for msg in official_mcap_messages]}')

        # Read each topic from the bag file
        with McapFileReader.from_file(path) as reader:
            for topic, expected in expected_per_topic.items():
                messages = list(reader.messages(topic, in_log_time_order=in_log_time_order))
                assert [msg.log_time for msg in messages] == [log_time for log_time, _ in expected]
                assert [msg.data.data for msg in messages] == [data for _, data in expected]
