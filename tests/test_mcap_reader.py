"""Tests for the MCAP reader."""
import random
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

import pytest
from rosbags.rosbag2 import Reader
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from mcap_ros2.writer import Writer as McapWriter
from rosbags.highlevel import AnyReader
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


def test_messages_filter(typestore: Typestore):
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
            all_messages = list(reader.messages("/rosbags"))
            assert len(all_messages) == 2
            assert all_messages[0].data.data == 1
            assert all_messages[1].data.data == -1

            positive = list(reader.messages("/rosbags", filter=lambda msg: msg.data.data > 0))
            assert len(positive) == 1
            assert positive[0].data.data == 1

            negative = list(reader.messages("/rosbags", filter=lambda msg: msg.data.data < 0))
            assert len(negative) == 1
            assert negative[0].data.data == -1


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(8, id="with_chunks"),
    ],
)
def test_messages_read_in_order_when_written_out_of_order(chunk_size):
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            writer.write_message("/unordered", 80, std_msgs.String(data="a"))
            writer.write_message("/unordered", 70, std_msgs.String(data="b"))
            writer.write_message("/unordered", 60, std_msgs.String(data="c"))
            writer.write_message("/unordered", 50, std_msgs.String(data="d"))
            writer.write_message("/unordered", 40, std_msgs.String(data="e"))
            writer.write_message("/unordered", 30, std_msgs.String(data="f"))
            writer.write_message("/unordered", 20, std_msgs.String(data="g"))
            writer.write_message("/unordered", 10, std_msgs.String(data="h"))

        with McapFileReader.from_file(path) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            # Read all the messages
            messages = list(reader.messages("/unordered"))

    assert [message.log_time for message in messages] == [10, 20, 30, 40, 50, 60, 70, 80]
    assert [message.data.data for message in messages] == ["h", "g", "f", "e", "d", "c", "b", "a"]


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(50, id="with_chunks"),
    ],
)
def test_overlapping_chunk_times_with_random_timestamps(chunk_size):
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "overlapping_chunks.mcap"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10, 100, 5))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(shuffled_timestamps)

        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for i, time in enumerate(shuffled_timestamps):
                writer.write_message("/overlapping", time, std_msgs.String(data=f"msg_{i}"))

        with McapFileReader.from_file(path) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            # Verify that all messages can still be read correctly
            messages = list(reader.messages("/overlapping"))
            assert len(messages) == len(sorted_timestamps)

            # Verify each message has the correct data based on its timestamp
            logging.info([msg.log_time for msg in messages])
            for i, msg in enumerate(messages):
                assert msg.data.data == f"msg_{i}"


def _read_with_pybag(mcap_path: Path, topic: str) -> list[tuple[int, str]]:
    """Read messages with pybag and return list of (timestamp, data) tuples."""
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages(topic))
        return [(msg.log_time, msg.data.data) for msg in messages]


def _read_with_rosbags(mcap_path: Path, topic: str) -> list[tuple[int, str]]:
    """Read messages with rosbags library and return list of (timestamp, data) tuples."""
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    with Reader(mcap_path.parent) as reader:
        messages = []
        for conn, timestamp, data in reader.messages(topic):
            if conn.topic == topic:
                deserialized = typestore.deserialize_cdr(data, conn.msgtype)
                messages.append((timestamp, deserialized.data))
        return messages


def _read_with_official_mcap(mcap_path: Path, topic: str) -> list[tuple[int, str]]:
    """Read messages with official mcap library and return list of (timestamp, data) tuples."""
    with open(mcap_path, 'rb') as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        messages = []
        for _, _, msg, ros_msg in reader.iter_decoded_messages(log_time_order=True):
            if ros_msg.__class__.__name__ == 'String' and hasattr(ros_msg, 'data'):
                messages.append((msg.log_time, ros_msg.data))
        return messages


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(50, id="with_chunks"),
    ],
)
def test_random_timestamps_behavior_comparison(chunk_size):
    """Test that pybag, rosbags, and official mcap library all return messages in the same order."""
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "comparison_test.mcap"
        topic = "/comparison_test"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10, 100, 5))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f"Shuffled timestamps: {shuffled_timestamps}")

        # Write messages with shuffled timestamps using pybag
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for i, time in enumerate(shuffled_timestamps):
                writer.write_message(topic, time, std_msgs.String(data=f"msg_{i}"))

        # Read messages with all three libraries
        pybag_messages = _read_with_pybag(path, topic)
        # rosbags_messages = _read_with_rosbags(path, topic)
        official_mcap_messages = _read_with_official_mcap(path, topic)

        # Verify all libraries return the same number of messages
        assert len(pybag_messages) == len(sorted_timestamps)
        logging.info(f'Pybag: {[msg[0] for msg in pybag_messages]}')

        assert len(official_mcap_messages) == len(sorted_timestamps)
        logging.info(f'Official mcap: {[msg[0] for msg in official_mcap_messages]}')

        assert False, "Trigger logging"


def _write_with_official_mcap(mcap_path: Path, topic: str, timestamps: list[int], chunk_size: int | None = None) -> None:
    """Write messages using official mcap library."""
    with open(mcap_path, 'wb') as f:
        if chunk_size is not None:
            writer = McapWriter(f, chunk_size=chunk_size)
        else:
            writer = McapWriter(f)

        try:
            schema_id = writer.register_msgdef('std_msgs/msg/String', 'string data\n')
            # Write messages
            for i, timestamp in enumerate(timestamps):
                writer.write_message(
                    topic=topic,
                    schema=schema_id,
                    message={'data': f'msg_{i}'},
                    log_time=timestamp,
                    publish_time=timestamp,
                )
        finally:
            writer.finish()


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(50, id="with_chunks"),
    ],
)
def test_random_timestamps_with_official_mcap_writer(chunk_size):
    """Test that messages written with official mcap library are read correctly by all libraries."""
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "official_mcap_writer_test.mcap"
        topic = "/official_mcap_writer_test"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10, 100, 5))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f"Shuffled timestamps: {shuffled_timestamps}")

        # Write messages with shuffled timestamps using official mcap library
        _write_with_official_mcap(path, topic, shuffled_timestamps, chunk_size)

        # Read messages with all three libraries
        pybag_messages = _read_with_pybag(path, topic)
        rosbags_messages = _read_with_rosbags(path, topic)
        official_mcap_messages = _read_with_official_mcap(path, topic)

        # Verify all libraries return the same number of messages
        assert len(pybag_messages) == len(sorted_timestamps)
        logging.info(f'Pybag: {[msg[0] for msg in pybag_messages]}')

        assert len(official_mcap_messages) == len(sorted_timestamps)
        logging.info(f'Official mcap: {[msg[0] for msg in official_mcap_messages]}')

        assert False, "Trigger logging"
