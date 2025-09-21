"""Tests for the MCAP reader."""
import random
import logging
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
        pytest.param(64, id="with_chunks"),
    ],
)
def test_reverse_order_messages(chunk_size):
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

        with McapFileReader.from_file(path) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            messages = list(reader.messages("/unordered"))
            logging.info(f'Timestamps: {[message.log_time for message in messages]}')
            for i, message in enumerate(messages):
                assert message.log_time == i
                assert message.data.data == f"msg_{i}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_random_order_messages_from_pybag(chunk_size):
    random.seed(42)  # Make tests reproducible

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "overlapping.mcap"

        # Create timestamps that will cause overlapping chunks
        sorted_timestamps = list(range(10))
        shuffled_timestamps = random.sample(sorted_timestamps, len(sorted_timestamps))
        logging.info(f'Shuffled timestamps: {shuffled_timestamps}')

        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for i, time in enumerate(shuffled_timestamps):
                writer.write_message("/overlapping", time, std_msgs.String(data=f"msg_{i}"))

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            pybag_messages = list(reader.messages("/overlapping"))
            logging.info(f'pybag: {[msg.log_time for msg in pybag_messages]}')
            assert len(pybag_messages) == len(sorted_timestamps)

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Check results at the end (so we see all logging)
        assert [msg.log_time for msg in pybag_messages] == sorted_timestamps
        assert [msg[-2].log_time for msg in official_mcap_messages] == sorted_timestamps


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_random_order_messages_from_official_mcap(chunk_size):
    """Test that pybag, rosbags, and official mcap library all return messages in the same order."""
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
                for i, timestamp in enumerate(shuffled_timestamps):
                    writer.write_message(
                        topic='/overlapping',
                        schema=schema_id,
                        message={'data': f'msg_{i}'},
                        log_time=timestamp,
                        publish_time=timestamp,
                    )
            finally:
                writer.finish()

        # Read messages with pybag
        with McapFileReader.from_file(path) as reader:
            # Check that we have multiple chunks if configured to do so
            chunk_indexes = reader._reader.get_chunk_indexes()
            logging.info(f'Number of chunks: {len(chunk_indexes)}')
            assert chunk_size is None or len(chunk_indexes) > 1, "Expected multiple chunks"

            pybag_messages = list(reader.messages("/overlapping"))
            logging.info(f'pybag: {[msg.log_time for msg in pybag_messages]}')
            assert len(pybag_messages) == len(sorted_timestamps)

        # Read messages with official mcap library
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            official_mcap_messages = list(reader.iter_decoded_messages(log_time_order=True))
            logging.info(f'mcap: {[msg[-2].log_time for msg in official_mcap_messages]}')

        # Check results at the end (so we see all logging)
        assert [msg.log_time for msg in pybag_messages] == sorted_timestamps
        assert [msg[-2].log_time for msg in official_mcap_messages] == sorted_timestamps
