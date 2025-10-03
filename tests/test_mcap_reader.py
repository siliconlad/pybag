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
def test_ordered_messages(chunk_size):
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

            messages = list(reader.messages("/unordered"))
            logging.info(f'pybag: {[message.log_time for message in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')
            assert [msg.log_time for msg in messages] == list(range(8))
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_reverse_ordered_messages(chunk_size):
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

            messages = list(reader.messages("/unordered"))
            logging.info(f'pybag: {[message.log_time for message in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')
            assert [msg.log_time for msg in messages] == list(range(8))
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_random_ordered_messages(chunk_size):
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

            messages = list(reader.messages("/overlapping"))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')
            assert [msg.log_time for msg in messages] == sorted_timestamps
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_duplicate_timestamps(chunk_size):
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

            messages = list(reader.messages("/test"))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"

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
def test_random_ordered_messages_from_official_mcap(chunk_size):
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

            messages = list(reader.messages("/overlapping"))
            logging.info(f'pybag: {[msg.log_time for msg in messages]}')
            logging.info(f'pybag: {[msg.data.data for msg in messages]}')
            assert [msg.log_time for msg in messages] == sorted_timestamps
            for i, message in enumerate(messages):
                assert message.data.data == f"msg_{i}"


def _assert_multi_topic_read_order(
    path: Path,
    chunk_size: int | None,
    expected_per_topic: dict[str, list[tuple[int, str]]],
    expected_combined: list[tuple[str, int, str]],
) -> None:
    with McapFileReader.from_file(path) as reader:
        for topic, expected in expected_per_topic.items():
            messages = list(reader.messages(topic))
            assert [msg.log_time for msg in messages] == [log_time for log_time, _ in expected]
            assert [msg.data.data for msg in messages] == [data for _, data in expected]

        channel_topics = {
            channel_id: channel.topic for channel_id, channel in reader._reader.get_channels().items()
        }
        all_records = list(reader._reader.get_messages())
        assert [record.log_time for record in all_records] == [log_time for _, log_time, _ in expected_combined]
        assert [
            (channel_topics[record.channel_id], record.log_time)
            for record in all_records
        ] == [(topic, log_time) for topic, log_time, _ in expected_combined]

    def _read_official(topics: list[str] | None = None):
        with open(path, "rb") as handle:
            reader = make_reader(handle, decoder_factories=[DecoderFactory()])
            return list(reader.iter_decoded_messages(topics=topics, log_time_order=True))

    for topic, expected in expected_per_topic.items():
        official_topic_messages = _read_official([topic])
        assert [
            (message.message.log_time, message.decoded_message.data)
            for message in official_topic_messages
        ] == expected

    official_all_messages = _read_official(None)
    assert [
        (message.channel.topic, message.message.log_time, message.decoded_message.data)
        for message in official_all_messages
    ] == expected_combined


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(None, id="without_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_multi_topic_log_time_order_pybag_writer(chunk_size):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "multi_topic_pybag.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            for log_time, data in ahead_messages:
                writer.write_message("/ahead", log_time, std_msgs.String(data=data))

            for log_time, data in behind_messages:
                writer.write_message("/behind", log_time, std_msgs.String(data=data))

        expected_per_topic = {
            "/ahead": ahead_messages,
            "/behind": behind_messages,
        }
        expected_combined = [
            ("/behind", 5, "behind_0"),
            ("/ahead", 10, "ahead_0"),
            ("/behind", 15, "behind_1"),
            ("/ahead", 20, "ahead_1"),
            ("/behind", 25, "behind_2"),
            ("/ahead", 30, "ahead_2"),
        ]

        _assert_multi_topic_read_order(path, chunk_size, expected_per_topic, expected_combined)


@pytest.mark.parametrize(
    "chunk_size",
    [
        pytest.param(1, id="tiny_chunks"),
        pytest.param(64, id="with_chunks"),
    ],
)
def test_multi_topic_log_time_order_official_writer(chunk_size):
    ahead_messages = [(10, "ahead_0"), (20, "ahead_1"), (30, "ahead_2")]
    behind_messages = [(5, "behind_0"), (15, "behind_1"), (25, "behind_2")]

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

        expected_per_topic = {
            "/ahead": ahead_messages,
            "/behind": behind_messages,
        }
        expected_combined = [
            ("/behind", 5, "behind_0"),
            ("/ahead", 10, "ahead_0"),
            ("/behind", 15, "behind_1"),
            ("/ahead", 20, "ahead_1"),
            ("/behind", 25, "behind_2"),
            ("/ahead", 30, "ahead_2"),
        ]

        _assert_multi_topic_read_order(path, chunk_size, expected_per_topic, expected_combined)
