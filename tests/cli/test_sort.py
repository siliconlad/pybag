"""Tests for the sort CLI command."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.cli.mcap_sort import sort_mcap
from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.mcap_writer import McapFileWriter
from pybag.serialize import MessageSerializerFactory


def create_test_mcap_with_multiple_topics(path: Path) -> Path:
    """Create a simple MCAP with multiple topics for testing."""
    from rosbags.rosbag2 import StoragePlugin, Writer
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.LATEST)
    String = typestore.types["std_msgs/msg/String"]

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        conn1 = writer.add_connection("/topic1", String.__msgtype__, typestore=typestore)
        conn2 = writer.add_connection("/topic2", String.__msgtype__, typestore=typestore)
        conn3 = writer.add_connection("/topic3", String.__msgtype__, typestore=typestore)

        # Write messages in interleaved order
        for i in range(10):
            msg1 = String(data=f"topic1_msg{i}")
            msg2 = String(data=f"topic2_msg{i}")
            msg3 = String(data=f"topic3_msg{i}")

            writer.write(conn1, i * 1_000_000_000, typestore.serialize_cdr(msg1, String.__msgtype__))
            writer.write(conn2, i * 1_000_000_000 + 100, typestore.serialize_cdr(msg2, String.__msgtype__))
            writer.write(conn3, i * 1_000_000_000 + 200, typestore.serialize_cdr(msg3, String.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def test_sort_by_topic_basic():
    """Test that sort_mcap with by_topic creates a valid output file."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True)

        # Verify output file exists
        assert result.exists()
        assert result.resolve() == output_mcap.resolve()

        # Verify we can read it and it has the same content
        with McapRecordReaderFactory.from_file(input_mcap) as input_reader:
            with McapRecordReaderFactory.from_file(output_mcap) as output_reader:
                input_messages = list(input_reader.get_messages())
                output_messages = list(output_reader.get_messages())

                # Should have same number of messages
                assert len(input_messages) == len(output_messages)
                assert len(output_messages) == 30  # 3 topics * 10 messages


def test_sort_by_topic_default_output():
    """Test that sort_mcap with by_topic uses default output path."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")

        result = sort_mcap(input_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True)

        # Should create output with _sorted suffix
        expected = input_mcap.with_name(f"{input_mcap.stem}_sorted.mcap")
        assert result.resolve() == expected.resolve()
        assert result.exists()


def test_sort_by_topic_same_input_output_error():
    """Test that using same input and output raises error."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")

        with pytest.raises(ValueError, match="Input path cannot be same as output"):
            sort_mcap(input_mcap, input_mcap, by_topic=True)


def test_sort_by_topic_overwrite():
    """Test that overwrite flag works."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output.mcap"

        # Create initial output
        sort_mcap(input_mcap, output_mcap, chunk_size=1024, by_topic=True)

        # Should fail without overwrite flag
        with pytest.raises(ValueError, match="Output mcap exists"):
            sort_mcap(input_mcap, output_mcap, chunk_size=1024, by_topic=True)

        # Should succeed with overwrite flag
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, overwrite=True, by_topic=True)
        assert result.exists()


def test_sort_no_flags_returns_input():
    """Test that sort_mcap with no flags returns input path without creating output."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        result = sort_mcap(input_mcap, output_mcap)

        # Should return input path
        assert result.resolve() == input_mcap.resolve()
        # Output should not be created
        assert not output_mcap.exists()


def test_sort_log_time_only():
    """Test that sort_mcap with log_time only sorts all messages by log time."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", log_time=True)

        # Verify output file exists
        assert result.exists()
        assert result.resolve() == output_mcap.resolve()

        # Verify messages are sorted by log time
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            messages = list(reader.get_messages(in_log_time_order=False))
            log_times = [m.log_time for m in messages]
            assert log_times == sorted(log_times)
            assert len(messages) == 30


def test_sort_by_topic_and_log_time():
    """Test that sort_mcap with both flags groups by topic and sorts by log time within."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True, log_time=True)

        # Verify output file exists
        assert result.exists()
        assert result.resolve() == output_mcap.resolve()

        # Verify messages are grouped by topic and sorted by log time within each topic
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            channels = reader.get_channels()
            messages = list(reader.get_messages(in_log_time_order=False))

            # Group messages by topic and check log time ordering within each
            current_topic = None
            topic_log_times: list[int] = []

            for msg in messages:
                topic = channels[msg.channel_id].topic
                if topic != current_topic:
                    # New topic - verify previous topic was sorted
                    if topic_log_times:
                        assert topic_log_times == sorted(topic_log_times)
                    current_topic = topic
                    topic_log_times = [msg.log_time]
                else:
                    topic_log_times.append(msg.log_time)

            # Check the last topic
            if topic_log_times:
                assert topic_log_times == sorted(topic_log_times)

            assert len(messages) == 30


def test_sort_preserves_attachments():
    """Test that sort_mcap preserves attachments from the source file."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = Path(tmpdir) / "input.mcap"
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        # Create MCAP with messages and attachments using pybag writer
        with McapFileWriter.open(input_mcap, chunk_size=1024, chunk_compression="lz4") as writer:
            # Write some messages
            writer.write_message("/topic1", 1_000_000_000, std_msgs.String(data="msg1"))
            writer.write_message("/topic2", 2_000_000_000, std_msgs.String(data="msg2"))
            writer.write_message("/topic1", 3_000_000_000, std_msgs.String(data="msg3"))

            # Write attachments
            writer.write_attachment(
                name="image.png",
                data=b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
                media_type="image/png",
                log_time=1_500_000_000,
                create_time=1_000_000_000,
            )
            writer.write_attachment(
                name="calibration.yaml",
                data=b"camera_matrix: [1, 0, 0, 0, 1, 0, 0, 0, 1]",
                media_type="application/x-yaml",
                log_time=2_500_000_000,
                create_time=2_000_000_000,
            )

        # Sort the MCAP
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True)
        assert result.exists()

        # Verify attachments are preserved
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            attachments = reader.get_attachments()
            assert len(attachments) == 2

            # Check attachment content
            attachment_by_name = {a.name: a for a in attachments}

            assert "image.png" in attachment_by_name
            assert attachment_by_name["image.png"].data == b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
            assert attachment_by_name["image.png"].media_type == "image/png"
            assert attachment_by_name["image.png"].log_time == 1_500_000_000
            assert attachment_by_name["image.png"].create_time == 1_000_000_000

            assert "calibration.yaml" in attachment_by_name
            assert attachment_by_name["calibration.yaml"].data == b"camera_matrix: [1, 0, 0, 0, 1, 0, 0, 0, 1]"
            assert attachment_by_name["calibration.yaml"].media_type == "application/x-yaml"

            # Also verify messages are still there
            messages = list(reader.get_messages())
            assert len(messages) == 3


def test_sort_preserves_metadata():
    """Test that sort_mcap preserves metadata from the source file."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = Path(tmpdir) / "input.mcap"
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        # Create MCAP with messages and metadata using pybag writer
        with McapFileWriter.open(input_mcap, chunk_size=1024, chunk_compression="lz4") as writer:
            # Write some messages
            writer.write_message("/topic1", 1_000_000_000, std_msgs.String(data="msg1"))
            writer.write_message("/topic2", 2_000_000_000, std_msgs.String(data="msg2"))

            # Write metadata
            writer.write_metadata(
                name="recording_info",
                metadata={"location": "warehouse", "operator": "robot1"}
            )
            writer.write_metadata(
                name="device_info",
                metadata={"device_id": "sensor_123", "firmware": "v2.0"}
            )

        # Sort the MCAP
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", log_time=True)
        assert result.exists()

        # Verify metadata is preserved
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            metadata_records = reader.get_metadata()
            assert len(metadata_records) == 2

            # Check metadata content
            metadata_by_name = {m.name: m for m in metadata_records}

            assert "recording_info" in metadata_by_name
            assert metadata_by_name["recording_info"].metadata == {"location": "warehouse", "operator": "robot1"}

            assert "device_info" in metadata_by_name
            assert metadata_by_name["device_info"].metadata == {"device_id": "sensor_123", "firmware": "v2.0"}

            # Also verify messages are still there
            messages = list(reader.get_messages())
            assert len(messages) == 2


def _create_mcap_with_custom_sequences(path: Path) -> None:
    """Create an MCAP file with specific sequence numbers for testing.

    Creates messages with non-zero sequence numbers to test preservation:
    - /topic1: messages with sequences 100, 101, 102
    - /topic2: messages with sequences 200, 201
    Messages are interleaved in write order.
    """
    serializer = MessageSerializerFactory.from_profile("ros2")
    assert serializer is not None

    with McapRecordWriterFactory.create_writer(
        FileWriter(path),
        chunk_size=1024,
        chunk_compression="lz4",
        profile="ros2",
    ) as writer:
        # Write schema
        schema = SchemaRecord(
            id=1,
            name="std_msgs/msg/String",
            encoding=serializer.schema_encoding,
            data=serializer.serialize_schema(std_msgs.String),
        )
        writer.write_schema(schema)

        # Write channels
        channel1 = ChannelRecord(
            id=1, schema_id=1, topic="/topic1",
            message_encoding="cdr", metadata={}
        )
        channel2 = ChannelRecord(
            id=2, schema_id=1, topic="/topic2",
            message_encoding="cdr", metadata={}
        )
        writer.write_channel(channel1)
        writer.write_channel(channel2)

        # Write interleaved messages with specific sequence numbers
        # Topic1: seq 100 at t=1s
        writer.write_message(MessageRecord(
            channel_id=1, sequence=100,
            log_time=1_000_000_000, publish_time=1_000_000_000,
            data=serializer.serialize_message(std_msgs.String(data="t1_msg0")),
        ))
        # Topic2: seq 200 at t=2s
        writer.write_message(MessageRecord(
            channel_id=2, sequence=200,
            log_time=2_000_000_000, publish_time=2_000_000_000,
            data=serializer.serialize_message(std_msgs.String(data="t2_msg0")),
        ))
        # Topic1: seq 101 at t=3s
        writer.write_message(MessageRecord(
            channel_id=1, sequence=101,
            log_time=3_000_000_000, publish_time=3_000_000_000,
            data=serializer.serialize_message(std_msgs.String(data="t1_msg1")),
        ))
        # Topic2: seq 201 at t=4s
        writer.write_message(MessageRecord(
            channel_id=2, sequence=201,
            log_time=4_000_000_000, publish_time=4_000_000_000,
            data=serializer.serialize_message(std_msgs.String(data="t2_msg1")),
        ))
        # Topic1: seq 102 at t=5s
        writer.write_message(MessageRecord(
            channel_id=1, sequence=102,
            log_time=5_000_000_000, publish_time=5_000_000_000,
            data=serializer.serialize_message(std_msgs.String(data="t1_msg2")),
        ))


def test_sort_by_topic_only_preserves_sequence_numbers():
    """Test that --by-topic without --log-time preserves original sequence numbers.

    When only grouping by topic (no time sorting), the per-channel message order
    is unchanged, so original sequence numbers from the publisher should remain
    intact for downstream consumers to detect dropped packets.
    """
    with TemporaryDirectory() as tmpdir:
        input_mcap = Path(tmpdir) / "input.mcap"
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        _create_mcap_with_custom_sequences(input_mcap)

        # Sort with by_topic only (no log_time)
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True)
        assert result.exists()

        # Verify sequence numbers are preserved
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            channels = reader.get_channels()
            messages = list(reader.get_messages(in_log_time_order=False))

            # Group messages by topic
            topic1_messages = [m for m in messages if channels[m.channel_id].topic == "/topic1"]
            topic2_messages = [m for m in messages if channels[m.channel_id].topic == "/topic2"]

            # Verify original sequences are preserved
            assert [m.sequence for m in topic1_messages] == [100, 101, 102]
            assert [m.sequence for m in topic2_messages] == [200, 201]


def test_sort_log_time_renumbers_sequences():
    """Test that --log-time renumbers sequence numbers.

    When sorting by log time, message order changes, so sequences must be
    renumbered to keep them monotonic (0, 1, 2, ...).
    """
    with TemporaryDirectory() as tmpdir:
        input_mcap = Path(tmpdir) / "input.mcap"
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        _create_mcap_with_custom_sequences(input_mcap)

        # Sort with log_time only
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", log_time=True)
        assert result.exists()

        # Verify sequence numbers are renumbered from 0
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            channels = reader.get_channels()
            messages = list(reader.get_messages(in_log_time_order=False))

            # Group messages by topic
            topic1_messages = [m for m in messages if channels[m.channel_id].topic == "/topic1"]
            topic2_messages = [m for m in messages if channels[m.channel_id].topic == "/topic2"]

            # Verify sequences are renumbered from 0
            assert [m.sequence for m in topic1_messages] == [0, 1, 2]
            assert [m.sequence for m in topic2_messages] == [0, 1]


def test_sort_by_topic_and_log_time_renumbers_sequences():
    """Test that --by-topic --log-time renumbers sequence numbers.

    When both flags are used, log time sorting changes the order within each
    topic, so sequences must be renumbered.
    """
    with TemporaryDirectory() as tmpdir:
        input_mcap = Path(tmpdir) / "input.mcap"
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        _create_mcap_with_custom_sequences(input_mcap)

        # Sort with both flags
        result = sort_mcap(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4", by_topic=True, log_time=True)
        assert result.exists()

        # Verify sequence numbers are renumbered from 0
        with McapRecordReaderFactory.from_file(output_mcap) as reader:
            channels = reader.get_channels()
            messages = list(reader.get_messages(in_log_time_order=False))

            # Group messages by topic
            topic1_messages = [m for m in messages if channels[m.channel_id].topic == "/topic1"]
            topic2_messages = [m for m in messages if channels[m.channel_id].topic == "/topic2"]

            # Verify sequences are renumbered from 0
            assert [m.sequence for m in topic1_messages] == [0, 1, 2]
            assert [m.sequence for m in topic2_messages] == [0, 1]
