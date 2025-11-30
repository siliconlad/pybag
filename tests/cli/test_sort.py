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


def create_test_mcap_with_multiple_topics(path: Path, chunk_size: int = 1024) -> Path:
    """Create a simple MCAP with multiple topics for testing.

    Args:
        path: Path to the output MCAP file.
        chunk_size: Chunk size in bytes for consistent test behavior.

    Returns:
        Path to the created MCAP file.
    """
    with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression="lz4") as writer:
        # Write messages in interleaved order (3 topics, 10 messages each)
        for i in range(10):
            writer.write_message("/topic1", i * 1_000_000_000, std_msgs.String(data=f"topic1_msg{i}"))
            writer.write_message("/topic2", i * 1_000_000_000 + 100, std_msgs.String(data=f"topic2_msg{i}"))
            writer.write_message("/topic3", i * 1_000_000_000 + 200, std_msgs.String(data=f"topic3_msg{i}"))

    return path


def test_sort_by_topic_same_input_output_error(tmp_path: Path):
    """Test that using same input and output raises error."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")

    with pytest.raises(ValueError, match="Input path cannot be same as output"):
        sort_mcap(input_mcap, input_mcap, sort_by_topic=True)


def test_sort_by_topic_overwrite(tmp_path: Path):
    """Test that overwrite flag works."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")
    output_mcap = tmp_path / "output.mcap"

    # Create initial output
    _ = sort_mcap(input_mcap, output_mcap, sort_by_topic=True)

    # Should fail without overwrite flag
    with pytest.raises(ValueError, match="Output mcap exists"):
        _ = sort_mcap(input_mcap, output_mcap, sort_by_topic=True)

    # Should succeed with overwrite flag
    result = sort_mcap(input_mcap, output_mcap, sort_by_topic=True, overwrite=True)
    assert result.exists()


def test_sort_no_flags_returns_input(tmp_path: Path):
    """Test that sort_mcap with no flags returns input path without creating output."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")
    output_mcap = tmp_path / "output_sorted.mcap"

    result = sort_mcap(input_mcap, output_mcap)

    # Should return input path
    assert result.resolve() == input_mcap.resolve()
    assert not output_mcap.exists()  # Output should not be created


def test_sort_by_topic(tmp_path: Path):
    """Test that sort_mcap with by_topic creates a valid output file."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")
    output_mcap = tmp_path / "output_sorted.mcap"

    result = sort_mcap(
        input_mcap,
        output_mcap,
        chunk_size=1024,
        chunk_compression="lz4",
        sort_by_topic=True
    )

    # Verify output file exists
    assert result.exists()
    assert result.resolve() == output_mcap.resolve()

    # Verify messages are grouped by topic
    with McapRecordReaderFactory.from_file(output_mcap) as reader:
        messages = list(reader.get_messages(in_log_time_order=False))
        assert len(messages) == 30

        # Group messages by topic and check log time ordering within each
        current_channel_id = None
        channel_ids: set[int] = set()

        for msg in messages:
            if msg.channel_id != current_channel_id:
                # New topic - verify previous topic was sorted
                assert msg.channel_id not in channel_ids
                current_channel_id = msg.channel_id
                channel_ids.add(msg.channel_id)


def test_sort_log_time(tmp_path: Path):
    """Test that sort_mcap with log_time only sorts all messages by log time."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")
    output_mcap = tmp_path / "output_sorted.mcap"

    result = sort_mcap(
        input_mcap,
        output_mcap,
        chunk_size=1024,
        chunk_compression="lz4",
        sort_by_log_time=True
    )

    # Verify output file exists
    assert result.exists()
    assert result.resolve() == output_mcap.resolve()

    # Verify messages are sorted by log time
    with McapRecordReaderFactory.from_file(output_mcap) as reader:
        messages = list(reader.get_messages(in_log_time_order=False))
        log_times = [m.log_time for m in messages]
        assert log_times == sorted(log_times)
        assert len(messages) == 30


def test_sort_by_topic_and_log_time(tmp_path: Path):
    """Test that sort_mcap with both flags groups by topic and sorts by log time within."""
    input_mcap = create_test_mcap_with_multiple_topics(tmp_path / "input.mcap")
    output_mcap = tmp_path / "output_sorted.mcap"

    result = sort_mcap(
        input_mcap,
        output_mcap,
        chunk_size=1024,
        chunk_compression="lz4",
        sort_by_topic=True,
        sort_by_log_time=True
    )

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
        if topic_log_times:  # Check the last topic
            assert topic_log_times == sorted(topic_log_times)

        assert len(messages) == 30


def test_sort_preserves_attachments(tmp_path: Path):
    """Test that sort_mcap preserves attachments from the source file."""
    input_mcap = tmp_path / "input.mcap"
    output_mcap = tmp_path / "output_sorted.mcap"

    # Create MCAP with messages and attachments using pybag writer
    with McapFileWriter.open(input_mcap, chunk_size=1024, chunk_compression="lz4") as writer:
        # Write some messages
        writer.write_message("/topic1", 1_000_000_000, std_msgs.String(data="msg1"))
        writer.write_message("/topic2", 2_000_000_000, std_msgs.String(data="msg2"))
        writer.write_message("/topic1", 3_000_000_000, std_msgs.String(data="msg3"))

        # Write attachments
        writer.write_attachment(
            name="calibration.yaml",
            data=b"key: value",
            media_type="application/x-yaml",
            log_time=2_500_000_000,
            create_time=2_000_000_000,
        )

    # Sort the MCAP
    result = sort_mcap(
        input_mcap,
        output_mcap,
        chunk_size=1024,
        chunk_compression="lz4",
        sort_by_topic=True,
    )
    assert result.exists()

    # Verify attachments are preserved
    with McapRecordReaderFactory.from_file(output_mcap) as reader:
        attachments = reader.get_attachments()
        assert len(attachments) == 1

        # Check attachment content
        attachment_by_name = {a.name: a for a in attachments}
        assert "calibration.yaml" in attachment_by_name
        assert attachment_by_name["calibration.yaml"].data == b"key: value"
        assert attachment_by_name["calibration.yaml"].media_type == "application/x-yaml"

        # Also verify messages are still there
        messages = list(reader.get_messages())
        assert len(messages) == 3


def test_sort_preserves_metadata(tmp_path: Path):
    """Test that sort_mcap preserves metadata from the source file."""
    input_mcap = tmp_path / "input.mcap"
    output_mcap = tmp_path / "output_sorted.mcap"

    # Create MCAP with messages and metadata using pybag writer
    with McapFileWriter.open(input_mcap, chunk_size=1024, chunk_compression="lz4") as writer:
        # Write some messages
        writer.write_message("/topic1", 1_000_000_000, std_msgs.String(data="msg1"))
        writer.write_message("/topic2", 2_000_000_000, std_msgs.String(data="msg2"))

        # Write metadata
        writer.write_metadata(
            name="device_info",
            metadata={"device_id": "sensor_123"}
        )

    # Sort the MCAP
    result = sort_mcap(
        input_mcap,
        output_mcap,
        chunk_size=1024,
        chunk_compression="lz4",
        sort_by_log_time=True
    )
    assert result.exists()

    # Verify metadata is preserved
    with McapRecordReaderFactory.from_file(output_mcap) as reader:
        metadata_records = reader.get_metadata()
        assert len(metadata_records) == 1

        # Check metadata content
        metadata_by_name = {m.name: m for m in metadata_records}

        assert "device_info" in metadata_by_name
        assert metadata_by_name["device_info"].metadata == {"device_id": "sensor_123"}

        # Also verify messages are still there
        messages = list(reader.get_messages())
        assert len(messages) == 2
