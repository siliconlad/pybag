"""Tests for the sort CLI command."""
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from pybag.cli.mcap_sort import sort_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory


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
