"""Tests for the sort-by-topic CLI command."""
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from pybag.cli.mcap_sort_by_topic import sort_by_topic
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
    """Test that sort_by_topic creates a valid output file."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output_sorted.mcap"

        result = sort_by_topic(input_mcap, output_mcap, chunk_size=1024, chunk_compression="lz4")

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
    """Test that sort_by_topic uses default output path."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")

        result = sort_by_topic(input_mcap, chunk_size=1024, chunk_compression="lz4")

        # Should create output with _sorted suffix
        expected = input_mcap.with_name(f"{input_mcap.stem}_sorted.mcap")
        assert result.resolve() == expected.resolve()
        assert result.exists()


def test_sort_by_topic_same_input_output_error():
    """Test that using same input and output raises error."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")

        with pytest.raises(ValueError, match="Input path cannot be same as output"):
            sort_by_topic(input_mcap, input_mcap)


def test_sort_by_topic_overwrite():
    """Test that overwrite flag works."""
    with TemporaryDirectory() as tmpdir:
        input_mcap = create_test_mcap_with_multiple_topics(Path(tmpdir) / "input")
        output_mcap = Path(tmpdir) / "output.mcap"

        # Create initial output
        sort_by_topic(input_mcap, output_mcap, chunk_size=1024)

        # Should fail without overwrite flag
        with pytest.raises(ValueError, match="Output mcap exists"):
            sort_by_topic(input_mcap, output_mcap, chunk_size=1024)

        # Should succeed with overwrite flag
        result = sort_by_topic(input_mcap, output_mcap, chunk_size=1024, overwrite=True)
        assert result.exists()
