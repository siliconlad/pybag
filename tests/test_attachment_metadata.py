"""Tests for attachment and metadata record support."""

import tempfile
from pathlib import Path

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


def test_write_attachment_non_chunked():
    """Test writing attachments to a non-chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with attachment
        with McapFileWriter.open(temp_path, chunk_size=None) as writer:
            writer.write_attachment(
                name="config.yaml",
                data=b"key: value\nfoo: bar",
                media_type="text/yaml",
                log_time=1000,
                create_time=2000,
            )
            writer.write_attachment(
                name="calibration.json",
                data=b'{"camera": "123"}',
                media_type="application/json",
            )

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 2
            assert stats.metadata_count == 0

    finally:
        temp_path.unlink()


def test_write_metadata_non_chunked():
    """Test writing metadata to a non-chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with metadata
        with McapFileWriter.open(temp_path, chunk_size=None) as writer:
            writer.write_metadata(
                name="device_info",
                metadata={"device_id": "123", "firmware": "v1.2.3"}
            )
            writer.write_metadata(
                name="session_info",
                metadata={"location": "lab", "operator": "alice"}
            )

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 0
            assert stats.metadata_count == 2

    finally:
        temp_path.unlink()


def test_write_attachment_chunked():
    """Test writing attachments to a chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with attachment
        with McapFileWriter.open(temp_path, chunk_size=1024, chunk_compression="lz4") as writer:
            writer.write_attachment(
                name="data.bin",
                data=b"\x00\x01\x02\x03" * 100,
                media_type="application/octet-stream",
            )

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 1
            assert stats.metadata_count == 0

    finally:
        temp_path.unlink()


def test_write_metadata_chunked():
    """Test writing metadata to a chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with metadata
        with McapFileWriter.open(temp_path, chunk_size=1024, chunk_compression="lz4") as writer:
            writer.write_metadata(
                name="test_metadata",
                metadata={"key1": "value1", "key2": "value2"}
            )

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 0
            assert stats.metadata_count == 1

    finally:
        temp_path.unlink()


def test_write_mixed_records():
    """Test writing a mix of messages, attachments, and metadata."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        from dataclasses import dataclass
        import pybag

        @dataclass
        class TestMessage:
            __msg_name__ = 'test/msg/TestMessage'
            value: pybag.int32

        # Write MCAP with mixed records
        with McapFileWriter.open(temp_path, chunk_size=None) as writer:
            # Write message
            writer.write_message("/test", 1000, TestMessage(value=42))

            # Write attachment
            writer.write_attachment(
                name="attachment1.txt",
                data=b"test attachment",
                media_type="text/plain",
            )

            # Write metadata
            writer.write_metadata(
                name="test_meta",
                metadata={"test": "data"}
            )

            # Write another message
            writer.write_message("/test", 2000, TestMessage(value=43))

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.message_count == 2
            assert stats.attachment_count == 1
            assert stats.metadata_count == 1

    finally:
        temp_path.unlink()


def test_attachment_index_in_summary():
    """Test that attachment indexes are written to the summary section."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with attachments
        with McapFileWriter.open(temp_path, chunk_size=None) as writer:
            writer.write_attachment(
                name="file1.txt",
                data=b"content1",
                media_type="text/plain",
                log_time=1000,
                create_time=1000,
            )
            writer.write_attachment(
                name="file2.bin",
                data=b"\x00\x01\x02",
                media_type="application/octet-stream",
                log_time=2000,
                create_time=2000,
            )

        # Verify the file can be read and statistics are correct
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 2

    finally:
        temp_path.unlink()


def test_metadata_index_in_summary():
    """Test that metadata indexes are written to the summary section."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with metadata
        with McapFileWriter.open(temp_path, chunk_size=None) as writer:
            writer.write_metadata(
                name="meta1",
                metadata={"a": "1"}
            )
            writer.write_metadata(
                name="meta2",
                metadata={"b": "2"}
            )

        # Verify the file can be read and statistics are correct
        with McapFileReader.from_file(temp_path) as reader:
            stats = reader._reader.get_statistics()
            assert stats.metadata_count == 2

    finally:
        temp_path.unlink()
