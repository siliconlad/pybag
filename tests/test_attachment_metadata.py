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


def test_read_attachments():
    """Test reading attachments from an MCAP file."""
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
            )
            writer.write_attachment(
                name="file2.bin",
                data=b"\x00\x01\x02",
                media_type="application/octet-stream",
                log_time=2000,
            )
            writer.write_attachment(
                name="file1.txt",  # Duplicate name
                data=b"content2",
                media_type="text/plain",
                log_time=3000,
            )

        # Read all attachments
        with McapFileReader.from_file(temp_path) as reader:
            all_attachments = reader.get_attachments()
            assert len(all_attachments) == 3
            assert all_attachments[0].name == "file1.txt"
            assert all_attachments[0].data == b"content1"
            assert all_attachments[0].media_type == "text/plain"
            assert all_attachments[1].name == "file2.bin"
            assert all_attachments[1].data == b"\x00\x01\x02"

        # Read attachments by name
        with McapFileReader.from_file(temp_path) as reader:
            file1_attachments = reader.get_attachments(name="file1.txt")
            assert len(file1_attachments) == 2
            assert all(a.name == "file1.txt" for a in file1_attachments)

            file2_attachments = reader.get_attachments(name="file2.bin")
            assert len(file2_attachments) == 1
            assert file2_attachments[0].name == "file2.bin"

            # Non-existent name should return empty list
            no_attachments = reader.get_attachments(name="nonexistent.txt")
            assert len(no_attachments) == 0

    finally:
        temp_path.unlink()


def test_read_metadata():
    """Test reading metadata from an MCAP file."""
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
            writer.write_metadata(
                name="device_info",  # Duplicate name
                metadata={"device_id": "456", "firmware": "v2.0.0"}
            )

        # Read all metadata
        with McapFileReader.from_file(temp_path) as reader:
            all_metadata = reader.get_metadata()
            assert len(all_metadata) == 3
            assert all_metadata[0].name == "device_info"
            assert all_metadata[0].metadata == {"device_id": "123", "firmware": "v1.2.3"}
            assert all_metadata[1].name == "session_info"
            assert all_metadata[1].metadata == {"location": "lab", "operator": "alice"}

        # Read metadata by name
        with McapFileReader.from_file(temp_path) as reader:
            device_metadata = reader.get_metadata(name="device_info")
            assert len(device_metadata) == 2
            assert all(m.name == "device_info" for m in device_metadata)

            session_metadata = reader.get_metadata(name="session_info")
            assert len(session_metadata) == 1
            assert session_metadata[0].name == "session_info"
            assert session_metadata[0].metadata == {"location": "lab", "operator": "alice"}

            # Non-existent name should return empty list
            no_metadata = reader.get_metadata(name="nonexistent")
            assert len(no_metadata) == 0

    finally:
        temp_path.unlink()


def test_read_attachments_chunked():
    """Test reading attachments from a chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write chunked MCAP with attachments
        with McapFileWriter.open(temp_path, chunk_size=1024, chunk_compression="lz4") as writer:
            writer.write_attachment(
                name="config.yaml",
                data=b"key: value",
                media_type="text/yaml",
            )

        # Read attachments
        with McapFileReader.from_file(temp_path) as reader:
            attachments = reader.get_attachments()
            assert len(attachments) == 1
            assert attachments[0].name == "config.yaml"
            assert attachments[0].data == b"key: value"

    finally:
        temp_path.unlink()


def test_read_metadata_chunked():
    """Test reading metadata from a chunked MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write chunked MCAP with metadata
        with McapFileWriter.open(temp_path, chunk_size=1024, chunk_compression="lz4") as writer:
            writer.write_metadata(
                name="test",
                metadata={"key": "value"}
            )

        # Read metadata
        with McapFileReader.from_file(temp_path) as reader:
            metadata = reader.get_metadata()
            assert len(metadata) == 1
            assert metadata[0].name == "test"
            assert metadata[0].metadata == {"key": "value"}

    finally:
        temp_path.unlink()
