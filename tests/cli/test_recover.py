from pathlib import Path

import pytest

from pybag.cli.main import main as cli_main
from pybag.io.raw_reader import FileReader
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32


def test_cli_recover_valid_file(tmp_path: Path) -> None:
    """Test that recovery works on a valid MCAP file."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid MCAP file
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

    # Run recovery command
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
        ]
    )

    # Verify that all messages were recovered
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/foo", "/bar"}

        foo_messages = list(reader.messages("/foo"))
        assert len(foo_messages) == 2
        assert [m.data.data for m in foo_messages] == [1, 3]

        bar_messages = list(reader.messages("/bar"))
        assert len(bar_messages) == 1
        assert [m.data.data for m in bar_messages] == [2]


def test_cli_recover_with_compression(tmp_path: Path) -> None:
    """Test recovery with compression options."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid MCAP file
    with McapFileWriter.open(input_path, chunk_size=1024, chunk_compression="lz4") as writer:
        for i in range(10):
            writer.write_message("/test", int((i + 1) * 1e9), Int32(data=i))

    # Run recovery command with zstd compression
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
            "--chunk-compression",
            "zstd",
            "--chunk-size",
            "512",
        ]
    )

    # Verify that all messages were recovered
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 10
        assert [m.data.data for m in messages] == list(range(10))


def test_cli_recover_overwrite(tmp_path: Path) -> None:
    """Test that recovery respects overwrite flag."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create input file
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), Int32(data=1))

    # Create existing output file
    with McapFileWriter.open(output_path, chunk_size=1024) as writer:
        writer.write_message("/old", int(1e9), Int32(data=999))

    # Try to recover without overwrite flag - should raise error
    with pytest.raises(ValueError):
        cli_main(
            [
                "recover",
                str(input_path),
                "--output",
                str(output_path),
            ]
        )

    # Now recover with overwrite flag - should succeed
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
            "--overwrite",
        ]
    )

    # Verify the output has the new data
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/test"}  # Not /old
        messages = list(reader.messages("/test"))
        assert len(messages) == 1
        assert messages[0].data.data == 1  # Not 999


def test_cli_recover_corrupted_chunk(tmp_path: Path) -> None:
    """Test recovery from MCAP with corrupted data inside a chunk.

    Creates an MCAP with multiple chunks, then corrupts the compressed data
    in the second chunk. Recovery should save messages from the first chunk
    and skip the corrupted second chunk.
    """
    input_path = tmp_path / "input.mcap"
    corrupted_path = tmp_path / "corrupted.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid MCAP file with small chunk size to force multiple chunks
    with McapFileWriter.open(input_path, chunk_size=100, chunk_compression="lz4") as writer:
        # Write messages that will span multiple chunks
        for i in range(10):
            writer.write_message("/test", int((i + 1) * 1e9), Int32(data=i))

    # Read and count chunks to find the second chunk's location
    with FileReader(input_path) as reader:
        McapRecordParser.parse_magic_bytes(reader)
        McapRecordParser.parse_header(reader)

        chunk_positions = []
        while True:
            pos = reader.tell()
            record_type = McapRecordParser.peek_record(reader)
            if record_type == 0 or record_type == McapRecordType.DATA_END:
                break
            if record_type == McapRecordType.CHUNK:
                chunk_positions.append(pos)
            McapRecordParser.skip_record(reader)

    assert len(chunk_positions) >= 2, "Need at least 2 chunks for this test"

    # Create corrupted file by copying and corrupting the second chunk
    with open(input_path, 'rb') as f:
        data = bytearray(f.read())

    # Corrupt the second chunk's compressed data (offset into chunk record)
    # Chunk header: 1 byte type + 8 bytes length + 8+8+8+4 bytes fields + string
    # We'll corrupt some bytes after the chunk header
    corrupt_offset = chunk_positions[1] + 50  # Offset into chunk data
    if corrupt_offset < len(data) - 10:
        # Corrupt several bytes to ensure decompression fails
        for i in range(10):
            data[corrupt_offset + i] = 0xFF

    with open(corrupted_path, 'wb') as f:
        f.write(data)

    # Run recovery command
    cli_main(
        [
            "recover",
            str(corrupted_path),
            "--output",
            str(output_path),
            "--verbose",
        ]
    )

    # Verify that at least some messages were recovered
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/test"))
        # Should have recovered messages from chunks before and after corruption
        # (corrupted chunk is skipped, but other chunks are still processed)
        assert len(messages) > 0, "Should have recovered at least some messages"
        assert len(messages) < 10, "Should not have recovered all messages"
        # Verify all recovered messages have valid data values (0-9)
        for msg in messages:
            assert 0 <= msg.data.data < 10, f"Invalid message data: {msg.data.data}"


def test_cli_recover_corrupted_non_chunked(tmp_path: Path) -> None:
    """Test recovery from MCAP with corrupted message outside of chunks.

    Creates a non-chunked MCAP, then truncates it mid-message.
    Recovery should save all complete messages before the truncation.
    """
    input_path = tmp_path / "input.mcap"
    corrupted_path = tmp_path / "corrupted.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid non-chunked MCAP file
    with McapFileWriter.open(input_path, chunk_size=None) as writer:
        for i in range(5):
            writer.write_message("/test", int((i + 1) * 1e9), Int32(data=i))

    # Find the position of messages in the file
    message_positions = []
    with FileReader(input_path) as reader:
        McapRecordParser.parse_magic_bytes(reader)
        McapRecordParser.parse_header(reader)

        while True:
            pos = reader.tell()
            record_type = McapRecordParser.peek_record(reader)
            if record_type == 0 or record_type == McapRecordType.DATA_END:
                break
            if record_type == McapRecordType.MESSAGE:
                message_positions.append(pos)
            McapRecordParser.skip_record(reader)

    assert len(message_positions) == 5, "Should have 5 messages"

    # Truncate the file in the middle of the 4th message
    # This simulates a crash during writing
    truncate_pos = message_positions[3] + 5  # A few bytes into the 4th message

    with open(input_path, 'rb') as f:
        data = f.read(truncate_pos)

    with open(corrupted_path, 'wb') as f:
        f.write(data)

    # Run recovery command
    cli_main(
        [
            "recover",
            str(corrupted_path),
            "--output",
            str(output_path),
            "--verbose",
        ]
    )

    # Verify that messages before the corruption were recovered
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/test"))
        # Should have recovered the first 3 messages (before truncation point)
        assert len(messages) == 3, f"Expected 3 messages, got {len(messages)}"
        assert [m.data.data for m in messages] == [0, 1, 2]
