import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from mcap.reader import make_reader
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory

import pybag.ros1.noetic.std_msgs as ros1_std_msgs
import pybag.ros2.humble.std_msgs as ros2_std_msgs
import pybag.types as t
from pybag import __version__
from pybag.encoding.cdr import CdrDecoder
from pybag.encoding.rosmsg import RosMsgDecoder
from pybag.io.raw_reader import BytesReader, CrcReader, FileReader
from pybag.mcap.crc import assert_data_crc
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.record_reader import McapChunkedReader
from pybag.mcap.records import RecordType
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.serialize import MessageSerializerFactory


@dataclass
class SubMessage:
    __msg_name__ = 'tests/msgs/SubMessage'
    value: t.int32


@dataclass
class ExampleMessage:
    __msg_name__ = 'tests/msgs/ExampleMessage'
    integer: t.int32
    text: t.string
    fixed: t.Array[t.int32, Literal[3]]
    dynamic: t.Array[t.int32]
    sub: SubMessage
    sub_array: t.Array[SubMessage, Literal[3]]


# Simple message for profile-parameterized tests
@dataclass
class SimpleMessage:
    __msg_name__ = 'test_msgs/SimpleMessage'
    value: t.int32
    text: t.string


@dataclass
class NestedMessage:
    __msg_name__ = 'test_msgs/NestedMessage'
    header_value: t.uint32
    name: t.string


@dataclass
class ComplexMessage:
    __msg_name__ = 'test_msgs/ComplexMessage'
    integer: t.int32
    text: t.string
    fixed_array: t.Array[t.int32, Literal[3]]
    dynamic_array: t.Array[t.int32]
    nested: NestedMessage


@pytest.mark.parametrize("profile", ["ros1", "ros2"])
@pytest.mark.parametrize("little_endian", [True, False])
def test_serialize_message_roundtrip(little_endian: bool, profile) -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    message_serializer = MessageSerializerFactory.from_profile(profile)
    assert message_serializer is not None
    assert message_serializer.schema_encoding == 'ros2msg' if profile == 'ros2' else 'ros1msg'
    assert message_serializer.message_encoding == 'cdr' if profile == 'ros2' else 'ros1'
    data = message_serializer.serialize_message(msg, little_endian=little_endian)

    decoder = CdrDecoder(data) if profile == 'ros2' else RosMsgDecoder(data)

    # integer
    assert decoder.int32() == 42
    # text
    assert decoder.string() == "hello"
    # fixed
    assert decoder.array("int32", 3) == [1, 2, 3]
    # dynamic
    assert decoder.sequence("int32") == [4, 5]
    # sub
    assert decoder.int32() == 7
    # sub_array
    assert decoder.int32() == 1
    assert decoder.int32() == 2
    assert decoder.int32() == 3


def test_serialize_message_endianness_diff() -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    message_serializer = MessageSerializerFactory.from_profile('ros2')
    assert message_serializer is not None
    le = message_serializer.serialize_message(msg, little_endian=True)
    be = message_serializer.serialize_message(msg, little_endian=False)
    assert le != be


@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_add_channel_and_write_message(profile) -> None:
    @dataclass
    class Example:
        __msg_name__ = "tests/msgs/Example"
        value: t.int32

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with McapFileWriter.open(file_path, profile=profile) as mcap:
            channel_id = mcap.add_channel("/example", schema=Example)
            mcap.write_message("/example", 1, Example(5))
        reader = CrcReader(BytesReader(file_path.read_bytes()))

    # Check the magic bytes
    version = McapRecordParser.parse_magic_bytes(reader)
    assert version == "0"

    # Check the header
    header = McapRecordParser.parse_header(reader)
    assert header.library == f"pybag {__version__}"
    assert header.profile == profile

    # Check the schema
    data_schema = McapRecordParser.parse_schema(reader)
    assert data_schema is not None
    assert data_schema.name == "tests/msgs/Example"
    assert data_schema.encoding == 'ros2msg' if profile == 'ros2' else 'ros1msg'
    assert data_schema.data == "int32 value\n".encode("utf-8")

    # Check the channel
    data_channel = McapRecordParser.parse_channel(reader)
    assert data_channel.id == channel_id
    assert data_channel.schema_id == 1
    assert data_channel.topic == "/example"
    assert data_channel.message_encoding == 'cdr' if profile == 'ros2' else 'ros1'
    assert data_channel.metadata == {}

    # Check the message
    data_message = McapRecordParser.parse_message(reader)
    assert data_message.channel_id == channel_id
    assert data_message.sequence == 1
    assert data_message.log_time == 1
    assert data_message.publish_time == 1
    message_serializer = MessageSerializerFactory.from_profile(profile)
    assert message_serializer is not None
    assert data_message.data == message_serializer.serialize_message(Example(5))

    crc_data_end = reader.get_crc()
    reader.clear_crc()

    # Check the data end
    data_end = McapRecordParser.parse_data_end(reader)
    assert data_end.data_section_crc == crc_data_end

    summary_start = reader.tell()
    reader.clear_crc()

    # Check the summary schema
    summary_schema_start = reader.tell()
    summary_schema = McapRecordParser.parse_schema(reader)
    assert summary_schema is not None
    assert summary_schema == data_schema

    # Check the summary channel
    summary_channel_start = reader.tell()
    summary_channel = McapRecordParser.parse_channel(reader)
    assert summary_channel == data_channel

    # Check the summary statistics
    summary_stats_start = reader.tell()
    stats = McapRecordParser.parse_statistics(reader)
    assert stats.message_count == 1
    assert stats.schema_count == 1
    assert stats.channel_count == 1
    assert stats.attachment_count == 0
    assert stats.metadata_count == 0
    assert stats.chunk_count == 0
    assert stats.message_start_time == 1
    assert stats.message_end_time == 1
    assert stats.channel_message_counts == {channel_id: 1}

    summary_offset_start = reader.tell()

    # Check the summary offsets
    offset_schema = McapRecordParser.parse_summary_offset(reader)
    assert offset_schema.group_opcode == RecordType.SCHEMA
    assert offset_schema.group_start == summary_schema_start
    # TODO: Test the group length

    # Check the summary offsets
    offset_channel = McapRecordParser.parse_summary_offset(reader)
    assert offset_channel.group_opcode == RecordType.CHANNEL
    assert offset_channel.group_start == summary_channel_start
    # TODO: Test the group length

    # Check the summary offsets
    offset_stats = McapRecordParser.parse_summary_offset(reader)
    assert offset_stats.group_opcode == RecordType.STATISTICS
    assert offset_stats.group_start == summary_stats_start
    # TODO: Test the group length

    # Get the crc value without reading the summary_crc
    footer_start = reader.tell()
    _ = McapRecordParser._parse_uint8(reader)   # record_type
    _ = McapRecordParser._parse_uint64(reader)  # length
    _ = McapRecordParser._parse_uint64(reader)  # summary_start
    _ = McapRecordParser._parse_uint64(reader)  # summary_offset_start
    crc_footer = reader.get_crc()
    reader.seek_from_start(footer_start)

    # Check the footer
    footer = McapRecordParser.parse_footer(reader)
    assert footer.summary_start == summary_start
    assert footer.summary_offset_start == summary_offset_start
    assert footer.summary_crc == crc_footer

    # Check the summary magic bytes
    summary_version = McapRecordParser.parse_magic_bytes(reader)
    assert summary_version == version


def test_invalid_profile() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with pytest.raises(ValueError, match="Unknown encoding type"):
            McapFileWriter.open(file_path, profile="invalid_profile")  # type: ignore


@pytest.mark.parametrize(
    "chunk_size,chunk_compression",
    [
        pytest.param(10, "lz4", id="chunked_lz4"),
        pytest.param(10, "zstd", id="chunked_zstd"),
    ],
)
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_chunk_roundtrip(chunk_size, chunk_compression, profile) -> None:
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "data.mcap"
        with McapFileWriter.open(
            path,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=profile
        ) as writer:
            writer.write_message("/pybag", 0, String(data="a"))
            writer.write_message("/pybag", 1, String(data="b"))

        # Check we can read the messages correctly
        decoder_factory = Ros1DecoderFactory() if profile == "ros1" else Ros2DecoderFactory()
        with open(path, "rb") as f:
            reader = make_reader(f, decoder_factories=[decoder_factory])
            msgs = [m.data for _, _, _, m in reader.iter_decoded_messages()]
            assert msgs == ["a", "b"]

        # Check we can read the chunk indexes correctly
        with McapChunkedReader.from_file(path) as random_reader:
            chunk_indexes = random_reader.get_chunk_indexes()
            assert len(chunk_indexes) == 2
            assert all(c.compression == chunk_compression for c in chunk_indexes)


@pytest.mark.parametrize(
    "chunk_size,chunk_compression",
    [
        pytest.param(None, None, id="non_chunked"),
        pytest.param(1024, "lz4", id="chunked_lz4"),
        pytest.param(1024, "zstd", id="chunked_zstd"),
    ],
)
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_attachments_roundtrip(chunk_size, chunk_compression, profile):
    """Test reading attachments from an MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with attachments
        with McapFileWriter.open(
            temp_path,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=profile,
        ) as writer:
            writer.write_attachment(
                name="file1.txt",
                data=b"content1",
                media_type="text/plain",
                log_time=1000,
                create_time=2000,
            )
            writer.write_attachment(
                name="file2.bin",
                data=b"\x00\x01\x02",
                media_type="application/octet-stream",
                log_time=2000,
                create_time=3000,
            )
            writer.write_attachment(
                name="file1.txt",  # Duplicate name
                data=b"content2",
                media_type="text/plain",
                log_time=3000,
                create_time=4000,
            )

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 3
            assert stats.metadata_count == 0

        # Read all attachments
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
            all_attachments = reader.get_attachments()
            assert len(all_attachments) == 3

            # Group attachments by name since order is not guaranteed
            attachments_by_name = {}
            for att in all_attachments:
                if att.name not in attachments_by_name:
                    attachments_by_name[att.name] = []
                attachments_by_name[att.name].append(att)

            # Verify file1.txt attachments (should have 2)
            # Sort by log_time to ensure consistent ordering
            file1_atts = sorted(attachments_by_name["file1.txt"], key=lambda x: x.log_time)
            assert len(file1_atts) == 2
            assert file1_atts[0].data == b"content1"
            assert file1_atts[0].media_type == "text/plain"
            assert file1_atts[0].log_time == 1000
            assert file1_atts[0].create_time == 2000
            assert file1_atts[1].data == b"content2"
            assert file1_atts[1].media_type == "text/plain"
            assert file1_atts[1].log_time == 3000
            assert file1_atts[1].create_time == 4000

            # Verify file2.bin attachment
            assert len(attachments_by_name["file2.bin"]) == 1
            assert attachments_by_name["file2.bin"][0].data == b"\x00\x01\x02"
            assert attachments_by_name["file2.bin"][0].media_type == "application/octet-stream"
            assert attachments_by_name["file2.bin"][0].log_time == 2000
            assert attachments_by_name["file2.bin"][0].create_time == 3000

        # Read attachments by name
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
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


@pytest.mark.parametrize(
    "chunk_size,chunk_compression",
    [
        pytest.param(None, None, id="non_chunked"),
        pytest.param(1024, "lz4", id="chunked_lz4"),
        pytest.param(1024, "zstd", id="chunked_zstd"),
    ],
)
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_metadata_roundtrip(chunk_size, chunk_compression, profile):
    """Test reading metadata from an MCAP file."""
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
        temp_path = Path(f.name)

    try:
        # Write MCAP with metadata
        with McapFileWriter.open(
            temp_path,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=profile,
        ) as writer:
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

        # Read and verify statistics
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
            stats = reader._reader.get_statistics()
            assert stats.attachment_count == 0
            assert stats.metadata_count == 3

        # Read all metadata
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
            all_metadata = reader.get_metadata()
            assert len(all_metadata) == 3

            # Group metadata by name since order is not guaranteed
            metadata_by_name = {}
            for meta in all_metadata:
                if meta.name not in metadata_by_name:
                    metadata_by_name[meta.name] = []
                metadata_by_name[meta.name].append(meta)

            # Verify device_info metadata (should have 2)
            assert len(metadata_by_name["device_info"]) == 2
            assert metadata_by_name["device_info"][0].metadata == {"device_id": "123", "firmware": "v1.2.3"}
            assert metadata_by_name["device_info"][1].metadata == {"device_id": "456", "firmware": "v2.0.0"}

            # Verify session_info metadata
            assert len(metadata_by_name["session_info"]) == 1
            assert metadata_by_name["session_info"][0].metadata == {"location": "lab", "operator": "alice"}

        # Read metadata by name
        with McapFileReader.from_file(temp_path) as reader:
            assert reader.profile == profile
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


# =============================================================================
# Append Mode Tests
# =============================================================================

@pytest.mark.parametrize('write_chunk_size', [None, 1024])
@pytest.mark.parametrize('append_chunk_size', [None, 1024])
@pytest.mark.parametrize('chunk_compression', [None, 'lz4', 'zstd'])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_append_mode_basic(
    tmp_path: Path,
    write_chunk_size: int | None,
    append_chunk_size: int | None,
    chunk_compression: Literal['lz4', 'zstd'] | None,
    profile: Literal["ros1", "ros2"],
):
    """Test basic append mode - append messages to existing file."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    temp_path = tmp_path / 'test.mcap'

    # Write initial MCAP file
    with McapFileWriter.open(
        temp_path,
        mode="w",
        chunk_size=write_chunk_size,
        chunk_compression=chunk_compression,
        profile=profile,
    ) as writer:
        writer.write_message("/topic1", 1000, String(data="msg1"))
        writer.write_message("/topic1", 2000, String(data="msg2"))

    # Verify initial file
    with McapFileReader.from_file(temp_path) as reader:
        msgs = list(reader.messages("*"))
        assert len(msgs) == 2
        stats = reader._reader.get_statistics()
        assert stats.message_count == 2

    # Append to the file
    with McapFileWriter.open(
        temp_path,
        mode="a",
        profile=profile,
        chunk_size=append_chunk_size,
        chunk_compression=chunk_compression,
    ) as writer:
        writer.write_message("/topic1", 3000, String(data="msg3"))
        writer.write_message("/topic1", 4000, String(data="msg4"))

    # Verify appended file
    with McapFileReader.from_file(temp_path) as reader:
        msgs = list(reader.messages("*"))
        assert [msg.sequence for msg in msgs] == [1, 2, 3, 4]
        assert [msg.data.data for msg in msgs] == ['msg1', 'msg2', 'msg3', 'msg4']

        stats = reader._reader.get_statistics()
        assert stats.message_count == 4
        assert stats.message_start_time == 1000
        assert stats.message_end_time == 4000


@pytest.mark.parametrize('chunk_size', [None, 1024])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_append_mode_crc_preserved(
    tmp_path: Path,
    chunk_size: int | None,
    profile: Literal["ros1", "ros2"],
) -> None:
    """Ensure appended files keep a valid data section CRC."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    temp_path = tmp_path / 'crc_append.mcap'

    with McapFileWriter.open(
        temp_path,
        mode="w",
        chunk_size=chunk_size,
        chunk_compression=None,
        profile=profile,
    ) as writer:
        writer.write_message("/topic", 1000, String(data="first"))

    with McapFileWriter.open(
        temp_path,
        mode="a",
        chunk_size=chunk_size,
        chunk_compression=None,
    ) as writer:
        writer.write_message("/topic", 2000, String(data="second"))

    with FileReader(temp_path) as reader:
        assert_data_crc(reader)


@pytest.mark.parametrize('write_chunk_size', [None, 1024])
@pytest.mark.parametrize('append_chunk_size', [None, 1024])
@pytest.mark.parametrize('chunk_compression', [None, 'lz4', 'zstd'])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_append_mode_new_topic(
    tmp_path: Path,
    write_chunk_size: int | None,
    append_chunk_size: int | None,
    chunk_compression: Literal['lz4', 'zstd'] | None,
    profile: Literal["ros1", "ros2"],
):
    """Test append mode with a new topic not in the original file."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    Int32 = ros1_std_msgs.Int32 if profile == 'ros1' else ros2_std_msgs.Int32
    temp_path = tmp_path / 'test.mcap'

    # Write initial MCAP file with one topic
    with McapFileWriter.open(
        temp_path,
        mode="w",
        chunk_size=write_chunk_size,
        chunk_compression=chunk_compression,
        profile=profile,
    ) as writer:
        writer.write_message("/topic1", 1000, String(data="msg1"))

    # Append with a different topic
    with McapFileWriter.open(
        temp_path,
        mode="a",
        profile=profile,
        chunk_size=append_chunk_size,
        chunk_compression=chunk_compression,
    ) as writer:
        writer.write_message("/topic2", 2000, Int32(data=42))

    # Verify both topics exist
    with McapFileReader.from_file(temp_path) as reader:
        topics = reader.get_topics()
        assert len(topics) == 2
        assert "/topic1" in topics
        assert "/topic2" in topics

        msgs = list(reader.messages("*"))
        assert len(msgs) == 2


@pytest.mark.parametrize('write_chunk_size', [None, 1024])
@pytest.mark.parametrize('append_chunk_size', [None, 1024])
@pytest.mark.parametrize('chunk_compression', [None, 'lz4', 'zstd'])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_append_mode_attachments(
    tmp_path: Path,
    write_chunk_size: int | None,
    append_chunk_size: int | None,
    chunk_compression: Literal['lz4', 'zstd'] | None,
    profile: Literal["ros1", "ros2"],
):
    """Test append mode preserves and adds attachments."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    temp_path = tmp_path / 'test.mcap'

    # Write initial file with attachment
    with McapFileWriter.open(
        temp_path,
        mode="w",
        chunk_size=write_chunk_size,
        chunk_compression=chunk_compression,
        profile=profile,
    ) as writer:
        writer.write_message("/test", 1000, String(data="msg1"))
        writer.write_attachment(
            name="file1.txt",
            data=b"original content",
            media_type="text/plain",
            log_time=1000,
        )

    # Append with another attachment
    with McapFileWriter.open(
        temp_path,
        mode="a",
        chunk_size=append_chunk_size,
        chunk_compression=chunk_compression,
    ) as writer:
        writer.write_message("/test", 2000, String(data="msg2"))
        writer.write_attachment(
            name="file2.txt",
            data=b"appended content",
            media_type="text/plain",
            log_time=2000,
        )

    # Verify all attachments are present
    with McapFileReader.from_file(temp_path) as reader:
        attachments = reader.get_attachments()
        assert len(attachments) == 2

        names = [a.name for a in attachments]
        assert "file1.txt" in names
        assert "file2.txt" in names

        stats = reader._reader.get_statistics()
        assert stats.attachment_count == 2


@pytest.mark.parametrize('write_chunk_size', [None, 1024])
@pytest.mark.parametrize('append_chunk_size', [None, 1024])
@pytest.mark.parametrize('chunk_compression', [None, 'lz4', 'zstd'])
@pytest.mark.parametrize("profile", ["ros1", "ros2"])
def test_append_mode_metadata(
    tmp_path: Path,
    write_chunk_size: int | None,
    append_chunk_size: int | None,
    chunk_compression: Literal['lz4', 'zstd'] | None,
    profile: Literal["ros1", "ros2"],
):
    """Test append mode preserves and adds metadata."""
    String = ros1_std_msgs.String if profile == 'ros1' else ros2_std_msgs.String
    temp_path = tmp_path / 'test.mcap'

    # Write initial file with metadata
    with McapFileWriter.open(
        temp_path,
        mode="w",
        chunk_size=write_chunk_size,
        chunk_compression=chunk_compression,
        profile=profile,
    ) as writer:
        writer.write_message("/test", 1000, String(data="msg1"))
        writer.write_metadata(name="info1", metadata={"key": "value1"})

    # Append with more metadata
    with McapFileWriter.open(
        temp_path,
        mode="a",
        chunk_size=append_chunk_size,
        chunk_compression=chunk_compression,
    ) as writer:
        writer.write_message("/test", 2000, String(data="msg2"))
        writer.write_metadata(name="info2", metadata={"key": "value2"})

    # Verify all metadata is present
    with McapFileReader.from_file(temp_path) as reader:
        metadata = reader.get_metadata()
        assert len(metadata) == 2

        names = [m.name for m in metadata]
        assert "info1" in names
        assert "info2" in names

        stats = reader._reader.get_statistics()
        assert stats.metadata_count == 2


def test_append_mode_file_not_exists(tmp_path: Path):
    """Test that append mode fails on non-existent file."""
    file_path = tmp_path / "nonexistent.mcap"
    with pytest.raises(FileNotFoundError):
        McapFileWriter.open(file_path, mode="a")
