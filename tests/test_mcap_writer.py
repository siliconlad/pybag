import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag
import pybag.types as t
from pybag.encoding.cdr import CdrDecoder
from pybag.io.raw_reader import BytesReader
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.records import RecordType
from pybag.mcap_writer import McapFileWriter, serialize_message


@dataclass
class SubMessage:
    value: pybag.int32


@dataclass
class ExampleMessage:
    integer: pybag.int32
    text: pybag.string
    fixed: pybag.Array(pybag.int32, length=3)
    dynamic: pybag.Array(pybag.int32, length=None)
    sub: pybag.Complex(SubMessage)
    sub_array: pybag.Array(pybag.Complex(SubMessage), length=3)


@pytest.mark.parametrize("little_endian", [True, False])
def test_serialize_message_roundtrip(little_endian: bool) -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    data = serialize_message(msg, little_endian=little_endian)

    decoder = CdrDecoder(data)
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
    le = serialize_message(msg, little_endian=True)
    be = serialize_message(msg, little_endian=False)
    assert le != be


def test_add_channel_and_write_message() -> None:
    @dataclass
    class Example:
        value: t.int32

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with McapFileWriter.open(file_path) as mcap:
            channel_id = mcap.add_channel("/example", Example)
            mcap.write_message("/example", 1, Example(5))
        reader = BytesReader(file_path.read_bytes())

    # Check the magic bytes
    version = McapRecordParser.parse_magic_bytes(reader)
    assert version == "0"

    # Check the header
    header = McapRecordParser.parse_header(reader)
    assert header.library == "pybag"
    assert header.profile == "ros2"

    # Check the schema
    data_schema = McapRecordParser.parse_schema(reader)
    assert data_schema.name == "Example"
    assert data_schema.encoding == "cdr"
    assert data_schema.data == serialize_message(Example(5))

    # Check the channel
    data_channel = McapRecordParser.parse_channel(reader)
    assert data_channel.id == channel_id
    assert data_channel.schema_id == 1
    assert data_channel.topic == "/example"
    assert data_channel.message_encoding == "cdr"
    assert data_channel.metadata == {}

    # Check the message
    data_message = McapRecordParser.parse_message(reader)
    assert data_message.channel_id == channel_id
    assert data_message.sequence == 0
    assert data_message.log_time == 1
    assert data_message.publish_time == 1
    assert data_message.data == serialize_message(Example(5))

    # Check the data end
    data_end = McapRecordParser.parse_data_end(reader)
    assert data_end.data_section_crc == 0  # TODO: Test the actual crc value
    assert data_end.data_section_length == 0

    summary_start = reader.tell()

    # Check the summary schema
    summary_schema_start = reader.tell()
    summary_schema = McapRecordParser.parse_schema(reader)
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

    # Check the footer
    footer = McapRecordParser.parse_footer(reader)
    assert footer.summary_start == summary_start
    assert footer.summary_offset_start == summary_offset_start
    # TODO: Test the summary crc

    summary_version = McapRecordParser.parse_magic_bytes(reader)
    assert summary_version == version
