import tempfile
from dataclasses import dataclass
from pathlib import Path

import pybag.types as t
from pybag.io.raw_reader import BytesReader
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap_writer import McapFileWriter, serialize_message


def test_add_channel_and_write_message() -> None:
    @dataclass
    class Example:
        value: t.int32

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with McapFileWriter.open(file_path) as mcap:
            channel_id = mcap.add_channel("/example", Example)
            mcap.write_message(channel_id, 1, Example(5))

        reader = BytesReader(file_path.read_bytes())

    McapRecordParser.parse_magic_bytes(reader)
    McapRecordParser.parse_header(reader)
    schema = McapRecordParser.parse_schema(reader)
    channel = McapRecordParser.parse_channel(reader)
    message = McapRecordParser.parse_message(reader)

    assert schema.name == "Example"
    assert channel.id == channel_id
    assert channel.topic == "/example"
    assert message.channel_id == channel_id
    assert message.sequence == 0
    assert message.log_time == 1
    assert message.data == serialize_message(Example(5))


def test_summary_section_written() -> None:
    @dataclass
    class Example:
        value: t.int32

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with McapFileWriter.open(file_path) as mcap:
            ch = mcap.add_channel("/example", Example)
            mcap.write_message(ch, 10, Example(7))

        reader = BytesReader(file_path.read_bytes())
    McapRecordParser.parse_magic_bytes(reader)
    McapRecordParser.parse_header(reader)
    McapRecordParser.parse_schema(reader)
    McapRecordParser.parse_channel(reader)
    McapRecordParser.parse_message(reader)
    McapRecordParser.parse_data_end(reader)
    summary_start = reader.tell()
    McapRecordParser.parse_schema(reader)
    McapRecordParser.parse_channel(reader)
    stats = McapRecordParser.parse_statistics(reader)
    summary_offset_start = reader.tell()
    offset_schema = McapRecordParser.parse_summary_offset(reader)
    offset_channel = McapRecordParser.parse_summary_offset(reader)
    offset_stats = McapRecordParser.parse_summary_offset(reader)
    footer = McapRecordParser.parse_footer(reader)
    McapRecordParser.parse_magic_bytes(reader)

    assert stats.message_count == 1
    assert footer.summary_start == summary_start
    assert footer.summary_offset_start == summary_offset_start
    assert offset_schema.group_opcode == 0x03
    assert offset_channel.group_opcode == 0x04
    assert offset_stats.group_opcode == 0x0B

