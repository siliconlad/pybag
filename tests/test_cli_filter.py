from dataclasses import dataclass
from pathlib import Path

import pybag
from pybag.cli.main import main as cli_main
from pybag.io.raw_reader import CrcReader, FileReader
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap_writer import McapFileWriter


def _create_mcap(path: Path) -> None:
    @dataclass
    class Example:
        __msg_name__ = "tests/Example"
        value: pybag.int32

    with McapFileWriter.open(path, chunk_size=1024) as writer:
        writer.add_channel("/foo", Example)
        writer.add_channel("/bar", Example)
        writer.write_message("/foo", int(1e9), Example(1))
        writer.write_message("/bar", int(2e9), Example(2))
        writer.write_message("/foo", int(3e9), Example(3))


def _read_topics(path: Path) -> dict[str, list[int]]:
    reader = CrcReader(FileReader(path))
    McapRecordParser.parse_magic_bytes(reader)
    McapRecordParser.parse_header(reader)

    channels: dict[int, str] = {}
    result: dict[str, list[int]] = {}

    while True:
        record_type = McapRecordParser.peek_record(reader)
        if record_type == 0:
            break
        if record_type == McapRecordType.SCHEMA:
            McapRecordParser.parse_schema(reader)
        elif record_type == McapRecordType.CHANNEL:
            channel = McapRecordParser.parse_channel(reader)
            channels[channel.id] = channel.topic
        elif record_type == McapRecordType.MESSAGE:
            message = McapRecordParser.parse_message(reader)
            topic = channels[message.channel_id]
            result.setdefault(topic, []).append(message.log_time)
        elif record_type == McapRecordType.DATA_END:
            McapRecordParser.parse_data_end(reader)
            break
        else:
            McapRecordParser.skip_record(reader)

    reader.close()
    return result


def test_cli_filter_include_and_time(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"
    _create_mcap(input_path)

    cli_main(
        [
            "filter",
            str(input_path),
            "--include-topic",
            "/foo",
            "--start-time",
            "2",
            "--end-time",
            "4",
            "--output",
            str(output_path),
        ]
    )

    topics = _read_topics(output_path)
    assert set(topics) == {"/foo"}
    assert topics["/foo"] == [int(3e9)]


def test_cli_filter_exclude(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"
    _create_mcap(input_path)

    cli_main(
        [
            "filter",
            str(input_path),
            "--exclude-topic",
            "/bar",
            "--output",
            str(output_path),
        ]
    )

    topics = _read_topics(output_path)
    assert set(topics) == {"/foo"}
    assert topics["/foo"] == [int(1e9), int(3e9)]
