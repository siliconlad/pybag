import tempfile
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from typing import Literal

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag
import pybag.ros2.humble.std_msgs as std_msgs
from pybag import __version__
from pybag.encoding.cdr import CdrDecoder
from pybag.io.raw_reader import BytesReader, CrcReader
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.record_reader import McapChunkedReader
from pybag.mcap.records import RecordType
from pybag.mcap_writer import McapFileWriter
from pybag.serialize import MessageSerializerFactory


@dataclass
class SubMessage:
    __msg_name__ = 'tests/msgs/SubMessage'
    value: pybag.int32


@dataclass
class ExampleMessage:
    __msg_name__ = 'tests/msgs/ExampleMessage'
    integer: pybag.int32
    text: pybag.string
    fixed: pybag.Array[pybag.int32, Literal[3]]
    dynamic: pybag.Array[pybag.int32]
    sub: pybag.Complex[SubMessage]
    sub_array: pybag.Array[pybag.Complex[SubMessage], Literal[3]]


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
    message_serializer = MessageSerializerFactory.from_profile('ros2')
    assert message_serializer is not None
    data = message_serializer.serialize_message(msg, little_endian=little_endian)

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
    message_serializer = MessageSerializerFactory.from_profile('ros2')
    assert message_serializer is not None
    le = message_serializer.serialize_message(msg, little_endian=True)
    be = message_serializer.serialize_message(msg, little_endian=False)
    assert le != be


def test_add_channel_and_write_message() -> None:
    @dataclass
    class Example:
        __msg_name__ = "tests/msgs/Example"
        value: pybag.int32

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with McapFileWriter.open(file_path, profile="ros2") as mcap:
            channel_id = mcap.add_channel("/example", Example)
            mcap.write_message("/example", 1, Example(5))
        reader = CrcReader(BytesReader(file_path.read_bytes()))

    # Check the magic bytes
    version = McapRecordParser.parse_magic_bytes(reader)
    assert version == "0"

    # Check the header
    header = McapRecordParser.parse_header(reader)
    assert header.library == f"pybag {__version__}"
    assert header.profile == "ros2"

    # Check the schema
    data_schema = McapRecordParser.parse_schema(reader)
    assert data_schema is not None
    assert data_schema.name == "tests/msgs/Example"
    assert data_schema.encoding == "ros2msg"
    assert data_schema.data == "int32 value\n".encode("utf-8")

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
    message_serializer = MessageSerializerFactory.from_profile('ros2')
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


def test_streaming_writer_over_http(tmp_path: Path) -> None:
    received_data: bytes | None = None
    request_event = Event()

    class StreamingHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            nonlocal received_data
            data = bytearray()
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                while True:
                    chunk_size_line = self.rfile.readline().strip()
                    if not chunk_size_line:
                        continue
                    size = int(chunk_size_line, 16)
                    if size == 0:
                        # Consume the trailing CRLF after the zero-length chunk
                        self.rfile.readline()
                        break
                    chunk = self.rfile.read(size)
                    data.extend(chunk)
                    # Each chunk is terminated by CRLF
                    self.rfile.read(2)
            else:
                length = int(self.headers.get("Content-Length", "0"))
                data.extend(self.rfile.read(length))

            received_data = bytes(data)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
            request_event.set()

        def log_message(self, format: str, *args: object) -> None:  # pragma: no cover - suppress server logs
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), StreamingHandler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    @dataclass
    class Example:
        __msg_name__ = "tests/msgs/NetworkExample"
        value: pybag.int32

    try:
        # Write MCAP to disk to obtain the expected byte sequence
        expected_path = tmp_path / "expected.mcap"
        with McapFileWriter.open(expected_path, chunk_compression=None) as writer:
            writer.write_message("/example", 1, Example(5))
        expected_bytes = expected_path.read_bytes()

        # Stream MCAP data to the HTTP server
        port = server.server_address[1]
        with McapFileWriter.open(f"http://127.0.0.1:{port}/stream", chunk_compression=None) as writer:
            writer.write_message("/example", 1, Example(5))

        assert request_event.wait(timeout=5)
        assert received_data is not None
        assert received_data == expected_bytes
    finally:
        server.shutdown()
        server_thread.join()


def test_invalid_profile() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / "test.mcap"
        with pytest.raises(ValueError, match="Unknown encoding type"):
            McapFileWriter.open(file_path, profile="invalid_profile")


def test_chunk_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "data.mcap"
        with McapFileWriter.open(path, chunk_size=1, chunk_compression="lz4") as writer:
            writer.write_message("/pybag", 0, std_msgs.String(data="a"))
            writer.write_message("/pybag", 1, std_msgs.String(data="b"))

        # Check we can read the messages correctly
        with open(path, "rb") as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            msgs = [m.data for _, _, _, m in reader.iter_decoded_messages()]
            assert msgs == ["a", "b"]

        # Check we can read the chunk indexes correctly
        with McapChunkedReader.from_file(path) as random_reader:
            chunk_indexes = random_reader.get_chunk_indexes()
            assert len(chunk_indexes) == 2
            assert all(c.compression == "lz4" for c in chunk_indexes)
