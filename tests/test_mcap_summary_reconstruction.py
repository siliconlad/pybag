import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pybag
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)
from pybag.mcap.record_reader import (
    McapChunkedReader,
    McapNonChunkedReader,
    McapRecordReaderFactory
)
from pybag.mcap.record_writer import McapRecordWriter
from pybag.mcap.records import FooterRecord
from pybag.mcap_writer import McapFileWriter


@dataclass
class SimpleMessage:
    __msg_name__ = 'tests/msgs/SimpleMessage'
    value: pybag.int32


def _create_mcap_bytes(chunked: bool) -> bytes:
    def open_writer(path: Path) -> McapFileWriter:
        if chunked:
            return McapFileWriter.open(path, chunk_size=1, chunk_compression=None)
        return McapFileWriter.open(path, chunk_compression=None)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / 'summary_reconstruction.mcap'
        writer_factory: Callable[[Path], McapFileWriter] = open_writer
        with writer_factory(tmp_path) as writer:
            writer.add_channel('/example', SimpleMessage)
            writer.write_message('/example', 1, SimpleMessage(1))
            writer.write_message('/example', 2, SimpleMessage(2))

        return tmp_path.read_bytes()


def _strip_summary(data: bytes) -> bytes:
    reader = BytesReader(data)
    reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
    footer = McapRecordParser.parse_footer(reader)
    if footer.summary_start == 0:
        return data

    new_footer = BytesWriter()
    McapRecordWriter.write_footer(new_footer, FooterRecord(0, 0, 0))
    footer_bytes = new_footer.as_bytes()

    return data[:footer.summary_start] + footer_bytes + data[-MAGIC_BYTES_SIZE:]


def test_reconstruct_summary_for_chunked_file() -> None:
    data = _create_mcap_bytes(chunked=True)
    stripped = _strip_summary(data)

    reader = McapRecordReaderFactory.from_bytes(stripped)
    assert isinstance(reader, McapChunkedReader)

    stats = reader.get_statistics()
    assert stats.message_count == 2

    channels = reader.get_channels()
    assert len(channels) == 1
    channel_id = next(iter(channels))

    messages = list(reader.get_messages(channel_id=channel_id))
    assert [message.log_time for message in messages] == [1, 2]

    reader.close()


def test_reconstruct_summary_for_non_chunked_file() -> None:
    data = _create_mcap_bytes(chunked=False)
    stripped = _strip_summary(data)

    reader = McapRecordReaderFactory.from_bytes(stripped)
    assert isinstance(reader, McapNonChunkedReader)

    stats = reader.get_statistics()
    assert stats.message_count == 2

    channels = reader.get_channels()
    assert len(channels) == 1
    channel_id = next(iter(channels))

    messages = list(reader.get_messages(channel_id=channel_id))
    assert [message.log_time for message in messages] == [1, 2]

    reader.close()
