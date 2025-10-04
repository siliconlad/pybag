from __future__ import annotations

import struct
from pathlib import Path

import pytest

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.mcap.record_parser import FOOTER_SIZE, MAGIC_BYTES_SIZE


def _strip_summary(path: Path) -> None:
    data = bytearray(path.read_bytes())
    footer_offset = len(data) - MAGIC_BYTES_SIZE - FOOTER_SIZE
    # Clear summary_start, summary_offset_start and summary_crc
    struct.pack_into('<Q', data, footer_offset + 9, 0)
    struct.pack_into('<Q', data, footer_offset + 17, 0)
    struct.pack_into('<I', data, footer_offset + 25, 0)
    path.write_bytes(data)


@pytest.mark.parametrize(
    'chunk_size',
    [pytest.param(None, id='without_chunks'), pytest.param(64, id='with_chunks')],
)
def test_reconstruct_summary_without_metadata(tmp_path, chunk_size):
    path = tmp_path / 'missing_summary.mcap'
    with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
        for timestamp in range(3):
            writer.write_message('/topic', timestamp, std_msgs.String(data=f'msg_{timestamp}'))

    _strip_summary(path)

    with McapFileReader.from_file(path) as reader:
        topics = reader.get_topics()
        assert topics == ['/topic']

        messages = list(reader.messages('/topic'))
        assert len(messages) == 3
        assert [message.data.data for message in messages] == [f'msg_{i}' for i in range(3)]
        log_times = [message.log_time for message in messages]
        assert reader.start_time == min(log_times)
        assert reader.end_time == max(log_times)
        assert reader.get_message_count('/topic') == 3
