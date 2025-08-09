from pathlib import Path
from tempfile import TemporaryDirectory
import zlib

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.io.raw_reader import BytesReader
from pybag.mcap import validate_crc
from pybag.mcap.record_parser import McapRecordParser, McapRecordType


def _create_mcap_bytes() -> bytes:
    with TemporaryDirectory() as temp_dir:
        typestore = get_typestore(Stores.LATEST)
        Vector3 = typestore.types['geometry_msgs/msg/Vector3']
        with Writer(Path(temp_dir) / 'rosbags', version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            conn = writer.add_connection('/rosbags', Vector3.__msgtype__, typestore=typestore)
            msg = Vector3(x=1.0, y=2.0, z=3.0)
            serialized = typestore.serialize_cdr(msg, Vector3.__msgtype__)
            writer.write(conn, 0, serialized)
        mcap_file = next(Path(temp_dir).rglob('*.mcap'))
        return mcap_file.read_bytes()


def _locate_offsets(data: bytes) -> tuple[int, int | None, int]:
    reader = BytesReader(data)
    McapRecordParser.parse_magic_bytes(reader)
    McapRecordParser.parse_header(reader)
    data_section_start = reader.tell()
    chunk_start = None
    while True:
        start = reader.tell()
        record_type = McapRecordParser.peek_record(reader)
        if record_type == McapRecordType.DATA_END:
            data_end_start = start
            break
        if record_type == McapRecordType.CHUNK and chunk_start is None:
            chunk_start = start
        McapRecordParser.skip_record(reader)
    return data_section_start, chunk_start, data_end_start


def test_validate_crc_valid():
    data = _create_mcap_bytes()
    assert validate_crc(data)


def test_validate_crc_invalid_data_end():
    data = bytearray(_create_mcap_bytes())
    _, _, data_end_start = _locate_offsets(data)
    crc_offset = data_end_start + 1 + 8
    data[crc_offset:crc_offset + 4] = (1).to_bytes(4, 'little')
    assert not validate_crc(bytes(data))


def test_validate_crc_invalid_chunk():
    data = bytearray(_create_mcap_bytes())
    data_section_start, chunk_start, data_end_start = _locate_offsets(data)
    assert chunk_start is not None
    chunk_crc_offset = chunk_start + 33
    data[chunk_crc_offset:chunk_crc_offset + 4] = (1).to_bytes(4, 'little')
    new_crc = zlib.crc32(data[data_section_start:data_end_start])
    data[data_end_start + 1 + 8:data_end_start + 1 + 8 + 4] = new_crc.to_bytes(4, 'little')
    assert not validate_crc(bytes(data))
