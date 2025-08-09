import zlib
from pathlib import Path

from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap.record_reader import decompress_chunk
from pybag.mcap.records import AttachmentRecord, ChunkRecord


def _to_reader(source: BaseReader | bytes | bytearray | str | Path) -> BaseReader:
    if isinstance(source, BaseReader):
        return source
    if isinstance(source, (bytes, bytearray)):
        return BytesReader(bytes(source))
    return FileReader(Path(source))


def validate_crc(source: BaseReader | bytes | bytearray | str | Path) -> bool:
    """Validate CRC values of an MCAP file.

    The CRCs are checked in the following order:
    1. DataEndRecord.data_section_crc
    2. AttachmentRecord.crc
    3. ChunkRecord.uncompressed_crc
    """
    reader = _to_reader(source)
    try:
        McapRecordParser.parse_magic_bytes(reader)

        data_section_crc = 0
        attachments: list[AttachmentRecord] = []
        chunks: list[ChunkRecord] = []

        while True:
            record_type = McapRecordParser.peek_record(reader)
            if record_type == McapRecordType.DATA_END:
                data_end = McapRecordParser.parse_data_end(reader)
                break
            record_start = reader.tell()
            record = McapRecordParser._parse_record(record_type, reader)
            record_end = reader.tell()
            reader.seek_from_start(record_start)
            raw = reader.read(record_end - record_start)
            data_section_crc = zlib.crc32(raw, data_section_crc)
            reader.seek_from_start(record_end)

            if record_type == McapRecordType.ATTACHMENT:
                attachments.append(record)
            elif record_type == McapRecordType.CHUNK:
                chunks.append(record)

        if data_end.data_section_crc != 0 and data_section_crc != data_end.data_section_crc:
            return False

        for attachment in attachments:
            if attachment.crc != 0 and zlib.crc32(attachment.data) != attachment.crc:
                return False

        for chunk in chunks:
            try:
                records = decompress_chunk(chunk)
            except Exception:
                return False
            if chunk.uncompressed_crc != 0 and zlib.crc32(records) != chunk.uncompressed_crc:
                return False
        return True
    finally:
        reader.close()


validate_mcap_crc = validate_crc
