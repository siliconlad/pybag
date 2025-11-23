import zlib

from pybag.io.raw_reader import BaseReader
from pybag.mcap.record_parser import (
    DATA_END_SIZE,
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)
from pybag.mcap.records import ChunkRecord, FooterRecord

DEFAULT_CRC_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB

class McapInvalidCrcError(Exception):
    """Exception raised when a CRC is invalid."""


def validate_crc(data: bytes, crc: int) -> bool:
    """Validate CRC values of an MCAP file."""
    return zlib.crc32(data) == crc


def assert_crc(data: bytes, crc: int) -> None:
    """Assert CRC values of an MCAP file."""
    if not validate_crc(data, crc):
        raise McapInvalidCrcError(f'Invalid CRC for data')


def compute_crc(data: bytes, start_value: int = 0) -> int:
    return zlib.crc32(data, start_value)


def compute_crc_batched(
    reader: BaseReader,
    num_bytes: int,
    chunk_size: int = DEFAULT_CRC_CHUNK_SIZE
) -> int:
    """Compute CRC32 over the next num_bytes from current reader position in chunks."""
    remaining = num_bytes
    crc_value = 0
    while remaining > 0:
        read_size = min(remaining, chunk_size)
        if not (chunk := reader.read(read_size)):
            break
        crc_value = zlib.crc32(chunk, crc_value)
        remaining -= read_size
    return crc_value


def validate_data_crc(
    reader: BaseReader,
    footer: FooterRecord | None = None,
    chunk_size: int = DEFAULT_CRC_CHUNK_SIZE
) -> bool:
    """Check the CRC of the data in the MCAP file using batched reads."""
    original_position = reader.tell()
    if footer is None:  # Get footer if not given
        reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer = McapRecordParser.parse_footer(reader)

    if footer.summary_start:
        reader.seek_from_start(footer.summary_start - DATA_END_SIZE)
    else:
        reader.seek_from_end(DATA_END_SIZE + FOOTER_SIZE + MAGIC_BYTES_SIZE)
    bytes_to_read = reader.tell()
    data_end = McapRecordParser.parse_data_end(reader)

    if data_end.data_section_crc == 0:
        return True

    reader.seek_from_start(0)
    computed_crc = compute_crc_batched(reader, bytes_to_read, chunk_size)

    reader.seek_from_start(original_position)
    return computed_crc == data_end.data_section_crc


def assert_data_crc(
    reader: BaseReader,
    footer: FooterRecord | None = None,
    chunk_size: int = DEFAULT_CRC_CHUNK_SIZE
) -> None:
    """Assert the CRC of the data in the MCAP file using batched reads."""
    if not validate_data_crc(reader, footer, chunk_size=chunk_size):
        raise McapInvalidCrcError(f'Invalid CRC for data')


def validate_summary_crc(
    reader: BaseReader,
    footer: FooterRecord | None = None,
    chunk_size: int = DEFAULT_CRC_CHUNK_SIZE
) -> bool:
    """Check the CRC of the summary in the MCAP file using batched reads."""
    original_position = reader.tell()
    if footer is None:  # Get footer if not given
        reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer = McapRecordParser.parse_footer(reader)

    if footer.summary_crc == 0:
        return True

    if footer.summary_start == 0:
        return True

    reader.seek_from_end(MAGIC_BYTES_SIZE + 4)
    bytes_to_read = reader.tell() - footer.summary_start

    reader.seek_from_start(footer.summary_start)
    computed_crc = compute_crc_batched(reader, bytes_to_read, chunk_size)

    reader.seek_from_start(original_position)
    return computed_crc == footer.summary_crc


def assert_summary_crc(
    reader: BaseReader,
    footer: FooterRecord | None = None,
    chunk_size: int = DEFAULT_CRC_CHUNK_SIZE
) -> None:
    """Assert the CRC of the summary in the MCAP file using batched reads."""
    if not validate_summary_crc(reader, footer, chunk_size=chunk_size):
        raise McapInvalidCrcError(f'Invalid CRC for summary')
