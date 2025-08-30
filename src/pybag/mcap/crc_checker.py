import zlib

from pybag.crc import (
    DEFAULT_CRC_CHUNK_SIZE,
    McapInvalidCrcError,
    compute_crc_batched
)
from pybag.io.raw_reader import BaseReader
from pybag.mcap.record_parser import (
    DATA_END_SIZE,
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)


def validate_data_crc(reader: BaseReader, chunk_size: int = DEFAULT_CRC_CHUNK_SIZE) -> bool:
    """Check the CRC of the data in the MCAP file using batched reads."""
    original_position = reader.tell()

    reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
    footer = McapRecordParser.parse_footer(reader)

    if footer.summary_start == 0:
        reader.seek_from_current(-DATA_END_SIZE)
        bytes_to_read = reader.tell()
        data_end = McapRecordParser.parse_data_end(reader)
    else:
        reader.seek_from_start(footer.summary_start - DATA_END_SIZE)
        bytes_to_read = reader.tell()
        data_end = McapRecordParser.parse_data_end(reader)

    if data_end.data_section_crc == 0:
        return True

    reader.seek_from_start(0)
    computed_crc = compute_crc_batched(reader, bytes_to_read, chunk_size)

    reader.seek_from_start(original_position)
    return computed_crc == data_end.data_section_crc


def assert_data_crc(reader: BaseReader, chunk_size: int = DEFAULT_CRC_CHUNK_SIZE) -> None:
    """Assert the CRC of the data in the MCAP file using batched reads."""
    if not validate_data_crc(reader, chunk_size=chunk_size):
        raise McapInvalidCrcError(f'Invalid CRC for data')


def validate_summary_crc(reader: BaseReader, chunk_size: int = DEFAULT_CRC_CHUNK_SIZE) -> bool:
    """Check the CRC of the summary in the MCAP file using batched reads."""
    original_position = reader.tell()
    reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
    footer = McapRecordParser.parse_footer(reader)

    if footer.summary_start == 0:
        return True

    reader.seek_from_current(-4)  # Go back 4 bytes to start of summary_crc
    bytes_to_read = reader.tell()

    reader.seek_from_start(0)
    computed_crc = compute_crc_batched(reader, bytes_to_read, chunk_size)

    reader.seek_from_start(original_position)
    return computed_crc == footer.summary_crc


def assert_summary_crc(reader: BaseReader, chunk_size: int = DEFAULT_CRC_CHUNK_SIZE) -> None:
    """Assert the CRC of the summary in the MCAP file using batched reads."""
    if not validate_summary_crc(reader, chunk_size=chunk_size):
        raise McapInvalidCrcError(f'Invalid CRC for summary')
