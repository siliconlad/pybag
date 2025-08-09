from pybag.io.raw_reader import BaseReader
from pybag.crc import McapInvalidCrcError, validate_crc
from pybag.mcap.record_parser import (
    McapRecordParser,
    MAGIC_BYTES_SIZE,
    FOOTER_SIZE,
    DATA_END_SIZE
)


def validate_data_crc(reader: BaseReader) -> bool:
    """Check the CRC of the data in the MCAP file."""
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

    if data_end.crc == 0:
        return True

    reader.seek_from_start(0)
    data = reader.read(bytes_to_read)

    reader.seek_from_start(original_position)
    return validate_crc(data, data_end.crc)


def assert_data_crc(reader: BaseReader) -> None:
    """Assert the CRC of the data in the MCAP file."""
    if not validate_data_crc(reader):
        raise McapInvalidCrcError(f'Invalid CRC for data')


def validate_summary_crc(reader: BaseReader) -> bool:
    """Check the CRC of the summary in the MCAP file."""
    original_position = reader.tell()
    reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
    footer = McapRecordParser.parse_footer(reader)

    if footer.summary_start == 0:
        return True

    reader.seek_from_current(-4)  # Go back 4 bytes to start of summary_crc
    bytes_to_read = reader.tell()

    reader.seek_from_start(0)
    data = reader.read(bytes_to_read)

    reader.seek_from_start(original_position)
    return validate_crc(data, footer.summary_crc)


def assert_summary_crc(reader: BaseReader) -> None:
    """Assert the CRC of the summary in the MCAP file."""
    if not validate_summary_crc(reader):
        raise McapInvalidCrcError(f'Invalid CRC for summary')
