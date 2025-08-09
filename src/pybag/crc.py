import zlib


class McapInvalidCrcError(Exception):
    """Exception raised when a CRC is invalid."""


def validate_crc(data: bytes, crc: int) -> bool:
    """Validate CRC values of an MCAP file.
    """
    return zlib.crc32(data) == crc


def assert_crc(data: bytes, crc: int) -> None:
    """Assert CRC values of an MCAP file.
    """
    if not validate_crc(data, crc):
        raise McapInvalidCrcError(f'Invalid CRC for data')
