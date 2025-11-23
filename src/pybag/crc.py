import zlib

from pybag.io.raw_reader import BaseReader

DEFAULT_CRC_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MiB

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


def compute_crc_batched(reader: BaseReader, num_bytes: int, chunk_size: int = DEFAULT_CRC_CHUNK_SIZE) -> int:
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


def compute_crc(data: bytes, start_value: int = 0) -> int:
    return zlib.crc32(data, start_value)


def validate_attachment_crc(data: bytes, crc: int) -> bool:
    """Validate the CRC of an attachment's data.

    Args:
        data: The attachment data bytes
        crc: The expected CRC value from the attachment record

    Returns:
        True if CRC is valid (or if crc is 0, indicating no validation needed)
    """
    if crc == 0:
        return True
    return zlib.crc32(data) == crc


def assert_attachment_crc(data: bytes, crc: int) -> None:
    """Assert the CRC of an attachment's data.

    Args:
        data: The attachment data bytes
        crc: The expected CRC value from the attachment record

    Raises:
        McapInvalidCrcError: If CRC validation fails
    """
    if not validate_attachment_crc(data, crc):
        raise McapInvalidCrcError(f'Invalid CRC for attachment data')
