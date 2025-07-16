from crc import Calculator, Crc32

__all__ = ["crc32c", "CrcMismatchError"]

_calculator = Calculator(Crc32.CRC32C)

class CrcMismatchError(Exception):
    """Raised when a CRC check fails."""


def crc32c(data: bytes) -> int:
    """Compute the CRC-32C checksum for the given bytes."""
    return _calculator.checksum(data)
