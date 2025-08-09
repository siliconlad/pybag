# Re-export CRC validation utility
from .crc import validate_crc, validate_mcap_crc

__all__ = ["validate_crc", "validate_mcap_crc"]
