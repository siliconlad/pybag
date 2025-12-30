"""ROS 1 message serialization format (rosmsg).

ROS 1 uses a simple little-endian serialization format with no alignment
requirements and no encapsulation header (unlike CDR used in ROS 2).

Key differences from CDR:
- No 4-byte encapsulation header
- No alignment padding
- Strings are length-prefixed without null terminator
- Always little-endian
"""

import logging
import struct
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter
from pybag.types import ros1

logger = logging.getLogger(__name__)


class _NoAlignBytesReader(BytesReader):
    """BytesReader that ignores alignment (for ROS 1 format)."""

    def align(self, size: int) -> '_NoAlignBytesReader':
        """No-op alignment for rosmsg format."""
        return self


class _NoAlignBytesWriter(BytesWriter):
    """BytesWriter that ignores alignment (for ROS 1 format)."""

    def align(self, size: int) -> None:
        """No-op alignment for rosmsg format."""
        pass


class RosMsgDecoder(MessageDecoder):
    """Decoder for ROS 1 message serialization format (rosmsg).

    Unlike CDR, rosmsg format:
    - Is always little-endian
    - Has no encapsulation header
    - Has no alignment requirements
    - Strings have no null terminator
    """

    def __init__(self, data: bytes):
        """Initialize decoder with serialized message data.

        Args:
            data: The serialized message bytes (no header, unlike CDR).
        """
        self._data = _NoAlignBytesReader(data)
        # For compatibility with schema compiler (always little-endian)
        self._is_little_endian = True

    def parse(self, type_str: str) -> Any:
        """Parse a value based on its type string."""
        return getattr(self, type_str)()

    # Primitive parsers -------------------------------------------------

    def bool(self) -> bool:
        """Parse a boolean (1 byte)."""
        return struct.unpack('<?', self._data.read(1))[0]

    def int8(self) -> int:
        """Parse a signed 8-bit integer."""
        return struct.unpack('<b', self._data.read(1))[0]

    def uint8(self) -> int:
        """Parse an unsigned 8-bit integer."""
        return struct.unpack('<B', self._data.read(1))[0]

    def byte(self) -> bytes:
        """Parse a single byte."""
        return self._data.read(1)

    def char(self) -> int:
        """Parse a ROS 1 char (uint8)."""
        return struct.unpack('<B', self._data.read(1))[0]

    def int16(self) -> int:
        """Parse a signed 16-bit integer (little-endian)."""
        return struct.unpack('<h', self._data.read(2))[0]

    def uint16(self) -> int:
        """Parse an unsigned 16-bit integer (little-endian)."""
        return struct.unpack('<H', self._data.read(2))[0]

    def int32(self) -> int:
        """Parse a signed 32-bit integer (little-endian)."""
        return struct.unpack('<i', self._data.read(4))[0]

    def uint32(self) -> int:
        """Parse an unsigned 32-bit integer (little-endian)."""
        return struct.unpack('<I', self._data.read(4))[0]

    def int64(self) -> int:
        """Parse a signed 64-bit integer (little-endian)."""
        return struct.unpack('<q', self._data.read(8))[0]

    def uint64(self) -> int:
        """Parse an unsigned 64-bit integer (little-endian)."""
        return struct.unpack('<Q', self._data.read(8))[0]

    def float32(self) -> float:
        """Parse a 32-bit float (IEEE 754, little-endian)."""
        return struct.unpack('<f', self._data.read(4))[0]

    def float64(self) -> float:
        """Parse a 64-bit float (IEEE 754, little-endian)."""
        return struct.unpack('<d', self._data.read(8))[0]

    def string(self) -> str:
        """Parse a string (length-prefixed, no null terminator).

        Format: 4-byte length (uint32 LE) + UTF-8 data
        Unlike CDR, rosmsg strings have NO null terminator.
        """
        length = self.uint32()
        if length == 0:
            return ''
        return self._data.read(length).decode('utf-8')

    # ROS 1 specific types -----------------------------------------------

    def time(self) -> ros1.Time:
        """Parse a ROS 1 time (secs: uint32, nsecs: uint32).

        Returns:
            Time object with secs and nsecs attributes.
        """
        secs, nsecs = struct.unpack('<II', self._data.read(8))
        return ros1.Time(secs=secs, nsecs=nsecs)

    def duration(self) -> ros1.Duration:
        """Parse a ROS 1 duration (secs: int32, nsecs: int32).

        Returns:
            Duration object with secs and nsecs attributes.
        """
        secs, nsecs = struct.unpack('<ii', self._data.read(8))
        return ros1.Duration(secs=secs, nsecs=nsecs)

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> list:
        """Parse a fixed-size array.

        Args:
            type: The type of array elements.
            length: The fixed array length.

        Returns:
            List of parsed values.
        """
        return [getattr(self, type)() for _ in range(length)]

    def sequence(self, type: str) -> list:
        """Parse a variable-length sequence.

        Format: 4-byte count (uint32 LE) + elements

        Args:
            type: The type of sequence elements.

        Returns:
            List of parsed values.
        """
        length = self.uint32()
        return [getattr(self, type)() for _ in range(length)]


class RosMsgEncoder(MessageEncoder):
    """Encoder for ROS 1 message serialization format (rosmsg).

    Unlike CDR, rosmsg format:
    - Is always little-endian
    - Has no encapsulation header
    - Has no alignment requirements
    - Strings have no null terminator
    """

    def __init__(self, **kwargs) -> None:
        """Initialize encoder.

        Note: Unlike CdrEncoder, rosmsg is always little-endian and has
        no options. The kwargs are accepted for API compatibility but ignored.
        """
        self._payload = _NoAlignBytesWriter()
        # For compatibility with schema compiler (always little-endian)
        self._is_little_endian = True

    @classmethod
    def encoding(cls) -> str:
        """Return the encoding name for MCAP ROS1 profile."""
        return "ros1"

    def encode(self, type_str: str, value: Any) -> None:
        """Encode a value based on its type string."""
        getattr(self, type_str)(value)

    def save(self) -> bytes:
        """Return the encoded byte stream (no header, unlike CDR)."""
        return self._payload.as_bytes()

    # Primitive encoders -------------------------------------------------

    def bool(self, value: bool) -> None:
        """Encode a boolean (1 byte)."""
        self._payload.write(struct.pack('<?', value))

    def int8(self, value: int) -> None:
        """Encode a signed 8-bit integer."""
        self._payload.write(struct.pack('<b', value))

    def uint8(self, value: int) -> None:
        """Encode an unsigned 8-bit integer."""
        self._payload.write(struct.pack('<B', value))

    def byte(self, value: bytes) -> None:
        """Encode a single byte."""
        self._payload.write(value[:1])

    def char(self, value: int) -> None:
        """Encode a ROS 1 char (uint8)."""
        self._payload.write(struct.pack('<B', value))

    def int16(self, value: int) -> None:
        """Encode a signed 16-bit integer (little-endian)."""
        self._payload.write(struct.pack('<h', value))

    def uint16(self, value: int) -> None:
        """Encode an unsigned 16-bit integer (little-endian)."""
        self._payload.write(struct.pack('<H', value))

    def int32(self, value: int) -> None:
        """Encode a signed 32-bit integer (little-endian)."""
        self._payload.write(struct.pack('<i', value))

    def uint32(self, value: int) -> None:
        """Encode an unsigned 32-bit integer (little-endian)."""
        self._payload.write(struct.pack('<I', value))

    def int64(self, value: int) -> None:
        """Encode a signed 64-bit integer (little-endian)."""
        self._payload.write(struct.pack('<q', value))

    def uint64(self, value: int) -> None:
        """Encode an unsigned 64-bit integer (little-endian)."""
        self._payload.write(struct.pack('<Q', value))

    def float32(self, value: float) -> None:
        """Encode a 32-bit float (IEEE 754, little-endian)."""
        self._payload.write(struct.pack('<f', value))

    def float64(self, value: float) -> None:
        """Encode a 64-bit float (IEEE 754, little-endian)."""
        self._payload.write(struct.pack('<d', value))

    def string(self, value: str) -> None:
        """Encode a string (length-prefixed, no null terminator).

        Format: 4-byte length (uint32 LE) + UTF-8 data
        Unlike CDR, rosmsg strings have NO null terminator.
        """
        encoded = value.encode('utf-8')
        self.uint32(len(encoded))
        self._payload.write(encoded)

    # ROS 1 specific types -----------------------------------------------

    def time(self, value: ros1.Time) -> None:
        """Encode a ROS 1 time (secs: uint32, nsecs: uint32).

        Args:
            value: Time object with secs and nsecs attributes.
        """
        self._payload.write(struct.pack('<II', value.secs, value.nsecs))

    def duration(self, value: ros1.Duration) -> None:
        """Encode a ROS 1 duration (secs: int32, nsecs: int32).

        Args:
            value: Duration object with secs and nsecs attributes.
        """
        self._payload.write(struct.pack('<ii', value.secs, value.nsecs))

    # Container encoders -------------------------------------------------

    def array(self, type: str, values: list[Any]) -> None:
        """Encode a fixed-size array.

        Args:
            type: The type of array elements.
            values: The values to encode.
        """
        for v in values:
            getattr(self, type)(v)

    def sequence(self, type: str, values: list[Any]) -> None:
        """Encode a variable-length sequence.

        Format: 4-byte count (uint32 LE) + elements

        Args:
            type: The type of sequence elements.
            values: The values to encode.
        """
        self.uint32(len(values))
        for v in values:
            getattr(self, type)(v)
