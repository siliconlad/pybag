import logging
import struct
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter

logger = logging.getLogger(__name__)


class Ros1Decoder(MessageDecoder):
    """Decoder for ROS1 serialization format.

    ROS1 uses a simpler serialization format than CDR:
    - No encapsulation header
    - Little endian
    - No alignment padding
    - Strings: 4-byte length + string data (no null terminator)
    """

    def __init__(self, data: bytes):
        self._data = BytesReader(data)
        self._is_little_endian = True  # ROS1 uses little endian

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

    # Primitive parsers -------------------------------------------------

    def bool(self) -> bool:
        value = struct.unpack('?', self._data.read(1))[0]
        return value

    def int8(self) -> int:
        value = struct.unpack('<b', self._data.read(1))[0]
        return value

    def uint8(self) -> int:
        value = struct.unpack('<B', self._data.read(1))[0]
        return value

    def byte(self) -> bytes:
        return self._data.read(1)

    def char(self) -> str:
        value = struct.unpack('<c', self._data.read(1))[0]
        return value.decode()

    def int16(self) -> int:
        value = struct.unpack('<h', self._data.read(2))[0]
        return value

    def uint16(self) -> int:
        value = struct.unpack('<H', self._data.read(2))[0]
        return value

    def int32(self) -> int:
        value = struct.unpack('<i', self._data.read(4))[0]
        return value

    def uint32(self) -> int:
        value = struct.unpack('<I', self._data.read(4))[0]
        return value

    def int64(self) -> int:
        value = struct.unpack('<q', self._data.read(8))[0]
        return value

    def uint64(self) -> int:
        value = struct.unpack('<Q', self._data.read(8))[0]
        return value

    def float32(self) -> float:
        value = struct.unpack('<f', self._data.read(4))[0]
        return value

    def float64(self) -> float:
        value = struct.unpack('<d', self._data.read(8))[0]
        return value

    def string(self) -> str:
        # ROS1 strings: 4-byte length + string data (no null terminator)
        length = self.uint32()
        if length == 0:
            return ''
        return self._data.read(length).decode()

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> list:
        return [getattr(self, f'{type}')() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return [getattr(self, f'{type}')() for _ in range(length)]


class Ros1Encoder(MessageEncoder):
    """Encoder for ROS1 serialization format.

    ROS1 uses a simpler serialization format than CDR:
    - No encapsulation header
    - Little endian
    - No alignment padding
    - Strings: 4-byte length + string data (no null terminator)
    """

    def __init__(self, *, little_endian: bool = True) -> None:
        """Create a new ROS1 encoder.

        Args:
            little_endian: Ignored for ROS1, always uses little endian.
        """
        self._is_little_endian = True  # ROS1 always uses little endian
        self._payload = BytesWriter()

    @classmethod
    def encoding(cls) -> str:
        return "ros1"

    def encode(self, type_str: str, value: Any) -> None:
        """Encode ``value`` based on ``type_str``."""
        getattr(self, type_str)(value)

    def save(self) -> bytes:
        """Return the encoded byte stream."""
        return self._payload.as_bytes()

    # Primitive encoders -------------------------------------------------

    def bool(self, value: bool) -> None:
        self._payload.write(struct.pack("?", value))

    def int8(self, value: int) -> None:
        self._payload.write(struct.pack("<b", value))

    def uint8(self, value: int) -> None:
        self._payload.write(struct.pack("<B", value))

    def byte(self, value: bytes) -> None:
        self._payload.write(value)

    def char(self, value: str) -> None:
        self._payload.write(struct.pack("<c", value.encode()))

    def int16(self, value: int) -> None:
        self._payload.write(struct.pack("<h", value))

    def uint16(self, value: int) -> None:
        self._payload.write(struct.pack("<H", value))

    def int32(self, value: int) -> None:
        self._payload.write(struct.pack("<i", value))

    def uint32(self, value: int) -> None:
        self._payload.write(struct.pack("<I", value))

    def int64(self, value: int) -> None:
        self._payload.write(struct.pack("<q", value))

    def uint64(self, value: int) -> None:
        self._payload.write(struct.pack("<Q", value))

    def float32(self, value: float) -> None:
        self._payload.write(struct.pack("<f", value))

    def float64(self, value: float) -> None:
        self._payload.write(struct.pack("<d", value))

    def string(self, value: str) -> None:
        encoded = value.encode()
        # Write length (no null terminator in ROS1)
        self.uint32(len(encoded))
        self._payload.write(encoded)

    # Container encoders -------------------------------------------------

    def array(self, type: str, values: list[Any]) -> None:
        for v in values:
            getattr(self, type)(v)

    def sequence(self, type: str, values: list[Any]) -> None:
        self.uint32(len(values))
        for v in values:
            getattr(self, type)(v)
