import logging
import struct
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter

logger = logging.getLogger(__name__)


class CdrDecoder(MessageDecoder):
    def __init__(self, data: bytes):
        assert len(data) >= 4, 'Data must be at least 4 bytes long (CDR header).'

        # Get endianness from second byte
        self._is_little_endian = bool(data[1])

        # Skip first 4 bytes
        self._data = BytesReader(data[4:])

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

    # Primitive parsers -------------------------------------------------

    def bool(self) -> bool:
        value = struct.unpack('?', self._data.align(1).read(1))[0]
        return value

    def int8(self) -> int:
        value = struct.unpack(
            '<b' if self._is_little_endian else '>b',
            self._data.align(1).read(1)
        )[0]
        return value

    def uint8(self) -> int:
        value = struct.unpack(
            '<B' if self._is_little_endian else '>B',
            self._data.align(1).read(1)
        )[0]
        return value

    def byte(self) -> bytes:
        return self._data.align(1).read(1)

    def char(self) -> str:
        value = struct.unpack(
            '<c' if self._is_little_endian else '>c',
            self._data.align(1).read(1)
        )[0]
        return value.decode()

    def int16(self) -> int:
        value = struct.unpack(
            '<h' if self._is_little_endian else '>h',
            self._data.align(2).read(2)
        )[0]
        return value

    def uint16(self) -> int:
        value = struct.unpack(
            '<H' if self._is_little_endian else '>H',
            self._data.align(2).read(2)
        )[0]
        return value

    def int32(self) -> int:
        value = struct.unpack(
            '<i' if self._is_little_endian else '>i',
            self._data.align(4).read(4)
        )[0]
        return value

    def uint32(self) -> int:
        value = struct.unpack(
            '<I' if self._is_little_endian else '>I',
            self._data.align(4).read(4)
        )[0]
        return value

    def int64(self) -> int:
        value = struct.unpack(
            '<q' if self._is_little_endian else '>q',
            self._data.align(8).read(8)
        )[0]
        return value

    def uint64(self) -> int:
        value = struct.unpack(
            '<Q' if self._is_little_endian else '>Q',
            self._data.align(8).read(8)
        )[0]
        return value

    def float32(self) -> float:
        value = struct.unpack(
            '<f' if self._is_little_endian else '>f',
            self._data.align(4).read(4)
        )[0]
        return value

    def float64(self) -> float:
        value = struct.unpack(
            '<d' if self._is_little_endian else '>d',
            self._data.align(8).read(8)
        )[0]
        return value

    def string(self) -> str:
        # Strings are null-terminated
        length = self.uint32()
        if length <= 1:
            self._data.read(length)  # discard
            return ''
        return self._data.read(length).decode()[:-1]

    def wstring(self) -> str:
        # Wide strings use 4 bytes per character (uint32)
        # Length is the number of characters including null terminator
        length = self.uint32()
        if length <= 1:
            # Read and discard the null terminator if present
            if length == 1:
                self._data.align(4).read(4)
            return ''
        # Read each character as a uint32 and convert to string
        chars = []
        for _ in range(length - 1):  # Exclude null terminator
            char_code = struct.unpack(
                '<I' if self._is_little_endian else '>I',
                self._data.align(4).read(4)
            )[0]
            chars.append(chr(char_code))
        # Read and discard null terminator
        self._data.align(4).read(4)
        return ''.join(chars)

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> list:
        return [getattr(self, f'{type}')() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return [getattr(self, f'{type}')() for _ in range(length)]


class CdrEncoder(MessageEncoder):
    """Encode primitive values into a CDR byte stream."""

    def __init__(self, *, little_endian: bool = True) -> None:
        """Create a new encoder.

        Args:
            little_endian: Whether the resulting CDR stream should be little endian.
        """
        self._is_little_endian = little_endian

        # Writer used for the payload after the 4 byte encapsulation header.
        self._payload = BytesWriter()

        # Store the encapsulation header.  The second byte contains the
        # endianness flag (1 for little endian, 0 for big endian).
        endian_flag = 1 if self._is_little_endian else 0
        self._header = bytes([0x00, endian_flag, 0x00, 0x00])

    @classmethod
    def encoding(cls) -> str:
        return "cdr"

    def encode(self, type_str: str, value: Any) -> None:
        """Encode ``value`` based on ``type_str``."""
        getattr(self, type_str)(value)

    def save(self) -> bytes:
        """Return the encoded byte stream."""
        return self._header + self._payload.as_bytes()

    # Primitive encoders -------------------------------------------------

    def bool(self, value: bool) -> None:
        self._payload.align(1)
        self._payload.write(struct.pack("?", value))

    def int8(self, value: int) -> None:
        self._payload.align(1)
        fmt = "<b" if self._is_little_endian else ">b"
        self._payload.write(struct.pack(fmt, value))

    def uint8(self, value: int) -> None:
        self._payload.align(1)
        fmt = "<B" if self._is_little_endian else ">B"
        self._payload.write(struct.pack(fmt, value))

    def byte(self, value: bytes) -> None:
        self._payload.align(1)
        self._payload.write(value)

    def char(self, value: str) -> None:
        self._payload.align(1)
        fmt = "<c" if self._is_little_endian else ">c"
        self._payload.write(struct.pack(fmt, value.encode()))

    def int16(self, value: int) -> None:
        self._payload.align(2)
        fmt = "<h" if self._is_little_endian else ">h"
        self._payload.write(struct.pack(fmt, value))

    def uint16(self, value: int) -> None:
        self._payload.align(2)
        fmt = "<H" if self._is_little_endian else ">H"
        self._payload.write(struct.pack(fmt, value))

    def int32(self, value: int) -> None:
        self._payload.align(4)
        fmt = "<i" if self._is_little_endian else ">i"
        self._payload.write(struct.pack(fmt, value))

    def uint32(self, value: int) -> None:
        self._payload.align(4)
        fmt = "<I" if self._is_little_endian else ">I"
        self._payload.write(struct.pack(fmt, value))

    def int64(self, value: int) -> None:
        self._payload.align(8)
        fmt = "<q" if self._is_little_endian else ">q"
        self._payload.write(struct.pack(fmt, value))

    def uint64(self, value: int) -> None:
        self._payload.align(8)
        fmt = "<Q" if self._is_little_endian else ">Q"
        self._payload.write(struct.pack(fmt, value))

    def float32(self, value: float) -> None:
        self._payload.align(4)
        fmt = "<f" if self._is_little_endian else ">f"
        self._payload.write(struct.pack(fmt, value))

    def float64(self, value: float) -> None:
        self._payload.align(8)
        fmt = "<d" if self._is_little_endian else ">d"
        self._payload.write(struct.pack(fmt, value))

    def string(self, value: str) -> None:
        encoded = value.encode()
        # Write length (including null terminator)
        self.uint32(len(encoded) + 1)
        self._payload.write(encoded + b"\x00")

    def wstring(self, value: str) -> None:
        # Wide strings use 4 bytes per character (uint32)
        # Write length (including null terminator)
        self.uint32(len(value) + 1)
        fmt = "<I" if self._is_little_endian else ">I"
        for char in value:
            self._payload.align(4)
            self._payload.write(struct.pack(fmt, ord(char)))
        # Write null terminator
        self._payload.align(4)
        self._payload.write(struct.pack(fmt, 0))

    # Container encoders -------------------------------------------------

    def array(self, type: str, values: list[Any]) -> None:
        for v in values:
            getattr(self, type)(v)

    def sequence(self, type: str, values: list[Any]) -> None:
        self.uint32(len(values))
        for v in values:
            getattr(self, type)(v)


if __name__ == '__main__':
    from pybag.mcap.records import MessageRecord

    point_msg = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=1748073056782514501,
        publish_time=1748073056782514501,
        data=b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@'
    )
    header_msg = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=1748177650605566125,
        publish_time=1748177650605566125,
        data=b'\x00\x01\x00\x00\n\x00\x00\x00\xe8\x03\x00\x00\t\x00\x00\x00frame_id\x00'
    )
    pose_with_covariance_msg = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=1748182265345264928,
        publish_time=1748182265345264928,
        data=b'\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@\x00\x00\x00\x00\x00\x00\x10@\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x18@\x00\x00\x00\x00\x00\x00\x1c@\x00\x00\x00\x00\x00\x00 @\x00\x00\x00\x00\x00\x00"@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00&@\x00\x00\x00\x00\x00\x00(@\x00\x00\x00\x00\x00\x00*@\x00\x00\x00\x00\x00\x00,@\x00\x00\x00\x00\x00\x00.@\x00\x00\x00\x00\x00\x000@\x00\x00\x00\x00\x00\x001@\x00\x00\x00\x00\x00\x002@\x00\x00\x00\x00\x00\x003@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x005@\x00\x00\x00\x00\x00\x006@\x00\x00\x00\x00\x00\x007@\x00\x00\x00\x00\x00\x008@\x00\x00\x00\x00\x00\x009@\x00\x00\x00\x00\x00\x00:@\x00\x00\x00\x00\x00\x00;@\x00\x00\x00\x00\x00\x00<@\x00\x00\x00\x00\x00\x00=@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00?@\x00\x00\x00\x00\x00\x00@@\x00\x00\x00\x00\x00\x80@@\x00\x00\x00\x00\x00\x00A@\x00\x00\x00\x00\x00\x80A@'
    )
    cdr = CdrDecoder(pose_with_covariance_msg.data)
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))

    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))

    # print(cdr.sequence('float64'))
    print(cdr.array('float64', 36))
