import logging
import struct
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter

logger = logging.getLogger(__name__)


class CdrDecoder(MessageDecoder):
    def __init__(self, data: bytes):
        assert len(data) > 4, 'Data must be at least 4 bytes long.'

        # Get endianness from second byte
        self._is_little_endian = bool(data[1])
        self._fmt = '<' if self._is_little_endian else '>'
        logger.debug(f'Little endian: {self._is_little_endian}')

        # Skip first 4 bytes
        self._data = BytesReader(data[4:])

        # Store the loaded types
        self._loaded = self._fmt
        self._buffer = tuple()

    def _align(self, size: int) -> 'CdrDecoder':
        loaded_size = struct.calcsize(self._loaded)
        if (self._data.tell() + loaded_size) % size > 0:
            align = size - ((self._data.tell() + loaded_size) % size)
            self._loaded += 'x' * align
        return self

    def _push(self, type_str: str) -> 'CdrDecoder':
        self._loaded += type_str
        return self

    def _last(self, size: int = 1) -> tuple[Any, ...]:
        read_size = struct.calcsize(self._loaded)
        data = struct.unpack(self._loaded, self._data.read(read_size))

        self._buffer += data
        self._loaded = self._fmt

        if size == 0:
            return tuple()

        data = self._buffer[-size:]
        self._buffer = self._buffer[:-size]

        return data

    def push(self, type_str: str) -> 'CdrDecoder':
        getattr(self, type_str)()
        return self

    def load(self) -> tuple[Any, ...]:
        if (size := struct.calcsize(self._loaded)) > 0:
            self._buffer += struct.unpack(self._loaded, self._data.read(size))
        data = self._buffer
        self._buffer = tuple()
        self._loaded = self._fmt

        return data

    # Primitive parsers -------------------------------------------------

    def bool(self) -> 'CdrDecoder':
        self._align(1)._push('?')
        return self

    def int8(self) -> 'CdrDecoder':
        self._align(1)._push('b')
        return self

    def uint8(self) -> 'CdrDecoder':
        self._align(1)._push('B')
        return self

    def char(self) -> 'CdrDecoder':
        c = self._align(1)._push('c')._last()[0]
        self._buffer += (c.decode(),)
        return self

    def byte(self) -> 'CdrDecoder':
        self._align(1)._push('c')
        return self

    def int16(self) -> 'CdrDecoder':
        self._align(2)._push('h')
        return self

    def uint16(self) -> 'CdrDecoder':
        self._align(2)._push('H')
        return self

    def int32(self) -> 'CdrDecoder':
        self._align(4)._push('i')
        return self

    def uint32(self) -> 'CdrDecoder':
        self._align(4)._push('I')
        return self

    def int64(self) -> 'CdrDecoder':
        self._align(8)._push('q')
        return self

    def uint64(self) -> 'CdrDecoder':
        self._align(8)._push('Q')
        return self

    def float32(self) -> 'CdrDecoder':
        self._align(4)._push('f')
        return self

    def float64(self) -> 'CdrDecoder':
        self._align(8)._push('d')
        return self

    def string(self) -> 'CdrDecoder':
        # Strings are null-terminated
        length = self.push('uint32')._last()[0]
        if length <= 1:
            self._data.read(length)  # discard
            self._buffer += ('',)
        else:
            string_data = self._data.read(length).decode()[:-1]
            self._buffer += (string_data,)
        return self

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> 'CdrDecoder':
        for _ in range(length):
            self.push(type)
        array_data = list(self._last(length))
        self._buffer += (array_data,)
        return self

    def sequence(self, type: str) -> 'CdrDecoder':
        length = self.push('uint32')._last()[0]
        for _ in range(length):
            self.push(type)
        sequence_data = list(self._last(length))
        self._buffer += (sequence_data,)
        return self


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
