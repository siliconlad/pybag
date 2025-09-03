import struct
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder
from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter


class Ros1Decoder(MessageDecoder):
    """Decode primitive values from a ROS1 byte stream."""

    def __init__(self, data: bytes, *, little_endian: bool = True) -> None:
        self._is_little_endian = little_endian
        self._data = BytesReader(data)

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

    # Primitive parsers -------------------------------------------------

    def bool(self) -> bool:
        return struct.unpack("?", self._data.read(1))[0]

    def int8(self) -> int:
        fmt = "<b" if self._is_little_endian else ">b"
        return struct.unpack(fmt, self._data.read(1))[0]

    def uint8(self) -> int:
        fmt = "<B" if self._is_little_endian else ">B"
        return struct.unpack(fmt, self._data.read(1))[0]

    def byte(self) -> bytes:
        return self._data.read(1)

    def char(self) -> str:
        fmt = "<c" if self._is_little_endian else ">c"
        return struct.unpack(fmt, self._data.read(1))[0].decode()

    def int16(self) -> int:
        fmt = "<h" if self._is_little_endian else ">h"
        return struct.unpack(fmt, self._data.read(2))[0]

    def uint16(self) -> int:
        fmt = "<H" if self._is_little_endian else ">H"
        return struct.unpack(fmt, self._data.read(2))[0]

    def int32(self) -> int:
        fmt = "<i" if self._is_little_endian else ">i"
        return struct.unpack(fmt, self._data.read(4))[0]

    def uint32(self) -> int:
        fmt = "<I" if self._is_little_endian else ">I"
        return struct.unpack(fmt, self._data.read(4))[0]

    def int64(self) -> int:
        fmt = "<q" if self._is_little_endian else ">q"
        return struct.unpack(fmt, self._data.read(8))[0]

    def uint64(self) -> int:
        fmt = "<Q" if self._is_little_endian else ">Q"
        return struct.unpack(fmt, self._data.read(8))[0]

    def float32(self) -> float:
        fmt = "<f" if self._is_little_endian else ">f"
        return struct.unpack(fmt, self._data.read(4))[0]

    def float64(self) -> float:
        fmt = "<d" if self._is_little_endian else ">d"
        return struct.unpack(fmt, self._data.read(8))[0]

    def string(self) -> str:
        length = self.uint32()
        if length == 0:
            return ""
        return self._data.read(length).decode()

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> list:
        return [getattr(self, type)() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return [getattr(self, type)() for _ in range(length)]


class Ros1Encoder(MessageEncoder):
    """Encode primitive values into a ROS1 byte stream."""

    def __init__(self, *, little_endian: bool = True) -> None:
        self._is_little_endian = little_endian
        self._payload = BytesWriter()

    def encode(self, type_str: str, value: Any) -> None:
        getattr(self, type_str)(value)

    def save(self) -> bytes:
        return self._payload.as_bytes()

    # Primitive encoders -------------------------------------------------

    def bool(self, value: bool) -> None:
        self._payload.write(struct.pack("?", value))

    def int8(self, value: int) -> None:
        fmt = "<b" if self._is_little_endian else ">b"
        self._payload.write(struct.pack(fmt, value))

    def uint8(self, value: int) -> None:
        fmt = "<B" if self._is_little_endian else ">B"
        self._payload.write(struct.pack(fmt, value))

    def byte(self, value: bytes) -> None:
        self._payload.write(value)

    def char(self, value: str) -> None:
        fmt = "<c" if self._is_little_endian else ">c"
        self._payload.write(struct.pack(fmt, value.encode()))

    def int16(self, value: int) -> None:
        fmt = "<h" if self._is_little_endian else ">h"
        self._payload.write(struct.pack(fmt, value))

    def uint16(self, value: int) -> None:
        fmt = "<H" if self._is_little_endian else ">H"
        self._payload.write(struct.pack(fmt, value))

    def int32(self, value: int) -> None:
        fmt = "<i" if self._is_little_endian else ">i"
        self._payload.write(struct.pack(fmt, value))

    def uint32(self, value: int) -> None:
        fmt = "<I" if self._is_little_endian else ">I"
        self._payload.write(struct.pack(fmt, value))

    def int64(self, value: int) -> None:
        fmt = "<q" if self._is_little_endian else ">q"
        self._payload.write(struct.pack(fmt, value))

    def uint64(self, value: int) -> None:
        fmt = "<Q" if self._is_little_endian else ">Q"
        self._payload.write(struct.pack(fmt, value))

    def float32(self, value: float) -> None:
        fmt = "<f" if self._is_little_endian else ">f"
        self._payload.write(struct.pack(fmt, value))

    def float64(self, value: float) -> None:
        fmt = "<d" if self._is_little_endian else ">d"
        self._payload.write(struct.pack(fmt, value))

    def string(self, value: str) -> None:
        encoded = value.encode()
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
