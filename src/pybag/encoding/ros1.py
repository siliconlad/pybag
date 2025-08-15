import struct
from typing import Any

from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter


class Ros1Decoder:
    """Decode primitive values from a ROS1 byte stream."""

    def __init__(self, data: bytes):
        self._data = BytesReader(data)

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

    # Primitive parsers -----------------------------------------------------
    def bool(self) -> bool:
        return struct.unpack("<?", self._data.read(1))[0]

    def int8(self) -> int:
        return struct.unpack("<b", self._data.read(1))[0]

    def uint8(self) -> int:
        return struct.unpack("<B", self._data.read(1))[0]

    # Aliases
    def char(self) -> int:
        """Alias for :meth:`int8`.

        In ROS1 the ``char`` type is represented as a signed byte.
        """
        return self.int8()

    def byte(self) -> int:
        """Alias for :meth:`uint8`.

        The ``byte`` type is an unsigned 8-bit integer in ROS1.
        """
        return self.uint8()

    def int16(self) -> int:
        return struct.unpack("<h", self._data.read(2))[0]

    def uint16(self) -> int:
        return struct.unpack("<H", self._data.read(2))[0]

    def int32(self) -> int:
        return struct.unpack("<i", self._data.read(4))[0]

    def uint32(self) -> int:
        return struct.unpack("<I", self._data.read(4))[0]

    def int64(self) -> int:
        return struct.unpack("<q", self._data.read(8))[0]

    def uint64(self) -> int:
        return struct.unpack("<Q", self._data.read(8))[0]

    def float32(self) -> float:
        return struct.unpack("<f", self._data.read(4))[0]

    def float64(self) -> float:
        return struct.unpack("<d", self._data.read(8))[0]

    def string(self) -> str:
        length = self.uint32()
        if length == 0:
            return ""
        return self._data.read(length).decode()

    def time(self) -> int:
        secs = self.int32()
        nsecs = self.int32()
        return secs * 1_000_000_000 + nsecs

    def duration(self) -> int:
        secs = self.int32()
        nsecs = self.int32()
        return secs * 1_000_000_000 + nsecs

    # Container parsers -----------------------------------------------------
    def array(self, type: str, length: int) -> list:
        return [getattr(self, type)() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return self.array(type, length)


class Ros1Encoder:
    """Encode primitive values into a ROS1 byte stream."""

    def __init__(self):
        self._payload = BytesWriter()

    def as_bytes(self) -> bytes:
        return self._payload.as_bytes()

    # Primitive encoders ----------------------------------------------------
    def bool(self, value: bool) -> None:
        self._payload.write(struct.pack("<?", value))

    def int8(self, value: int) -> None:
        self._payload.write(struct.pack("<b", value))

    def uint8(self, value: int) -> None:
        self._payload.write(struct.pack("<B", value))

    # Aliases
    def char(self, value: int) -> None:
        """Alias for :meth:`int8`."""
        self.int8(value)

    def byte(self, value: int) -> None:
        """Alias for :meth:`uint8`."""
        self.uint8(value)

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
        self.uint32(len(encoded))
        self._payload.write(encoded)

    def time(self, value: int) -> None:
        secs = value // 1_000_000_000
        nsecs = value % 1_000_000_000
        self.int32(secs)
        self.int32(nsecs)

    def duration(self, value: int) -> None:
        secs = value // 1_000_000_000
        nsecs = value % 1_000_000_000
        self.int32(secs)
        self.int32(nsecs)

    # Container encoders ----------------------------------------------------
    def array(self, type: str, values: list[Any]) -> None:
        for v in values:
            getattr(self, type)(v)

    def sequence(self, type: str, values: list[Any]) -> None:
        self.uint32(len(values))
        self.array(type, values)

