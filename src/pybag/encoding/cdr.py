import struct
from pybag.io.raw_reader import BytesReader


class CdrParser:
    def __init__(self, data: bytes):
        assert len(data) > 4, 'Data must be at least 4 bytes long.'

        # Get endianness from second byte
        self._is_little_endian = bool(data[1])
        print(f'Little endian: {self._is_little_endian}')

        # Skip first 4 bytes
        self._data = BytesReader(data[4:])

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

    def array(self, type: str, length: int) -> list:
        return [getattr(self, f'{type}')() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return [getattr(self, f'{type}')() for _ in range(length)]
