import struct
import logging
from typing import Any

from pybag.io.raw_reader import BytesReader

logger = logging.getLogger(__name__)


class CdrParser:
    def __init__(self, data: bytes):
        assert len(data) > 4, 'Data must be at least 4 bytes long.'

        # Get endianness from second byte
        self._is_little_endian = bool(data[1])
        logger.debug(f'Little endian: {self._is_little_endian}')

        # Skip first 4 bytes
        self._data = BytesReader(data[4:])

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

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


class CdrEncoder:
    def __init__(self):
        # TODO: implement
        pass


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
    cdr = CdrParser(pose_with_covariance_msg.data)
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))

    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))
    print(cdr.parse('float64'))

    # print(cdr.sequence('float64'))
    print(cdr.array('float64', 36))
