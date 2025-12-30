import struct
import zlib
from abc import ABC, abstractmethod
from enum import IntEnum
from io import BufferedReader
from pathlib import Path
from typing import Any


class FilePosition(IntEnum):
    START = 0    # Start of the file
    CURRENT = 1  # Current file position
    END = 2      # End of the file


class BaseReader(ABC):
    @abstractmethod
    def peek(self, size: int) -> bytes:
        """Peek at the next bytes in the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def read(self, size: int | None = None) -> bytes:
        """Read the next bytes in the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def seek_from_start(self, offset: int) -> int:
        """Seek from the start of the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def seek_from_end(self, offset: int) -> int:
        """Seek from the end of the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def seek_from_current(self, offset: int) -> int:
        """Seek from the current position of the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def tell(self) -> int:
        """Get the current position in the reader."""
        ...  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        """Close the reader and release all resources."""
        ...  # pragma: no cover


class FileReader(BaseReader):
    def __init__(self, file_path: Path | str, mode: str = 'rb'):
        self._file_path = Path(file_path).absolute()
        self._file: BufferedReader = open(self._file_path, mode)

    def peek(self, size: int) -> bytes:
        # Returns empty bytes when end of file
        return self._file.peek(size)

    def read(self, size: int | None = None) -> bytes:
        return self._file.read(size)

    def seek_from_start(self, offset: int) -> int:
        return self._file.seek(offset, FilePosition.START)

    def seek_from_end(self, offset: int) -> int:
        return self._file.seek(-offset, FilePosition.END)

    def seek_from_current(self, offset: int) -> int:
        return self._file.seek(offset, FilePosition.CURRENT)

    def tell(self) -> int:
        return self._file.tell()

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> 'FileReader':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class BytesReader(BaseReader):
    def __init__(self, data: bytes):
        self._data = data
        self.view = memoryview(data)
        self.position = 0
        self._length = len(data)

    def reset(self, data: bytes):
        self._data = data
        self.view = memoryview(data)
        self.position = 0
        self._length = len(data)

    def peek(self, size: int) -> bytes:
        # Returns empty bytes when end of data
        return self._data[self.position:self.position + size]

    def read(self, size: int | None = None) -> bytes:
        if size is None:
            result = self._data[self.position:]
            self.position = len(self._data)
            return result
        result = self._data[self.position:self.position + size]
        self.position += size
        return result

    def seek_from_start(self, offset: int) -> int:
        self.position = offset
        return self.position

    def seek_from_end(self, offset: int) -> int:
        self.position = len(self._data) - offset
        return self.position

    def seek_from_current(self, offset: int) -> int:
        self.position += offset
        return self.position

    def tell(self) -> int:
        return self.position

    def align(self, size: int) -> 'BytesReader':
        # Faster bit-based alignment for power-of-2 sizes only
        if remainder := self.position & (size - 1):
            self.position += size - remainder
        return self

    def unpack_from(self, fmt: str | struct.Struct, size: int) -> tuple[Any, ...]:
        """Unpack data directly from buffer without creating intermediate bytes.

        This is faster than read() + struct.unpack() because it avoids creating
        a bytes object. Uses struct.unpack_from() internally.

        Args:
            fmt: struct format string or pre-compiled Struct object
            size: number of bytes to consume (must match fmt size)

        Returns:
            Tuple of unpacked values
        """
        if isinstance(fmt, struct.Struct):
            result = fmt.unpack_from(self.view, self.position)
        else:
            result = struct.unpack_from(fmt, self.view, self.position)
        self.position += size
        return result

    def unpack_one(self, fmt: str | struct.Struct, size: int) -> Any:
        """Unpack a single value directly from buffer.

        Convenience method for unpacking a single value without tuple indexing.

        Args:
            fmt: struct format string or pre-compiled Struct object
            size: number of bytes to consume

        Returns:
            Single unpacked value
        """
        if isinstance(fmt, struct.Struct):
            result = fmt.unpack_from(self.view, self.position)[0]
        else:
            result = struct.unpack_from(fmt, self.view, self.position)[0]
        self.position += size
        return result

    def size(self) -> int:
        return self._length

    def close(self) -> None:
        pass


class CrcReader(BaseReader):
    def __init__(self, reader: BaseReader):
        self._reader = reader
        self._crc = 0

    def peek(self, size: int) -> bytes:
        return self._reader.peek(size)

    def read(self, size: int | None = None) -> bytes:
        data = self._reader.read(size)
        self._crc = zlib.crc32(data, self._crc)
        return data

    def seek_from_start(self, offset: int) -> int:
        return self._reader.seek_from_start(offset)

    def seek_from_end(self, offset: int) -> int:
        return self._reader.seek_from_end(offset)

    def seek_from_current(self, offset: int) -> int:
        return self._reader.seek_from_current(offset)

    def tell(self) -> int:
        return self._reader.tell()

    def close(self) -> None:
        self._reader.close()

    def get_crc(self) -> int:
        return self._crc

    def clear_crc(self) -> None:
        self._crc = 0
