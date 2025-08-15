import zlib
from abc import ABC, abstractmethod
from enum import IntEnum
from pathlib import Path


class FilePosition(IntEnum):
    START = 0    # Start of the file
    CURRENT = 1  # Current file position
    END = 2      # End of the file


class BaseReader(ABC):
    @abstractmethod
    def peek(self, size: int) -> bytes:
        """Peek at the next bytes in the reader."""
        ...

    @abstractmethod
    def read(self, size: int | None = None) -> bytes:
        """Read the next bytes in the reader."""
        ...

    @abstractmethod
    def seek_from_start(self, offset: int) -> int:
        """Seek from the start of the reader."""
        ...

    @abstractmethod
    def seek_from_end(self, offset: int) -> int:
        """Seek from the end of the reader."""
        ...

    @abstractmethod
    def seek_from_current(self, offset: int) -> int:
        """Seek from the current position of the reader."""
        ...

    @abstractmethod
    def tell(self) -> int:
        """Get the current position in the reader."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the reader and release all resources."""
        ...


class FileReader(BaseReader):
    def __init__(self, file_path: Path | str, mode: str = 'rb'):
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, mode)

    def peek(self, size: int) -> bytes:
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


class BytesReader(BaseReader):
    def __init__(self, data: bytes):
        self._data = data
        self._position = 0

    def peek(self, size: int) -> bytes:
        return self._data[self._position:self._position + size]

    def read(self, size: int | None = None) -> bytes:
        if size is None:
            return self._data
        result = self._data[self._position:self._position + size]
        self._position += size
        return result

    def seek_from_start(self, offset: int) -> int:
        self._position = offset
        return self._position

    def seek_from_end(self, offset: int) -> int:
        self._position = len(self._data) - offset
        return self._position

    def seek_from_current(self, offset: int) -> int:
        self._position += offset
        return self._position

    def tell(self) -> int:
        return self._position

    def align(self, size: int) -> 'BytesReader':
        if self._position % size > 0:
            self._position += size - (self._position % size)
        return self

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
