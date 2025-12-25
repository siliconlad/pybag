import zlib
from abc import ABC, abstractmethod
from enum import IntEnum
from pathlib import Path


class FilePosition(IntEnum):
    START = 0    # Start of the file
    CURRENT = 1  # Current file position
    END = 2      # End of the file


class BaseWriter(ABC):
    """Abstract base class for binary writers."""

    @abstractmethod
    def write(self, data: bytes) -> int:
        """Write bytes."""
        ...  # pragma: no cover

    @abstractmethod
    def tell(self) -> int:
        """Get the current position in the writer."""
        ...  # pragma: no cover

    @abstractmethod
    def read(self, size: int = -1) -> bytes:
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
    def truncate(self) -> None:
        """Truncate at current position"""
        ...  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        """Close the writer."""
        ...  # pragma: no cover


class FileWriter(BaseWriter):
    """Write binary data to a file."""

    def __init__(self, file_path: Path | str, mode: str = "w+b"):
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, mode)

    def write(self, data: bytes) -> int:
        return self._file.write(data)

    def tell(self) -> int:
        return self._file.tell()

    def seek_from_start(self, offset: int) -> int:
        return self._file.seek(offset, FilePosition.START)

    def seek_from_end(self, offset: int) -> int:
        return self._file.seek(-offset, FilePosition.END)

    def seek_from_current(self, offset: int) -> int:
        return self._file.seek(offset, FilePosition.CURRENT)

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def truncate(self) -> None:
        """Truncate the file at the current position."""
        self._file.truncate()

    def close(self) -> None:
        self._file.close()


class BytesWriter(BaseWriter):
    """Write binary data to an in-memory bytes buffer."""

    def __init__(self):
        self._buffer = bytearray()

    def write(self, data: bytes) -> int:
        self._buffer.extend(data)
        return len(data)

    def tell(self) -> int:
        return len(self._buffer)

    def align(self, size: int) -> None:
        # Faster bit-based alignment for power-of-2 sizes only
        if remainder := len(self._buffer) & (size - 1):
            padding = size - remainder
            self._buffer.extend(b"\x00" * padding)

    def size(self) -> int:
        return len(self._buffer)

    def as_bytes(self) -> bytes:
        return bytes(self._buffer)

    def clear(self) -> None:
        self._buffer.clear()

    def seek_from_start(self, offset: int) -> int:
        # TODO: Implement for BytesWriter
        raise NotImplementedError("seek_from_start is not yet supported")

    def seek_from_end(self, offset: int) -> int:
        # TODO: Implement for BytesWriter
        raise NotImplementedError("seek_from_end is not yet supported")

    def seek_from_current(self, offset: int) -> int:
        # TODO: Implement for BytesWriter
        raise NotImplementedError("seek_from_current is not yet supported")

    def read(self, size: int | None = None) -> bytes:
        # TODO: Implement for BytesWriter
        raise NotImplementedError("read is not yet supported")

    def truncate(self) -> None:
        # TODO: Implement for BytesWriter
        raise NotImplementedError("truncate is not yet supported")

    def close(self) -> None:
        self._buffer.clear()


class CrcWriter(BaseWriter):
    """Write binary data and track CRC32."""

    def __init__(self, writer: BaseWriter, initial_crc: int = 0):
        self._writer = writer
        self._crc = initial_crc

    def write(self, data: bytes) -> int:
        self._crc = zlib.crc32(data, self._crc)
        return self._writer.write(data)

    def tell(self) -> int:
        return self._writer.tell()

    def seek_from_start(self, offset: int) -> int:
        return self._writer.seek_from_start(offset)

    def seek_from_end(self, offset: int) -> int:
        return self._writer.seek_from_end(offset)

    def seek_from_current(self, offset: int) -> int:
        return self._writer.seek_from_current(offset)

    def read(self, size: int = -1) -> bytes:
        return self._writer.read(size)

    def truncate(self) -> None:
        self._writer.truncate()

    def get_crc(self) -> int:
        return self._crc

    def clear_crc(self) -> None:
        self._crc = 0

    def close(self) -> None:
        self._writer.close()
