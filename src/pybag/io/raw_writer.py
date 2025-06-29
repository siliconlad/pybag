
from abc import ABC, abstractmethod
from pathlib import Path

class BaseWriter(ABC):
    """Abstract base class for binary writers."""

    @abstractmethod
    def write(self, data: bytes) -> int:
        """Write bytes to the writer."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the writer and release all resources."""
        ...


class FileWriter(BaseWriter):
    """Write binary data to a file."""

    def __init__(self, file_path: Path | str, mode: str = "wb"):
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, mode)

    def write(self, data: bytes) -> int:
        return self._file.write(data)

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class BytesWriter(BaseWriter):
    """Write binary data to an in-memory bytes buffer."""

    def __init__(self):
        self._buffer = bytearray()

    def write(self, data: bytes) -> int:
        self._buffer.extend(data)
        return len(data)

    def align(self, size: int) -> None:
        """Pad the buffer with zeros so the next write is aligned to ``size`` bytes."""
        padding = (-len(self._buffer)) % size
        if padding:
            self._buffer.extend(b"\x00" * padding)

    def size(self) -> int:
        """Return the total number of bytes written."""
        return len(self._buffer)

    def as_bytes(self) -> bytes:
        """Return the written bytes and clear the buffer."""
        data = bytes(self._buffer)
        self._buffer.clear()
        return data

    def close(self) -> None:
        self._buffer.clear()
