from abc import ABC, abstractmethod
from pathlib import Path


class BaseWriter(ABC):
    """Abstract base class for binary writers."""

    @abstractmethod
    def write(self, data: bytes) -> int:
        """Write bytes."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the writer."""
        ...


class FileWriter(BaseWriter):
    """Write binary data to a file."""

    def __init__(self, file_path: Path | str, mode: str = "wb"):
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, mode)

    def write(self, data: bytes) -> int:
        return self._file.write(data)

    def close(self) -> None:
        self._file.close()


class BytesWriter(BaseWriter):
    """Write binary data to an in-memory bytes buffer."""

    def __init__(self):
        self._buffer = bytearray()

    def write(self, data: bytes) -> int:
        self._buffer.extend(data)
        return len(data)

    def align(self, size: int) -> None:
        current_length = len(self._buffer)
        if current_length % size > 0:
            padding = size - (current_length % size)
            self._buffer.extend(b"\x00" * padding)

    def size(self) -> int:
        return len(self._buffer)

    def as_bytes(self) -> bytes:
        return bytes(self._buffer)

    def close(self) -> None:
        self._buffer.clear()
