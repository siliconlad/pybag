from abc import ABC, abstractmethod
from pathlib import Path

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

    def align(self, size: int) -> 'BytesReader':
        if self._position % size > 0:
            self._position += size - (self._position % size)
        return self

    def close(self) -> None:
        pass
