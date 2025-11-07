import zlib
from abc import ABC, abstractmethod
from http.client import HTTPConnection, HTTPSConnection
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit


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
    def close(self) -> None:
        """Close the writer."""
        ...  # pragma: no cover


class FileWriter(BaseWriter):
    """Write binary data to a file."""

    def __init__(self, file_path: Path | str, mode: str = "wb"):
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, mode)
        self._bytes_written = 0

    def write(self, data: bytes) -> int:
        self._bytes_written += len(data)
        return self._file.write(data)

    def tell(self) -> int:
        return self._bytes_written

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
        current_length = len(self._buffer)
        if current_length % size > 0:
            padding = size - (current_length % size)
            self._buffer.extend(b"\x00" * padding)

    def size(self) -> int:
        return len(self._buffer)

    def as_bytes(self) -> bytes:
        return bytes(self._buffer)

    def clear(self) -> None:
        self._buffer.clear()

    def close(self) -> None:
        self._buffer.clear()


class CrcWriter(BaseWriter):
    """Write binary data and track CRC32."""

    def __init__(self, writer: BaseWriter):
        self._writer = writer
        self._crc = 0

    def write(self, data: bytes) -> int:
        self._crc = zlib.crc32(data, self._crc)
        return self._writer.write(data)

    def tell(self) -> int:
        return self._writer.tell()

    def get_crc(self) -> int:
        return self._crc

    def clear_crc(self) -> None:
        self._crc = 0

    def close(self) -> None:
        self._writer.close()


class NetworkWriter(BaseWriter):
    """Stream binary data to a network destination using HTTP chunked transfer."""

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported URL scheme for network writer: {parsed.scheme!r}")
        if parsed.hostname is None:
            raise ValueError(f"URL must include a hostname: {url!r}")

        self._bytes_written = 0
        self._closed = False

        connection_cls = HTTPConnection if parsed.scheme == "http" else HTTPSConnection
        port = parsed.port
        self._connection = connection_cls(parsed.hostname, port=port, timeout=timeout)

        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        self._connection.putrequest(method, path)
        headers = {"Content-Type": "application/octet-stream", **(headers or {})}
        # Use chunked transfer to avoid buffering the entire MCAP file in memory.
        self._connection.putheader("Transfer-Encoding", "chunked")
        for header, value in headers.items():
            self._connection.putheader(header, value)
        self._connection.endheaders()

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("Cannot write to a closed NetworkWriter")
        if not data:
            return 0

        chunk_header = f"{len(data):X}\r\n".encode("ascii")
        self._connection.send(chunk_header)
        self._connection.send(data)
        self._connection.send(b"\r\n")
        self._bytes_written += len(data)
        return len(data)

    def tell(self) -> int:
        return self._bytes_written

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Terminate the chunked transfer stream.
        self._connection.send(b"0\r\n\r\n")
        try:
            response = self._connection.getresponse()
            # Ensure the response body is fully consumed so the server can reuse the connection.
            response.read()
        finally:
            self._connection.close()
