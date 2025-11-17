"""Rust-accelerated CDR encoder/decoder."""
import logging
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder

# Import Rust implementations
try:
    from pybag.pybag_rust import RustCdrEncoder as _RustCdrEncoder, RustCdrDecoder as _RustCdrDecoder
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False
    _RustCdrEncoder = None  # type: ignore
    _RustCdrDecoder = None  # type: ignore

logger = logging.getLogger(__name__)


class RustBytesWriterWrapper:
    """Wrapper to make Rust encoder's internal state compatible with BytesWriter interface."""

    def __init__(self, encoder):
        self._encoder = encoder

    def align(self, size: int) -> None:
        # The encoder handles alignment internally
        pass

    def read(self, size: int) -> bytes:
        # Not used by encoder
        raise NotImplementedError()

    def write(self, data: bytes) -> int:
        # Not used by generated code, encoder methods are called directly
        return len(data)


class RustBytesReaderWrapper:
    """Wrapper to make Rust decoder's internal state compatible with BytesReader interface."""

    def __init__(self, decoder):
        self._decoder = decoder
        self._position = 0

    def align(self, size: int) -> 'RustBytesReaderWrapper':
        self._decoder.align(size)
        return self

    def read(self, size: int) -> bytes:
        result = self._decoder.read(size)
        self._position += size
        return bytes(result)


class CdrDecoder(MessageDecoder):
    """Rust-accelerated CDR decoder."""

    def __init__(self, data: bytes):
        if not RUST_AVAILABLE:
            raise ImportError("Rust extension not available. Install with: maturin develop")

        # Create Rust decoder
        self._rust_decoder = _RustCdrDecoder(data)
        self._is_little_endian = self._rust_decoder._is_little_endian

        # Create wrapper for compatibility with schema compiler
        self._data = RustBytesReaderWrapper(self._rust_decoder)

    def parse(self, type_str: str) -> Any:
        return getattr(self._rust_decoder, type_str)()

    # Primitive parsers - delegate to Rust
    def bool(self) -> bool:
        return self._rust_decoder.bool()

    def int8(self) -> int:
        return self._rust_decoder.int8()

    def uint8(self) -> int:
        return self._rust_decoder.uint8()

    def byte(self) -> bytes:
        result = self._rust_decoder.byte()
        return bytes(result)

    def char(self) -> str:
        return self._rust_decoder.char()

    def int16(self) -> int:
        return self._rust_decoder.int16()

    def uint16(self) -> int:
        return self._rust_decoder.uint16()

    def int32(self) -> int:
        return self._rust_decoder.int32()

    def uint32(self) -> int:
        return self._rust_decoder.uint32()

    def int64(self) -> int:
        return self._rust_decoder.int64()

    def uint64(self) -> int:
        return self._rust_decoder.uint64()

    def float32(self) -> float:
        return self._rust_decoder.float32()

    def float64(self) -> float:
        return self._rust_decoder.float64()

    def string(self) -> str:
        return self._rust_decoder.string()

    # Container parsers
    def array(self, type: str, length: int) -> list:
        return [getattr(self._rust_decoder, f'{type}')() for _ in range(length)]

    def sequence(self, type: str) -> list:
        length = self.uint32()
        return [getattr(self._rust_decoder, f'{type}')() for _ in range(length)]


class CdrEncoder(MessageEncoder):
    """Rust-accelerated CDR encoder."""

    def __init__(self, *, little_endian: bool = True) -> None:
        if not RUST_AVAILABLE:
            raise ImportError("Rust extension not available. Install with: maturin develop")

        self._rust_encoder = _RustCdrEncoder(little_endian=little_endian)
        self._is_little_endian = little_endian

        # Create wrapper for compatibility with schema compiler
        self._payload = RustBytesWriterWrapper(self._rust_encoder)

    @classmethod
    def encoding(cls) -> str:
        return "cdr"

    def encode(self, type_str: str, value: Any) -> None:
        """Encode ``value`` based on ``type_str``."""
        getattr(self._rust_encoder, type_str)(value)

    def save(self) -> bytes:
        """Return the encoded byte stream."""
        return bytes(self._rust_encoder.save())

    # Primitive encoders - delegate to Rust
    def bool(self, value: bool) -> None:
        self._rust_encoder.bool(value)

    def int8(self, value: int) -> None:
        self._rust_encoder.int8(value)

    def uint8(self, value: int) -> None:
        self._rust_encoder.uint8(value)

    def byte(self, value: bytes) -> None:
        self._rust_encoder.byte(value)

    def char(self, value: str) -> None:
        self._rust_encoder.char(value)

    def int16(self, value: int) -> None:
        self._rust_encoder.int16(value)

    def uint16(self, value: int) -> None:
        self._rust_encoder.uint16(value)

    def int32(self, value: int) -> None:
        self._rust_encoder.int32(value)

    def uint32(self, value: int) -> None:
        self._rust_encoder.uint32(value)

    def int64(self, value: int) -> None:
        self._rust_encoder.int64(value)

    def uint64(self, value: int) -> None:
        self._rust_encoder.uint64(value)

    def float32(self, value: float) -> None:
        self._rust_encoder.float32(value)

    def float64(self, value: float) -> None:
        self._rust_encoder.float64(value)

    def string(self, value: str) -> None:
        self._rust_encoder.string(value)

    # Container encoders
    def array(self, type: str, values: list[Any]) -> None:
        for v in values:
            getattr(self._rust_encoder, type)(v)

    def sequence(self, type: str, values: list[Any]) -> None:
        self.uint32(len(values))
        for v in values:
            getattr(self._rust_encoder, type)(v)


__all__ = ['CdrEncoder', 'CdrDecoder', 'RUST_AVAILABLE']
