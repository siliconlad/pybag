"""JSON message encoding and decoding."""

import json
from typing import Any

from pybag.encoding import MessageDecoder, MessageEncoder


class JsonDecoder(MessageDecoder):
    """Decode messages from JSON format.

    The JSON format is a flat array of values in field order.
    This matches the sequential access pattern expected by the compiled schema.
    """

    def __init__(self, data: bytes):
        """Initialize decoder with JSON data.

        Args:
            data: UTF-8 encoded JSON bytes representing an array of values.
        """
        self._values: list[Any] = json.loads(data.decode('utf-8'))
        self._index = 0

    def parse(self, type_str: str) -> Any:
        return getattr(self, type_str)()

    def _next(self) -> Any:
        """Get the next value and advance the index."""
        value = self._values[self._index]
        self._index += 1
        return value

    # Primitive parsers -------------------------------------------------

    def bool(self) -> bool:
        return bool(self._next())

    def int8(self) -> int:
        return int(self._next())

    def uint8(self) -> int:
        return int(self._next())

    def byte(self) -> bytes:
        value = self._next()
        if isinstance(value, int):
            return bytes([value])
        return bytes(value)

    def char(self) -> str:
        value = self._next()
        if isinstance(value, int):
            return chr(value)
        return str(value)

    def int16(self) -> int:
        return int(self._next())

    def uint16(self) -> int:
        return int(self._next())

    def int32(self) -> int:
        return int(self._next())

    def uint32(self) -> int:
        return int(self._next())

    def int64(self) -> int:
        return int(self._next())

    def uint64(self) -> int:
        return int(self._next())

    def float32(self) -> float:
        return float(self._next())

    def float64(self) -> float:
        return float(self._next())

    def string(self) -> str:
        return str(self._next())

    # Container parsers --------------------------------------------------

    def array(self, type: str, length: int) -> list:
        """Parse a fixed-size array.

        In JSON format, arrays are stored as nested lists.
        """
        values = self._next()
        if isinstance(values, list):
            return [self._convert_value(type, v) for v in values]
        # Handle case where values were encoded inline
        return [self._convert_value(type, values)] + [
            self._convert_value(type, self._next()) for _ in range(length - 1)
        ]

    def sequence(self, type: str) -> list:
        """Parse a variable-size sequence.

        In JSON format, sequences are stored as nested lists.
        """
        values = self._next()
        if isinstance(values, list):
            return [self._convert_value(type, v) for v in values]
        return [self._convert_value(type, values)]

    def _convert_value(self, type_str: str, value: Any) -> Any:
        """Convert a value to the appropriate type."""
        if type_str == 'bool':
            return bool(value)
        elif type_str in ('int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64'):
            return int(value)
        elif type_str in ('float32', 'float64'):
            return float(value)
        elif type_str == 'string':
            return str(value)
        elif type_str == 'byte':
            return bytes([value]) if isinstance(value, int) else bytes(value)
        elif type_str == 'char':
            return chr(value) if isinstance(value, int) else str(value)
        return value


class JsonEncoder(MessageEncoder):
    """Encode messages to JSON format.

    The JSON format is a flat array of values in field order.
    This matches the sequential encoding pattern expected by the compiled serializer.
    """

    def __init__(self, *, little_endian: bool = True) -> None:
        """Initialize encoder.

        Args:
            little_endian: Ignored for JSON, kept for API compatibility.
        """
        # little_endian is ignored for JSON but needed for API compatibility
        self._is_little_endian = little_endian
        self._values: list[Any] = []

    @classmethod
    def encoding(cls) -> str:
        return "json"

    def encode(self, type_str: str, value: Any) -> None:
        getattr(self, type_str)(value)

    def save(self) -> bytes:
        """Return the encoded JSON bytes."""
        return json.dumps(self._values).encode('utf-8')

    # Primitive encoders -------------------------------------------------

    def bool(self, value: bool) -> None:
        self._values.append(bool(value))

    def int8(self, value: int) -> None:
        self._values.append(int(value))

    def uint8(self, value: int) -> None:
        self._values.append(int(value))

    def byte(self, value: bytes) -> None:
        if isinstance(value, int):
            self._values.append(value)
        elif isinstance(value, (bytes, bytearray)):
            self._values.append(value[0] if len(value) == 1 else list(value))
        else:
            self._values.append(int(value))

    def char(self, value: str) -> None:
        if isinstance(value, str):
            self._values.append(ord(value) if len(value) == 1 else value)
        else:
            self._values.append(int(value))

    def int16(self, value: int) -> None:
        self._values.append(int(value))

    def uint16(self, value: int) -> None:
        self._values.append(int(value))

    def int32(self, value: int) -> None:
        self._values.append(int(value))

    def uint32(self, value: int) -> None:
        self._values.append(int(value))

    def int64(self, value: int) -> None:
        self._values.append(int(value))

    def uint64(self, value: int) -> None:
        self._values.append(int(value))

    def float32(self, value: float) -> None:
        self._values.append(float(value))

    def float64(self, value: float) -> None:
        self._values.append(float(value))

    def string(self, value: str) -> None:
        self._values.append(str(value))

    # Container encoders -------------------------------------------------

    def array(self, type: str, values: list[Any]) -> None:
        """Encode a fixed-size array."""
        self._values.append(list(values))

    def sequence(self, type: str, values: list[Any]) -> None:
        """Encode a variable-size sequence."""
        self._values.append(list(values))
