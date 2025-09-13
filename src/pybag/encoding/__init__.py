from abc import ABC, abstractmethod
from typing import Any


class MessageDecoder(ABC):
    @abstractmethod
    def push(self, type_str: str) -> 'MessageDecoder':
        ...

    @abstractmethod
    def load(self, *type_strs: str) -> tuple[Any, ...]:
        ...

    # Primitive parsers -------------------------------------------------

    @abstractmethod
    def bool(self):
        ...

    @abstractmethod
    def int8(self):
        ...

    @abstractmethod
    def uint8(self):
        ...

    @abstractmethod
    def byte(self):
        ...

    @abstractmethod
    def char(self):
        ...

    @abstractmethod
    def int16(self):
        ...

    @abstractmethod
    def uint16(self):
        ...

    @abstractmethod
    def int32(self):
        ...

    @abstractmethod
    def uint32(self):
        ...

    @abstractmethod
    def int64(self):
        ...

    @abstractmethod
    def uint64(self):
        ...

    @abstractmethod
    def float32(self):
        ...

    @abstractmethod
    def float64(self):
        ...

    @abstractmethod
    def string(self):
        ...

    # Container parsers --------------------------------------------------

    @abstractmethod
    def array(self, type: str, length: int):
        ...

    @abstractmethod
    def sequence(self, type: str):
        ...


class MessageEncoder(ABC):
    @classmethod
    @abstractmethod
    def encoding(cls) -> str:
        """The encoding to use for the message."""

    @abstractmethod
    def encode(self, type_str: str, value: Any) -> None:
        ...

    @abstractmethod
    def save(self) -> bytes:
        ...

    # Primitive encoders -------------------------------------------------

    @abstractmethod
    def bool(self, value: bool) -> None:
        ...

    @abstractmethod
    def int8(self, value: int) -> None:
        ...

    @abstractmethod
    def uint8(self, value: int) -> None:
        ...

    @abstractmethod
    def byte(self, value: bytes) -> None:
        ...

    @abstractmethod
    def char(self, value: str) -> None:
        ...

    @abstractmethod
    def int16(self, value: int) -> None:
        ...

    @abstractmethod
    def uint16(self, value: int) -> None:
        ...

    @abstractmethod
    def int32(self, value: int) -> None:
        ...

    @abstractmethod
    def uint32(self, value: int) -> None:
        ...

    @abstractmethod
    def int64(self, value: int) -> None:
        ...

    @abstractmethod
    def uint64(self, value: int) -> None:
        ...

    @abstractmethod
    def float32(self, value: float) -> None:
        ...

    @abstractmethod
    def float64(self, value: float) -> None:
        ...

    @abstractmethod
    def string(self, value: str) -> None:
        ...

    # Container encoders -------------------------------------------------

    @abstractmethod
    def array(self, type: str, values: list[Any]) -> None:
        ...

    @abstractmethod
    def sequence(self, type: str, values: list[Any]) -> None:
        ...
