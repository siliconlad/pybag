from abc import ABC, abstractmethod
from typing import Any


class MessageDecoder(ABC):
    @abstractmethod
    def parse(self, type_str: str) -> Any:
        ...

    # Primitive parsers -------------------------------------------------

    @abstractmethod
    def bool(self) -> bool:
        ...

    @abstractmethod
    def int8(self) -> int:
        ...

    @abstractmethod
    def uint8(self) -> int:
        ...

    @abstractmethod
    def byte(self) -> bytes:
        ...

    @abstractmethod
    def char(self) -> str:
        ...

    @abstractmethod
    def int16(self) -> int:
        ...

    @abstractmethod
    def uint16(self) -> int:
        ...

    @abstractmethod
    def int32(self) -> int:
        ...

    @abstractmethod
    def uint32(self) -> int:
        ...

    @abstractmethod
    def int64(self) -> int:
        ...

    @abstractmethod
    def uint64(self) -> int:
        ...

    @abstractmethod
    def float32(self) -> float:
        ...

    @abstractmethod
    def float64(self) -> float:
        ...

    @abstractmethod
    def string(self) -> str:
        ...

    # Container parsers --------------------------------------------------

    @abstractmethod
    def array(self, type: str, length: int) -> list:
        ...

    @abstractmethod
    def sequence(self, type: str) -> list:
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
