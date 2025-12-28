from abc import ABC, abstractmethod
from typing import Any


class MessageDecoder(ABC):
    @abstractmethod
    def parse(self, type_str: str) -> Any:
        ...  # pragma: no cover

    # Primitive parsers -------------------------------------------------

    @abstractmethod
    def bool(self) -> bool:
        ...  # pragma: no cover

    @abstractmethod
    def int8(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def uint8(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def byte(self) -> bytes:
        ...  # pragma: no cover

    @abstractmethod
    def char(self) -> str | int:  # ros1: int, ros2: str
        ...  # pragma: no cover

    @abstractmethod
    def int16(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def uint16(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def int32(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def uint32(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def int64(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def uint64(self) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def float32(self) -> float:
        ...  # pragma: no cover

    @abstractmethod
    def float64(self) -> float:
        ...  # pragma: no cover

    @abstractmethod
    def string(self) -> str:
        ...  # pragma: no cover

    # Container parsers --------------------------------------------------

    @abstractmethod
    def array(self, type: str, length: int) -> list:
        ...  # pragma: no cover

    @abstractmethod
    def sequence(self, type: str) -> list:
        ...  # pragma: no cover


class MessageEncoder(ABC):
    @classmethod
    @abstractmethod
    def encoding(cls) -> str:
        """The encoding to use for the message."""
        ...  # pragma: no cover

    @abstractmethod
    def encode(self, type_str: str, value: Any) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def save(self) -> bytes:
        ...  # pragma: no cover

    # Primitive encoders -------------------------------------------------

    @abstractmethod
    def bool(self, value: bool) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def int8(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def uint8(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def byte(self, value: bytes) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def char(self, value: Any) -> None:  # ros1: int, ros2: str
        ...  # pragma: no cover

    @abstractmethod
    def int16(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def uint16(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def int32(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def uint32(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def int64(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def uint64(self, value: int) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def float32(self, value: float) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def float64(self, value: float) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def string(self, value: str) -> None:
        ...  # pragma: no cover

    # Container encoders -------------------------------------------------

    @abstractmethod
    def array(self, type: str, values: list[Any]) -> None:
        ...  # pragma: no cover

    @abstractmethod
    def sequence(self, type: str, values: list[Any]) -> None:
        ...  # pragma: no cover
