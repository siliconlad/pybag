from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pybag.types import Message

from pybag.mcap.records import SchemaRecord

# Map ROS2 types to Python types
# https://docs.ros.org/en/foxy/Concepts/About-ROS-Interfaces.html#field-types
PRIMITIVE_TYPE_MAP = {
    'bool': bool,
    'byte': int,
    'char': str,
    'float32': float,
    'float64': float,
    'int8': int,
    'uint8': int,
    'int16': int,
    'uint16': int,
    'int32': int,
    'uint32': int,
    'int64': int,
    'uint64': int,
}
STRING_TYPE_MAP = {
    'string': str,
    'wstring': str,
}


@dataclass
class SchemaFieldType(ABC):
    ...


@dataclass
class Primitive(SchemaFieldType):
    type: str

    @classmethod
    def is_primitive(cls, type: str) -> bool:
        return type in PRIMITIVE_TYPE_MAP


@dataclass
class String(SchemaFieldType):
    type: str = 'string'
    max_length: int | None = None


@dataclass
class Array(SchemaFieldType):
    type: 'SchemaFieldType'
    length: int
    is_bounded: bool = False  # False == fixed length


@dataclass
class Sequence(SchemaFieldType):
    type: 'SchemaFieldType'


@dataclass
class Complex(SchemaFieldType):
    type: str


@dataclass
class SchemaEntry(ABC):
    ...


@dataclass
class SchemaConstant(SchemaEntry):
    type: SchemaFieldType
    value: int | float | bool | str | bytes


@dataclass
class SchemaField(SchemaEntry):
    type: SchemaFieldType
    default: Any = None


@dataclass
class Schema:
    name: str
    fields: dict[str, SchemaEntry]


class SchemaDecoder(ABC):
    @abstractmethod
    def parse_schema(self, schema: SchemaRecord) -> tuple[Schema, dict[str, Schema]]:
        """Decode a schema into a Python object."""
        ...  # pragma: no cover


class SchemaEncoder(ABC):
    @classmethod
    @abstractmethod
    def encoding(cls) -> str:
        """The encoding to use for the schema."""
        ...  # pragma: no cover

    @abstractmethod
    def encode(self, schema: Message | type[Message]) -> bytes:
        """Encode a schema into a bytes object."""
        ...  # pragma: no cover

    @abstractmethod
    def parse_schema(self, schema: Message | type[Message]) -> tuple[Schema, dict[str, Schema]]:
        """Parse a schema into a Python object."""
        ...  # pragma: no cover
