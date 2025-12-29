from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    runtime_checkable
)

int8 = Annotated[int, ("int8",)]
int16 = Annotated[int, ("int16",)]
int32 = Annotated[int, ("int32",)]
int64 = Annotated[int, ("int64",)]

uint8 = Annotated[int, ("uint8",)]
uint16 = Annotated[int, ("uint16",)]
uint32 = Annotated[int, ("uint32",)]
uint64 = Annotated[int, ("uint64",)]

float32 = Annotated[float, ("float32",)]
float64 = Annotated[float, ("float64",)]

bool = Annotated[bool, ("bool",)]
byte = Annotated[int, ("byte",)]
string = Annotated[str, ("string",)]
wstring = Annotated[str, ("wstring",)]


# ROS 1 time and duration types
# Two-integer timestamp that is expressed as:
# * secs: seconds since epoch
# * nsecs: nanoseconds since secs
@dataclass(frozen=True, slots=True)
class Time:
    """ROS 1 time representation with secs and nsecs attributes.

    This is a ROS 1 specific type. For ROS 2, use builtin_interfaces/Time instead.
    """
    secs: int
    nsecs: int

    def to_ns(self) -> int:
        """Convert to nanoseconds since epoch."""
        return self.secs * 1_000_000_000 + self.nsecs

    def to_sec(self) -> float:
        """Convert to seconds since epoch."""
        return self.secs + self.nsecs / 1_000_000_000

    @classmethod
    def from_ns(cls, nsec: int) -> 'Time':
        """Create Time from nanoseconds since epoch."""
        return cls(secs=nsec // 1_000_000_000, nsecs=nsec % 1_000_000_000)


@dataclass(frozen=True, slots=True)
class Duration:
    """ROS 1 duration representation with secs and nsecs attributes.

    This is a ROS 1 specific type. For ROS 2, use builtin_interfaces/Duration instead.
    """
    secs: int
    nsecs: int

    def to_ns(self) -> int:
        """Convert to nanoseconds."""
        return self.secs * 1_000_000_000 + self.nsecs

    def to_sec(self) -> float:
        """Convert to seconds since epoch."""
        return self.secs + self.nsecs / 1_000_000_000

    @classmethod
    def from_ns(cls, nsec: int) -> 'Duration':
        """Create Duration from nanoseconds."""
        return cls(secs=nsec // 1_000_000_000, nsecs=nsec % 1_000_000_000)


# ROS 1 namespace for ROS 1 specific types
# Usage: pybag.ros1.Time, pybag.ros1.Duration or t.ros1.Time, t.ros1.Duration
ros1 = SimpleNamespace(
    Time=Time,
    Duration=Duration,
    # Type annotations for time and duration fields (ROS 1 only)
    time = Annotated[Time, ("time",)],
    duration = Annotated[Duration, ("duration",)],
    # ROS 1 char is uint8
    char = Annotated[int, ("char",)]
)

# ROS 2 namespace for ROS 2 specific types
# Usage: t.ros2.char
ros2 = SimpleNamespace(
    # ROS 2 char is a single character (string)
    char = Annotated[str, ("char",)]
)


T = TypeVar("T")


@runtime_checkable
class Message(Protocol):
    """Message protocol for reading and writing."""

    __msg_name__: str


@dataclass(frozen=True, slots=True)
class SchemaText:
    """Bundled schema name and text for writing messages.

    This class holds the textual representation of a message schema,
    including the message type name and the schema definition text.

    Attributes:
        name: The message type name (e.g., "std_msgs/Header" for ROS1
              or "std_msgs/msg/Header" for ROS2).
        text: The schema definition text in ros1msg or ros2msg format.
    """
    name: str
    text: str


def _is_message_type(type_: Any) -> bool:
    """Check if a type is a message type (has __msg_name__ attribute)."""
    return hasattr(type_, '__msg_name__')


def _wrap_if_message(type_: Any) -> Any:
    """Wrap a type with Complex annotation if it's a message type."""
    if _is_message_type(type_):
        return Annotated[type_, ("complex", type_.__msg_name__)]
    return type_


# Constant is a type alias that resolves to the underlying type at the type level.
# At runtime, Constant[T] creates an Annotated type for schema encoding.
class _ConstantType(Generic[T]):
    """Generic type for constants."""

    def __class_getitem__(cls, type_: Any) -> Any:
        return Annotated[type_, ("constant", type_)]


# For type checking, Constant[T] should be equivalent to T
# We use a TypeAlias with the class for runtime behavior
Constant: TypeAlias = T  # type: ignore[type-arg]
# Override at runtime with the class that provides __class_getitem__
Constant = _ConstantType  # type: ignore[misc,assignment]


class _ArrayType:
    """Generic type for arrays that accepts both single type and type+length."""
    @classmethod
    def __class_getitem__(cls, params: Any) -> Any:
        if isinstance(params, tuple):
            # Array[type, length] - fixed size array
            if len(params) == 2:
                type_, length = params
                # Auto-wrap message types with Complex
                wrapped_type = _wrap_if_message(type_)
                return Annotated[list[wrapped_type], ("array", wrapped_type, length)]
            else:
                raise TypeError("Array expects either 1 or 2 parameters")
        else:
            # Array[type] - variable size array
            # Auto-wrap message types with Complex
            wrapped_type = _wrap_if_message(params)
            return Annotated[list[wrapped_type], ("array", wrapped_type, None)]


class _ComplexType(Generic[T]):
    """Generic type for complex/nested types."""
    def __class_getitem__(cls, type_: Any) -> Any:
        return Annotated[type_, ("complex", type_.__msg_name__)]


# Type aliases for use in type annotations
Array: TypeAlias = _ArrayType
Complex: TypeAlias = _ComplexType


__all__ = [
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "bool",
    "byte",
    "string",
    "wstring",
    "ros1",
    "ros2",
    "Time",
    "Duration",
    "Message",
    "SchemaText",
    "Array",
    "Complex",
    "Constant",
]
