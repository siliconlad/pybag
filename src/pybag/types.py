from __future__ import annotations

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
byte = Annotated[bytes, ("byte",)]
char = Annotated[str, ("char",)]
string = Annotated[str, ("string",)]
wstring = Annotated[str, ("wstring",)]

T = TypeVar("T")


@runtime_checkable
class Message(Protocol):
    """Message protocol for reading and writing."""

    __msg_name__: str


def _is_message_type(type_: Any) -> bool:
    """Check if a type is a message type (has __msg_name__ attribute)."""
    return hasattr(type_, '__msg_name__')


def _wrap_if_message(type_: Any) -> Any:
    """Wrap a type with Complex annotation if it's a message type."""
    if _is_message_type(type_):
        return Annotated[type_, ("complex", type_.__msg_name__)]
    return type_


# Type-checker compatible version using Generic classes
class _ConstantType(Generic[T]):
    """Generic type for constants."""
    def __class_getitem__(cls, type_: type[T]) -> type[T]:
        return Annotated[type_, ("constant", type_)]


class _ArrayType:
    """Generic type for arrays that accepts both single type and type+length."""
    @classmethod
    def __class_getitem__(cls, params: Any) -> type[list]:
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
    def __class_getitem__(cls, type_: type[T]) -> type[T]:
        return Annotated[type_, ("complex", type_.__msg_name__)]


# Type aliases for use in type annotations
Constant: TypeAlias = _ConstantType
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
    "string",
    "wstring",
    "Message",
    "Array",
    "Complex",
    "Constant",
]
