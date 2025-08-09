from typing import Annotated, TypeVar

Int8 = Annotated[int, ("int", 8, True)]
Int16 = Annotated[int, ("int", 16, True)]
Int32 = Annotated[int, ("int", 32, True)]
Int64 = Annotated[int, ("int", 64, True)]

UInt8 = Annotated[int, ("int", 8, False)]
UInt16 = Annotated[int, ("int", 16, False)]
UInt32 = Annotated[int, ("int", 32, False)]
UInt64 = Annotated[int, ("int", 64, False)]

Float32 = Annotated[float, ("float", 32)]
Float64 = Annotated[float, ("float", 64)]

String = Annotated[str, ("string",)]

T = TypeVar("T")


def Sequence(type_: type[T]) -> type[list[T]]:
    return Annotated[list[type_], ("sequence", type_)]


def Array(type_: type[T], length: int, *, bounded: bool = False) -> type[list[T]]:
    return Annotated[list[type_], ("array", type_, length, bounded)]

__all__ = [
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "String",
    "Sequence",
    "Array",
]
