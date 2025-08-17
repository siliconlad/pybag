from typing import Annotated, TypeVar

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


def Constant(type_: type[T]) -> type[T]:
    return Annotated[type_, ("constant", type_)]


def Array(type_: type[T], length: int | None = None) -> type[list[T]]:
    return Annotated[list[type_], ("array", type_, length)]


def Complex(type_: type[T]) -> type[T]:
    return Annotated[type_, ("complex", type_.__msg_name__)]


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
    "Array",
    "Complex",
    "Constant",
]
