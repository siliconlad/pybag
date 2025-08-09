import pytest
from dataclasses import dataclass
from typing import Annotated, List

from pybag.encoding.cdr import CdrDecoder
from pybag.encoding.serializer import serialize


@dataclass
class Example:
    integer: Annotated[int, "int32"]
    name: Annotated[str, "string"]
    flag: Annotated[bool, "bool"]


def test_serialize_dataclass_primitives() -> None:
    obj = Example(42, "hi", True)
    data = serialize(obj)

    decoder = CdrDecoder(data)
    assert decoder.int32() == 42
    assert decoder.string() == "hi"
    assert decoder.bool() is True


@dataclass
class SeqExample:
    numbers: Annotated[List[int], ("sequence", "int32")]


def test_serialize_sequence() -> None:
    obj = SeqExample([1, 2, 3])
    data = serialize(obj)

    decoder = CdrDecoder(data)
    assert decoder.sequence("int32") == [1, 2, 3]


@dataclass
class Missing:
    value: int


def test_missing_annotation_error() -> None:
    with pytest.raises(TypeError):
        serialize(Missing(1))


@dataclass
class Unsupported:
    value: Annotated[int, "unknown"]


def test_unsupported_annotation_error() -> None:
    with pytest.raises(ValueError):
        serialize(Unsupported(1))
