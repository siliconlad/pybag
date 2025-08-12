from dataclasses import dataclass
from typing import Annotated

import pytest

import pybag.types
from pybag.encoding.cdr import CdrDecoder
from pybag.mcap_writer import serialize_message


@dataclass
class SubMessage:
    value: pybag.types.int32


@dataclass
class ExampleMessage:
    integer: pybag.types.int32
    text: pybag.types.string
    fixed: pybag.types.Array(pybag.types.int32, 3)
    dynamic: pybag.types.Array(pybag.types.int32)
    sub: Annotated[SubMessage, ("SubMessage",)]


def _create_example() -> ExampleMessage:
    return ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
    )


@pytest.mark.parametrize("little_endian", [True, False])
def test_serialize_message_roundtrip(little_endian: bool) -> None:
    msg = _create_example()
    data = serialize_message(msg, little_endian=little_endian)

    decoder = CdrDecoder(data)
    assert decoder.int32() == 42
    assert decoder.string() == "hello"
    assert decoder.array("int32", 3) == [1, 2, 3]
    assert decoder.sequence("int32") == [4, 5]
    assert decoder.int32() == 7


def test_serialize_message_endianness_diff() -> None:
    msg = _create_example()
    le = serialize_message(msg, little_endian=True)
    be = serialize_message(msg, little_endian=False)
    assert le != be

