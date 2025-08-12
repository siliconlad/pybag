from dataclasses import dataclass
from typing import Annotated

import pytest

import pybag.types
from pybag.encoding.cdr import CdrDecoder
from pybag.mcap_writer import serialize_message


@dataclass
class SubMessage:
    value: pybag.int32


@dataclass
class ExampleMessage:
    integer: pybag.int32
    text: pybag.string
    fixed: pybag.Array(pybag.int32, length=3)
    dynamic: pybag.Array(pybag.int32, length=None)
    sub: pybag.Complex(SubMessage)
    sub_array: pybag.Array(pybag.Complex(SubMessage), length=3)


@pytest.mark.parametrize("little_endian", [True, False])
def test_serialize_message_roundtrip(little_endian: bool) -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    data = serialize_message(msg, little_endian=little_endian)

    decoder = CdrDecoder(data)
    # integer
    assert decoder.int32() == 42
    # text
    assert decoder.string() == "hello"
    # fixed
    assert decoder.array("int32", 3) == [1, 2, 3]
    # dynamic
    assert decoder.sequence("int32") == [4, 5]
    # sub
    assert decoder.int32() == 7
    # sub_array
    assert decoder.int32() == 1
    assert decoder.int32() == 2
    assert decoder.int32() == 3


def test_serialize_message_endianness_diff() -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    le = serialize_message(msg, little_endian=True)
    be = serialize_message(msg, little_endian=False)
    assert le != be
