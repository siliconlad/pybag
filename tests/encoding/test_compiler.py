from dataclasses import dataclass
from typing import Literal

import pybag
from pybag.serialize import MessageSerializerFactory
from pybag.schema.ros2msg import Complex


@dataclass
class SubMessage:
    __msg_name__ = "tests/msgs/SubMessage"
    value: pybag.int32


@dataclass
class ExampleMessage:
    __msg_name__ = "tests/msgs/ExampleMessage"
    integer: pybag.int32
    text: pybag.string
    fixed: pybag.Array[pybag.int32, Literal[3]]
    dynamic: pybag.Array[pybag.int32]
    sub: pybag.Complex[SubMessage]
    sub_array: pybag.Array[pybag.Complex[SubMessage], Literal[3]]


def test_compiled_encoder_matches() -> None:
    msg = ExampleMessage(
        integer=42,
        text="hello",
        fixed=[1, 2, 3],
        dynamic=[4, 5],
        sub=SubMessage(7),
        sub_array=[SubMessage(1), SubMessage(2), SubMessage(3)],
    )
    serializer = MessageSerializerFactory.from_profile("ros2")
    assert serializer is not None

    data_compiled = serializer.serialize_message(msg)

    encoder_ref = serializer._message_encoder(little_endian=True)
    schema, sub_schemas = serializer._schema_encoder.parse_schema(msg)
    if isinstance(schema, Complex):  # pragma: no cover - defensive
        schema = sub_schemas[schema.type]
    serializer._encode_message(encoder_ref, msg, schema, sub_schemas)
    data_ref = encoder_ref.save()

    assert data_compiled == data_ref
