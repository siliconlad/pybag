from dataclasses import dataclass, field

import pytest
import pybag.types
from pybag.schema.ros2msg import (
    Primitive,
    Sequence,
    String,
    Array,
    SchemaField,
    Ros2MsgError,
    Ros2MsgSchemaEncoder,
)


def test_serialize_dataclass_primitives() -> None:
    @dataclass
    class Example:
        integer: pybag.types.int32
        name: pybag.types.string
        flag: pybag.types.bool

    obj = Example(42, "hi", True)
    schema = Ros2MsgSchemaEncoder().encode(Example)

    assert schema.name == 'Example'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=None)
    assert schema.fields['name'] == SchemaField(String('string'), default=None)
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=None)


def test_serialize_dataclass_primitives_with_default() -> None:
    @dataclass
    class ExampleDefault:
        integer: pybag.types.int32 = 42
        name: pybag.types.string = "hi"
        flag: pybag.types.bool = True

    schema = Ros2MsgSchemaEncoder().encode(ExampleDefault)
    assert schema.name == 'ExampleDefault'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=42)
    assert schema.fields['name'] == SchemaField(String('string'), default="hi")
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=True)


def test_serialize_array() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.types.Array(pybag.types.int32)
    schema = Ros2MsgSchemaEncoder().encode(ArrayExample)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=None)


def test_serialize_array_with_length() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.types.Array(pybag.types.int32, 3)
    schema = Ros2MsgSchemaEncoder().encode(ArrayExample)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Array(Primitive('int32'), length=3, is_bounded=False), default=None)


def test_serialize_array_with_default() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.types.Array(pybag.types.int32) = field(default_factory=lambda: [1, 2, 3])
    schema = Ros2MsgSchemaEncoder().encode(ArrayExample)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=[1, 2, 3])


def test_serialize_python_types_int() -> None:
    @dataclass
    class Missing:
        value: int

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().encode(Missing)


def test_serialize_python_types_str() -> None:
    @dataclass
    class Unsupported:
        value: str

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().encode(Unsupported)


def test_serialize_python_types_float() -> None:
    @dataclass
    class Unsupported:
        value: float

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().encode(Unsupported)


def test_serialize_python_types_bool() -> None:
    @dataclass
    class Unsupported:
        value: bool

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().encode(Unsupported)
