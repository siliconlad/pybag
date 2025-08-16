from dataclasses import dataclass, field

import pytest

import pybag.types
from pybag.schema.ros2msg import (
    Array,
    Primitive,
    Ros2MsgError,
    Ros2MsgSchemaEncoder,
    SchemaField,
    Sequence,
    String
)


def test_serialize_dataclass_primitives() -> None:
    @dataclass
    class Example:
        __msg_name__ = 'tests/msgs/Example'

        integer: pybag.int32
        name: pybag.string
        flag: pybag.bool

    obj = Example(42, "hi", True)
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/Example'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=None)
    assert schema.fields['name'] == SchemaField(String('string'), default=None)
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_dataclass_primitives_with_default() -> None:
    @dataclass
    class ExampleDefault:
        __msg_name__ = 'tests/msgs/ExampleDefault'

        integer: pybag.int32 = 42
        name: pybag.string = "hi"
        flag: pybag.bool = True

    obj = ExampleDefault()
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)
    assert schema.name == 'tests/msgs/ExampleDefault'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=42)
    assert schema.fields['name'] == SchemaField(String('string'), default="hi")
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=True)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array() -> None:
    @dataclass
    class ArrayExample:
        __msg_name__ = 'tests/msgs/ArrayExample'
        numbers: pybag.Array(pybag.int32)

    obj = ArrayExample(numbers=[1, 2, 3])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array_with_length() -> None:
    @dataclass
    class ArrayExample:
        __msg_name__ = 'tests/msgs/ArrayExample'
        numbers: pybag.Array(pybag.int32, 3)

    obj = ArrayExample(numbers=[1, 2, 3])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Array(Primitive('int32'), length=3, is_bounded=False), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array_with_default() -> None:
    @dataclass
    class ArrayExample:
        __msg_name__ = 'tests/msgs/ArrayExample'
        numbers: pybag.Array(pybag.int32) = field(default_factory=lambda: [1, 2, 3])

    obj = ArrayExample()
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=[1, 2, 3])
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_python_types_int() -> None:
    @dataclass
    class Missing:
        __msg_name__ = 'tests/msgs/Missing'
        value: int

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Missing)


def test_serialize_python_types_str() -> None:
    @dataclass
    class Unsupported:
        __msg_name__ = 'tests/msgs/Unsupported'
        value: str

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)


def test_serialize_python_types_float() -> None:
    @dataclass
    class Unsupported:
        __msg_name__ = 'tests/msgs/Unsupported'
        value: float

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)


def test_serialize_python_types_bool() -> None:
    @dataclass
    class Unsupported:
        __msg_name__ = 'tests/msgs/Unsupported'
        value: bool

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)
