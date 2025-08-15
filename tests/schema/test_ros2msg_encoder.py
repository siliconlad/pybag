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
        integer: pybag.int32
        name: pybag.string
        flag: pybag.bool

    obj = Example(42, "hi", True)
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'Example'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=None)
    assert schema.fields['name'] == SchemaField(String('string'), default=None)
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_dataclass_primitives_with_default() -> None:
    @dataclass
    class ExampleDefault:
        integer: pybag.int32 = 42
        name: pybag.string = "hi"
        flag: pybag.bool = True

    obj = ExampleDefault()
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)
    assert schema.name == 'ExampleDefault'
    assert len(schema.fields) == 3
    assert schema.fields['integer'] == SchemaField(Primitive('int32'), default=42)
    assert schema.fields['name'] == SchemaField(String('string'), default="hi")
    assert schema.fields['flag'] == SchemaField(Primitive('bool'), default=True)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.Array(pybag.int32)
    obj = ArrayExample(numbers=[1, 2, 3])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array_with_length() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.Array(pybag.int32, 3)
    obj = ArrayExample(numbers=[1, 2, 3])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Array(Primitive('int32'), length=3, is_bounded=False), default=None)
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_array_with_default() -> None:
    @dataclass
    class ArrayExample:
        numbers: pybag.Array(pybag.int32) = field(default_factory=lambda: [1, 2, 3])
    obj = ArrayExample()
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'ArrayExample'
    assert len(schema.fields) == 1
    assert schema.fields['numbers'] == SchemaField(Sequence(Primitive('int32')), default=[1, 2, 3])
    assert len(sub_schemas) == 0  # No sub-schemas for primitive types


def test_serialize_python_types_int() -> None:
    @dataclass
    class Missing:
        value: int

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Missing)


def test_serialize_python_types_str() -> None:
    @dataclass
    class Unsupported:
        value: str

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)


def test_serialize_python_types_float() -> None:
    @dataclass
    class Unsupported:
        value: float

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)


def test_serialize_python_types_bool() -> None:
    @dataclass
    class Unsupported:
        value: bool

    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaEncoder().parse_schema(Unsupported)
