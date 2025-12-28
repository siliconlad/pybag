from dataclasses import dataclass, field
from textwrap import dedent
from typing import Literal

import pytest

import pybag
import pybag.types as t
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgError,
    Ros2MsgSchemaEncoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String
)


def test_serialize_dataclass_primitives() -> None:
    @dataclass
    class Example:
        __msg_name__ = 'tests/msgs/Example'

        integer: t.int32
        name: t.string
        flag: t.bool

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

        integer: t.int32 = 42
        name: t.string = "hi"
        flag: t.bool = True

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
        numbers: t.Array[t.int32]

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
        numbers: t.Array[t.int32, Literal[3]]

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
        numbers: t.Array[t.int32] = field(default_factory=lambda: [1, 2, 3])

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


def test_serialize_constants() -> None:
    @dataclass(kw_only=True)
    class Example:
        __msg_name__ = 'tests/msgs/ExampleConst'
        FOO: t.Constant[t.int32] = 1
        bar: t.int32

    obj = Example(bar=42)
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/ExampleConst'
    assert schema.fields['FOO'] == SchemaConstant(Primitive('int32'), value=1)
    assert schema.fields['bar'] == SchemaField(Primitive('int32'), default=None)
    assert len(sub_schemas) == 0

    encoded = Ros2MsgSchemaEncoder().encode(obj)
    assert encoded == b'int32 FOO=1\nint32 bar\n'


def test_serialize_message_type() -> None:
    """Test that message types can be used directly without Complex wrapper."""
    @dataclass
    class Point:
        __msg_name__ = 'geometry_msgs/msg/Point'
        x: t.float64
        y: t.float64
        z: t.float64

    @dataclass
    class Pose:
        __msg_name__ = 'geometry_msgs/msg/Pose'
        position: Point  # Direct usage without Complex wrapper

    obj = Pose(position=Point(x=1.0, y=2.0, z=3.0))
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'geometry_msgs/msg/Pose'
    assert schema.fields['position'] == SchemaField(Complex('geometry_msgs/msg/Point'), default=None)
    assert len(sub_schemas) == 1
    assert 'geometry_msgs/msg/Point' in sub_schemas


def test_serialize_array_with_message_type() -> None:
    """Test that Array can use message types directly without Complex wrapper."""
    @dataclass
    class Point32:
        __msg_name__ = 'geometry_msgs/msg/Point32'
        x: t.float32
        y: t.float32
        z: t.float32

    @dataclass
    class Polygon:
        __msg_name__ = 'geometry_msgs/msg/Polygon'
        points: t.Array[Point32]  # Direct usage without Complex wrapper

    obj = Polygon(points=[Point32(x=1.0, y=2.0, z=3.0)])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'geometry_msgs/msg/Polygon'
    assert schema.fields['points'] == SchemaField(Sequence(Complex('geometry_msgs/msg/Point32')), default=None)
    assert len(sub_schemas) == 1
    assert 'geometry_msgs/msg/Point32' in sub_schemas


def test_serialize_fixed_array_with_message_type() -> None:
    """Test that Array with size can use message types directly without Complex wrapper."""
    @dataclass
    class Header:
        __msg_name__ = 'std_msgs/msg/Header'
        stamp: t.int64
        frame_id: t.string

    @dataclass
    class MultiHeader:
        __msg_name__ = 'tests/msgs/MultiHeader'
        headers: t.Array[Header, Literal[3]]  # Direct usage without Complex wrapper

    obj = MultiHeader(headers=[
        Header(stamp=1, frame_id='a'),
        Header(stamp=2, frame_id='b'),
        Header(stamp=3, frame_id='c')
    ])
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'tests/msgs/MultiHeader'
    assert schema.fields['headers'] == SchemaField(Array(Complex('std_msgs/msg/Header'), length=3, is_bounded=False), default=None)
    assert len(sub_schemas) == 1
    assert 'std_msgs/msg/Header' in sub_schemas


def test_serialize_nested_complex_array() -> None:
    @dataclass()
    class MultiArrayDimension:
        __msg_name__ = 'std_msgs/msg/MultiArrayDimension'

        label: t.string
        size: t.uint32
        stride: t.uint32

    @dataclass()
    class MultiArrayLayout:
        __msg_name__ = 'std_msgs/msg/MultiArrayLayout'

        dim: t.Array[MultiArrayDimension]
        data_offset: t.uint32

    @dataclass()
    class Float32MultiArray:
        __msg_name__ = 'std_msgs/msg/Float32MultiArray'

        layout: MultiArrayLayout
        data: t.Array[t.float32]

    obj = Float32MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='test', size=10, stride=5)],
            data_offset=0
        ),
        data=[1.1, 2.2, 3.3]
    )
    schema, sub_schemas = Ros2MsgSchemaEncoder().parse_schema(obj)

    assert schema.name == 'std_msgs/msg/Float32MultiArray'
    assert schema.fields['layout'] == SchemaField(Complex('std_msgs/msg/MultiArrayLayout'), default=None)
    assert schema.fields['data'] == SchemaField(Sequence(Primitive('float32')), default=None)
    assert len(sub_schemas) == 2
    assert sub_schemas['std_msgs/msg/MultiArrayLayout'] == Schema(
        name='std_msgs/msg/MultiArrayLayout',
        fields={
            'dim': SchemaField(Sequence(Complex('std_msgs/msg/MultiArrayDimension')), default=None),
            'data_offset': SchemaField(Primitive('uint32'), default=None)
        }
    )
    assert sub_schemas['std_msgs/msg/MultiArrayDimension'] == Schema(
        name='std_msgs/msg/MultiArrayDimension',
        fields={
            'label': SchemaField(String('string'), default=None),
            'size': SchemaField(Primitive('uint32'), default=None),
            'stride': SchemaField(Primitive('uint32'), default=None)
        }
    )

    encoded = Ros2MsgSchemaEncoder().encode(obj)
    print(encoded)
    assert encoded == dedent("""\
        std_msgs/MultiArrayLayout layout
        float32[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
        """).encode()
