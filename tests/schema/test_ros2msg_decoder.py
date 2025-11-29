import pytest

from pybag.mcap.records import SchemaRecord
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgError,
    Ros2MsgSchemaDecoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String
)


def test_parse_primitive_field():
    schema_text = "int32 my_int\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/Primitive",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/Primitive"
    assert len(ros2_schema.fields) == 1

    assert "my_int" in ros2_schema.fields
    field = ros2_schema.fields["my_int"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"

    assert sub_schemas == {}


def test_parse_unbounded_sequence_field():
    schema_text = "int32[] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/SeqArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/SeqArray"
    assert len(ros2_schema.fields) == 1

    assert "values" in ros2_schema.fields
    field = ros2_schema.fields["values"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Sequence)

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_caches_schema():
    schema_text = "int32 my_int\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/Primitive",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    decoder = Ros2MsgSchemaDecoder()
    first = decoder.parse_schema(schema)
    second = decoder.parse_schema(schema)

    assert first is second


def test_parse_bounded_array():
    schema_text = "int32[<=5] limited\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/BoundedArray"
    assert len(ros2_schema.fields) == 1

    assert "limited" in ros2_schema.fields
    field = ros2_schema.fields["limited"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Array)
    assert field.type.length == 5
    assert field.type.is_bounded is True

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_static_array_field():
    schema_text = "int32[5] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StaticArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StaticArray"
    assert len(ros2_schema.fields) == 1

    assert "values" in ros2_schema.fields
    field = ros2_schema.fields["values"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Array)
    assert field.type.length == 5
    assert field.type.is_bounded is False

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_complex_header_field():
    schema_text = "Header header\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/HasHeader",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/HasHeader"
    assert len(ros2_schema.fields) == 1

    assert "header" in ros2_schema.fields
    field = ros2_schema.fields["header"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Complex)
    assert field.type.type == "std_msgs/Header"

    assert sub_schemas == {}


def test_parse_constant_field():
    schema_text = "int32 X=5\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/Const",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/Const"
    assert len(ros2_schema.fields) == 1

    assert "X" in ros2_schema.fields
    field = ros2_schema.fields["X"]
    assert isinstance(field, SchemaConstant)

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"
    assert field.value == 5

    assert sub_schemas == {}


def test_parse_string_with_length_limit():
    schema_text = "string<=10 short_name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedString",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/BoundedString"
    assert len(ros2_schema.fields) == 1

    assert "short_name" in ros2_schema.fields
    field = ros2_schema.fields["short_name"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, String)
    assert field.type.type == "string"
    assert field.type.max_length == 10

    assert sub_schemas == {}


def test_parse_wstring_with_length_limit():
    schema_text = "wstring<=10 short_name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedString",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/BoundedString"
    assert len(ros2_schema.fields) == 1

    assert "short_name" in ros2_schema.fields
    field = ros2_schema.fields["short_name"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, String)
    assert field.type.type == "wstring"
    assert field.type.max_length == 10

    assert sub_schemas == {}


def test_parse_default_integer_value():
    schema_text = "int32 count 100\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/DefaultValue",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/DefaultValue"
    assert len(ros2_schema.fields) == 1

    assert "count" in ros2_schema.fields
    field = ros2_schema.fields["count"]
    assert isinstance(field, SchemaField)
    assert field.default == 100

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"

    assert sub_schemas == {}


def test_parse_bounded_string_array():
    schema_text = "string<=10[<=5] names\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedStringArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/BoundedStringArray"
    assert len(ros2_schema.fields) == 1

    assert "names" in ros2_schema.fields
    field = ros2_schema.fields["names"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Array)
    assert field.type.length == 5
    assert field.type.is_bounded is True

    assert isinstance(field.type.type, String)
    assert field.type.type.type == "string"
    assert field.type.type.max_length == 10
    assert sub_schemas == {}


def test_parse_default_string_value_double_quotes():
    schema_text = 'string name "John"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringDefaultDouble",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringDefaultDouble"
    assert len(ros2_schema.fields) == 1

    assert "name" in ros2_schema.fields
    field = ros2_schema.fields["name"]
    assert isinstance(field, SchemaField)
    assert field.default == "John"

    assert isinstance(field.type, String)
    assert field.type.type == "string"

    assert sub_schemas == {}


def test_parse_default_string_value_single_quotes():
    schema_text = "string nickname 'Johnny'\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringDefaultSingle",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringDefaultSingle"
    assert len(ros2_schema.fields) == 1

    assert "nickname" in ros2_schema.fields
    field = ros2_schema.fields["nickname"]
    assert isinstance(field, SchemaField)
    assert field.default == "Johnny"

    assert isinstance(field.type, String)
    assert field.type.type == "string"

    assert sub_schemas == {}


def test_parse_default_string_value_with_hash_double_quotes():
    schema_text = 'string tag "hello#world"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringWithHash"
    assert len(ros2_schema.fields) == 1

    assert "tag" in ros2_schema.fields
    field = ros2_schema.fields["tag"]
    assert isinstance(field, SchemaField)
    assert field.default == "hello#world"

    assert isinstance(field.type, String)
    assert field.type.type == "string"

    assert sub_schemas == {}


def test_parse_default_string_value_with_hash_single_quotes():
    schema_text = 'string tag \'hello#world\'\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringWithHash"
    assert len(ros2_schema.fields) == 1

    assert "tag" in ros2_schema.fields
    field = ros2_schema.fields["tag"]
    assert isinstance(field, SchemaField)
    assert field.default == "hello#world"

    assert isinstance(field.type, String)
    assert field.type.type == "string"

    assert sub_schemas == {}


def test_parse_default_array_of_ints():
    schema_text = "int32[] samples [-200, -100, 0, 100, 200]\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/ArrayDefault",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/ArrayDefault"
    assert len(ros2_schema.fields) == 1

    assert "samples" in ros2_schema.fields
    field = ros2_schema.fields["samples"]
    assert isinstance(field, SchemaField)
    assert field.default == [-200, -100, 0, 100, 200]

    assert isinstance(field.type, Sequence)

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_constant_string_field_double_quotes():
    schema_text = 'string GREETING="hello#world"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringConstWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringConstWithHash"
    assert len(ros2_schema.fields) == 1

    assert "GREETING" in ros2_schema.fields
    field = ros2_schema.fields["GREETING"]
    assert isinstance(field, SchemaConstant)
    assert field.value == "hello#world"

    assert isinstance(field.type, String)
    assert field.type.type == "string"
    assert field.type.max_length is None

    assert sub_schemas == {}


def test_parse_constant_string_field_single_quotes():
    schema_text = "string GREETING='hello#world'\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringConstWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/StringConstWithHash"
    assert len(ros2_schema.fields) == 1

    assert "GREETING" in ros2_schema.fields
    field = ros2_schema.fields["GREETING"]
    assert isinstance(field, SchemaConstant)
    assert field.value == "hello#world"

    assert isinstance(field.type, String)
    assert field.type.type == "string"
    assert field.type.max_length is None

    assert sub_schemas == {}


def test_parse_complex_array_field():
    schema_text = (
        "geometry_msgs/Point[] points\n"
        + "=" * 80
        + "\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n"
    )
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/ComplexArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/ComplexArray"
    assert len(ros2_schema.fields) == 1

    assert "points" in ros2_schema.fields
    field = ros2_schema.fields["points"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Sequence)

    assert isinstance(field.type.type, Complex)
    assert field.type.type.type == "geometry_msgs/Point"

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]

    assert isinstance(point_schema.fields["x"], SchemaField)
    assert isinstance(point_schema.fields["x"].type, Primitive)
    assert point_schema.fields["x"].type.type == "float64"

    assert isinstance(point_schema.fields["y"], SchemaField)
    assert isinstance(point_schema.fields["y"].type, Primitive)
    assert point_schema.fields["y"].type.type == "float64"

    assert isinstance(point_schema.fields["z"], SchemaField)
    assert isinstance(point_schema.fields["z"].type, Primitive)
    assert point_schema.fields["z"].type.type == "float64"


def test_parse_complex_fixed_array_field():
    schema_text = (
        "geometry_msgs/Point[3] points\n"
        + "=" * 80
        + "\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n"
    )
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/ComplexFixedArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/ComplexFixedArray"
    assert len(ros2_schema.fields) == 1

    assert "points" in ros2_schema.fields
    field = ros2_schema.fields["points"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Array)
    assert field.type.length == 3
    assert field.type.is_bounded is False

    assert isinstance(field.type.type, Complex)
    assert field.type.type.type == "geometry_msgs/Point"

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]

    assert isinstance(point_schema.fields["x"], SchemaField)
    assert isinstance(point_schema.fields["x"].type, Primitive)
    assert point_schema.fields["x"].type.type == "float64"

    assert isinstance(point_schema.fields["y"], SchemaField)
    assert isinstance(point_schema.fields["y"].type, Primitive)
    assert point_schema.fields["y"].type.type == "float64"

    assert isinstance(point_schema.fields["z"], SchemaField)
    assert isinstance(point_schema.fields["z"].type, Primitive)
    assert point_schema.fields["z"].type.type == "float64"


def test_parse_sub_message_schema():
    schema_text = (
        "geometry_msgs/Point point\n"
        + "=" * 80
        + "\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n"
    )
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/WithPoint",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/WithPoint"
    assert len(ros2_schema.fields) == 1

    assert "point" in ros2_schema.fields
    field = ros2_schema.fields["point"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Complex)
    assert field.type.type == "geometry_msgs/Point"

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]

    assert isinstance(point_schema.fields["x"], SchemaField)
    assert isinstance(point_schema.fields["x"].type, Primitive)
    assert point_schema.fields["x"].type.type == "float64"

    assert isinstance(point_schema.fields["y"], SchemaField)
    assert isinstance(point_schema.fields["y"].type, Primitive)
    assert point_schema.fields["y"].type.type == "float64"

    assert isinstance(point_schema.fields["z"], SchemaField)
    assert isinstance(point_schema.fields["z"].type, Primitive)
    assert point_schema.fields["z"].type.type == "float64"


def test_field_with_inline_comment():
    schema_text = "int32 value # comment\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InlineComment",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, sub_schemas = Ros2MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros2_schema, Schema)
    assert ros2_schema.name == "pkg/msg/InlineComment"
    assert len(ros2_schema.fields) == 1

    assert "value" in ros2_schema.fields
    field = ros2_schema.fields["value"]
    assert isinstance(field, SchemaField)
    assert field.default is None

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"

    assert sub_schemas == {}


def test_invalid_constant_name_must_be_uppercase():
    schema_text = "int32 notupper=1\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BadConst",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_invalid_field_name_double_underscore():
    schema_text = "int32 invalid__name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_invalid_field_name_end_with_underscore():
    schema_text = "int32 invalid_name_\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_invalid_field_name_starts_with_number():
    schema_text = "int32 123_name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_invalid_field_name_contains_uppercase():
    schema_text = "int32 InvalidName\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_invalid_field_name_contains_special_characters():
    schema_text = "int32 invalid_name!\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchemaDecoder().parse_schema(schema)


def test_byte_constant_parsing():
    """Parsing a schema with byte constants should not raise TypeError."""
    diagnostic_status_schema = SchemaRecord(
        id=1,
        name='diagnostic_msgs/msg/DiagnosticStatus',
        encoding='ros2msg',
        data=b'byte OK=0\nbyte WARN=1\nbyte ERROR=2\nbyte STALE=3\nbyte level'
    )

    decoder = Ros2MsgSchemaDecoder()
    schema, _ = decoder.parse_schema(diagnostic_status_schema)

    # Verify the constants were parsed correctly
    assert 'OK' in schema.fields
    assert 'WARN' in schema.fields
    assert 'ERROR' in schema.fields
    assert 'STALE' in schema.fields


def test_byte_field_with_default_parsing():
    """Parsing a schema with byte field default should not raise TypeError."""
    schema = SchemaRecord(
        id=1,
        name='test_msgs/msg/ByteTest',
        encoding='ros2msg',
        data=b'byte value 42\n'
    )

    decoder = Ros2MsgSchemaDecoder()
    parsed_schema, _ = decoder.parse_schema(schema)

    # Verify the default value was parsed correctly
    assert 'value' in parsed_schema.fields
    field = parsed_schema.fields['value']
    assert isinstance(field, SchemaField)
    assert field.default == 42


def test_byte_array_default_parsing():
    """Parsing a schema with byte array default should work."""
    schema = SchemaRecord(
        id=1,
        name='test_msgs/msg/ByteArrayTest',
        encoding='ros2msg',
        data=b'byte[3] data [1, 2, 3]\n'
    )

    decoder = Ros2MsgSchemaDecoder()
    parsed_schema, _ = decoder.parse_schema(schema)

    assert 'data' in parsed_schema.fields
    field = parsed_schema.fields['data']
    assert isinstance(field, SchemaField)
    assert field.default == [1, 2, 3]
