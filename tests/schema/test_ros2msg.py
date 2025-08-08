import pytest

from pybag.mcap.records import SchemaRecord
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Constant,
    Primitive,
    Ros2MsgError,
    Ros2MsgSchema,
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
    ros2_schema, sub_schemas = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/Primitive"
    assert len(ros2_schema.fields) == 1
    assert "my_int" in ros2_schema.fields
    field = ros2_schema.fields["my_int"]
    assert isinstance(field, Primitive)
    assert field.type == "int32"
    assert sub_schemas == {}


def test_parse_unbounded_sequence_field():
    schema_text = "int32[] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/SeqArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/SeqArray"
    assert len(ros2_schema.fields) == 1
    assert "values" in ros2_schema.fields
    field = ros2_schema.fields["values"]
    assert isinstance(field, Sequence)
    assert field.type == "int32"


def test_parse_bounded_array():
    schema_text = "int32[<=5] limited\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/BoundedArray"
    assert len(ros2_schema.fields) == 1
    assert "limited" in ros2_schema.fields
    field = ros2_schema.fields["limited"]
    assert isinstance(field, Array)
    assert field.type == "int32"
    assert field.length == 5
    # TODO: Check the length limit


def test_parse_static_array_field():
    schema_text = "int32[5] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StaticArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StaticArray"
    assert len(ros2_schema.fields) == 1
    assert "values" in ros2_schema.fields
    field = ros2_schema.fields["values"]
    assert isinstance(field, Array)
    assert field.type == "int32"
    assert field.length == 5


def test_parse_complex_header_field():
    schema_text = "Header header\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/HasHeader",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/HasHeader"
    assert len(ros2_schema.fields) == 1
    assert "header" in ros2_schema.fields
    field = ros2_schema.fields["header"]
    assert isinstance(field, Complex)
    assert field.type == "std_msgs/Header"


def test_parse_constant_field():
    schema_text = "int32 X=5\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/Const",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/Const"
    assert len(ros2_schema.fields) == 1
    assert "X" in ros2_schema.fields
    field = ros2_schema.fields["X"]
    assert isinstance(field, Constant)
    assert field.type == "int32"
    assert field.value == 5


def test_parse_string_with_length_limit():
    schema_text = "string<=10 short_name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedString",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/BoundedString"
    assert len(ros2_schema.fields) == 1
    assert "short_name" in ros2_schema.fields
    field = ros2_schema.fields["short_name"]
    assert isinstance(field, String)
    assert field.type == "string"
    assert field.length == 10
    # TODO: Check the length limit


def test_parse_default_integer_value():
    schema_text = "int32 count 100\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/DefaultValue",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/DefaultValue"
    assert len(ros2_schema.fields) == 1
    assert "count" in ros2_schema.fields
    field = ros2_schema.fields["count"]
    assert isinstance(field, Primitive)
    assert field.type == "int32"
    assert field.default == 100
    assert isinstance(field.default, int)


def test_parse_bounded_string_array():
    schema_text = "string<=10[<=5] names\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BoundedStringArray",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/BoundedStringArray"
    assert len(ros2_schema.fields) == 1
    assert "names" in ros2_schema.fields
    field = ros2_schema.fields["names"]
    assert isinstance(field, Array)
    assert field.type == "string"
    assert field.length == 5
    # TODO: Check the length limit of the string


def test_parse_default_string_value_double_quotes():
    schema_text = 'string name "John"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringDefaultDouble",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringDefaultDouble"
    assert len(ros2_schema.fields) == 1
    assert "name" in ros2_schema.fields
    field = ros2_schema.fields["name"]
    assert isinstance(field, String)
    assert field.type == "string"
    assert field.default == "John"
    assert isinstance(field.default, str)


def test_parse_default_string_value_single_quotes():
    schema_text = "string nickname 'Johnny'\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringDefaultSingle",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringDefaultSingle"
    assert len(ros2_schema.fields) == 1
    assert "nickname" in ros2_schema.fields
    field = ros2_schema.fields["nickname"]
    assert isinstance(field, String)
    assert field.type == "string"
    assert field.default == "Johnny"
    assert isinstance(field.default, str)


def test_parse_default_string_value_with_hash_double_quotes():
    schema_text = 'string tag "hello#world"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringWithHash"
    assert len(ros2_schema.fields) == 1
    assert "tag" in ros2_schema.fields
    field = ros2_schema.fields["tag"]
    assert isinstance(field, String)
    assert field.type == "string"
    assert field.default == "hello#world"
    assert isinstance(field.default, str)


def test_parse_default_string_value_with_hash_single_quotes():
    schema_text = 'string tag \'hello#world\'\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringWithHash"
    assert len(ros2_schema.fields) == 1
    assert "tag" in ros2_schema.fields
    field = ros2_schema.fields["tag"]
    assert isinstance(field, String)
    assert field.type == "string"
    assert field.default == "hello#world"
    assert isinstance(field.default, str)


def test_parse_default_array_of_ints():
    schema_text = "int32[] samples [-200, -100, 0, 100, 200]\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/ArrayDefault",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/ArrayDefault"
    assert len(ros2_schema.fields) == 1
    assert "samples" in ros2_schema.fields
    field = ros2_schema.fields["samples"]
    assert isinstance(field, Sequence)
    assert field.type == "int32"
    assert field.default == [-200, -100, 0, 100, 200]


def test_parse_constant_string_field_double_quotes():
    schema_text = 'string GREETING="hello#world"\n'
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringConstWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringConstWithHash"
    assert len(ros2_schema.fields) == 1
    assert "GREETING" in ros2_schema.fields
    field = ros2_schema.fields["GREETING"]
    assert isinstance(field, Constant)
    assert field.type == "string"
    assert field.value == "hello#world"
    assert isinstance(field.value, str)


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
    ros2_schema, sub_schemas = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/ComplexArray"
    assert len(ros2_schema.fields) == 1
    assert "points" in ros2_schema.fields
    field = ros2_schema.fields["points"]
    assert isinstance(field, Sequence)
    assert field.type == "geometry_msgs/Point"

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]
    assert isinstance(point_schema.fields["x"], Primitive)
    assert point_schema.fields["x"].type == "float64"
    assert isinstance(point_schema.fields["y"], Primitive)
    assert point_schema.fields["y"].type == "float64"
    assert isinstance(point_schema.fields["z"], Primitive)
    assert point_schema.fields["z"].type == "float64"


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
    ros2_schema, sub_schemas = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/ComplexFixedArray"
    assert len(ros2_schema.fields) == 1
    assert "points" in ros2_schema.fields
    field = ros2_schema.fields["points"]
    assert isinstance(field, Array)
    assert field.type == "geometry_msgs/Point"
    assert field.length == 3

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]
    assert isinstance(point_schema.fields["x"], Primitive)
    assert point_schema.fields["x"].type == "float64"
    assert isinstance(point_schema.fields["y"], Primitive)
    assert point_schema.fields["y"].type == "float64"
    assert isinstance(point_schema.fields["z"], Primitive)
    assert point_schema.fields["z"].type == "float64"


def test_parse_constant_string_field_single_quotes():
    schema_text = "string GREETING='hello#world'\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/StringConstWithHash",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/StringConstWithHash"
    assert len(ros2_schema.fields) == 1
    assert "GREETING" in ros2_schema.fields
    field = ros2_schema.fields["GREETING"]
    assert isinstance(field, Constant)
    assert field.type == "string"
    assert field.value == "hello#world"
    assert isinstance(field.value, str)


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
    ros2_schema, sub_schemas = Ros2MsgSchema().parse(schema)

    assert ros2_schema.name == "pkg/msg/WithPoint"
    assert len(ros2_schema.fields) == 1
    assert "point" in ros2_schema.fields
    field = ros2_schema.fields["point"]
    assert isinstance(field, Complex)
    assert field.type == "geometry_msgs/Point"

    assert len(sub_schemas) == 1
    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]
    assert isinstance(point_schema.fields["x"], Primitive)
    assert point_schema.fields["x"].type == "float64"
    assert isinstance(point_schema.fields["y"], Primitive)
    assert point_schema.fields["y"].type == "float64"
    assert isinstance(point_schema.fields["z"], Primitive)
    assert point_schema.fields["z"].type == "float64"


def test_constant_name_must_be_uppercase():
    schema_text = "int32 notupper=1\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/BadConst",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    parser = Ros2MsgSchema()
    with pytest.raises(Ros2MsgError):
        parser.parse(schema)


def test_field_with_inline_comment():
    schema_text = "int32 value # comment\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InlineComment",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    ros2_schema, _ = Ros2MsgSchema().parse(schema)

    field = ros2_schema.fields["value"]
    assert isinstance(field, Primitive)
    assert field.type == "int32"


def test_invalid_field_name_raises_error():
    schema_text = "int32 invalid__name\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/msg/InvalidName",
        encoding="ros2msg",
        data=schema_text.encode("utf-8"),
    )
    with pytest.raises(Ros2MsgError):
        Ros2MsgSchema().parse(schema)
