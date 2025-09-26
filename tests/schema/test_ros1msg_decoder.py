from pybag.schema.ros1msg import (
    Array,
    Complex,
    Primitive,
    Ros1MsgSchemaDecoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
)
from pybag.mcap.records import SchemaRecord


def test_parse_primitive_field():
    schema_text = "int32 my_int\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/Primitive",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    ros1_schema, sub_schemas = Ros1MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros1_schema, Schema)
    assert ros1_schema.name == "pkg/Primitive"
    assert len(ros1_schema.fields) == 1

    assert "my_int" in ros1_schema.fields
    field = ros1_schema.fields["my_int"]
    assert isinstance(field, SchemaField)

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"

    assert sub_schemas == {}


def test_parse_unbounded_sequence_field():
    schema_text = "int32[] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/SeqArray",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    ros1_schema, sub_schemas = Ros1MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros1_schema, Schema)
    assert ros1_schema.name == "pkg/SeqArray"
    assert len(ros1_schema.fields) == 1

    assert "values" in ros1_schema.fields
    field = ros1_schema.fields["values"]
    assert isinstance(field, SchemaField)

    assert isinstance(field.type, Sequence)

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_static_array_field():
    schema_text = "int32[5] values\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/StaticArray",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    ros1_schema, sub_schemas = Ros1MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros1_schema, Schema)
    assert ros1_schema.name == "pkg/StaticArray"
    assert len(ros1_schema.fields) == 1

    assert "values" in ros1_schema.fields
    field = ros1_schema.fields["values"]
    assert isinstance(field, SchemaField)

    assert isinstance(field.type, Array)
    assert field.type.length == 5

    assert isinstance(field.type.type, Primitive)
    assert field.type.type.type == "int32"

    assert sub_schemas == {}


def test_parse_constant():
    schema_text = "int32 VALUE=5\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/Const",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    ros1_schema, sub_schemas = Ros1MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros1_schema, Schema)
    assert ros1_schema.name == "pkg/Const"
    assert len(ros1_schema.fields) == 1

    assert "VALUE" in ros1_schema.fields
    field = ros1_schema.fields["VALUE"]
    assert isinstance(field, SchemaConstant)
    assert field.value == 5

    assert isinstance(field.type, Primitive)
    assert field.type.type == "int32"

    assert sub_schemas == {}


def test_parse_complex_field():
    schema_text = (
        "geometry_msgs/Point point\n"
        + "=" * 80
        + "\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n"
    )
    schema = SchemaRecord(
        id=1,
        name="pkg/UsePoint",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    ros1_schema, sub_schemas = Ros1MsgSchemaDecoder().parse_schema(schema)

    assert isinstance(ros1_schema, Schema)
    assert ros1_schema.name == "pkg/UsePoint"
    assert len(ros1_schema.fields) == 1

    field = ros1_schema.fields["point"]
    assert isinstance(field, SchemaField)
    assert isinstance(field.type, Complex)
    assert field.type.type == "geometry_msgs/Point"

    assert "geometry_msgs/Point" in sub_schemas
    point_schema = sub_schemas["geometry_msgs/Point"]
    assert isinstance(point_schema, Schema)
    assert len(point_schema.fields) == 3


def test_parse_caches_schema():
    schema_text = "int32 my_int\n"
    schema = SchemaRecord(
        id=1,
        name="pkg/Primitive",
        encoding="ros1msg",
        data=schema_text.encode("utf-8"),
    )
    decoder = Ros1MsgSchemaDecoder()
    first = decoder.parse_schema(schema)
    second = decoder.parse_schema(schema)

    assert first is second
