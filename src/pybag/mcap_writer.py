import logging
from dataclasses import fields
from typing import Any

from pybag.encoding.cdr import CdrEncoder
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgSchemaEncoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String
)

logger = logging.getLogger(__name__)


def serialize_message(message: Any, little_endian: bool = True) -> bytes:
    encoder = CdrEncoder(little_endian=little_endian)
    schema, sub_schemas = Ros2MsgSchemaEncoder().encode(message)

    def _encode_field(message: Any, schema_field: SchemaField, sub_schemas: dict[str, Schema]) -> None:
        if isinstance(schema_field.type, Primitive):
            primitive_type = schema_field.type
            encoder.encode(primitive_type.type, message)

        if isinstance(schema_field.type, String):
            string_type = schema_field.type
            encoder.encode(string_type.type, message)

        if isinstance(schema_field.type, Array):
            array_type = schema_field.type
            if isinstance(array_type.type, (Primitive, String)):
                encoder.array(array_type.type.type, message)
            elif isinstance(array_type.type, Complex):
                for item in message:
                    _encode_message(item, array_type.type, sub_schemas)
            else:
                raise ValueError(f"Unknown array type: {array_type.type}")

        if isinstance(schema_field.type, Sequence):
            sequence_type = schema_field.type
            if isinstance(sequence_type.type, (Primitive, String)):
                encoder.sequence(sequence_type.type.type, message)
            elif isinstance(sequence_type.type, Complex):
                encoder.uint32(len(message))
                for item in message:
                    _encode_message(item, sequence_type.type, sub_schemas)
            else:
                raise ValueError(f"Unknown sequence type: {sequence_type.type}")

        if isinstance(schema_field.type, Complex):
            complex_type = schema_field.type
            if complex_type.type not in sub_schemas:
                raise ValueError(f"Complex type {complex_type.type} not found in sub_schemas")
            _encode_message(message, sub_schemas[complex_type.type], sub_schemas)

    def _encode_message(message: Any, schema: Schema, sub_schemas: dict[str, Schema]) -> None:
        if isinstance(schema, Complex):
            _encode_message(message, sub_schemas[schema.type], sub_schemas)
        else:
            for field_name, schema_field in schema.fields.items():
                if isinstance(schema_field, SchemaConstant):
                    continue  # Nothing to do for constants
                _encode_field(getattr(message, field_name), schema_field, sub_schemas)

    if isinstance(schema, Complex):
        schema = sub_schemas[schema.type]
    _encode_message(message, schema, sub_schemas)

    return encoder.save()
