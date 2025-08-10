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
            encoder.string(string_type.type, message)

        if isinstance(schema_field.type, Array):
            array_type = schema_field.type
            encoder.array(array_type.type, array_type.length, message)

        if isinstance(schema_field.type, Sequence):
            sequence_type = schema_field.type
            encoder.sequence(sequence_type.type, message)

        if isinstance(schema_field.type, Complex):
            complex_type = schema_field.type
            if complex_type.type not in sub_schemas:
                raise ValueError(f"Complex type {complex_type.type} not found in sub_schemas")
            _encode_message(message, sub_schemas[complex_type.type], sub_schemas)

    def _encode_message(message: Any, schema: Schema, sub_schemas: dict[str, Schema]) -> None:
        for field_name, schema_field in fields(schema).items():
            if isinstance(schema_field, SchemaConstant):
                continue  # Nothing to do for constants
            _encode_field(getattr(message, field_name), schema_field, sub_schemas)

    _encode_message(message, schema, sub_schemas)
