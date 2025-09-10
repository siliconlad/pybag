from dataclasses import is_dataclass
from typing import Any

from pybag.encoding import MessageEncoder
from pybag.encoding.cdr import CdrEncoder
from pybag.mcap.records import ChannelRecord, SchemaRecord
from pybag.schema import SchemaEncoder
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
from pybag.types import Message


class MessageSerializer:
    """Serialize dataclass messages using a schema and message encoder."""

    def __init__(
        self,
        schema_encoder: SchemaEncoder,
        message_encoder: type[MessageEncoder],
    ) -> None:
        self._schema_encoder = schema_encoder
        self._message_encoder = message_encoder

    @property
    def schema_encoding(self) -> str:
        return self._schema_encoder.encoding()

    @property
    def message_encoding(self) -> str:
        return self._message_encoder.encoding()

    def _encode_field(
        self,
        encoder: MessageEncoder,
        value: Any,
        schema_field: SchemaField,
        sub_schemas: dict[str, Schema],
    ) -> None:
        if isinstance(schema_field.type, Primitive):
            encoder.encode(schema_field.type.type, value)
            return

        if isinstance(schema_field.type, String):
            encoder.encode(schema_field.type.type, value)
            return

        if isinstance(schema_field.type, Array):
            array_type = schema_field.type
            if isinstance(array_type.type, (Primitive, String)):
                encoder.array(array_type.type.type, value)
                return
            if isinstance(array_type.type, Complex):
                for item in value:
                    self._encode_message(
                        encoder,
                        item,
                        sub_schemas[array_type.type.type],
                        sub_schemas,
                    )
                return
            raise ValueError(f"Unknown array type: {array_type.type}")

        if isinstance(schema_field.type, Sequence):
            sequence_type = schema_field.type
            if isinstance(sequence_type.type, (Primitive, String)):
                encoder.sequence(sequence_type.type.type, value)
                return
            if isinstance(sequence_type.type, Complex):
                encoder.uint32(len(value))
                for item in value:
                    self._encode_message(
                        encoder,
                        item,
                        sub_schemas[sequence_type.type.type],
                        sub_schemas,
                    )
                return
            raise ValueError(f"Unknown sequence type: {sequence_type.type}")

        if isinstance(schema_field.type, Complex):
            complex_type = schema_field.type
            if complex_type.type not in sub_schemas:
                raise ValueError(
                    f"Complex type {complex_type.type} not found in sub_schemas"
                )
            self._encode_message(
                encoder,
                value,
                sub_schemas[complex_type.type],
                sub_schemas,
            )
            return

        raise ValueError(f"Unknown field type: {schema_field.type}")

    def _encode_message(
        self,
        encoder: MessageEncoder,
        message: Message,
        schema: Schema,
        sub_schemas: dict[str, Schema],
    ) -> None:
        for field_name, schema_field in schema.fields.items():
            if isinstance(schema_field, SchemaConstant):
                continue
            if isinstance(schema_field, SchemaField):
                value = getattr(message, field_name)
                self._encode_field(encoder, value, schema_field, sub_schemas)
                continue
            raise ValueError(f"Unknown schema field type: {schema_field}")

    def serialize_message(self, message: Message, *, little_endian: bool = True) -> bytes:
        if not is_dataclass(message):  # pragma: no cover - defensive programming
            raise TypeError("Expected a dataclass instance")
        encoder = self._message_encoder(little_endian=little_endian)
        schema, sub_schemas = self._schema_encoder.parse_schema(message)
        if isinstance(schema, Complex):
            schema = sub_schemas[schema.type]
        self._encode_message(encoder, message, schema, sub_schemas)
        return encoder.save()

    def serialize_schema(self, schema: type[Message]) -> bytes:
        return self._schema_encoder.encode(schema)


class MessageSerializerFactory:
    """Factory for creating message serializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageSerializer | None:
        if profile == "ros2":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder)
        return None

    @staticmethod
    def from_channel(channel: ChannelRecord, schema: SchemaRecord) -> MessageSerializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder)
        return None
