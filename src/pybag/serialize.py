from dataclasses import is_dataclass
from typing import Callable

from pybag.encoding import MessageEncoder
from pybag.encoding.cdr import CdrEncoder
from pybag.encoding.json import JsonEncoder
from pybag.mcap.records import ChannelRecord, SchemaRecord
from pybag.schema import SchemaEncoder
from pybag.schema.compiler import compile_serializer
from pybag.schema.json_compiler import compile_json_serializer
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder
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
        self._compiled: dict[type[Message], Callable[[MessageEncoder, Message], None]] = {}

    @property
    def schema_encoding(self) -> str:
        return self._schema_encoder.encoding()

    @property
    def message_encoding(self) -> str:
        return self._message_encoder.encoding()

    def serialize_message(self, message: Message, *, little_endian: bool = True) -> bytes:
        if not is_dataclass(message):  # pragma: no cover - defensive programming
            raise TypeError("Expected a dataclass instance")

        message_type = type(message)
        if (serializer := self._compiled.get(message_type)) is None:
            schema, sub_schemas = self._schema_encoder.parse_schema(message_type)
            serializer = compile_serializer(schema, sub_schemas)
            self._compiled[message_type] = serializer

        encoder = self._message_encoder(little_endian=little_endian)
        serializer(encoder, message)
        return encoder.save()

    def serialize_schema(self, schema: type[Message]) -> bytes:
        return self._schema_encoder.encode(schema)


class JsonMessageSerializer:
    """Serialize dataclass messages to JSON format using a schema encoder."""

    def __init__(self, schema_encoder: SchemaEncoder) -> None:
        self._schema_encoder = schema_encoder
        self._compiled: dict[type[Message], Callable[[MessageEncoder, Message], None]] = {}

    @property
    def schema_encoding(self) -> str:
        return self._schema_encoder.encoding()

    @property
    def message_encoding(self) -> str:
        return JsonEncoder.encoding()

    def serialize_message(self, message: Message, *, little_endian: bool = True) -> bytes:
        if not is_dataclass(message):  # pragma: no cover - defensive programming
            raise TypeError("Expected a dataclass instance")

        message_type = type(message)
        if (serializer := self._compiled.get(message_type)) is None:
            schema, sub_schemas = self._schema_encoder.parse_schema(message_type)
            serializer = compile_json_serializer(schema, sub_schemas)
            self._compiled[message_type] = serializer

        encoder = JsonEncoder(little_endian=little_endian)
        serializer(encoder, message)
        return encoder.save()

    def serialize_schema(self, schema: type[Message]) -> bytes:
        return self._schema_encoder.encode(schema)


class MessageSerializerFactory:
    """Factory for creating message serializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageSerializer | JsonMessageSerializer | None:
        if profile == "ros2":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder)
        if profile == "json":
            return JsonMessageSerializer(Ros2MsgSchemaEncoder())
        return None

    @staticmethod
    def from_channel(
        channel: ChannelRecord, schema: SchemaRecord
    ) -> MessageSerializer | JsonMessageSerializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder)
        if channel.message_encoding == "json" and schema.encoding == "ros2msg":
            return JsonMessageSerializer(Ros2MsgSchemaEncoder())
        return None
