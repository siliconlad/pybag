from dataclasses import is_dataclass
from typing import Callable

from pybag.encoding import MessageEncoder
from pybag.encoding.cdr import CdrEncoder
from pybag.encoding.rosmsg import RosMsgEncoder
from pybag.mcap.records import ChannelRecord, SchemaRecord
from pybag.schema import SchemaEncoder
from pybag.schema.compiler import compile_serializer
from pybag.schema.ros1_compiler import compile_ros1_serializer
from pybag.schema.ros1msg import Ros1MsgSchemaEncoder
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder
from pybag.types import Message


class MessageSerializer:
    """Serialize dataclass messages using a schema and message encoder."""

    def __init__(
        self,
        schema_encoder: SchemaEncoder,
        message_encoder: type[MessageEncoder],
        schema_compiler: Callable,
    ) -> None:
        self._schema_encoder = schema_encoder
        self._message_encoder = message_encoder
        self._schema_compiler = schema_compiler
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
            serializer = self._schema_compiler(schema, sub_schemas)
            self._compiled[message_type] = serializer

        encoder = self._message_encoder(little_endian=little_endian)
        serializer(encoder, message)
        return encoder.save()

    def serialize_schema(self, schema: type[Message]) -> bytes:
        return self._schema_encoder.encode(schema)


class MessageSerializerFactory:
    """Factory for creating message serializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageSerializer | None:
        if profile == "ros2":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder, compile_serializer)
        if profile == "ros1":
            return MessageSerializer(Ros1MsgSchemaEncoder(), RosMsgEncoder, compile_ros1_serializer)
        return None

    @staticmethod
    def from_channel(channel: ChannelRecord, schema: SchemaRecord) -> MessageSerializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder, compile_serializer)
        if channel.message_encoding == "ros1" and schema.encoding == "ros1msg":
            return MessageSerializer(Ros1MsgSchemaEncoder(), RosMsgEncoder, compile_ros1_serializer)
        return None
