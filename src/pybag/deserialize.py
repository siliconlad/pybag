from typing import Callable

from google.protobuf.message import Message as ProtobufMessage

from pybag.encoding import MessageDecoder
from pybag.encoding.cdr import CdrDecoder
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.schema import SchemaDecoder
from pybag.schema.compiler import compile_schema
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder


class MessageDeserializer:
    """Abstract base class for message deserializers."""

    def __init__(
        self,
        schema_decoder: SchemaDecoder,
        message_decoder: type[MessageDecoder],
    ):
        self._schema_decoder = schema_decoder
        self._message_decoder = message_decoder
        self._compiled: dict[int, Callable[[MessageDecoder], type]] = {}

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> type:
        """Deserialize a message using the provided schema."""
        decoder = self._message_decoder(message.data)
        if schema.id not in self._compiled:
            msg_schema, schema_msgs = self._schema_decoder.parse_schema(schema)
            self._compiled[schema.id] = compile_schema(msg_schema, schema_msgs)
        return self._compiled[schema.id](decoder)


class ProtobufMessageDeserializer:
    """Deserialize protobuf messages using their native deserialization."""

    def __init__(self, schema_decoder: "ProtobufSchemaDecoder") -> None:  # type: ignore[name-defined]
        self._schema_decoder = schema_decoder

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> ProtobufMessage:
        """Deserialize a protobuf message.

        Args:
            message: The message record containing the serialized data.
            schema: The schema record containing the FileDescriptorSet.

        Returns:
            The deserialized protobuf message.
        """
        # Ensure the schema has been parsed and message class is cached
        if schema.id not in self._schema_decoder._message_classes:
            self._schema_decoder.parse_schema(schema)

        # Get the message class and deserialize
        message_class = self._schema_decoder.get_message_class(schema.id)
        proto_msg = message_class()
        proto_msg.ParseFromString(message.data)
        return proto_msg


class MessageDeserializerFactory:
    """Factory for creating message deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageDeserializer | ProtobufMessageDeserializer | None:
        if profile == "ros2":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        elif profile == "protobuf":
            from pybag.schema.protobuf import ProtobufSchemaDecoder
            return ProtobufMessageDeserializer(ProtobufSchemaDecoder())
        return None

    @staticmethod
    def from_channel(
        channel: ChannelRecord,
        schema: SchemaRecord
    ) -> MessageDeserializer | ProtobufMessageDeserializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        elif channel.message_encoding == "protobuf" and schema.encoding == "protobuf":
            from pybag.schema.protobuf import ProtobufSchemaDecoder
            return ProtobufMessageDeserializer(ProtobufSchemaDecoder())
        return None
