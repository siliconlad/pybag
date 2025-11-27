from typing import Callable

from pybag.encoding import MessageDecoder
from pybag.encoding.cdr import CdrDecoder
from pybag.encoding.json import JsonDecoder
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.schema import SchemaDecoder
from pybag.schema.compiler import compile_schema
from pybag.schema.json_compiler import compile_json_schema
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


class JsonMessageDeserializer:
    """Deserialize JSON-encoded messages."""

    def __init__(self, schema_decoder: SchemaDecoder):
        self._schema_decoder = schema_decoder
        self._compiled: dict[int, Callable[[MessageDecoder], type]] = {}

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> type:
        """Deserialize a JSON message using the provided schema."""
        decoder = JsonDecoder(message.data)
        if schema.id not in self._compiled:
            msg_schema, schema_msgs = self._schema_decoder.parse_schema(schema)
            self._compiled[schema.id] = compile_json_schema(msg_schema, schema_msgs)
        return self._compiled[schema.id](decoder)


class MessageDeserializerFactory:
    """Factory for creating message deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageDeserializer | JsonMessageDeserializer | None:
        if profile == "ros2":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        if profile == "json":
            return JsonMessageDeserializer(Ros2MsgSchemaDecoder())
        return None

    @staticmethod
    def from_channel(
        channel: ChannelRecord, schema: SchemaRecord
    ) -> MessageDeserializer | JsonMessageDeserializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        if channel.message_encoding == "json" and schema.encoding == "ros2msg":
            return JsonMessageDeserializer(Ros2MsgSchemaDecoder())
        return None
