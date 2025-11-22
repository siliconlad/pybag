from typing import Callable

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

            # Try to use pre-compiled decoder first
            from pybag import precompiled
            precompiled_decoder = precompiled.get_decoder(msg_schema.name)

            if precompiled_decoder is not None:
                self._compiled[schema.id] = precompiled_decoder
            else:
                # Fall back to runtime compilation
                self._compiled[schema.id] = compile_schema(msg_schema, schema_msgs)
        return self._compiled[schema.id](decoder)


class MessageDeserializerFactory:
    """Factory for creating message deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageDeserializer | None:
        if profile == "ros2":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        return None

    @staticmethod
    def from_channel(channel: ChannelRecord, schema: SchemaRecord) -> MessageDeserializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
        return None
