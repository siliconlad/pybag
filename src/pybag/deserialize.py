from typing import Callable

from pybag.encoding import MessageDecoder
from pybag.encoding.cdr import CdrDecoder
from pybag.encoding.rosmsg import RosMsgDecoder
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.schema import SchemaDecoder
from pybag.schema.compiler import compile_schema
from pybag.schema.ros1_compiler import compile_ros1_schema
from pybag.schema.ros1msg import Ros1McapSchemaDecoder
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder


class MessageDeserializer:
    """
    This class caches compiled schema decoders and reuses a single decoder
    instance across all messages to minimize object allocation overhead.
    """

    def __init__(
        self,
        schema_decoder: SchemaDecoder,
        message_decoder: type[MessageDecoder],
        schema_compiler: Callable,
    ):
        self._schema_decoder = schema_decoder
        self._message_decoder_class = message_decoder
        self._schema_compiler = schema_compiler
        self._compiled: dict[int, Callable[[MessageDecoder], type]] = {}
        # Reusable decoder instance - created lazily on first use
        self._reusable_decoder: MessageDecoder | None = None

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> type:
        """Deserialize a message using the provided schema.

        This method reuses the decoder instance across calls when possible,
        which significantly reduces object allocation overhead when decoding
        many messages in sequence.
        """
        # Reuse existing decoder if available, otherwise create new one
        if self._reusable_decoder is not None:
            # Reset existing decoder with new data (zero allocation)
            decoder = self._reusable_decoder.reset(message.data)
        else:
            # Create new decoder and cache it for reuse
            decoder = self._message_decoder_class(message.data)
            self._reusable_decoder = decoder

        # Compile schema decoder if not already cached
        if schema.id not in self._compiled:
            msg_schema, schema_msgs = self._schema_decoder.parse_schema(schema)
            self._compiled[schema.id] = self._schema_compiler(msg_schema, schema_msgs)
        return self._compiled[schema.id](decoder)


class MessageDeserializerFactory:
    """Factory for creating message deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageDeserializer | None:
        if profile == "ros2":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder, compile_schema)
        if profile == "ros1":
            return MessageDeserializer(Ros1McapSchemaDecoder(), RosMsgDecoder, compile_ros1_schema)
        return None

    @staticmethod
    def from_channel(channel: ChannelRecord, schema: SchemaRecord) -> MessageDeserializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder, compile_schema)
        if channel.message_encoding == "ros1" and schema.encoding == "ros1msg":
            return MessageDeserializer(Ros1McapSchemaDecoder(), RosMsgDecoder, compile_ros1_schema)
        return None
