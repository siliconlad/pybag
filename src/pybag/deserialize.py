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

            # Try to find message class with decode() method
            message_class = self._find_message_class(msg_schema.name)

            if message_class is not None and hasattr(message_class, 'decode'):
                # Use the decode() static method from the message class
                self._compiled[schema.id] = message_class.decode
            else:
                # Fall back to runtime compilation
                self._compiled[schema.id] = compile_schema(msg_schema, schema_msgs)
        return self._compiled[schema.id](decoder)

    def _find_message_class(self, msg_name: str) -> type | None:
        """Find the message class by name (e.g., 'std_msgs/msg/Header')."""
        try:
            # Parse message name: 'std_msgs/msg/Header' -> ('std_msgs', 'Header')
            parts = msg_name.split('/')
            if len(parts) != 3:
                return None

            package = parts[0]
            class_name = parts[2]

            # Try to import from ros2.humble (can be extended to support other distros)
            try:
                module = __import__(f'pybag.ros2.humble.{package}', fromlist=[class_name])
                return getattr(module, class_name, None)
            except (ImportError, AttributeError):
                return None

        except Exception:
            return None


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
