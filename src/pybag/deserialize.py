from pybag.encoding import MessageDecoder
from pybag.encoding.cdr import CdrDecoder
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.schema import SchemaDecoder
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgSchemaDecoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String
)


class MessageDeserializer:
    """Abstract base class for message deserializers."""

    def __init__(
        self,
        schema_decoder: SchemaDecoder,
        message_decoder: type[MessageDecoder],
    ):
        self._schema_decoder = schema_decoder
        self._message_decoder = message_decoder

    def _decode_field(
        self,
        message_decoder: MessageDecoder,
        schema: Schema,
        sub_schemas: dict[str, Schema]
    ) -> type:
        field = {}
        for field_name, field_schema in schema.fields.items():
            # Handle constants
            if isinstance(field_schema, SchemaConstant):
                field[field_name] = field_schema.value

            # Handle fields
            elif isinstance(field_schema, SchemaField):
                # Handle primitive and string types
                if isinstance(field_schema.type, (Primitive, String)):
                    field[field_name] = message_decoder.parse(field_schema.type.type)

                # Handle arrays
                elif isinstance(field_schema.type, Array):
                    array_type = field_schema.type
                    if isinstance(array_type.type, (Primitive, String)):
                        length = array_type.length
                        primitive_type = array_type.type
                        field[field_name] = message_decoder.array(primitive_type.type, length)
                    elif isinstance(array_type.type, Complex):
                        complex_type = array_type.type
                        if complex_type.type in sub_schemas:
                            length = array_type.length
                            sub_schema = sub_schemas[complex_type.type]
                            fields = [
                                self._decode_field(
                                    message_decoder,
                                    sub_schema,
                                    sub_schemas
                                ) for i in range(length)
                            ]
                            field[field_name] = fields
                        else:
                            raise ValueError(f'Unknown field type: {complex_type.type}')
                    else:
                        raise ValueError(f'Unknown field type: {array_type.type}')

                # Handle sequences
                elif isinstance(field_schema.type, Sequence):
                    sequence_type = field_schema.type
                    if isinstance(sequence_type.type, (Primitive, String)):
                        primitive_type = sequence_type.type
                        field[field_name] = message_decoder.sequence(primitive_type.type)
                    elif isinstance(sequence_type.type, Complex):
                        complex_type = sequence_type.type
                        if complex_type.type in sub_schemas:
                            length = message_decoder.uint32()
                            sub_schema = sub_schemas[complex_type.type]
                            fields = [
                                self._decode_field(
                                    message_decoder,
                                    sub_schema,
                                    sub_schemas
                                ) for i in range(length)
                            ]
                            field[field_name] = fields
                        else:
                            raise ValueError(f'Unknown field type: {complex_type.type}')
                    else:
                        raise ValueError(f'Unknown field type: {field_schema}')

                # Handle complex types
                elif isinstance(field_schema.type, Complex):
                    complex_type = field_schema.type
                    if complex_type.type in sub_schemas:
                        sub_schema = sub_schemas[complex_type.type]
                        field[field_name] = self._decode_field(
                            message_decoder,
                            sub_schema,
                            sub_schemas
                        )
                    else:
                        raise ValueError(f'Unknown field type: {field_schema}')

                else:
                    raise ValueError(f'Unknown field type: {field_schema}')

            # Throw error for unknown field types
            else:
                raise ValueError(f'Unknown field type: {field_schema}')
        return type(schema.name.replace('/', '.'), (), field)

    def deserialize_message(self, message: MessageRecord, schema: SchemaRecord) -> type:
        """Deserialize a message using the provided schema."""
        message_decoder = self._message_decoder(message.data)
        msg_schema, schema_msgs = self._schema_decoder.parse_schema(schema)
        return self._decode_field(message_decoder, msg_schema, schema_msgs)


class MessageDeserializerFactory:
    """Factory for creating message deserializers."""

    @staticmethod
    def from_profile(profile: str) -> MessageDeserializer | None:
        if profile == "ros2":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

    @staticmethod
    def from_message(channel: ChannelRecord, schema: SchemaRecord) -> MessageDeserializer | None:
        if channel.message_encoding == "cdr" and schema.encoding == "ros2msg":
            return MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)
