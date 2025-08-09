import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pybag.encoding.cdr import CdrDecoder
from pybag.mcap.error import McapUnknownEncodingError, McapUnknownTopicError
from pybag.mcap.record_reader import (
    BaseMcapRecordReader,
    McapRecordReaderFactory
)
from pybag.mcap.records import MessageRecord, SchemaRecord
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgSchema,
    Schema,
    SchemaConstant,
    SchemaEntry,
    SchemaField,
    Sequence,
    String
)

# GLOBAL TODOs:
# - TODO: Add tests with mcaps
# - TODO: Improve performance by batching the reads (maybe)
# - TODO: Do something with CRC
# - TODO: Generate summary section of mcap file
logger = logging.getLogger(__name__)


@dataclass
class DecodedMessage:
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: Any  # TODO: Figure out how to type this


def decode_message(message: MessageRecord, schema: SchemaRecord) -> dict:
    """Decode a message using a schema."""
    # TODO: Support other encodings (e.g. ROS 1)
    if schema.encoding != 'ros2msg':
        error_msg = f'Unknown encoding type: {schema.encoding}'
        raise McapUnknownEncodingError(error_msg)

    cdr = CdrDecoder(message.data)
    msg_schema, schema_msgs = Ros2MsgSchema().parse(schema)  # TODO: Store more permanently

    def decode_field(schema: SchemaEntry, sub_schemas: dict[str, SchemaEntry]) -> type:
        field = {}
        for field_name, field_schema in schema.fields.items():
            # Handle constants
            if isinstance(field_schema, SchemaConstant):
                field[field_name] = field_schema.value

            # Handle primitive and string types
            elif isinstance(field_schema.type, (Primitive, String)):
                field[field_name] = cdr.parse(field_schema.type.type)

            # Handle arrays
            elif isinstance(field_schema.type, Array):
                array_type = field_schema.type
                if isinstance(array_type.type, (Primitive, String)):
                    length = array_type.length
                    primitive_type = array_type.type
                    field[field_name] = cdr.array(primitive_type.type, length)
                elif isinstance(array_type.type, Complex):
                    complex_type = array_type.type
                    if complex_type.type in sub_schemas:
                        length = array_type.length
                        sub_schema = sub_schemas[complex_type.type]
                        fields = [decode_field(sub_schema, sub_schemas) for i in range(length)]
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
                    field[field_name] = cdr.sequence(primitive_type.type)
                elif isinstance(sequence_type.type, Complex):
                    complex_type = sequence_type.type
                    if complex_type.type in sub_schemas:
                        length = cdr.uint32()
                        sub_schema = sub_schemas[complex_type.type]
                        fields = [decode_field(sub_schema, sub_schemas) for i in range(length)]
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
                    field[field_name] = decode_field(sub_schema, sub_schemas)
                else:
                    raise ValueError(f'Unknown field type: {field_schema}')

            # Throw error for unknown field types
            else:
                raise ValueError(f'Unknown field type: {field_schema}')
        return type(schema.name.replace('/', '.'), (), field)
    return decode_field(msg_schema, schema_msgs)


class McapFileReader:
    """Class to read MCAP file"""

    def __init__(self, reader: BaseMcapRecordReader):
        self._reader = reader

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapFileReader':
        reader = McapRecordReaderFactory.from_file(file_path)
        return McapFileReader(reader)

    @staticmethod
    def from_bytes(data: bytes) -> 'McapFileReader':
        reader = McapRecordReaderFactory.from_bytes(data)
        return McapFileReader(reader)

    def get_topics(self) -> list[str]:
        """Get all topics in the MCAP file."""
        return [c.topic for c in self._reader.get_channels().values()] # TODO: Use a set?

    def get_message_count(self, topic: str) -> int:
        """Get the number of messages in a given topic."""
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP file')
        return self._reader.get_statistics().channel_message_counts[channel_id]

    @property
    def start_time(self) -> int:
        """Get the start time of the MCAP file in nanoseconds since epoch."""
        return self._reader.get_statistics().message_start_time

    @property
    def end_time(self) -> int:
        """Get the end time of the MCAP file in nanoseconds since epoch."""
        return self._reader.get_statistics().message_end_time

    # Message Access

    def messages(
        self,
        topic: str,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> Generator[DecodedMessage, None, None]:
        """
        Iterate over messages in the MCAP file.

        Args:
            topic: Topic to filter by.
            start_time: Start time to filter by. If None, start from the beginning of the file.
            end_time: End time to filter by. If None, read to the end of the file.

        Returns:
            An iterator over DecodedMessage objects.
        """
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            raise McapUnknownEncodingError(f'Topic {topic} not found in MCAP file')

        for message in self._reader.get_messages(channel_id, start_time, end_time):
            yield DecodedMessage(
                message.channel_id,
                message.sequence,
                message.log_time,
                message.publish_time,
                decode_message(message, self._reader.get_message_schema(message))
            )


if __name__ == '__main__':
    import json
    reader = McapFileReader.from_file(Path('/pybag/mcaps/pose_with_covariance.mcap'))
    for msg in reader.messages(topic='/pose_with_covariance'):
        print(json.dumps(msg.data, indent=4, sort_keys=True))
