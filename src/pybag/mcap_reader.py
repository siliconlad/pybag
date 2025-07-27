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
from pybag.schema.ros2msg import Ros2MsgFieldType, parse_ros2msg

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
    msg_schema, schema_msgs = parse_ros2msg(schema)

    def decode_field(field_schema: dict, field_schemas: dict) -> dict:
        field = {}
        for field_name, field_dict in field_schema.items():
            if field_dict['field_type'] == Ros2MsgFieldType.PRIMITIVE:
                field[field_name] = cdr.parse(field_dict['data_type'])
            elif field_dict['field_type'] == Ros2MsgFieldType.ARRAY:
                field[field_name] = cdr.array(field_dict['data_type'], field_dict['length'])
            elif field_dict['field_type'] == Ros2MsgFieldType.SEQUENCE:
                field[field_name] = cdr.sequence(field_dict['data_type'])
            elif field_dict['field_type'] == Ros2MsgFieldType.COMPLEX:
                new_msg_schema = field_schemas[field_dict['data_type']]
                field[field_name] = decode_field(new_msg_schema, field_schemas)
            else:
                raise ValueError(f'Unknown field type: {field_dict["field_type"]}')
        return field

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
