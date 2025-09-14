import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

from pybag.deserialize import MessageDeserializerFactory
from pybag.mcap.error import (
    McapUnknownEncodingError,
    McapUnknownSchemaError,
    McapUnknownTopicError
)
from pybag.mcap.record_reader import (
    BaseMcapRecordReader,
    McapRecordReaderFactory
)

# GLOBAL TODOs:
# - TODO: Add tests with mcaps
# - TODO: Improve performance by batching the reads (maybe)
# - TODO: Do something with CRC
# - TODO: Generate summary section of mcap file
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DecodedMessage():
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: Any  # TODO: Figure out how to type this


class McapFileReader:
    """Class to read MCAP file"""

    def __init__(self, reader: BaseMcapRecordReader):
        self._reader = reader

        header = self._reader.get_header()
        self._profile = header.profile
        self._message_deserializer = MessageDeserializerFactory.from_profile(self._profile)

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
        filter: Callable[[DecodedMessage], bool] | None = None,
    ) -> Generator[DecodedMessage, None, None]:
        """
        Iterate over messages in the MCAP file.

        Args:
            topic: Topic to filter by.
            start_time: Start time to filter by. If None, start from the beginning of the file.
            end_time: End time to filter by. If None, read to the end of the file.
            filter: Callable used to filter messages. If None, all messages are returned.

        Returns:
            An iterator over DecodedMessage objects.
        """
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            raise McapUnknownEncodingError(f'Topic {topic} not found in MCAP file')

        channel_record = self._reader.get_channel(channel_id)
        if (message_schema := self._reader.get_channel_schema(channel_id)) is None:
            raise McapUnknownSchemaError(f'Unknown schema for channel {channel_id}')

        if (message_deserializer := self._message_deserializer) is None:
            message_deserializer = MessageDeserializerFactory.from_channel(
                channel_record, message_schema
            )
        if message_deserializer is None:
            raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

        deserialize = message_deserializer.deserialize_message
        for message in self._reader.get_messages(channel_id, start_time, end_time):
            decoded = DecodedMessage(
                message.channel_id,
                message.sequence,
                message.log_time,
                message.publish_time,
                deserialize(message, message_schema),
            )
            if filter is None or filter(decoded):
                yield decoded

    def close(self) -> None:
        """Close the MCAP reader and release all resources."""
        self._reader.close()

    def __enter__(self) -> "McapFileReader":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None
    ) -> None:
        self.close()
