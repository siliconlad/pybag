import fnmatch
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

    def _expand_topics(self, topic: str | list[str]) -> list[str]:
        """Expand topic specification to list of concrete topic names.

        Handles:
        - Single topic string (may contain glob pattern like "/sensor/*")
        - List of topic strings (each may contain glob patterns)

        Args:
            topic: Topic specification (string or list of strings)

        Returns:
            Deduplicated list of concrete topic names that exist in the file
        """
        # Get all available topics
        available_topics = self.get_topics()

        # Normalize input to list
        topic_patterns = [topic] if isinstance(topic, str) else topic

        # Expand patterns to concrete topics
        matched_topics = set()
        for pattern in topic_patterns:
            # Check if pattern contains glob characters
            if '*' in pattern or '?' in pattern:
                # Use fnmatch to find matching topics
                matches = fnmatch.filter(available_topics, pattern)
                matched_topics.update(matches)
            else:
                # Exact match - check if topic exists
                if pattern in available_topics:
                    matched_topics.add(pattern)

        # Return sorted list for consistent ordering
        return sorted(matched_topics)

    def messages(
        self,
        topic: str | list[str],
        start_time: int | None = None,
        end_time: int | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
        *,
        in_log_time_order: bool = True
    ) -> Generator[DecodedMessage, None, None]:
        """
        Iterate over messages in the MCAP file.

        Args:
            topic: Topic(s) to filter by. Can be:
                - Single topic string (e.g., "/camera")
                - Glob pattern (e.g., "/sensor/*")
                - List of topics/patterns (e.g., ["/topic1", "/sensor/*"])
                - Empty list [] returns no messages
            start_time: Start time to filter by. If None, start from the beginning.
            end_time: End time to filter by. If None, read to the end.
            filter: Callable to filter messages. If None, all messages are returned.
            in_log_time_order: Return messages in log time order if True, otherwise in write order.

        Returns:
            Generator yielding DecodedMessage objects from matching topics.
        """
        # If empty list we return no messages
        if (concrete_topics := self._expand_topics(topic)) == []:
            return

        channel_infos = {}  # dict[channel_id, tuple[channel_record, schema]]
        for topic_name in concrete_topics:
            channel_id = self._reader.get_channel_id(topic_name)
            if channel_id is None:
                continue  # Skip topics that don't exist

            channel_record = self._reader.get_channel(channel_id)
            if channel_record is None:
                continue

            message_schema = self._reader.get_channel_schema(channel_id)
            if message_schema is None:
                raise McapUnknownSchemaError(f'Unknown schema for channel {channel_id}')

            channel_infos[channel_id] = (channel_record, message_schema)

        if not channel_infos:
            return

        if (message_deserializer := self._message_deserializer) is None:
            # TODO: Do not assume all channels use the same encoding
            channel_record, message_schema = next(iter(channel_infos.values()))
            message_deserializer = MessageDeserializerFactory.from_channel(
                channel_record, message_schema
            )
        if message_deserializer is None:
            raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

        for msg in self._reader.get_messages(
            list(channel_infos.keys()),
            start_time,
            end_time,
            in_log_time_order=in_log_time_order
        ):
            _, schema = channel_infos[msg.channel_id]
            decoded = DecodedMessage(
                msg.channel_id,
                msg.sequence,
                msg.log_time,
                msg.publish_time,
                message_deserializer.deserialize_message(msg, schema),
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
