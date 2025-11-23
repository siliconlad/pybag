import fnmatch
import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

from pybag.deserialize import MessageDeserializerFactory
from pybag.mcap.error import McapUnknownEncodingError, McapUnknownTopicError
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

    @property
    def profile(self) -> str:
        return self._profile

    def get_topics(self) -> list[str]:
        """Get all topics in the MCAP file."""
        return [c.topic for c in self._reader.get_channels().values()] # TODO: Use a set?

    def get_message_count(self, topic: str) -> int:
        """Get the number of messages in a given topic.

        Args:
            topic: The topic name.

        Returns:
            The number of messages in the topic.

        Raises:
            McapUnknownTopicError: If the topic is not found in the MCAP file.
        """
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
        """Expand topic patterns to list of concrete topic names.

        Handles:
        - Single topic string (may contain glob pattern like "/sensor/*")
        - List of topic strings (each may contain glob patterns)

        Args:
            topic: Topic pattern (string or list of strings)

        Returns:
            Deduplicated list of concrete topic names that exist in the file
        """
        available_topics = self.get_topics()
        topic_patterns = [topic] if isinstance(topic, str) else topic
        matched_topics = set()
        for pattern in topic_patterns:
            matches = fnmatch.filter(available_topics, pattern)
            matched_topics.update(matches)
        return list(matched_topics)

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
        logging.debug(f"Expanded topics: {concrete_topics}")

        channel_infos = {}  # dict[channel_id, tuple[channel_record, schema]]
        for topic_name in concrete_topics:
            channel_id = self._reader.get_channel_id(topic_name)
            if channel_id is None:
                logging.warning(f"{topic_name} corresponds to no channel")
                continue  # Skip topics that don't exist

            channel_record = self._reader.get_channel(channel_id)
            if channel_record is None:
                logging.warning(f"No channel record for {topic_name} ({channel_id})")
                continue

            message_schema = self._reader.get_channel_schema(channel_id)
            if message_schema is None:
                logging.warning(f"Unknown schema for {topic_name} ({channel_id})")
                continue

            channel_infos[channel_id] = (channel_record, message_schema)

        if not channel_infos:
            logging.warning(f'Nothing to retrieve!')
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

    # Index-based Message Access

    def get_message_at_index(self, topic: str, index: int) -> DecodedMessage | None:
        """
        Get a message at a specific index within a topic.

        This provides O(1) random access to messages by their positional index.
        Messages are ordered by (log_time, offset) which matches chronological order.

        Args:
            topic: The topic name.
            index: The positional index (0-based) of the message to retrieve.

        Returns:
            A DecodedMessage object or None if the index is out of bounds or topic not found.

        Raises:
            McapUnknownTopicError: If the topic is not found in the MCAP file.
            McapUnknownEncodingError: If the message encoding is unknown.

        Example:
            # Get the first message from the "/camera" topic
            msg = reader.get_message_at_index("/camera", 0)

            # Get the 100th message from the "/lidar" topic
            msg = reader.get_message_at_index("/lidar", 99)
        """
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP file')

        # Get the raw message record
        message = self._reader.get_message_at_index(channel_id, index)
        if message is None:
            return None

        # Get schema and deserializer
        schema = self._reader.get_channel_schema(channel_id)
        if schema is None:
            logger.warning(f"Unknown schema for {topic} ({channel_id})")
            return None

        if (message_deserializer := self._message_deserializer) is None:
            channel_record = self._reader.get_channel(channel_id)
            if channel_record is None:
                logger.warning(f"No channel record for {topic} ({channel_id})")
                return None
            message_deserializer = MessageDeserializerFactory.from_channel(
                channel_record, schema
            )
        if message_deserializer is None:
            raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

        # Deserialize and return
        return DecodedMessage(
            message.channel_id,
            message.sequence,
            message.log_time,
            message.publish_time,
            message_deserializer.deserialize_message(message, schema),
        )

    def messages_by_index(
        self,
        topic: str,
        start_index: int,
        end_index: int | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
    ) -> Generator[DecodedMessage, None, None]:
        """
        Iterate over messages within a specific index range for a topic.

        This provides efficient batch access to contiguous ranges of messages.
        Messages are ordered by (log_time, offset) which matches chronological order.

        Args:
            topic: The topic name.
            start_index: The starting index (inclusive, 0-based).
            end_index: The ending index (exclusive). If None, reads to the end.
            filter: Optional callable to filter messages.

        Yields:
            DecodedMessage objects in the specified index range.

        Raises:
            McapUnknownTopicError: If the topic is not found in the MCAP file.
            McapUnknownEncodingError: If the message encoding is unknown.

        Example:
            # Get messages 0-99 (first 100 messages) from "/camera"
            for msg in reader.messages_by_index("/camera", 0, 100):
                process(msg)

            # Get all messages starting from index 1000 from "/lidar"
            for msg in reader.messages_by_index("/lidar", 1000):
                process(msg)

            # Get messages with filtering
            for msg in reader.messages_by_index("/sensor", 0, 1000,
                                                 filter=lambda m: m.data.value > 10):
                process(msg)
        """
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP file')

        # Get schema and deserializer
        schema = self._reader.get_channel_schema(channel_id)
        if schema is None:
            logger.warning(f"Unknown schema for {topic} ({channel_id})")
            return

        if (message_deserializer := self._message_deserializer) is None:
            channel_record = self._reader.get_channel(channel_id)
            if channel_record is None:
                logger.warning(f"No channel record for {topic} ({channel_id})")
                return
            message_deserializer = MessageDeserializerFactory.from_channel(
                channel_record, schema
            )
        if message_deserializer is None:
            raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

        # Iterate over the range and deserialize messages
        for message in self._reader.get_messages_by_index_range(channel_id, start_index, end_index):
            decoded = DecodedMessage(
                message.channel_id,
                message.sequence,
                message.log_time,
                message.publish_time,
                message_deserializer.deserialize_message(message, schema),
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
