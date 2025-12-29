import fnmatch
import heapq
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
from pybag.mcap.records import (
    AttachmentRecord,
    ChannelRecord,
    MetadataRecord,
    SchemaRecord
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DecodedMessage():
    topic: str
    msg_type: str
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
    def from_file(
        file_path: Path | str,
        *,
        enable_crc_check: bool = False,
    ) -> 'McapFileReader':
        reader = McapRecordReaderFactory.from_file(
            file_path,
            enable_crc_check=enable_crc_check,
        )
        return McapFileReader(reader)

    @staticmethod
    def from_bytes(
        data: bytes,
        *,
        enable_crc_check: bool = False,
    ) -> 'McapFileReader':
        reader = McapRecordReaderFactory.from_bytes(
            data,
            enable_crc_check=enable_crc_check,
        )
        return McapFileReader(reader)

    @property
    def profile(self) -> str:
        return self._profile

    def get_topics(self) -> list[str]:
        """Get all topics in the MCAP file."""
        return [c.topic for c in self._reader.get_channels().values()] # TODO: Use a set?

    def get_channels(self) -> list[ChannelRecord]:
        """Get all channels in the MCAP file.

        Returns:
            List of ChannelRecord objects.
        """
        return list(self._reader.get_channels().values())

    def get_schema(self, topic: str) -> SchemaRecord | None:
        """Get the schema for a particular topic.

        Args:
            topic: The topic name to get the schema for.

        Returns:
            SchemaRecord for the topic, or None if not found.
        """
        channel_id = self._reader.get_channel_id(topic)
        if channel_id is None:
            return None
        return self._reader.get_channel_schema(channel_id)

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
        in_log_time_order: bool = True,
        in_reverse: bool = False,
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
            in_reverse: Return messages in reverse order (last first) if True.

        Returns:
            Generator yielding DecodedMessage objects from matching topics.
        """
        # If empty list we return no messages
        if (concrete_topics := self._expand_topics(topic)) == []:
            return
        logging.debug(f"Expanded topics: {concrete_topics}")

        # Get the channels corresponding to the topics given
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
            in_log_time_order=in_log_time_order,
            in_reverse=in_reverse,
        ):
            channel_record, schema = channel_infos[msg.channel_id]
            decoded = DecodedMessage(
                topic=channel_record.topic,
                msg_type=schema.name,
                channel_id=msg.channel_id,
                sequence=msg.sequence,
                log_time=msg.log_time,
                publish_time=msg.publish_time,
                data=message_deserializer.deserialize_message(msg, schema),
            )
            if filter is None or filter(decoded):
                yield decoded

    def get_attachments(self, name: str | None = None) -> list[AttachmentRecord]:
        """Get attachments from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all attachments.
                  If provided, returns only attachments with matching name.

        Returns:
            List of AttachmentRecord objects containing attachment data.
        """
        return self._reader.get_attachments(name)

    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all metadata records.
                  If provided, returns only metadata records with matching name.

        Returns:
            List of MetadataRecord objects containing metadata key-value pairs.
        """
        return self._reader.get_metadata(name)

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


class McapMultipleFileReader:
    """Reader that seamlessly reads from multiple MCAP files."""

    def __init__(self, readers: list[McapFileReader]):
        self._readers = readers

        self._profiles = set(r.profile for r in self._readers)
        self._message_deserializer = {
            profile: MessageDeserializerFactory.from_profile(profile)
            for profile in self._profiles
        }

    @staticmethod
    def from_files(file_paths: list[Path | str], *, enable_crc_check: bool = False) -> 'McapMultipleFileReader':
        readers = [McapFileReader.from_file(p, enable_crc_check=enable_crc_check) for p in file_paths]
        return McapMultipleFileReader(readers)

    @property
    def profiles(self) -> set[str]:
        return self._profiles

    def get_topics(self) -> list[str]:
        topics: set[str] = set()
        for reader in self._readers:
            topics.update(reader.get_topics())
        return list(topics)

    def get_message_count(self, topic: str) -> int:
        count = 0
        for reader in self._readers:
            if topic in reader.get_topics():
                count += reader.get_message_count(topic)
        if count == 0:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP files')
        return count

    @property
    def start_time(self) -> int:
        return min(reader.start_time for reader in self._readers)

    @property
    def end_time(self) -> int:
        return max(reader.end_time for reader in self._readers)

    def messages(
        self,
        topic: str | list[str],
        start_time: int | None = None,
        end_time: int | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[DecodedMessage, None, None]:
        # in_log_time_order being false makes less sense when multiple files are involved
        # possible strategies could be to iterate through all messages in one file before
        # iterating through the next file, but seems a bit weird so will leave it for now
        if not in_log_time_order:
            raise ValueError('in_log_time_order must be True')

        # Initialize the heap with the first message of each file
        heap: list[tuple[int, int, DecodedMessage, Generator[DecodedMessage, None, None]]] = []
        for reader in self._readers:
            it = iter(reader.messages(topic, start_time, end_time, in_log_time_order=in_log_time_order, in_reverse=in_reverse))
            try:
                msg = next(it)
                # For reverse iteration, negate log_time so heap gives us largest times first
                heap_key = -msg.log_time if in_reverse else msg.log_time
                heapq.heappush(heap, (heap_key, len(heap), msg, it))
            except StopIteration:
                continue

        # Yield messages from each file in log time order (or reverse)
        # Ties are split by the index the files were provided to in the constructor
        while heap:
            _, idx, msg, it = heapq.heappop(heap)
            if filter is None or filter(msg):
                yield msg
            try:
                next_msg = next(it)
                heap_key = -next_msg.log_time if in_reverse else next_msg.log_time
                heapq.heappush(heap, (heap_key, idx, next_msg, it))
            except StopIteration:
                pass

    def get_attachments(self, name: str | None = None) -> list[AttachmentRecord]:
        """Get attachments from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all attachments.
                  If provided, returns only attachments with matching name.

        Returns:
            List of AttachmentRecord objects containing attachment data.
        """
        attachments = []
        for reader in self._readers:
            attachments.extend(reader.get_attachments(name))
        return attachments

    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all metadata records.
                  If provided, returns only metadata records with matching name.

        Returns:
            List of MetadataRecord objects containing metadata key-value pairs.
        """
        metadata = []
        for reader in self._readers:
            metadata.extend(reader.get_metadata(name))
        return metadata

    def close(self) -> None:
        for reader in self._readers:
            reader.close()

    def __enter__(self) -> "McapMultipleFileReader":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None
    ) -> None:
        self.close()
