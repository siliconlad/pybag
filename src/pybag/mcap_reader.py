import logging
import heapq
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Iterable

from pybag.deserialize import MessageDeserializerFactory
from pybag.mcap.error import McapUnknownEncodingError, McapUnknownTopicError
from pybag.mcap.record_reader import (
    BaseMcapRecordReader,
    McapRecordReaderFactory,
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


class McapFileReader:
    """Class to read MCAP file"""

    def __init__(self, reader: BaseMcapRecordReader):
        self._reader = reader

        header = self._reader.get_header()
        self._profile = header.profile
        self._message_deserializer = MessageDeserializerFactory.from_profile(self._profile)

    @staticmethod
    def from_file(file_path: Path | str | Iterable[Path | str]) -> 'McapFileReader':
        """Create a reader from a file path or iterable of file paths."""
        if isinstance(file_path, Iterable) and not isinstance(file_path, (str, Path)):
            return McapMultipleFileReader.from_files(list(file_path))
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

        for message in self._reader.get_messages(channel_id, start_time, end_time):
            message_deserializer = self._message_deserializer
            if message_deserializer is None:
                message_deserializer = MessageDeserializerFactory.from_channel(
                    self._reader.get_channel(channel_id),
                    self._reader.get_message_schema(message)
                )
            if message_deserializer is None:
                raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

            decoded = DecodedMessage(
                message.channel_id,
                message.sequence,
                message.log_time,
                message.publish_time,
                message_deserializer.deserialize_message(
                    message,
                    self._reader.get_message_schema(message)
                ),
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


class McapMultipleFileReader(McapFileReader):
    """Reader that seamlessly reads from multiple MCAP files."""

    def __init__(self, readers: list[McapFileReader]):
        self._readers = readers

    @staticmethod
    def from_files(file_paths: list[Path | str]) -> 'McapMultipleFileReader':
        readers = [McapFileReader.from_file(p) for p in file_paths]
        return McapMultipleFileReader(readers)

    def get_topics(self) -> list[str]:  # type: ignore[override]
        topics: set[str] = set()
        for reader in self._readers:
            topics.update(reader.get_topics())
        return list(topics)

    def get_message_count(self, topic: str) -> int:  # type: ignore[override]
        count = 0
        for reader in self._readers:
            if topic in reader.get_topics():
                count += reader.get_message_count(topic)
        if count == 0:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP files')
        return count

    @property  # type: ignore[override]
    def start_time(self) -> int:
        return min(reader.start_time for reader in self._readers)

    @property  # type: ignore[override]
    def end_time(self) -> int:
        return max(reader.end_time for reader in self._readers)

    def messages(  # type: ignore[override]
        self,
        topic: str,
        start_time: float | None = None,
        end_time: float | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
    ) -> Generator[DecodedMessage, None, None]:
        iterators = []
        for reader in self._readers:
            if topic in reader.get_topics():
                iterators.append(iter(reader.messages(topic, start_time, end_time)))
        if not iterators:
            raise McapUnknownTopicError(f'Topic {topic} not found in MCAP files')

        heap: list[tuple[int, int, DecodedMessage, Generator[DecodedMessage, None, None]]] = []
        for idx, it in enumerate(iterators):
            try:
                msg = next(it)
                heapq.heappush(heap, (msg.log_time, idx, msg, it))
            except StopIteration:
                continue

        while heap:
            _, idx, msg, it = heapq.heappop(heap)
            if filter is None or filter(msg):
                yield msg
            try:
                next_msg = next(it)
                heapq.heappush(heap, (next_msg.log_time, idx, next_msg, it))
            except StopIteration:
                pass

    def close(self) -> None:  # type: ignore[override]
        for reader in self._readers:
            reader.close()
