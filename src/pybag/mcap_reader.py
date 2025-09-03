import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
        topic: str | list[str] | None = None,
        topics: list[str] | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> Generator[DecodedMessage, None, None]:
        """Iterate over messages in the MCAP file.

        Args:
            topic: Single topic or pattern to filter by. Deprecated, use ``topics`` instead.
            topics: List of topics or glob patterns to filter by.
            start_time: Start time to filter by. If ``None``, start from the beginning of the
                file.
            end_time: End time to filter by. If ``None``, read to the end of the file.

        Returns:
            An iterator over :class:`DecodedMessage` objects.
        """

        if topics is None:
            if topic is None:
                raise McapUnknownTopicError('No topics provided')
            topics = [topic] if isinstance(topic, str) else list(topic)
        else:
            if topic is not None:
                raise ValueError('Specify either "topic" or "topics", not both')

        import fnmatch
        channel_map = {c.topic: cid for cid, c in self._reader.get_channels().items()}
        channel_ids: set[int] = set()
        for pattern in topics:
            matched = fnmatch.filter(channel_map.keys(), pattern)
            if not matched:
                raise McapUnknownTopicError(f'Topic {pattern} not found in MCAP file')
            channel_ids.update(channel_map[m] for m in matched)

        import heapq

        generators = [
            self._reader.get_messages(cid, start_time, end_time)
            for cid in sorted(channel_ids)
        ]
        heap: list[tuple[int, int, Any, Any]] = []
        for idx, gen in enumerate(generators):
            try:
                msg = next(gen)
            except StopIteration:
                continue
            heapq.heappush(heap, (msg.log_time, idx, msg, gen))

        while heap:
            log_time, idx, message, gen = heapq.heappop(heap)
            channel_id = message.channel_id
            message_deserializer = self._message_deserializer
            if message_deserializer is None:
                message_deserializer = MessageDeserializerFactory.from_message(
                    self._reader.get_channel(channel_id),
                    self._reader.get_message_schema(message)
                )
            if message_deserializer is None:
                raise McapUnknownEncodingError(f'Unknown encoding type: {self._profile}')

            yield DecodedMessage(
                channel_id,
                message.sequence,
                log_time,
                message.publish_time,
                message_deserializer.deserialize_message(
                    message,
                    self._reader.get_message_schema(message)
                )
            )

            try:
                next_msg = next(gen)
            except StopIteration:
                continue
            heapq.heappush(heap, (next_msg.log_time, idx, next_msg, gen))


if __name__ == '__main__':
    import json
    reader = McapFileReader.from_file(Path('/pybag/mcaps/pose_with_covariance.mcap'))
    for msg in reader.messages(topic='/pose_with_covariance'):
        print(json.dumps(msg.data, indent=4, sort_keys=True))
