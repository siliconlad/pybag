import logging
from abc import ABC, abstractmethod
from collections import namedtuple
from pathlib import Path
from typing import Generator

from pybag.crc import assert_crc
from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.mcap.error import (
    McapNoChunkError,
    McapNoChunkIndexError,
    McapNoStatisticsError,
    McapNoSummaryIndexError,
    McapNoSummarySectionError,
    McapUnexpectedChunkIndexError,
    McapUnknownCompressionError,
    McapUnknownSchemaError
)
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser,
    McapRecordType
)
from pybag.mcap.records import (
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    FooterRecord,
    HeaderRecord,
    MessageIndexRecord,
    MessageRecord,
    SchemaRecord,
    StatisticsRecord
)

logger = logging.getLogger(__name__)


def decompress_chunk(chunk: ChunkRecord, *, check_crc: bool = False) -> bytes:
    """Decompress the records field of a chunk."""
    if chunk.compression == 'zstd':
        import zstandard as zstd
        chunk_data = zstd.ZstdDecompressor().decompress(chunk.records)
    elif chunk.compression == 'lz4':
        import lz4.frame
        chunk_data = lz4.frame.decompress(chunk.records)
    elif chunk.compression == '':
        chunk_data = chunk.records
    else:
        error_msg = f'Unknown compression type: {chunk.compression}'
        raise McapUnknownCompressionError(error_msg)

    # Validate the CRC if requested
    if check_crc and chunk.uncompressed_crc != 0:
        assert_crc(chunk_data, chunk.uncompressed_crc)
    return chunk_data


# TODO: Is this the minimal set of methods needed?
class BaseMcapRecordReader(ABC):
    @abstractmethod
    def __enter__(self) -> 'BaseMcapRecordReader':
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        ...

    @abstractmethod
    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        ...

    @abstractmethod
    def get_header(self) -> HeaderRecord:
        """Get the header record from the MCAP file."""
        ...

    @abstractmethod
    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        ...

    @abstractmethod
    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        ...

    # Schema Management

    @abstractmethod
    def get_schemas(self) -> dict[int, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        ...

    @abstractmethod
    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """Get a schema by its ID."""
        ...

    @abstractmethod
    def get_channel_schema(self, channel_id: int) -> SchemaRecord | None:
        """Get the schema for a given channel ID."""
        ...

    @abstractmethod
    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        """Get the schema for a given message."""
        ...

    # Channel Management

    @abstractmethod
    def get_channels(self) -> dict[int, ChannelRecord]:
        """Get all channels/topics in the MCAP file."""
        ...

    @abstractmethod
    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        """Get a channel by its ID."""
        ...

    @abstractmethod
    def get_channel_id(self, topic: str) -> int | None:
        """Get a channel ID by its topic."""
        ...

    # Message Index Management

    @abstractmethod
    def get_message_indexes(self, chunk_index: ChunkIndexRecord) -> dict[int, MessageIndexRecord]:
        """Get all message indexes from the MCAP file."""
        ...

    @abstractmethod
    def get_message_index(self, chunk_index: ChunkIndexRecord, channel_id: int) -> MessageIndexRecord | None:
        """Get a message index for a given channel ID."""
        ...

    # Chunk Management

    @abstractmethod
    def get_chunk_indexes(self, channel_id: int | None = None) -> list[ChunkIndexRecord]:
        """Get all chunk indexes from the MCAP file."""
        ...

    @abstractmethod
    def get_chunk(self, chunk_index: ChunkIndexRecord) -> ChunkRecord:
        """Get a chunk by its index."""
        ...

    # Message Management

    @abstractmethod
    def get_message(
        self,
        channel_id: int,
        timestamp: int | None = None,
    ) -> MessageRecord | None:
        ...

    @abstractmethod
    def get_messages(
        self,
        channel_id: int | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        ...


class McapChunkedReader(BaseMcapRecordReader):
    """Class to efficiently get records from a chunked MCAP file.

    Args:
        file: The file to read from.
        check_crc: Whether to validate the crc values in the mcap
    """

    def __init__(self, file: BaseReader, *, check_crc: bool = False):
        self._file = file
        self._check_crc = check_crc

        self._version = McapRecordParser.parse_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        footer = self.get_footer()

        # Summary section start
        self._summary_start = footer.summary_start
        if self._summary_start == 0:
            error_msg = 'No summary section detected in MCAP'
            raise McapNoSummarySectionError(error_msg)

        # Summary offset section start
        self._summary_offset_start = footer.summary_offset_start
        if self._summary_offset_start == 0:
            error_msg = 'No summary offset section detected in MCAP'
            raise McapNoSummaryIndexError(error_msg)

        # Load summary offsets
        Offset = namedtuple('Offset', ['group_start', 'group_length'])
        self._summary_offset: dict[int, Offset] = {}
        self._file.seek_from_start(self._summary_offset_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordParser.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = Offset(record.group_start, record.group_length)

        # Load chunk indexes
        self._chunk_indexes: list[ChunkIndexRecord] = []
        chunk_summary_offset = self._summary_offset.get(McapRecordType.CHUNK_INDEX)
        if chunk_summary_offset is None:
            error_msg = 'No chunk index records founds in mcap'
            raise McapNoChunkIndexError(error_msg)
        self._file.seek_from_start(chunk_summary_offset.group_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.CHUNK_INDEX:
            chunk_index = McapRecordParser.parse_chunk_index(self._file)
            self._chunk_indexes.append(chunk_index)
        self._chunk_indexes.sort(key=lambda x: x.message_start_time)

        # Caches for message indexes
        self._message_indexes: dict[int, dict[int, MessageIndexRecord]] = {}

        # Cached schema and channel dictionaries populated on first access
        self._schemas: dict[int, SchemaRecord] | None = None
        self._channels: dict[int, ChannelRecord] | None = None

    # Helpful Constructors

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapChunkedReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapChunkedReader(FileReader(file_path))

    @staticmethod
    def from_bytes(data: bytes) -> 'McapChunkedReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapChunkedReader(BytesReader(data))

    # Destructors

    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

    # Context Managers

    def __enter__(self) -> 'McapChunkedReader':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    # Getters for records

    def get_header(self) -> HeaderRecord:
        """Get the header record from the MCAP file."""
        self._file.seek_from_start(MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_header(self._file)

    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_footer(self._file)

    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        if McapRecordType.STATISTICS not in self._summary_offset:
            raise McapNoStatisticsError('No statistics section detected in MCAP')
        self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS].group_start)
        return McapRecordParser.parse_statistics(self._file)

    # Schema Management

    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        if self._schemas is None:
            self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA].group_start)
            schemas: dict[int, SchemaRecord] = {}
            while McapRecordParser.peek_record(self._file) == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(self._file)
                if schema is None:  # Invalid schema, should be ignored
                    continue
                schemas[schema.id] = schema
            self._schemas = schemas
        return self._schemas

    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """
        Get a schema by its ID.

        Args:
            schema_id: The ID of the schema.

        Returns:
            The schema or None if the schema does not exist.
        """
        return self.get_schemas().get(schema_id)

    def get_channel_schema(self, channel_id: int) -> SchemaRecord | None:
        """
        Get the schema for a given channel ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The schema of the channel or None if the channel/schema does not exist.
        """
        channel = self.get_channel(channel_id)
        if channel is None:
            return None
        return self.get_schema(channel.schema_id)

    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        """
        Get the schema for a given message.

        Args:
            message: The message to get the schema for.

        Returns:
            The schema for the message.
        """
        schema = self.get_channel_schema(message.channel_id)
        if schema is None:
            raise McapUnknownSchemaError(f'Unknown schema for channel {message.channel_id}')
        return schema

    # Channel Management

    def get_channels(self) -> dict[int, ChannelRecord]:
        """
        Get all channels/topics in the MCAP file.

        Returns:
            A dictionary mapping channel IDs to channel information.
        """
        if self._channels is None:
            self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL].group_start)
            channels: dict[int, ChannelRecord] = {}
            while McapRecordParser.peek_record(self._file) == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
            self._channels = channels
        return self._channels

    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        """
        Get channel information by its ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The channel information or None if the channel does not exist.
        """
        return self.get_channels().get(channel_id)

    def get_channel_id(self, topic: str) -> int | None:
        """Get a channel ID by its topic."""
        for channel in self.get_channels().values():
            if channel.topic == topic:
                return channel.id
        return None

    # Message Index Management

    def get_message_indexes(self, chunk_index: ChunkIndexRecord) -> dict[int, MessageIndexRecord]:
        """
        Get all message indexes from the MCAP file.

        Args:
            chunk_index: The chunk index to get the message indexes from.

        Returns:
            A list of MessageIndexRecord objects.
        """
        key = chunk_index.chunk_start_offset
        if key in self._message_indexes:
            return self._message_indexes[key]

        message_index: dict[int, MessageIndexRecord] = {}
        for channel_id, message_index_offset in chunk_index.message_index_offsets.items():
            self._file.seek_from_start(message_index_offset)
            message_index[channel_id] = McapRecordParser.parse_message_index(self._file)
            message_index[channel_id].records.sort(key=lambda x: (x[0], x[1]))

        self._message_indexes[key] = message_index
        return message_index

    def get_message_index(self, chunk_index: ChunkIndexRecord, channel_id: int) -> MessageIndexRecord | None:
        """
        Get a message index for a given channel ID.

        Args:
            chunk_index: The chunk index to get the message indexes from.
            channel_id: The ID of the channel.

        Returns:
            A MessageIndexRecord object or None if the channel does not exist for the chunk.
        """
        return self.get_message_indexes(chunk_index).get(channel_id)

    # Chunk Management

    def _load_chunk_indexes(self) -> None:
        if self._chunk_indexes is not None:
            return

        self._chunk_indexes = []
        self._file.seek_from_start(self._summary_offset[McapRecordType.CHUNK_INDEX].group_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.CHUNK_INDEX:
            chunk_index = McapRecordParser.parse_chunk_index(self._file)
            self._chunk_indexes.append(chunk_index)
        self._chunk_indexes.sort(key=lambda x: x.message_start_time)

    def get_chunk_indexes(self, channel_id: int | None = None) -> list[ChunkIndexRecord]:
        """
        Get all chunk indexes from the MCAP file.

        Args:
            channel_id: The ID of the channel to get the chunk indexes for.
                        If None, all chunk indexes are returned.

        Returns:
            A list of ChunkIndexRecord objects.
        """
        self._load_chunk_indexes()
        if self._chunk_indexes is None:
            return []
        if channel_id is None:
            return self._chunk_indexes
        return [ci for ci in self._chunk_indexes if channel_id in ci.message_index_offsets]

    def get_chunk(self, chunk_index: ChunkIndexRecord) -> ChunkRecord:
        """
        Get a chunk by its index.

        Args:
            chunk_index: The chunk index to get the chunk from.

        Returns:
            A ChunkRecord object.
        """
        self._file.seek_from_start(chunk_index.chunk_start_offset)
        return McapRecordParser.parse_chunk(self._file)

    # Message Management

    def get_message(
        self,
        channel_id: int,
        timestamp: int | None = None,
    ) -> MessageRecord | None:
        """
        Get a message from a given channel at a given timestamp.

        If the timestamp is not provided, the first message in the channel is returned.

        Args:
            channel_id: The ID of the channel.
            timestamp: The timestamp of the message.

        Returns:
            A MessageRecord object or None if the message does not exist.
        """
        chunk_indexes = self.get_chunk_indexes(channel_id)
        for chunk_index in chunk_indexes:
            if timestamp is None or (chunk_index.message_start_time <= timestamp <= chunk_index.message_end_time):
                # The message must be in the chunk based on the start and end times
                message_index = self.get_message_index(chunk_index, channel_id)
                if message_index is None:
                    return None

                # Make sure the timestamp is in the message index
                offset = next((r[1] for r in message_index.records if r[0] == timestamp), None)
                if offset is None:
                    return None

                # Read data from chunk
                chunk = self.get_chunk(chunk_index)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                reader.seek_from_start(offset)
                return McapRecordParser.parse_message(reader)
        return None

    def get_messages(
        self,
        channel_id: int | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        """
        Get messages from the MCAP file.

        If no channel is provided, messages from all channels are returned.
        If the start and end timestamps are not provided, the entire available range is returned.

        Args:
            channel_id: Optional channel ID to filter by. If None, all channels are included.
            start_timestamp: The start timestamp to filter by. If None, no filtering is done.
            end_timestamp: The end timestamp to filter by. If None, no filtering is done.

        Returns:
            A generator of MessageRecord objects.
        """
        relevant_chunks = []
        for chunk_index in self.get_chunk_indexes(channel_id):
            # Skip chunk that do not match the timestamp range
            if start_timestamp is not None and chunk_index.message_end_time < start_timestamp:
                continue
            if end_timestamp is not None and chunk_index.message_start_time > end_timestamp:
                continue
            relevant_chunks.append(chunk_index)

        if not relevant_chunks:
            return

        # Check if chunks have overlapping time ranges - if so, use overlap-safe approach
        has_overlaps = self._has_overlapping_chunks(relevant_chunks)

        if has_overlaps:
            # Warn user about performance impact
            logger.warning(
                "Detected overlapping chunks with random message ordering. "
                "Reading performance is affected because multiple chunks need to be processed to maintain temporal order."
            )
            # Use overlap-safe reading approach
            yield from self._get_messages_with_overlaps(
                relevant_chunks, channel_id, start_timestamp, end_timestamp
            )
        else:
            # Use fast sequential reading approach
            yield from self._get_messages_sequential(
                relevant_chunks, channel_id, start_timestamp, end_timestamp
            )

    def _has_overlapping_chunks(self, chunks: list[ChunkIndexRecord]) -> bool:
        """Check if chunks have overlapping time ranges."""
        if len(chunks) <= 1:
            return False

        # Sort chunks by start time for overlap detection
        sorted_chunks = sorted(chunks, key=lambda x: x.message_start_time)

        for i in range(len(sorted_chunks) - 1):
            current_chunk = sorted_chunks[i]
            next_chunk = sorted_chunks[i + 1]

            # Check if current chunk's end time overlaps with next chunk's start time
            if current_chunk.message_end_time >= next_chunk.message_start_time:
                return True

        return False

    def _get_messages_sequential(
        self,
        chunks: list[ChunkIndexRecord],
        channel_id: int | None,
        start_timestamp: int | None,
        end_timestamp: int | None,
    ) -> Generator[MessageRecord, None, None]:
        """Fast sequential reading for non-overlapping chunks (original implementation)."""
        for chunk_index in chunks:
            if channel_id is None:
                message_indexes = self.get_message_indexes(chunk_index).values()
            elif message_index := self.get_message_index(chunk_index, channel_id):
                message_indexes = [message_index]
            else:
                message_indexes = None

            if not message_indexes:
                continue

            offsets: list[tuple[int, int]] = []
            for message_index in message_indexes:
                for timestamp, offset in message_index.records:
                    if start_timestamp is not None and timestamp < start_timestamp:
                        continue
                    if end_timestamp is not None and timestamp > end_timestamp:
                        continue
                    offsets.append((timestamp, offset))
            if not offsets:
                continue

            chunk = self.get_chunk(chunk_index)
            reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
            for _timestamp, offset in offsets:
                reader.seek_from_start(offset)
                yield McapRecordParser.parse_message(reader)

    def _get_messages_with_overlaps(
        self,
        chunks: list[ChunkIndexRecord],
        channel_id: int | None,
        start_timestamp: int | None,
        end_timestamp: int | None,
    ) -> Generator[MessageRecord, None, None]:
        """Streaming overlap-safe reading using heap-based merge of per-chunk iterators."""
        import heapq
        from typing import Iterator

        def chunk_message_iterator(
            chunk_index: ChunkIndexRecord
        ) -> Iterator[tuple[int, MessageRecord]]:
            """Create an iterator that yields (timestamp, message) tuples for a chunk."""
            if channel_id is None:
                message_indexes = self.get_message_indexes(chunk_index).values()
            elif message_index := self.get_message_index(chunk_index, channel_id):
                message_indexes = [message_index]
            else:
                message_indexes = None

            if not message_indexes:
                return

            # Collect and sort message references for this chunk
            message_refs = []
            for message_index in message_indexes:
                for timestamp, offset in message_index.records:
                    if start_timestamp is not None and timestamp < start_timestamp:
                        continue
                    if end_timestamp is not None and timestamp > end_timestamp:
                        continue
                    message_refs.append((timestamp, offset))

            if not message_refs:
                return

            # Sort by timestamp for this chunk
            message_refs.sort(key=lambda x: x[0])

            # Load the chunk once and parse messages as needed
            chunk = self.get_chunk(chunk_index)
            reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))

            for timestamp, offset in message_refs:
                reader.seek_from_start(offset)
                message = McapRecordParser.parse_message(reader)
                yield timestamp, message

        # Create iterators for each chunk
        chunk_iterators = []
        for i, chunk_index in enumerate(chunks):
            iterator = chunk_message_iterator(chunk_index)
            try:
                timestamp, message = next(iterator)
                # Use (timestamp, iterator_id, message, iterator) to break ties consistently
                heapq.heappush(chunk_iterators, (timestamp, i, message, iterator))
            except StopIteration:
                # Skip empty chunks
                continue

        # Merge iterators using heap for timestamp ordering
        while chunk_iterators:
            timestamp, iterator_id, message, iterator = heapq.heappop(chunk_iterators)

            # Yield the message with the earliest timestamp
            yield message

            # Try to get the next message from this iterator
            try:
                next_timestamp, next_message = next(iterator)
                heapq.heappush(chunk_iterators, (next_timestamp, iterator_id, next_message, iterator))
            except StopIteration:
                # This iterator is exhausted, don't push it back
                continue


    # TODO: Low Priority
    # - Metadata Index
    # - Attachment Index


class McapNonChunkedReader(BaseMcapRecordReader):
    """Class to efficiently get records from an mcap file with no chunks.

    This reader handles MCAP files that don't contain chunks but have a proper
    summary section. It builds an index of message locations during initialization
    to enable efficient random access without loading the entire file into memory.

    Args:
        file: The file to read from.
        check_crc: Whether to validate the crc values in the mcap
    """


    def __init__(self, file: BaseReader, *, check_crc: bool = False):
        self._file = file
        self._check_crc = check_crc

        # Parse file structure
        self._version = McapRecordParser.parse_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        footer = self.get_footer()

        # Summary section start
        self._summary_start = footer.summary_start
        if self._summary_start == 0:
            error_msg = 'No summary section detected in MCAP'
            raise McapNoSummarySectionError(error_msg)

        # Summary offset section start
        self._summary_offset_start = footer.summary_offset_start
        if self._summary_offset_start == 0:
            error_msg = 'No summary offset section detected in MCAP'
            raise McapNoSummaryIndexError(error_msg)

        # Load summary offsets
        Offset = namedtuple('Offset', ['group_start', 'group_length'])
        self._summary_offset: dict[int, Offset] = {}
        self._file.seek_from_start(self._summary_offset_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordParser.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = Offset(record.group_start, record.group_length)

        # Check if this is indeed a non-chunked file
        if McapRecordType.CHUNK_INDEX in self._summary_offset:
            error_msg = 'MCAP file contains chunks, use McapChunkedReader instead'
            raise McapUnexpectedChunkIndexError(error_msg)

        # Cached schema and channel dictionaries populated on first access
        self._schemas: dict[int, SchemaRecord] | None = None
        self._channels: dict[int, ChannelRecord] | None = None

        # Build message index by scanning through the data section
        self._message_index = self._build_message_index()

    def _build_message_index(self) -> dict[int, dict[int, list[int]]]:
        """Build an index of all messages in the file by scanning the data section."""
        logger.debug('Building message index for non-chunked MCAP')

        # Start after header, end before summary section
        self._file.seek_from_start(MAGIC_BYTES_SIZE)
        _ = McapRecordParser.parse_header(self._file)

        message_count = 0
        message_index: dict[int, dict[int, list[int]]] = {}

        while True:
            current_pos = self._file.tell()

            if current_pos >= self._summary_start:
                break  # Stop if we've reached the summary section

            try:
                record_type = McapRecordParser.peek_record(self._file)
                if record_type == 0:  # EOF
                    break

                if record_type == McapRecordType.MESSAGE:
                    message = McapRecordParser.parse_message(self._file)
                    channel_message_indexes = message_index.setdefault(message.channel_id, {})
                    channel_log_time_offsets = channel_message_indexes.setdefault(message.log_time, [])
                    channel_log_time_offsets.append(current_pos)
                    message_count += 1
                else:
                    # Skip non-message records in data section
                    McapRecordParser.skip_record(self._file)
            except Exception as e:
                logger.warning(f'Error parsing record at position {current_pos}: {e}')
                break
        logger.debug(f'Built message index with {message_count} messages across {len(message_index)} channels')

        return message_index

    # Helpful Constructors

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapNonChunkedReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapNonChunkedReader(FileReader(file_path))

    @staticmethod
    def from_bytes(data: bytes) -> 'McapNonChunkedReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapNonChunkedReader(BytesReader(data))

    # Destructors

    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

    # Context Managers

    def __enter__(self) -> 'McapNonChunkedReader':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    # Getters for records

    def get_header(self) -> HeaderRecord:
        """Get the header record from the MCAP file."""
        self._file.seek_from_start(MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_header(self._file)

    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_footer(self._file)

    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        if McapRecordType.STATISTICS not in self._summary_offset:
            raise McapNoStatisticsError('No statistics section detected in MCAP')
        self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS].group_start)
        return McapRecordParser.parse_statistics(self._file)

    # Schema Management

    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        if self._schemas is None:
            if McapRecordType.SCHEMA not in self._summary_offset:
                self._schemas = {}
                return self._schemas

            schemas: dict[int, SchemaRecord] = {}
            self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA].group_start)
            while McapRecordParser.peek_record(self._file) == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(self._file)
                if schema is None:  # Invalid schema, should be ignored
                    continue
                schemas[schema.id] = schema
            self._schemas = schemas
        return self._schemas

    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """
        Get a schema by its ID.

        Args:
            schema_id: The ID of the schema.

        Returns:
            The schema or None if the schema does not exist.
        """
        return self.get_schemas().get(schema_id)

    def get_channel_schema(self, channel_id: int) -> SchemaRecord | None:
        """
        Get the schema for a given channel ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The schema of the channel or None if the channel/schema does not exist.
        """
        channel = self.get_channel(channel_id)
        if channel is None:
            return None
        return self.get_schema(channel.schema_id)

    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        """
        Get the schema for a given message.

        Args:
            message: The message to get the schema for.

        Returns:
            The schema for the message.
        """
        schema = self.get_channel_schema(message.channel_id)
        if schema is None:
            raise McapUnknownSchemaError(f'Unknown schema for channel {message.channel_id}')
        return schema

    # Channel Management

    def get_channels(self) -> dict[int, ChannelRecord]:
        """
        Get all channels/topics in the MCAP file.

        Returns:
            A dictionary mapping channel IDs to channel information.
        """
        if self._channels is None:
            if McapRecordType.CHANNEL not in self._summary_offset:
                self._channels = {}
                return self._channels

            channels: dict[int, ChannelRecord] = {}
            self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL].group_start)
            while McapRecordParser.peek_record(self._file) == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
            self._channels = channels
        return self._channels

    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        """
        Get channel information by its ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The channel information or None if the channel does not exist.
        """
        return self.get_channels().get(channel_id)

    def get_channel_id(self, topic: str) -> int | None:
        """Get a channel ID by its topic."""
        for channel in self.get_channels().values():
            if channel.topic == topic:
                return channel.id
        return None

    # Message Index Management (placeholders for compatibility)

    def get_message_indexes(self, chunk_index: ChunkIndexRecord) -> dict[int, MessageIndexRecord]:
        """
        Get all message indexes from the MCAP file.

        Note: Non-chunked files don't have chunk indexes or message indexes.
        This method is provided for interface compatibility.
        """
        return {}

    def get_message_index(self, chunk_index: ChunkIndexRecord, channel_id: int) -> MessageIndexRecord | None:
        """
        Get a message index for a given channel ID.

        Note: Non-chunked files don't have chunk indexes or message indexes.
        This method is provided for interface compatibility.
        """
        return None

    # Chunk Management (placeholders for compatibility)

    def get_chunk_indexes(self, channel_id: int | None = None) -> list[ChunkIndexRecord]:
        """
        Get all chunk indexes from the MCAP file.

        Note: Non-chunked files don't have chunks.
        This method is provided for interface compatibility.
        """
        return []

    def get_chunk(self, chunk_index: ChunkIndexRecord) -> ChunkRecord:
        """
        Get a chunk by its index.

        Note: Non-chunked files don't have chunks.
        This method is provided for interface compatibility.
        """
        raise McapNoChunkError('Non-chunked MCAP files do not have chunks')

    # Message Management

    def get_message(
        self,
        channel_id: int,
        timestamp: int | None = None,
    ) -> MessageRecord | None:
        """
        Get a message from a given channel at a given timestamp.

        If the timestamp is not provided, the first message in the channel is returned.

        Args:
            channel_id: The ID of the channel.
            timestamp: The timestamp of the message.

        Returns:
            A MessageRecord object or None if the message does not exist.
        """
        if channel_id not in self._message_index:
            return None

        messages = self._message_index.get(channel_id)
        if messages is None:
            logger.warning('Channel ID not in MCAP!')
            return None

        if timestamp is None:  # Return first
            first_time = min(list(messages.keys()))
            offsets = messages[first_time]
        else:
            # Find exact timestamp match
            offsets = [offset for ts, offset in messages.items() if ts == timestamp]
            offsets = [o for offset in offsets for o in offset]  # unpack list
            if not offsets:
                logger.warning(f'No message with time {timestamp} found')
                return None
            if len(offsets) > 1:
                logger.warning('Multiple records with the same log time found, choosing one.')

        # Read message from file
        self._file.seek_from_start(offsets[0])
        return McapRecordParser.parse_message(self._file)

    def get_messages(
        self,
        channel_id: int | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        """
        Get messages from the MCAP file.

        If no channel is provided, messages from all channels are returned.
        If the start and end timestamps are not provided, the entire available range is returned.

        Args:
            channel_id: Optional channel ID to filter by. If None, all channels are included.
            start_timestamp: The start timestamp to filter by. If None, no filtering is done.
            end_timestamp: The end timestamp to filter by. If None, no filtering is done.

        Returns:
            A generator of MessageRecord objects.
        """
        # Determine which channels to process
        if channel_id is not None:
            if channel_id not in self._message_index:
                logger.warning('Channel ID not in MCAP!')
                return
            channels_to_process = [channel_id]
        else:
            channels_to_process = list(self._message_index.keys())
        logger.debug(f'Channels requested: {channels_to_process}')

        # Collect all matching message offsets with timestamps
        message_offsets: list[tuple[int, list[int]]] = []
        for cid in channels_to_process:
            logger.debug(f'{len(self._message_index[cid])} messages for channel {cid}')
            for timestamp, offset in self._message_index[cid].items():
                # Apply timestamp filtering
                if start_timestamp is not None and timestamp < start_timestamp:
                    continue
                if end_timestamp is not None and timestamp > end_timestamp:
                    continue
                message_offsets.append((timestamp, offset))

        # Sort by timestamp to return messages in chronological order
        message_offsets.sort(key=lambda x: x[0])
        logger.debug(f'Found {len(message_offsets)} messages')

        # Yield messages
        for _, offsets in message_offsets:
            for offset in offsets:
                self._file.seek_from_start(offset)
                yield McapRecordParser.parse_message(self._file)

    # TODO: Low Priority
    # - Metadata Index
    # - Attachment Index


class McapRecordReaderFactory:
    """Factory to create a McapFileSequentialReader or McapFileRandomAccessReader."""

    @staticmethod
    def from_file(file_path: Path | str) -> BaseMcapRecordReader:
        """Create a new MCAP reader from a file."""
        try:
            # Try to create a chunked reader first
            return McapChunkedReader.from_file(file_path)
        except McapNoChunkIndexError:
            # If no chunks exist, use the non-chunked reader
            # TODO: Handle chunked MCAP files that lack chunk indexes by decoding CHUNK records directly.
            logger.warning('No chunk indexes detected, using non-chunked reader')
            return McapNonChunkedReader.from_file(file_path)
        except McapNoSummarySectionError:
            # If no summary section exists, fall back to sequential reader
            # TODO: Implement the sequential reader
            logger.warning('No summary section exists in MCAP, falling back to sequential reader')
            raise NotImplementedError('Sequential readers are not implemented yet')
        except McapNoSummaryIndexError:
            # If no summary index exists, fall back to sequential reader
            # TODO: Use sequential reader? Or just create the index?
            logger.warning('No summary index exists in MCAP, falling back to sequential reader')
            raise NotImplementedError('Sequential readers are not implemented yet')

    @staticmethod
    def from_bytes(data: bytes) -> BaseMcapRecordReader:
        """Create a new MCAP reader from a bytes object."""
        try:
            # Try to create a chunked reader first
            return McapChunkedReader.from_bytes(data)
        except McapNoChunkIndexError:
            # If no chunks exist, use the non-chunked reader
            logger.warning('No chunk indexes detected, using non-chunked reader')
            return McapNonChunkedReader.from_bytes(data)
        except McapNoSummarySectionError:
            # If no summary section exists, fall back to sequential reader
            # TODO: Implement the sequential reader
            logger.warning('No summary section exists in MCAP, falling back to sequential reader')
            raise NotImplementedError('Sequential readers are not implemented yet')
        except McapNoSummaryIndexError:
            # If no summary index exists, fall back to sequential reader
            # TODO: Use sequential reader? Or just create the index?
            logger.warning('No summary index exists in MCAP, falling back to sequential reader')
            raise NotImplementedError('Sequential readers are not implemented yet')
