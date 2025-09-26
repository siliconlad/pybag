import logging
from abc import ABC, abstractmethod
from collections import namedtuple
from pathlib import Path
from typing import Generator

from pybag.crc import assert_crc
from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.mcap.error import (
    McapNoStatisticsError,
    McapNoSummaryIndexError,
    McapNoSummarySectionError,
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


class McapRecordRandomAccessReader(BaseMcapRecordReader):
    """Class to efficiently get records from an MCAP file.

    Args:
        file: The file to read from.
    """

    def __init__(self, file: BaseReader, *, check_crc: bool = False):
        self._file = file
        self._check_crc = check_crc

        self._version = McapRecordParser.parse_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        footer = self.get_footer()

        # Summary section start
        self._summary_start = footer.summary_start
        logger.debug(f'Summary start: {self._summary_start}')
        if self._summary_start == 0:
            error_msg = 'No summary section detected in MCAP'
            raise McapNoSummarySectionError(error_msg)

        # Summary offset section start
        self._summary_offset_start = footer.summary_offset_start
        logger.debug(f'Summary offset start: {self._summary_offset_start}')
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

        # Caches for chunk and message indexes
        self._chunk_indexes: list[ChunkIndexRecord] | None = None
        self._message_indexes: dict[int, dict[int, MessageIndexRecord]] = {}

        # Cached schema and channel dictionaries populated on first access
        self._schemas: dict[int, SchemaRecord] | None = None
        self._channels: dict[int, ChannelRecord] | None = None

    # Helpful Constructors

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapRecordRandomAccessReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapRecordRandomAccessReader(FileReader(file_path))

    @staticmethod
    def from_bytes(data: bytes) -> 'McapRecordRandomAccessReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapRecordRandomAccessReader(BytesReader(data))

    # Destructors

    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

    # Context Managers

    def __enter__(self) -> 'McapRecordRandomAccessReader':
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

        self._file.seek_from_start(self._summary_offset[McapRecordType.CHUNK_INDEX].group_start)
        self._chunk_indexes = []
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
        for chunk_index in self.get_chunk_indexes(channel_id):
            # Skip chunk that do not match the timestamp range
            if start_timestamp is not None and chunk_index.message_end_time < start_timestamp:
                continue
            if end_timestamp is not None and chunk_index.message_start_time > end_timestamp:
                continue

            if channel_id is None:
                message_indexes = self.get_message_indexes(chunk_index).values()
            else:
                message_indexes = [self.get_message_index(chunk_index, channel_id)]
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

    # TODO: Low Priority
    # - Metadata Index
    # - Attachment Index


class McapRecordReaderFactory:
    """Factory to create a McapFileSequentialReader or McapFileRandomAccessReader."""

    @staticmethod
    def from_file(file_path: Path | str) -> McapRecordRandomAccessReader:
        """Create a new MCAP reader from a file."""
        try:
            # Try to create a random access reader first
            return McapRecordRandomAccessReader.from_file(file_path)
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
    def from_bytes(data: bytes) -> McapRecordRandomAccessReader:
        """Create a new MCAP reader from a bytes object."""
        try:
            # Try to create a random access reader first
            return McapRecordRandomAccessReader.from_bytes(data)
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
