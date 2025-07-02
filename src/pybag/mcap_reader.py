import logging
from pathlib import Path
from collections import namedtuple
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from pybag.mcap.records import (
    StatisticsRecord,
    SchemaRecord,
    ChannelRecord,
    FooterRecord,
    ChunkIndexRecord,
    MessageIndexRecord,
    MessageRecord,
    ChunkRecord,
)
from pybag.schema.ros2msg import parse_ros2msg, Ros2MsgFieldType
from pybag.encoding.cdr import CdrParser
from pybag.io.raw_reader import BaseReader, FileReader, BytesReader
from pybag.mcap.record_reader import (
    McapRecordReader,
    MAGIC_BYTES_SIZE,
    FOOTER_SIZE,
    McapRecordType,
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


class McapNoSummarySectionError(Exception):
    """Exception raised when a MCAP file has no summary section."""
    def __init__(self, message: str):
        super().__init__(message)


class McapNoSummaryIndexError(Exception):
    """Exception raised when a MCAP file has no summary index."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownCompressionError(Exception):
    """Exception raised when a MCAP file has an unknown compression type."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownEncodingError(Exception):
    """Exception raised when a MCAP file has an unknown encoding type."""
    def __init__(self, message: str):
        super().__init__(message)


def decompress_chunk(chunk: ChunkRecord) -> bytes:
    """Decompress the records field of a chunk."""
    if chunk.compression == 'zstd':
        import zstandard as zstd
        return zstd.ZstdDecompressor().decompress(chunk.records)
    elif chunk.compression == 'lz4':
        import lz4.frame
        return lz4.frame.decompress(chunk.records)
    elif chunk.compression == '':
        return chunk.records
    else:
        error_msg = f'Unknown compression type: {chunk.compression}'
        raise McapUnknownCompressionError(error_msg)


def decode_message(message: MessageRecord, schema: SchemaRecord) -> dict:
    """Decode a message using a schema."""
    # TODO: Support other encodings (e.g. ROS 1)
    if schema.encoding != 'ros2msg':
        error_msg = f'Unknown encoding type: {schema.encoding}'
        raise McapUnknownEncodingError(error_msg)

    cdr = CdrParser(message.data)
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


class McapFileRandomAccessReader:
    """Class to efficiently get records from an MCAP file."""

    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP reader.

        Args:
            file: The file to read from.
        """
        self._file = file
        self._version = McapRecordReader.read_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        # Check magic bytes at the end of the file
        self._file.seek_from_end(MAGIC_BYTES_SIZE)
        mcap_version = McapRecordReader.read_magic_bytes(self._file)
        assert self._version == mcap_version

        # Check footer at the end of the file
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
        self._summary_offset = {}
        Offset = namedtuple('Offset', ['group_start', 'group_length'])
        self._file.seek_from_start(self._summary_offset_start)
        while McapRecordReader.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordReader.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = Offset(record.group_start, record.group_length)

    # Helpful Constructors

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapFileRandomAccessReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapFileRandomAccessReader(FileReader(file_path))


    @staticmethod
    def from_bytes(data: bytes) -> 'McapFileRandomAccessReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapFileRandomAccessReader(BytesReader(data))

    # Destructors

    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

    # Statistics Management

    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordReader.parse_footer(self._file)

    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS].group_start)
        return McapRecordReader.parse_statistics(self._file)

    def get_start_time(self) -> int:
        """
        Get the start time of the MCAP file in nanoseconds since epoch.
        """
        return self.get_statistics().message_start_time

    def get_end_time(self) -> int:
        """
        Get the end time of the MCAP file in nanoseconds since epoch.
        """
        return self.get_statistics().message_end_time

    def get_message_counts(self) -> dict[int, int]:
        """
        Get the number of messages in the MCAP file for each channel.

        Returns:
            A dictionary mapping channel IDs to the number of messages.
        """
        return self.get_statistics().channel_message_counts

    # Schema Management

    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA].group_start)
        schemas = {}
        while McapRecordReader.peek_record(self._file) == McapRecordType.SCHEMA:
            schema = McapRecordReader.parse_schema(self._file)
            schemas[schema.id] = schema
        return schemas

    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """
        Get a schema by its ID.

        Args:
            schema_id: The ID of the schema.

        Returns:
            The schema or None if the schema does not exist.
        """
        return self.get_schemas().get(schema_id)

    # Channel Management

    def get_channel_id(self, topic: str) -> int | None:
        """
        Get the channel ID for a given topic.
        """
        return next((c.id for c in self.get_channels().values() if c.topic == topic), None)

    def get_channels(self) -> dict[int, ChannelRecord]:
        """
        Get all channels/topics in the MCAP file.

        Returns:
            A dictionary mapping channel IDs to channel information.
        """
        self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL].group_start)
        channels = {}
        while McapRecordReader.peek_record(self._file) == McapRecordType.CHANNEL:
            channel = McapRecordReader.parse_channel(self._file)
            channels[channel.id] = channel
        return channels

    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        """
        Get channel information by its ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The channel information or None if the channel does not exist.
        """
        return self.get_channels().get(channel_id)

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

    # Index Management

    def get_chunk_indexes(self, channel_id: int | None = None) -> list[ChunkIndexRecord]:
        """
        Get all chunk indexes from the MCAP file.

        Args:
            channel_id: The ID of the channel to get the chunk indexes for. If None, all chunk indexes are returned.

        Returns:
            A list of ChunkIndexRecord objects.
        """
        self._file.seek_from_start(self._summary_offset[McapRecordType.CHUNK_INDEX].group_start)
        chunk_indexes = []
        while McapRecordReader.peek_record(self._file) == McapRecordType.CHUNK_INDEX:
            chunk_index = McapRecordReader.parse_chunk_index(self._file)
            if channel_id is None or channel_id in chunk_index.message_index_offsets:
                chunk_indexes.append(chunk_index)
        return chunk_indexes

    def get_message_indexes(self, chunk_index: ChunkIndexRecord) -> dict[int, MessageIndexRecord]:
        """
        Get all message indexes from the MCAP file.

        Args:
            chunk_index: The chunk index to get the message indexes from.

        Returns:
            A list of MessageIndexRecord objects.
        """
        message_index = {}
        for channel_id, message_index_offset in chunk_index.message_index_offsets.items():
            self._file.seek_from_start(message_index_offset)
            message_index[channel_id] = McapRecordReader.parse_message_index(self._file)
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

    def get_chunk(self, chunk_index: ChunkIndexRecord) -> ChunkRecord:
        """
        Get a chunk by its index.

        Args:
            chunk_index: The chunk index to get the chunk from.

        Returns:
            A ChunkRecord object.
        """
        self._file.seek_from_start(chunk_index.chunk_start_offset)
        return McapRecordReader.parse_chunk(self._file)

    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        """
        Get the schema for a given message.

        Args:
            message: The message to get the schema for.

        Returns:
            The schema for the message.
        """
        return self.get_channel_schema(message.channel_id)

    # TODO: Low Priority
    # - Metadata Index
    # - Attachment Index

    # Message Access

    def get_topic_message(
        self,
        channel_id: int,
        timestamp: int,
    ) -> MessageRecord | None:
        """
        Get a message from a given channel in a given chunk at a given timestamp.

        Args:
            chunk_index: The chunk index to get the message from.
            channel_id: The ID of the channel.
            timestamp: The timestamp of the message.

        Returns:
            A MessageRecord object or None if the message does not exist.
        """
        chunk_indexes = self.get_chunk_indexes(channel_id)
        for chunk_index in chunk_indexes:
            if chunk_index.message_start_time <= timestamp <= chunk_index.message_end_time:
                # The message must be in the chunk based on the start and end times
                message_index = self.get_message_index(chunk_index, channel_id)
                if message_index is None:
                    return None

                # Make sure the timestamp is in the message index
                sorted_records = sorted(message_index.records, key=lambda x: x[0])
                offset = next((r[1] for r in sorted_records if r[0] == timestamp), None)
                if offset is None:
                    return None

                # Read data from chunk
                chunk = self.get_chunk(chunk_index)
                reader = BytesReader(decompress_chunk(chunk))
                reader.seek_from_start(offset)
                return McapRecordReader.parse_message(reader)
        return None

    def get_topic_messages(
        self,
        channel_id: int,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        """
        Get all messages from a given channel in a given chunk.

        Args:
            chunk_index: The chunk index to get the messages from.
            channel_id: The ID of the channel.
            start_timestamp: The start timestamp to filter by. If None, no filtering is done.
            end_timestamp: The end timestamp to filter by. If None, no filtering is done.

        Returns:
            A generator of MessageRecord objects.
        """
        chunk_indexes = self.get_chunk_indexes(channel_id)
        for chunk_index in sorted(chunk_indexes, key=lambda x: x.message_start_time):
            # Skip chunk that do not match the timestamp range
            if start_timestamp is not None and chunk_index.message_end_time < start_timestamp:
                continue
            if end_timestamp is not None and chunk_index.message_start_time > end_timestamp:
                continue

            # Get the message index for the chunk
            message_index = self.get_message_index(chunk_index, channel_id)
            if message_index is None:
                continue

            # Read all messages in the chunk
            chunk = self.get_chunk(chunk_index)
            reader = BytesReader(decompress_chunk(chunk))
            for timestamp, offset in sorted(message_index.records, key=lambda x: x[0]):
                # Skip messages that do not match the timestamp range
                if start_timestamp is not None and timestamp < start_timestamp:
                    continue
                if end_timestamp is not None and timestamp > end_timestamp:
                    continue

                # Read the message
                reader.seek_from_start(offset)
                yield McapRecordReader.parse_message(reader)


class McapFileSequentialReader:
    """Class to read messages from an MCAP file sequentially."""

    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP file sequential reader.
        """
        logger.warning("Using SequentialReader, operations will be slow")

        self._file = file
        self._version = McapRecordReader.read_magic_bytes(self._file)
        logger.debug(f"MCAP version: {self._version}")

        # Read the footer for statistics and boundary information
        self._file.seek_from_end(MAGIC_BYTES_SIZE)
        mcap_version = McapRecordReader.read_magic_bytes(self._file)
        assert self._version == mcap_version

        # Caches for expensive operations
        self._schemas_cache: dict[int, SchemaRecord] | None = None
        self._channels_cache: dict[int, ChannelRecord] | None = None
        self._stats_cache: StatisticsRecord | None = None

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapFileSequentialReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapFileSequentialReader(FileReader(file_path))

    @staticmethod
    def from_bytes(data: bytes) -> 'McapFileSequentialReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapFileSequentialReader(BytesReader(data))

    # ---------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------

    def _iterate_data_records(self) -> Generator[tuple[int, Any], None, None]:
        """Iterate over records in the data section of the file."""
        # Start after the magic bytes and header
        self._file.seek_from_start(MAGIC_BYTES_SIZE)
        _ = McapRecordReader.parse_header(self._file)

        while (record_type := McapRecordReader.peek_record(self._file)) != McapRecordType.DATA_END:
            yield record_type, McapRecordReader._parse_record(record_type, self._file)

    def _iterate_records(self) -> Generator[tuple[int, Any], None, None]:
        """Iterate over all records, decompressing chunks as needed."""
        for record_type, record in self._iterate_data_records():
            if record_type == McapRecordType.CHUNK:
                reader = BytesReader(decompress_chunk(record))
                for inner_type, inner_record in McapRecordReader.read_record(reader):
                    yield inner_type, inner_record
            else:
                yield record_type, record

    def _iterate_messages_from_chunk(
        self, chunk: ChunkRecord
    ) -> Generator[MessageRecord, None, None]:
        reader = BytesReader(decompress_chunk(chunk))
        for r_type, r in McapRecordReader.read_record(reader):
            if r_type == McapRecordType.MESSAGE:
                yield r

    def _iterate_messages(
        self,
        channel_id: int | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        """Iterate over message records with optional filtering."""
        for record_type, record in self._iterate_data_records():
            if record_type == McapRecordType.MESSAGE:
                if channel_id is not None and record.channel_id != channel_id:
                    continue
                if start_timestamp is not None and record.log_time < start_timestamp:
                    continue
                if end_timestamp is not None and record.log_time > end_timestamp:
                    continue
                yield record
            elif record_type == McapRecordType.CHUNK:
                if start_timestamp is not None and record.message_end_time < start_timestamp:
                    continue
                if end_timestamp is not None and record.message_start_time > end_timestamp:
                    continue
                for msg in self._iterate_messages_from_chunk(record):
                    if channel_id is not None and msg.channel_id != channel_id:
                        continue
                    if start_timestamp is not None and msg.log_time < start_timestamp:
                        continue
                    if end_timestamp is not None and msg.log_time > end_timestamp:
                        continue
                    yield msg

    # Resource management

    def close(self) -> None:
        self._file.close()

    def _build_cache(self) -> None:
        # Statistics
        message_start_time = None
        message_end_time = None
        channel_message_counts: dict[int, int] = {}
        schema_ids: set[int] = set()
        channel_ids: set[int] = set()
        attachment_count = 0
        metadata_count = 0
        chunk_count = 0

        # Iterate over all records to build the cache
        for record_type, record in self._iterate_records():
            if record_type == McapRecordType.SCHEMA:
                schema_ids.add(record.id)
                self._schemas_cache[record.id] = record
            elif record_type == McapRecordType.CHANNEL:
                channel_ids.add(record.id)
                self._channels_cache[record.id] = record
            elif record_type == McapRecordType.ATTACHMENT:
                attachment_count += 1
            elif record_type == McapRecordType.METADATA:
                metadata_count += 1
            elif record_type == McapRecordType.CHUNK:
                chunk_count += 1
            elif record_type == McapRecordType.MESSAGE:
                channel_message_counts[record.channel_id] = (
                    channel_message_counts.get(record.channel_id, 0) + 1
                )
                message_start_time = (
                    record.log_time
                    if message_start_time is None
                    else min(message_start_time, record.log_time)
                )
                message_end_time = (
                    record.log_time
                    if message_end_time is None
                    else max(message_end_time, record.log_time)
                )

        message_count = sum(channel_message_counts.values())
        self._stats_cache = StatisticsRecord(
            message_count,
            len(schema_ids),
            len(channel_ids),
            attachment_count,
            metadata_count,
            chunk_count,
            message_start_time or 0,
            message_end_time or 0,
            channel_message_counts,
        )

    # Statistics

    def get_statistics(self) -> StatisticsRecord:
        if self._stats_cache is None:
            self._stats_cache = self._build_cache()
        return self._stats_cache

    def get_start_time(self) -> int:
        return self.get_statistics().message_start_time

    def get_end_time(self) -> int:
        return self.get_statistics().message_end_time

    def get_message_counts(self) -> dict[int, int]:
        return self.get_statistics().channel_message_counts

    # Schema / channel helpers

    def get_schemas(self) -> dict[int, SchemaRecord]:
        if self._schemas_cache is None:
            self._schemas_cache = self._build_cache()
        return self._schemas_cache

    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        return self.get_schemas().get(schema_id)

    def get_channels(self) -> dict[int, ChannelRecord]:
        if self._channels_cache is None:
            self._channels_cache = self._build_cache()
        return self._channels_cache

    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        return self.get_channels().get(channel_id)

    def get_channel_id(self, topic: str) -> int | None:
        return next((c.id for c in self.get_channels().values() if c.topic == topic), None)

    def get_channel_schema(self, channel_id: int) -> SchemaRecord | None:
        channel = self.get_channel(channel_id)
        if channel is None:
            return None
        return self.get_schema(channel.schema_id)

    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        return self.get_channel_schema(message.channel_id)

    # Message helpers

    def get_topic_message(
        self,
        channel_id: int,
        timestamp: int,
    ) -> MessageRecord | None:
        for msg in self._iterate_messages(channel_id, timestamp, timestamp):
            if msg.log_time == timestamp:
                return msg
        return None

    def get_topic_messages(
        self,
        channel_id: int,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
    ) -> Generator[MessageRecord, None, None]:
        yield from self._iterate_messages(channel_id, start_timestamp, end_timestamp)


class McapFileReaderFactory:
    """Factory to create a McapFileSequentialReader or McapFileRandomAccessReader."""

    @staticmethod
    def from_file(file_path: Path | str) -> McapFileSequentialReader | McapFileRandomAccessReader:
        """Create a new MCAP reader from a file."""
        try:
            # Try to create a random access reader first
            return McapFileRandomAccessReader.from_file(file_path)
        except McapNoSummarySectionError:
            # If no summary section exists, fall back to sequential reader
            logger.warning('No summary section exists in MCAP, falling back to sequential reader')
            return McapFileSequentialReader.from_file(file_path)
        except McapNoSummaryIndexError:
            # If no summary index exists, fall back to sequential reader
            logger.warning('No summary index exists in MCAP, falling back to sequential reader')
            return McapFileSequentialReader.from_file(file_path)

    @staticmethod
    def from_bytes(data: bytes) -> McapFileSequentialReader | McapFileRandomAccessReader:
        """Create a new MCAP reader from a bytes object."""
        try:
            # Try to create a random access reader first
            return McapFileRandomAccessReader.from_bytes(data)
        except McapNoSummarySectionError:
            # If no summary section exists, fall back to sequential reader
            logger.warning('No summary section exists in MCAP, falling back to sequential reader')
            return McapFileSequentialReader.from_bytes(data)
        except McapNoSummaryIndexError:
            # If no summary index exists, fall back to sequential reader
            logger.warning('No summary index exists in MCAP, falling back to sequential reader')
            return McapFileSequentialReader.from_bytes(data)


class McapIndexBuilder:
    """Class to build an index for an MCAP file."""

    @staticmethod
    def build(reader: McapFileSequentialReader) -> McapFileRandomAccessReader:
        """Build an index for the MCAP file."""
        # TODO: Implement
        pass


class McapReader:
    """High-level Mcap reader class that exposes an opinionated interface."""

    # TODO: Create base class for sequential and random access readers
    def __init__(self, reader: McapFileSequentialReader | McapFileRandomAccessReader):
        """
        Initialize the MCAP reader.

        Args:
            reader: The reader to read from.
        """
        self._reader = reader

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapReader(McapFileReaderFactory.from_file(file_path))

    @staticmethod
    def from_bytes(data: bytes) -> 'McapReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapReader(McapFileReaderFactory.from_bytes(data))

    def get_topics(self) -> list[str]:
        """Get all topics in the MCAP file."""
        return [c.topic for c in self._reader.get_channels().values()]

    def get_start_time(self) -> float:
        """Get the start time of the MCAP file."""
        return self._reader.get_start_time()

    def get_end_time(self) -> float:
        """Get the end time of the MCAP file."""
        return self._reader.get_end_time()

    def get_message_counts(self) -> dict[int, int]:
        """Get the number of messages in the MCAP file for each topic."""
        return self._reader.get_message_counts()

    # TODO: Make interface nicer

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
        for message in self._reader.get_topic_messages(channel_id, start_time, end_time):
            yield DecodedMessage(
                message.channel_id,
                message.sequence,
                message.log_time,
                message.publish_time,
                decode_message(message, self._reader.get_message_schema(message))
            )


if __name__ == '__main__':
    import json
    reader = McapReader.from_file(Path('/pybag/mcaps/pose_with_covariance.mcap'))
    for msg in reader.messages(topic='/pose_with_covariance'):
        print(json.dumps(msg.data, indent=4, sort_keys=True))
