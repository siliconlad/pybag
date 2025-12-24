import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Literal, TypeAlias

from pybag.io.raw_reader import BaseReader, BytesReader
from pybag.io.raw_writer import CrcWriter
from pybag.mcap.chunk import decompress_chunk
from pybag.mcap.crc import assert_data_crc, assert_summary_crc
from pybag.mcap.error import McapNoChunkIndexError, McapNoSummarySectionError
from pybag.mcap.record_encoder import McapRecordWriter
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser,
    McapRecordType
)
from pybag.mcap.records import (
    AttachmentIndexRecord,
    ChannelRecord,
    ChunkIndexRecord,
    DataEndRecord,
    FooterRecord,
    MessageIndexRecord,
    MessageRecord,
    MetadataIndexRecord,
    RecordType,
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)
from pybag.types import Message

ChannelId: TypeAlias = int
"""Integer representing the channel ID."""

SchemaId: TypeAlias = int
"""Integer representing the schema ID."""

RecordId: TypeAlias = int
"""ID of an MCAP record (i.e. the record type)."""

LogTime: TypeAlias = int
"""Integer representing the log time."""

Offset: TypeAlias = int
"""Integer representing an offset from the start of a file/bytes."""

# Footer payload size: 8 bytes summary_start + 8 bytes summary_offset_start + 4 bytes summary_crc
FOOTER_PAYLOAD_SIZE = 20

# TODO: Summary should load enough to work without reading summary section again

class McapSummary(ABC):
    @abstractmethod
    def next_schema_id(self) -> SchemaId:
        ...  # pragma: no cover

    @abstractmethod
    def next_channel_id(self) -> ChannelId:
        ...  # pragma: no cover

    @abstractmethod
    def next_sequence_id(self, channel_id: ChannelId) -> int:
        ...  # pragma: no cover

    @abstractmethod
    def get_channel_id(self, topic: str) -> ChannelId | None:
        ...  # pragma: no cover

    @abstractmethod
    def get_schema_id(self, message: type[Message]) -> SchemaId | None:
        ...  # pragma: no cover

    @abstractmethod
    def get_schemas(self) -> dict[SchemaId, SchemaRecord]:
        ...  # pragma: no cover

    @abstractmethod
    def get_channels(self) -> dict[ChannelId, ChannelRecord]:
        ...  # pragma: no cover

    @abstractmethod
    def get_statistics(self) -> StatisticsRecord | None:
        ...  # pragma: no cover

    @abstractmethod
    def get_chunk_indexes(self) -> list[ChunkIndexRecord]:
        ...  # pragma: no cover

    @abstractmethod
    def get_message_indexes(
        self,
        chunk_index: ChunkIndexRecord,
    ) -> dict[ChannelId, MessageIndexRecord]:
        ...  # pragma: no cover

    @abstractmethod
    def get_attachment_indexes(self) -> dict[str, list[AttachmentIndexRecord]]:
        ...  # pragma: no cover

    @abstractmethod
    def get_metadata_indexes(self) -> dict[str, list[MetadataIndexRecord]]:
        ...  # pragma: no cover

    @abstractmethod
    def add_schema(self, schema: SchemaRecord):
        ...  # pragma: no cover

    @abstractmethod
    def add_channel(self, channel: ChannelRecord):
        ...  # pragma: no cover

    @abstractmethod
    def add_message(self, message: MessageRecord):
        ...  # pragma: no cover

    @abstractmethod
    def add_attachment_index(self, attachment_index: AttachmentIndexRecord):
        ...  # pragma: no cover

    @abstractmethod
    def add_metadata_index(self, metadata_index: MetadataIndexRecord):
        ...  # pragma: no cover

    @abstractmethod
    def write_summary(self, writer: CrcWriter):
        ...  # pragma: no cover


class McapChunkedSummary(McapSummary):
    """Summary information for a chunked MCAP file.

    This class handles loading summary information from an MCAP file, either from
    existing summary sections or by reconstructing it from the data section.

    Args:
        file: The file reader to read from.
        enable_crc_check: Whether to validate the CRC checksums
        enable_reconstruction: Controls reconstruction behavior:
            - 'never': Raise error if summary sections are missing
            - 'missing': Load from summary if present, otherwise reconstruct
            - 'always': Always reconstruct even if summary exists
    """

    def __init__(
        self,
        file: BaseReader | None = None,
        *,
        enable_crc_check: bool = False,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        load_summary_eagerly: bool = True,
    ) -> None:
        logging.debug('Creating McapChunkedSummary')
        self._file = file
        self._enable_reconstruction = enable_reconstruction
        self._check_crc = enable_crc_check
        self._load_summary_eagerly = load_summary_eagerly

        # Initialize cache variables
        self._summary_offset: dict[RecordId, Offset] = {}
        self._cached_schemas: dict[SchemaId, SchemaRecord] = {}
        self._cached_channels: dict[ChannelId, ChannelRecord] = {}
        self._cached_chunk_indexes: list[ChunkIndexRecord] = []
        self._cached_metadata_indexes: dict[str, list[MetadataIndexRecord]] = defaultdict(list)
        self._cached_attachment_indexes: dict[str, list[AttachmentIndexRecord]] = defaultdict(list)
        self._message_indexes: dict[Offset, dict[ChannelId, MessageIndexRecord]] = {}
        # If self._file is None, then we start a new summary and so we can track stats from the beginning
        self._cached_statistics: StatisticsRecord | None = StatisticsRecord() if self._file is None else None

        # If we have no file, then we have the summary built (i.e. nothing)
        self._has_built_summary = self._file is None

        # Keep track of what we have searched for already
        self._has_searched_schemas = False
        self._has_searched_channels = False
        self._has_searched_chunks = False
        self._has_searched_attachments = False
        self._has_searched_metadata = False

        # If no file, then we are creating a new summary
        if self._file is None:
            logging.debug('Summary has no file to search')
            return

        # Read footer to determine if summary sections exist
        _ = self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        self._footer = McapRecordParser.parse_footer(self._file)

        self._has_summary = self._footer.summary_start != 0
        self._has_summary_offset = self._footer.summary_offset_start != 0

        # Validate CRCs if requested
        if enable_crc_check:
            assert_data_crc(self._file, self._footer)
            assert_summary_crc(self._file, self._footer)

        if enable_reconstruction == 'never':
            if not self._has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)

        if enable_reconstruction == 'always' or not self._has_summary:
            logging.debug('Building summary from data section')
            self._build_summary()
            self._has_built_summary = True
        else:
            # TODO: What do we do if the summary is incomplete?
            logging.debug('Loading summary from summary section')
            self._load_summary()
            if self._cached_statistics is None and self._load_summary_eagerly:
                logging.warning('Statistics record not found in summary, generating')
                self._cached_statistics = self.get_statistics()

        # After loading summary, if we have records, then we assume they are complete
        # and we disable searching through the file for more records of the same type
        self._has_searched_schemas = len(self._cached_schemas) > 0
        self._has_searched_channels = len(self._cached_channels) > 0
        self._has_searched_chunks = len(self._cached_chunk_indexes) > 0
        self._has_searched_attachments = len(self._cached_attachment_indexes) > 0
        self._has_searched_metadata = len(self._cached_metadata_indexes) > 0

        # If not chunk index records are found, then we cannot use this class
        # TODO: For MCAP files that do not have chunks, this could be slow
        if not self.get_chunk_indexes() and self._file:
            raise McapNoChunkIndexError("No ChunkIndex records found!")

    def _load_summary(self) -> None:
        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Load summary: file not initialized!')
            return None

        # Seek to start of summary section
        _ = self._file.seek_from_start(self._footer.summary_start)

        # Iterate through records until we reach the footer
        while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.FOOTER:
            # Exit when we reach the end of the summary section
            if record_type == McapRecordType.SUMMARY_OFFSET:
                break

            if record_type == McapRecordType.SCHEMA:
                if schema := McapRecordParser.parse_schema(self._file):
                    self._cached_schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                self._cached_channels[channel.id] = channel
            elif record_type == McapRecordType.ATTACHMENT_INDEX:
                attachment_index = McapRecordParser.parse_attachment_index(self._file)
                self._cached_attachment_indexes[attachment_index.name].append(attachment_index)
            elif record_type == McapRecordType.METADATA_INDEX:
                metadata_index = McapRecordParser.parse_metadata_index(self._file)
                self._cached_metadata_indexes[metadata_index.name].append(metadata_index)
            elif record_type == McapRecordType.STATISTICS:
                self._cached_statistics = McapRecordParser.parse_statistics(self._file)
            elif record_type == McapRecordType.CHUNK_INDEX:

                chunk_index = McapRecordParser.parse_chunk_index(self._file)
                self._cached_chunk_indexes.append(chunk_index)
            else:
                logging.error(f'Unexpected record in summary: {record_type}')
                McapRecordParser.skip_record(self._file)

        # Sort chunk indexes by message start time
        # TODO: Is this the best place to do this?
        self._cached_chunk_indexes.sort(key=lambda ci: ci.message_start_time)

    def _build_summary(self) -> None:
        """Build summary information by scanning the data section.

        This method populates instance variables directly and is only called
        when no summary section exists.
        """
        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Build summary: file not initialized!')
            return None

        # Track message statistics
        message_count = 0
        chunk_count = 0
        attachment_count = 0
        metadata_count = 0
        message_start_time: int | None = None
        message_end_time: int | None = None
        channel_message_counts: dict[ChannelId, int] = defaultdict(int)

        found_schemas: dict[SchemaId, SchemaRecord] = {}
        found_channels: dict[ChannelId, ChannelRecord] = {}
        found_chunk_indexes: list[ChunkIndexRecord] = []

        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Iterate through records until we reach the footer
        while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.FOOTER:
            if record_type == McapRecordType.SCHEMA:
                if schema := McapRecordParser.parse_schema(self._file):
                    found_schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                found_channels[channel.id] = channel
            elif record_type == McapRecordType.CHUNK:
                chunk_count += 1

                chunk_start_offset = self._file.tell()
                chunk = McapRecordParser.parse_chunk(self._file)
                chunk_length = self._file.tell() - chunk_start_offset

                # Read the message index records
                message_index_length = 0
                reconstruct_message_index = True
                message_index_offsets: dict[int, int] = {}
                if self._enable_reconstruction != 'always':  # i.e. if 'missing'
                    start_post_chunk_message_index_offset = self._file.tell()
                    while McapRecordParser.peek_record(self._file) == McapRecordType.MESSAGE_INDEX:
                        post_chunk_message_index_offset = self._file.tell()
                        post_chunk_message_index = McapRecordParser.parse_message_index(self._file)
                        message_index_offsets[post_chunk_message_index.channel_id] = post_chunk_message_index_offset
                    message_index_length = self._file.tell() - start_post_chunk_message_index_offset
                    # If message index records exist, assume they are complete
                    reconstruct_message_index = not message_index_offsets
                    if reconstruct_message_index:
                        logging.warning("No message indexes found for chunk!")

                # Continue parsing the chunk
                compression = chunk.compression
                chunk_message_start_time: int | None = None
                chunk_message_end_time: int | None = None
                chunk_message_indexes: dict[ChannelId, list[tuple[LogTime, Offset]]] = defaultdict(list)

                compressed_size = len(chunk.records)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                uncompressed_size = reader.size()

                while chunk_record_type := McapRecordParser.peek_record(reader):
                    if chunk_record_type == McapRecordType.SCHEMA:
                        if schema := McapRecordParser.parse_schema(reader):
                            found_schemas[schema.id] = schema
                    elif chunk_record_type == McapRecordType.CHANNEL:
                        channel = McapRecordParser.parse_channel(reader)
                        found_channels[channel.id] = channel
                    elif chunk_record_type == McapRecordType.MESSAGE:
                        message_offset = reader.tell()
                        chunk_message = McapRecordParser.parse_message(reader)
                        channel_id = chunk_message.channel_id
                        log_time = chunk_message.log_time

                        message_count += 1
                        channel_message_counts[channel_id] += 1
                        if reconstruct_message_index:
                            chunk_message_indexes[channel_id].append((log_time, message_offset))

                        if message_start_time is None or log_time < message_start_time:
                            message_start_time = log_time
                        if message_end_time is None or log_time > message_end_time:
                            message_end_time = log_time

                        if chunk_message_start_time is None or log_time < chunk_message_start_time:
                            chunk_message_start_time = log_time
                        if chunk_message_end_time is None or log_time > chunk_message_end_time:
                            chunk_message_end_time = log_time
                    elif chunk_record_type == McapRecordType.METADATA:
                        logging.warning(f'Metadata in chunk! This is not allowed in mcap spec. Ignoring...')
                        McapRecordParser.skip_record(reader)
                    elif chunk_record_type == McapRecordType.ATTACHMENT:
                        logging.warning(f'Attachment in chunk! This is not allowed in mcap spec. Ignoring...')
                        McapRecordParser.skip_record(reader)
                    else:
                        McapRecordParser.skip_record(reader)

                self._message_indexes[chunk_start_offset] = {
                    channel_id: MessageIndexRecord(
                        channel_id=channel_id,
                        records=sorted(records, key=lambda x: (x[0], x[1])),
                    )
                    for channel_id, records in chunk_message_indexes.items()
                }

                found_chunk_indexes.append(
                    ChunkIndexRecord(
                        message_start_time=chunk_message_start_time or 0,
                        message_end_time=chunk_message_end_time or 0,
                        chunk_start_offset=chunk_start_offset,
                        chunk_length=chunk_length,
                        message_index_offsets=message_index_offsets,
                        message_index_length=message_index_length,
                        compression=compression,
                        compressed_size=compressed_size,
                        uncompressed_size=uncompressed_size,
                    )
                )
            elif record_type == McapRecordType.MESSAGE:
                # TODO: Is there a way not to ignore?
                logging.warning('Found Message record outside of a chunk! Ignoring...')
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.ATTACHMENT:
                attachment_count += 1
                start_offset = self._file.tell()
                attachment_record = McapRecordParser.parse_attachment(self._file)
                attachment_index = AttachmentIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    log_time=attachment_record.log_time,
                    create_time=attachment_record.create_time,
                    data_size=len(attachment_record.data),
                    name=attachment_record.name,
                    media_type=attachment_record.media_type,
                )
                self._cached_attachment_indexes[attachment_record.name].append(attachment_index)
            elif record_type == McapRecordType.METADATA:
                metadata_count += 1
                start_offset = self._file.tell()
                metadata_record = McapRecordParser.parse_metadata(self._file)
                metadata_index = MetadataIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    name=metadata_record.name,
                )
                self._cached_metadata_indexes[metadata_record.name].append(metadata_index)
            else:
                McapRecordParser.skip_record(self._file)

        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(found_schemas),
            channel_count=len(found_channels),
            attachment_count=attachment_count,
            metadata_count=metadata_count,
            chunk_count=chunk_count,
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )
        self._cached_schemas = found_schemas
        self._cached_channels = found_channels
        self._cached_chunk_indexes = found_chunk_indexes

        # Sort chunk indexes by message start time
        # TODO: Is this the best place to do this?
        self._cached_chunk_indexes.sort(key=lambda ci: ci.message_start_time)

    def next_schema_id(self) -> int:
        # Schema ID must be non-zero
        return max(self.get_schemas().keys(), default=0) + 1

    def next_channel_id(self) -> int:
        # No harm in starting channel IDs from 1 too
        return max(self.get_channels().keys(), default=0) + 1

    def next_sequence_id(self, channel_id: ChannelId) -> int:
        if self._cached_statistics is not None:
            # We should always start at 1 because 0 indicates "not relevant"
            return self._cached_statistics.channel_message_counts.get(channel_id, 0) + 1
        return 0

    def get_channel_id(self, topic: str) -> ChannelId | None:
        for id, record in self._cached_channels.items():
            if record.topic == topic:
                return id
        return None

    def get_schema_id(self, message: type[Message]) -> SchemaId | None:
        for id, record in self._cached_schemas.items():
            if record.name == message.__msg_name__:
                return id
        return None

    def add_schema(self, schema: SchemaRecord):
        if schema.id in self._cached_schemas:
            logging.warning(f'Schema (id {schema.id}) already written to file')
        else:
            self._cached_schemas[schema.id] = schema
            if self._cached_statistics is not None:
                self._cached_statistics.schema_count += 1

    def add_channel(self, channel: ChannelRecord):
        if channel.id in self._cached_channels:
            logging.warning(f'Channel (id {channel.id}) already written to file')
        else:
            self._cached_channels[channel.id] = channel
            if self._cached_statistics is not None:
                self._cached_statistics.channel_count += 1
                self._cached_statistics.channel_message_counts[channel.id] = 0

    def add_message(self, message: MessageRecord):
        # Update statistics
        if self._cached_statistics is not None:
            self._cached_statistics.message_count += 1
            self._cached_statistics.channel_message_counts[message.channel_id] += 1
            self._cached_statistics.message_start_time = min(
                message.log_time if self._cached_statistics.message_start_time <= 0 else self._cached_statistics.message_start_time,
                message.log_time,
            )
            self._cached_statistics.message_end_time = max(
                message.log_time if self._cached_statistics.message_end_time <= 0 else self._cached_statistics.message_end_time,
                message.log_time,
            )

    def add_attachment_index(self, attachment_index: AttachmentIndexRecord):
        self._cached_attachment_indexes[attachment_index.name].append(attachment_index)
        if self._cached_statistics is not None:
            self._cached_statistics.attachment_count += 1

    def add_metadata_index(self, metadata_index: MetadataIndexRecord):
        self._cached_metadata_indexes[metadata_index.name].append(metadata_index)
        if self._cached_statistics is not None:
            self._cached_statistics.metadata_count += 1

    def add_chunk_index(self, chunk_index: ChunkIndexRecord, length: int):
        # Track chunk index for summary
        self._cached_chunk_indexes.append(chunk_index)
        if self._cached_statistics is not None:
            self._cached_statistics.chunk_count += 1

    def get_schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            Dictionary mapping schema ID to SchemaRecord
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_schemas:
            return self._cached_schemas

        # If reconstruction not allowed, return empty
        if self._enable_reconstruction == 'never':
            logging.warning('No schema records found in summary and searching is disabled.')
            return self._cached_schemas

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Schemas: file not initialized!')
            return self._cached_schemas

        # Search for schema records in the summary section
        logging.warning('No schema records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.SCHEMA:
                if record := McapRecordParser.parse_schema(self._file):
                    self._cached_schemas[record.id] = record
            elif next_record == McapRecordType.CHUNK:
                chunk = McapRecordParser.parse_chunk(self._file)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                while chunk_record_type := McapRecordParser.peek_record(reader):
                    if chunk_record_type == McapRecordType.SCHEMA:
                        if schema := McapRecordParser.parse_schema(reader):
                            self._cached_schemas[schema.id] = schema
                    else:
                        McapRecordParser.skip_record(reader)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_schemas = True
        return self._cached_schemas

    def get_channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            Dictionary mapping channel ID to ChannelRecord
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more channel records, so we return what we have.
        if self._has_built_summary or self._has_searched_channels:
            return self._cached_channels

        # If reconstruction not allowed, return empty
        if self._enable_reconstruction == 'never':
            logging.warning('No channel records found in summary and reconstruction is disabled.')
            self._cached_channels = {}
            return self._cached_channels

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Channels: file not initialized!')
            return self._cached_channels

        # Search for channel records in the summary section
        logging.warning('No channel records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                self._cached_channels[channel.id] = channel
            elif next_record == McapRecordType.CHUNK:
                chunk = McapRecordParser.parse_chunk(self._file)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                while chunk_record_type := McapRecordParser.peek_record(reader):
                    if chunk_record_type == McapRecordType.CHANNEL:
                        channel = McapRecordParser.parse_channel(reader)
                        self._cached_channels[channel.id] = channel
                    else:
                        McapRecordParser.skip_record(reader)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_channels = True
        return self._cached_channels

    def get_chunk_indexes(self) -> list[ChunkIndexRecord]:
        """Get all chunk indexes, sorted by message_start_time.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            List of ChunkIndexRecords sorted by message_start_time
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_chunks:
            return self._cached_chunk_indexes

        # If reconstruction not allowed, return empty
        if self._enable_reconstruction == 'never':
            logging.warning('No chunk index records found in summary and reconstruction is disabled.')
            self._cached_chunk_indexes = []
            return self._cached_chunk_indexes

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Chunk indexes: file not initialized!')
            return self._cached_chunk_indexes

        # Search for chunk index records in the summary section
        logging.warning('No chunk index records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.CHUNK:
                chunk_start_offset = self._file.tell()
                chunk = McapRecordParser.parse_chunk(self._file)
                chunk_length = self._file.tell() - chunk_start_offset

                # Read the message index records
                message_index_length = 0
                reconstruct_message_index = True
                message_index_offsets: dict[int, int] = {}
                if self._enable_reconstruction != 'always':  # i.e. if 'missing'
                    start_post_chunk_message_index_offset = self._file.tell()
                    while McapRecordParser.peek_record(self._file) == McapRecordType.MESSAGE_INDEX:
                        post_chunk_message_index_offset = self._file.tell()
                        post_chunk_message_index = McapRecordParser.parse_message_index(self._file)
                        message_index_offsets[post_chunk_message_index.channel_id] = post_chunk_message_index_offset
                    message_index_length = self._file.tell() - start_post_chunk_message_index_offset
                    # If message index records exist, assume they are complete
                    reconstruct_message_index = not message_index_offsets
                    if reconstruct_message_index:
                        logging.warning("No message indexes found for chunk!")

                # Continue parsing the chunk
                compression = chunk.compression
                chunk_message_start_time: int | None = None
                chunk_message_end_time: int | None = None
                chunk_message_indexes: dict[ChannelId, list[tuple[LogTime, Offset]]] = defaultdict(list)

                compressed_size = len(chunk.records)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                uncompressed_size = reader.size()

                while chunk_record_type := McapRecordParser.peek_record(reader):
                    if chunk_record_type == McapRecordType.MESSAGE:
                        message_offset = reader.tell()
                        chunk_message = McapRecordParser.parse_message(reader)

                        channel_id = chunk_message.channel_id
                        log_time = chunk_message.log_time
                        if reconstruct_message_index:
                            chunk_message_indexes[channel_id].append((log_time, message_offset))

                        if chunk_message_start_time is None or log_time < chunk_message_start_time:
                            chunk_message_start_time = log_time
                        if chunk_message_end_time is None or log_time > chunk_message_end_time:
                            chunk_message_end_time = log_time
                    else:
                        McapRecordParser.skip_record(reader)

                self._message_indexes[chunk_start_offset] = {
                    channel_id: MessageIndexRecord(
                        channel_id=channel_id,
                        records=sorted(records, key=lambda x: (x[0], x[1])),
                    )
                    for channel_id, records in chunk_message_indexes.items()
                }

                self._cached_chunk_indexes.append(
                    ChunkIndexRecord(
                        message_start_time=chunk_message_start_time or 0,
                        message_end_time=chunk_message_end_time or 0,
                        chunk_start_offset=chunk_start_offset,
                        chunk_length=chunk_length,
                        message_index_offsets=message_index_offsets,
                        message_index_length=message_index_length,
                        compression=compression,
                        compressed_size=compressed_size,
                        uncompressed_size=uncompressed_size,
                    )
                )
            else:
                McapRecordParser.skip_record(self._file)

        # Sort chunk indexes by message start time
        # TODO: Is this the best place to do this?
        self._cached_chunk_indexes.sort(key=lambda ci: ci.message_start_time)

        self._has_searched_chunks = True
        return self._cached_chunk_indexes

    def get_message_indexes(
        self,
        chunk_index: ChunkIndexRecord,
    ) -> dict[ChannelId, MessageIndexRecord]:
        """Get reconstructed message indexes for a chunk if available."""
        chunk_offset = chunk_index.chunk_start_offset
        if chunk_offset in self._message_indexes:
            return self._message_indexes[chunk_offset]

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Message indexes: file not initialized!')
            return {}

        current_pos = self._file.tell()
        try:
            _ = self._file.seek_from_start(chunk_offset)
            chunk = McapRecordParser.parse_chunk(self._file)
        finally:
            _ = self._file.seek_from_start(current_pos)

        reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
        chunk_message_indexes: dict[ChannelId, list[tuple[int, int]]] = defaultdict(list)
        while chunk_record_type := McapRecordParser.peek_record(reader):
            if chunk_record_type == McapRecordType.MESSAGE:
                message_offset = reader.tell()
                message = McapRecordParser.parse_message(reader)
                chunk_message_indexes[message.channel_id].append((message.log_time, message_offset))
            else:
                McapRecordParser.skip_record(reader)

        reconstructed = {
            channel_id: MessageIndexRecord(
                channel_id=channel_id,
                records=sorted(records, key=lambda x: (x[0], x[1])),
            )
            for channel_id, records in chunk_message_indexes.items()
        }
        self._message_indexes[chunk_offset] = reconstructed

        return reconstructed

    def get_statistics(self) -> StatisticsRecord | None:
        """Get statistics about the MCAP file.

        Uses lazy loading: checks cache first, then loads from summary if available,
        or calculates from chunk indexes, otherwise returns pre-built data.

        Returns:
            StatisticsRecord containing file statistics
        """
        # If we have the statistics record return it, going through the data section
        # is dangerous to do because we might overwrite stuff we have added.
        if self._cached_statistics is not None:
            return self._cached_statistics

        # If reconstruction is not allowed, then nothing we can do
        if self._enable_reconstruction == 'never':
            logging.warning('No statistics record found in summary and reconstruction is disabled.')
            return None

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Statistics: file not initialized!')
            return None

        # Track message statistics
        chunk_count = 0
        message_count = 0
        attachment_count = 0
        metadata_count = 0
        message_start_time: int | None = None
        message_end_time: int | None = None
        channel_message_counts: dict[ChannelId, int] = defaultdict(int)
        schema_ids: set[SchemaId] = set()
        channel_ids: set[ChannelId] = set()

        # Seek to start of data section (after magic bytes)
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Iterate through records until we reach the footer
        logging.warning('No statistics record found in summary. Recreating from data section!')
        while McapRecordParser.peek_record(self._file) != McapRecordType.FOOTER:
            record_type = McapRecordParser.peek_record(self._file)
            if record_type == McapRecordType.SCHEMA:
                if schema := McapRecordParser.parse_schema(self._file):
                    schema_ids.add(schema.id)
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channel_ids.add(channel.id)
            elif record_type == McapRecordType.CHUNK:
                chunk_count += 1
                chunk_start_offset = self._file.tell()
                chunk = McapRecordParser.parse_chunk(self._file)
                reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                chunk_message_indexes: dict[ChannelId, list[tuple[int, int]]] = defaultdict(list)
                while chunk_record_type := McapRecordParser.peek_record(reader):
                    if chunk_record_type == McapRecordType.SCHEMA:
                        if schema := McapRecordParser.parse_schema(reader):
                            schema_ids.add(schema.id)
                    elif chunk_record_type == McapRecordType.CHANNEL:
                        channel = McapRecordParser.parse_channel(reader)
                        channel_ids.add(channel.id)
                    elif chunk_record_type == McapRecordType.MESSAGE:
                        message_offset = reader.tell()
                        chunk_message = McapRecordParser.parse_message(reader)
                        channel_id = chunk_message.channel_id
                        log_time = chunk_message.log_time
                        channel_ids.add(channel_id)
                        message_count += 1
                        channel_message_counts[channel_id] += 1
                        chunk_message_indexes[channel_id].append((log_time, message_offset))
                        if message_start_time is None or log_time < message_start_time:
                            message_start_time = log_time
                        if message_end_time is None or log_time > message_end_time:
                            message_end_time = log_time
                    else:
                        McapRecordParser.skip_record(reader)

                if chunk_message_indexes and chunk_start_offset not in self._message_indexes:
                    self._message_indexes[chunk_start_offset] = {
                        channel_id: MessageIndexRecord(
                            channel_id=channel_id,
                            records=sorted(records, key=lambda x: (x[0], x[1])),
                        )
                        for channel_id, records in chunk_message_indexes.items()
                    }

                # Advance past any existing message index records following the chunk
                while McapRecordParser.peek_record(self._file) == McapRecordType.MESSAGE_INDEX:
                    McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.MESSAGE:
                logging.warning('Found Message record outside of a chunk! Ignoring...')
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.ATTACHMENT:
                attachment_count += 1
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.METADATA:
                metadata_count += 1
                McapRecordParser.skip_record(self._file)
            else:
                McapRecordParser.skip_record(self._file)

        # Create statistics record from collected data
        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(schema_ids),
            channel_count=len(channel_ids),
            attachment_count=attachment_count,
            metadata_count=metadata_count,
            chunk_count=chunk_count,
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=dict(channel_message_counts),
        )
        return self._cached_statistics

    def get_attachment_indexes(self) -> dict[str, list[AttachmentIndexRecord]]:
        """Get all attachment indexes from the MCAP file.

        Returns:
            List of AttachmentIndexRecord objects.
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_schemas:
            return self._cached_attachment_indexes

        # If reconstruction not allowed, return empty
        if self._enable_reconstruction == 'never':
            logging.warning('No attachment index records found in summary and reconstruction is disabled.')
            self._cached_attachment_indexes = {}
            return self._cached_attachment_indexes

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Attachment indexes: file not initialized!')
            return self._cached_attachment_indexes

        # Search for channel records in the summary section
        logging.warning('No attachment index records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.ATTACHMENT:
                start_offset = self._file.tell()
                attachment_record = McapRecordParser.parse_attachment(self._file)
                attachment_index = AttachmentIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    log_time=attachment_record.log_time,
                    create_time=attachment_record.create_time,
                    data_size=len(attachment_record.data),
                    name=attachment_record.name,
                    media_type=attachment_record.media_type,
                )
                self._cached_attachment_indexes[attachment_record.name].append(attachment_index)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_attachments = True  # Prevent searching through data section again
        return self._cached_attachment_indexes

    def get_metadata_indexes(self) -> dict[str, list[MetadataIndexRecord]]:
        """Get all metadata indexes from the MCAP file.

        Returns:
            List of MetadataIndexRecord objects.
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_metadata:
            return self._cached_metadata_indexes

        # If reconstruction not allowed, return empty
        if self._enable_reconstruction == 'never':
            logging.warning('No metadata index records found in summary and reconstruction is disabled.')
            self._cached_metadata_indexes = {}
            return self._cached_metadata_indexes

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Metadata indexes: file not initialized!')
            return self._cached_metadata_indexes

        # Search for channel records in the summary section
        logging.warning('No attachment index records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.METADATA:
                start_offset = self._file.tell()
                metadata_record = McapRecordParser.parse_metadata(self._file)
                metadata_index = MetadataIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    name=metadata_record.name,
                )
                self._cached_metadata_indexes[metadata_record.name].append(metadata_index)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_metadata = True  # Prevent searching through data section again
        return self._cached_metadata_indexes

    def write_summary(self, writer: CrcWriter):
        # Write DataEnd record
        data_end = DataEndRecord(data_section_crc=writer.get_crc())
        McapRecordWriter.write_data_end(writer, data_end)

        # Write summary section using shared helper
        summary_start, summary_offset_start = _write_summary_section(
            writer,
            schema_records=list(self._cached_schemas.values()),
            channel_records=list(self._cached_channels.values()),
            statistics_record=self._cached_statistics,
            attachment_indexes=self._cached_attachment_indexes,
            chunk_indexes=self._cached_chunk_indexes,
            metadata_indexes=self._cached_metadata_indexes
        )

        # Write footer record manually for CRC calculation
        # TODO: Find a more elegant solution
        writer.write(McapRecordWriter._encode_record_type(McapRecordType.FOOTER))
        writer.write(McapRecordWriter._encode_uint64(FOOTER_PAYLOAD_SIZE))
        writer.write(McapRecordWriter._encode_uint64(summary_start))
        writer.write(McapRecordWriter._encode_uint64(summary_offset_start))
        writer.write(McapRecordWriter._encode_uint32(writer.get_crc()))

        # Write magic bytes again
        McapRecordWriter.write_magic_bytes(writer)


class McapNonChunkedSummary(McapSummary):
    """Summary information for a non-chunked MCAP file.

    This class handles loading summary information from a non-chunked MCAP file,
    either from existing summary sections or by reconstructing it from the data section.

    Args:
        file: The file reader to read from.
        enable_crc_check: Whether to validate the CRC checksums
        enable_reconstruction: Controls reconstruction behavior:
            - 'never': Raise error if summary sections are missing
            - 'missing': Load from summary if present, otherwise reconstruct
            - 'always': Always reconstruct even if summary exists
    """

    def __init__(
        self,
        file: BaseReader | None = None,
        *,
        enable_crc_check: bool = False,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        load_summary_eagerly: bool = True,
    ) -> None:
        logging.debug('Creating McapNonChunkedSummary')
        self._file = file
        self._enable_reconstruction = enable_reconstruction
        self._check_crc = enable_crc_check
        self._load_summary_eagerly = load_summary_eagerly

        # Initialize cache variables for lazy loading
        self._summary_offset: dict[RecordId, Offset] = {}
        self._cached_schemas: dict[SchemaId, SchemaRecord] = {}
        self._cached_channels: dict[ChannelId, ChannelRecord] = {}
        self._cached_metadata_indexes: dict[str, list[MetadataIndexRecord]] = defaultdict(list)
        self._cached_attachment_indexes: dict[str, list[AttachmentIndexRecord]] = defaultdict(list)
        # If self._file is None, then we start a new summary and so we can track stats from the beginning
        self._cached_statistics: StatisticsRecord | None = StatisticsRecord() if self._file is None else None

        # If we have no file, then we have the summary built (i.e. nothing)
        self._has_built_summary = self._file is None
        self._has_loaded_summary = False

        self._has_searched_schemas = False
        self._has_searched_channels = False
        self._has_searched_attachments = False
        self._has_searched_metadata = False

        # If no file, then we are creating a new summary
        if self._file is None:
            logging.debug('Summary has no file to search')
            return

        logging.debug(f'Tell: {self._file.tell()}')

        _ = self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        self._footer: FooterRecord = McapRecordParser.parse_footer(self._file)

        self._has_summary: bool = self._footer.summary_start != 0
        self._has_summary_offset: bool = self._footer.summary_offset_start != 0

        # Validate CRCs if requested
        if enable_crc_check:
            assert_data_crc(self._file, self._footer)
            assert_summary_crc(self._file, self._footer)

        # TODO: Figure out how to use offset
        if enable_reconstruction == 'never':
            if not self._has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)

        if enable_reconstruction == 'always' or not self._has_summary:
            logging.debug('Building summary from data section')
            self._build_summary()
            self._has_built_summary = True
        else:
            # TODO: What do we do if the summary is incomplete?
            logging.debug('Loading summary from summary section')
            self._load_summary()
            if self._cached_statistics is None and self._load_summary_eagerly:
                logging.warning('Statistics record not found in summary, generating')
                self._cached_statistics = self.get_statistics()

        # After loading summary, if we have records, then we assume they are complete
        # and we disable searching through the file for more records of the same type
        self._has_searched_schemas = len(self._cached_schemas) > 0
        self._has_searched_channels = len(self._cached_channels) > 0
        self._has_searched_attachments = len(self._cached_attachment_indexes) > 0
        self._has_searched_metadata = len(self._cached_metadata_indexes) > 0

    def _load_summary(self) -> None:
        if self._file is None:
            logging.warning('Load summary: file not initialized!')
            return None

        # Seek to start of summary section
        _ = self._file.seek_from_start(self._footer.summary_start)

        # Iterate through records until we reach the footer
        while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.FOOTER:
            # Exit when we reach the end of the summary section
            if record_type == McapRecordType.SUMMARY_OFFSET:
                break

            if record_type == McapRecordType.SCHEMA:
                if schema := McapRecordParser.parse_schema(self._file):
                    self._cached_schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                self._cached_channels[channel.id] = channel
            elif record_type == McapRecordType.ATTACHMENT_INDEX:
                attachment_index = McapRecordParser.parse_attachment_index(self._file)
                self._cached_attachment_indexes[attachment_index.name].append(attachment_index)
            elif record_type == McapRecordType.METADATA_INDEX:
                metadata_index = McapRecordParser.parse_metadata_index(self._file)
                self._cached_metadata_indexes[metadata_index.name].append(metadata_index)
            elif record_type == McapRecordType.STATISTICS:
                self._cached_statistics = McapRecordParser.parse_statistics(self._file)
            elif record_type == McapRecordType.CHUNK_INDEX:
                logging.warning('Found CHUNK_INDEX in a non-chunked mcap!')
                McapRecordParser.skip_record(self._file)
            else:
                logging.error(f'Unexpected record in summary: {record_type}')
                McapRecordParser.skip_record(self._file)

    def _build_summary(self) -> None:
        """Build summary information by scanning the data section."""
        if self._file is None:
            logging.warning('Build summary: file not initialized!')
            return None

        # Track message statistics
        message_count = 0
        attachment_count = 0
        metadata_count = 0
        message_start_time: int | None = None
        message_end_time: int | None = None
        channel_message_counts: dict[ChannelId, int] = defaultdict(int)

        found_schemas: dict[SchemaId, SchemaRecord] = {}
        found_channels: dict[ChannelId, ChannelRecord]= {}

        # Seek to start of data section (after magic bytes)
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Iterate through records until we reach the footer
        while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.FOOTER:
            if record_type == McapRecordType.SCHEMA:
                if schema := McapRecordParser.parse_schema(self._file):
                    found_schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                found_channels[channel.id] = channel
            elif record_type == McapRecordType.MESSAGE:
                # Parse message to extract statistics
                message = McapRecordParser.parse_message(self._file)
                channel_id = message.channel_id
                log_time = message.log_time

                # Update statistics
                message_count += 1
                channel_message_counts[channel_id] += 1
                if message_start_time is None or log_time < message_start_time:
                    message_start_time = log_time
                if message_end_time is None or log_time > message_end_time:
                    message_end_time = log_time
            elif record_type == McapRecordType.ATTACHMENT:
                attachment_count += 1
                start_offset = self._file.tell()
                attachment_record = McapRecordParser.parse_attachment(self._file)
                attachment_index = AttachmentIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    log_time=attachment_record.log_time,
                    create_time=attachment_record.create_time,
                    data_size=len(attachment_record.data),
                    name=attachment_record.name,
                    media_type=attachment_record.media_type,
                )
                self._cached_attachment_indexes[attachment_record.name].append(attachment_index)
            elif record_type == McapRecordType.METADATA:
                metadata_count += 1
                start_offset = self._file.tell()
                metadata_record = McapRecordParser.parse_metadata(self._file)
                metadata_index = MetadataIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    name=metadata_record.name,
                )
                self._cached_metadata_indexes[metadata_record.name].append(metadata_index)
            else:
                McapRecordParser.skip_record(self._file)

        # Create statistics record from collected data
        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(found_schemas),
            channel_count=len(found_channels),
            attachment_count=attachment_count,
            metadata_count=metadata_count,
            chunk_count=0,  # Non-chunked files have no chunks
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )
        self._cached_schemas = found_schemas
        self._cached_channels = found_channels

    def next_schema_id(self) -> int:
        # Schema ID must be non-zero
        return max(self.get_schemas().keys(), default=0) + 1

    def next_channel_id(self) -> int:
        # No harm in starting channel IDs from 1 too
        return max(self.get_channels().keys(), default=0) + 1

    def next_sequence_id(self, channel_id: ChannelId) -> int:
        if self._cached_statistics is not None:
            # We should always start at 1 because 0 indicates "not relevant"
            return self._cached_statistics.channel_message_counts.get(channel_id, 0) + 1
        return 0

    def get_channel_id(self, topic: str) -> ChannelId | None:
        for id, record in self._cached_channels.items():
            if record.topic == topic:
                return id
        return None

    def get_schema_id(self, message: type[Message]) -> SchemaId | None:
        for id, record in self._cached_schemas.items():
            if record.name == message.__msg_name__:
                return id
        return None

    def add_schema(self, schema: SchemaRecord):
        if schema.id in self._cached_schemas:
            logging.warning(f'Schema (id {schema.id}) already written to file')
        else:
            self._cached_schemas[schema.id] = schema
            if self._cached_statistics is not None:
                self._cached_statistics.schema_count += 1

    def add_channel(self, channel: ChannelRecord):
        if channel.id in self._cached_channels:
            logging.warning(f'Channel (id {channel.id}) already written to file')
        else:
            self._cached_channels[channel.id] = channel
            if self._cached_statistics is not None:
                self._cached_statistics.channel_count += 1
                self._cached_statistics.channel_message_counts[channel.id] = 0

    def add_message(self, message: MessageRecord):
        if self._cached_statistics is not None:
            self._cached_statistics.message_count += 1
            self._cached_statistics.channel_message_counts[message.channel_id] += 1
            # TODO: Find something more elegant
            self._cached_statistics.message_start_time = min(
                message.log_time if self._cached_statistics.message_start_time <= 0 else self._cached_statistics.message_start_time,
                message.log_time,
            )
            self._cached_statistics.message_end_time = max(
                message.log_time if self._cached_statistics.message_end_time <= 0 else self._cached_statistics.message_end_time,
                message.log_time,
            )

    def add_attachment_index(self, attachment_index: AttachmentIndexRecord):
        self._cached_attachment_indexes[attachment_index.name].append(attachment_index)
        if self._cached_statistics is not None:
            self._cached_statistics.attachment_count += 1

    def add_metadata_index(self, metadata_index: MetadataIndexRecord):
        self._cached_metadata_indexes[metadata_index.name].append(metadata_index)
        if self._cached_statistics is not None:
            self._cached_statistics.metadata_count += 1

    def get_schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_schemas:
            return self._cached_schemas

        # Finally, we have yet to search through the data section to find schemas
        # If reconstruction not allowed, we cannot do anything so we return what we have
        if self._enable_reconstruction == 'never':
            logging.warning('No schema records found in summary and reconstruction is disabled.')
            return self._cached_schemas

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Schemas: file not initialized!')
            return self._cached_schemas

        # We are allowed to search through the data section for any schemas that we can find
        logging.warning('No schema records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while (next_record := McapRecordParser.peek_record(self._file)) != McapRecordType.DATA_END:
            if next_record != McapRecordType.SCHEMA:
                McapRecordParser.skip_record(self._file)
                continue
            if record := McapRecordParser.parse_schema(self._file):
                self._cached_schemas[record.id] = record
        self._has_searched_schemas = True  # Prevent searching through data section again
        return self._cached_schemas

    def get_channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file."""
        # If we have already searched through the data section, there is nothing else
        # we can do to find more channel records, so we return what we have.
        if self._has_built_summary or self._has_searched_channels:
            return self._cached_channels

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Channels: file not initialized!')
            return self._cached_channels

        # Finally, we have yet to search through the data section to find channels
        # If reconstruction not allowed, we cannot do anything so we return what we have
        if self._enable_reconstruction == 'never':
            logging.warning('No channel records found in summary and reconstruction is disabled.')
            return self._cached_channels

        # We are allowed to search through the data section for any schemas that we can find
        logging.warning('No channel records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while (next_record := McapRecordParser.peek_record(self._file)) != McapRecordType.DATA_END:
            if next_record != McapRecordType.CHANNEL:
                McapRecordParser.skip_record(self._file)
                continue
            if record := McapRecordParser.parse_channel(self._file):
                self._cached_channels[record.id] = record
        self._has_searched_channels = True  # Prevent searching through data section again
        return self._cached_channels

    def get_chunk_indexes(self) -> list[ChunkIndexRecord]:
        return []

    def get_message_indexes(
        self,
        chunk_index: ChunkIndexRecord,
    ) -> dict[ChannelId, MessageIndexRecord]:
        return {}  # TODO: Move from McapNonChunkedReader

    def get_statistics(self) -> StatisticsRecord | None:
        """Get statistics about the MCAP file."""
        # If we have the statistics record return it, going through the data section
        # is dangerous to do because we might overwrite stuff we have added.
        if self._cached_statistics is not None:
            return self._cached_statistics

        # If reconstruction is not allowed, then nothing we can do
        if self._enable_reconstruction == 'never':
            logging.warning('No statistics record found in summary and reconstruction is disabled.')
            return None

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Statistics: file not initialized!')
            return None

        # We use None to indicate that we have not yet searched through the data section
        # because once we have, the cache will no longer be None.

        # Track message statistics
        schema_count = 0
        channel_count = 0
        message_count = 0
        attachment_count = 0
        metadata_count = 0
        message_start_time: int | None = None
        message_end_time: int | None = None
        channel_message_counts: dict[ChannelId, int] = defaultdict(int)

        # Seek to start of data section (after magic bytes)
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Iterate through records until we reach the footer
        logging.warning('No statistics record found in summary. Recreating from data section!')
        while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.FOOTER:
            if record_type == McapRecordType.SCHEMA:
                schema_count += 1
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.CHANNEL:
                channel_count += 1
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.MESSAGE:
                # Parse message to extract statistics
                message = McapRecordParser.parse_message(self._file)
                channel_id = message.channel_id
                log_time = message.log_time

                # Update statistics
                message_count += 1
                channel_message_counts[channel_id] += 1
                if message_start_time is None or log_time < message_start_time:
                    message_start_time = log_time
                if message_end_time is None or log_time > message_end_time:
                    message_end_time = log_time
            elif record_type == McapRecordType.ATTACHMENT:
                attachment_count += 1
                McapRecordParser.skip_record(self._file)
            elif record_type == McapRecordType.METADATA:
                metadata_count += 1
                McapRecordParser.skip_record(self._file)
            else:
                McapRecordParser.skip_record(self._file)

        # Create statistics record from collected data
        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=schema_count,
            channel_count=channel_count,
            attachment_count=attachment_count,
            metadata_count=metadata_count,
            chunk_count=0,  # Non-chunked files have no chunks
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )
        return self._cached_statistics

    def get_attachment_indexes(self) -> dict[str, list[AttachmentIndexRecord]]:
        """Get all attachment indexes from the MCAP file.

        Returns:
            List of AttachmentIndexRecord objects.
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_attachments:
            return self._cached_attachment_indexes

        # If reconstruction not allowed, there is nothing we can do
        if self._enable_reconstruction == 'never':
            logging.warning('No attachment index records found in summary and reconstruction is disabled.')
            return self._cached_attachment_indexes

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Attachment indexes: file not initialized!')
            return self._cached_attachment_indexes

        # Search for channel records in the summary section
        logging.warning('No attachment index records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.ATTACHMENT:
                start_offset = self._file.tell()
                attachment_record = McapRecordParser.parse_attachment(self._file)
                attachment_index = AttachmentIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    log_time=attachment_record.log_time,
                    create_time=attachment_record.create_time,
                    data_size=len(attachment_record.data),
                    name=attachment_record.name,
                    media_type=attachment_record.media_type,
                )
                self._cached_attachment_indexes[attachment_record.name].append(attachment_index)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_attachments = True  # Prevent searching through data section again
        return self._cached_attachment_indexes

    def get_metadata_indexes(self) -> dict[str, list[MetadataIndexRecord]]:
        """Get all metadata indexes from the MCAP file.

        Returns:
            List of MetadataIndexRecord objects.
        """
        # If we have already searched through the data section, there is nothing else
        # we can do to find more schema records, so we return what we have.
        if self._has_built_summary or self._has_searched_metadata:
            return self._cached_metadata_indexes

        # If reconstruction not allowed, there is nothing we can do
        if self._enable_reconstruction == 'never':
            logging.warning('No metadata index records found in summary and reconstruction is disabled.')
            return self._cached_metadata_indexes

        # If no file is given, we cannot search through the data
        if self._file is None:
            logging.warning('Metadata indexes: file not initialized!')
            return self._cached_metadata_indexes

        # Search for channel records in the summary section
        logging.warning('No attachment index records found in summary. Searching through file!')
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
            next_record = McapRecordParser.peek_record(self._file)
            if next_record == McapRecordType.METADATA:
                start_offset = self._file.tell()
                metadata_record = McapRecordParser.parse_metadata(self._file)
                metadata_index = MetadataIndexRecord(
                    offset=start_offset,
                    length=self._file.tell() - start_offset,
                    name=metadata_record.name,
                )
                self._cached_metadata_indexes[metadata_record.name].append(metadata_index)
            else:
                McapRecordParser.skip_record(self._file)
        self._has_searched_metadata = True  # Prevent searching through data section again
        return self._cached_metadata_indexes

    def write_summary(self, writer: CrcWriter):
        # Write DataEnd record
        data_end = DataEndRecord(data_section_crc=writer.get_crc())
        McapRecordWriter.write_data_end(writer, data_end)

        # Write summary section using shared helper
        summary_start, summary_offset_start = _write_summary_section(
            writer,
            schema_records=list(self._cached_schemas.values()),
            channel_records=list(self._cached_channels.values()),
            statistics_record=self._cached_statistics,
            attachment_indexes=self._cached_attachment_indexes,
            metadata_indexes=self._cached_metadata_indexes
        )

        # Write footer record manually for CRC calculation
        writer.write(McapRecordWriter._encode_record_type(McapRecordType.FOOTER))
        writer.write(McapRecordWriter._encode_uint64(FOOTER_PAYLOAD_SIZE))
        writer.write(McapRecordWriter._encode_uint64(summary_start))
        writer.write(McapRecordWriter._encode_uint64(summary_offset_start))
        writer.write(McapRecordWriter._encode_uint32(writer.get_crc()))

        # Write magic bytes again
        McapRecordWriter.write_magic_bytes(writer)


class McapSummaryFactory:
    @staticmethod
    def create_summary(
        file: BaseReader | None = None,
        chunk_size: int | None = None,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        load_summary_eagerly: bool = True,
    ) -> McapSummary:
        if file:  # Append mode
            try:
                return McapChunkedSummary(
                    file,
                    enable_reconstruction=enable_reconstruction,
                    load_summary_eagerly=load_summary_eagerly,
                )
            except McapNoChunkIndexError:
                return McapNonChunkedSummary(
                    file,
                    enable_reconstruction=enable_reconstruction,
                    load_summary_eagerly=load_summary_eagerly,
                )
        elif chunk_size is None:
            return McapNonChunkedSummary(
                enable_reconstruction=enable_reconstruction,
                load_summary_eagerly=load_summary_eagerly,
            )
        return McapChunkedSummary(
            enable_reconstruction=enable_reconstruction,
            load_summary_eagerly=load_summary_eagerly,
        )


def _write_summary_section(
    writer: CrcWriter,
    schema_records: list[SchemaRecord] | None = None,
    channel_records: list[ChannelRecord] | None = None,
    statistics_record: StatisticsRecord | None = None,
    chunk_indexes: list[ChunkIndexRecord] | None = None,
    attachment_indexes: dict[str, list[AttachmentIndexRecord]] | None = None,
    metadata_indexes: dict[str, list[MetadataIndexRecord]] | None = None
) -> tuple[int, int]:
    """Write the summary section and return (summary_start, summary_offset_start).

    Args:
        chunk_indexes: Optional list of chunk index records (for chunked writers).
        attachment_indexes: Optional list of attachment index records.
        metadata_indexes: Optional list of metadata index records.

    Returns:
        Tuple of (summary_start, summary_offset_start) positions.
    """
    # Start summary section
    summary_start = writer.tell()
    writer.clear_crc()

    # Write schema records to summary
    schema_group_start = summary_start
    if schema_records:
        logging.debug(f'Writing {len(schema_records)} schema records')
        for record in schema_records:
            McapRecordWriter.write_schema(writer, record)
    schema_group_length = writer.tell() - schema_group_start

    # Write channel records to summary
    channel_group_start = writer.tell()
    if channel_records:
        logging.debug(f'Writing {len(channel_records)} channel records')
        for record in channel_records:
            McapRecordWriter.write_channel(writer, record)
    channel_group_length = writer.tell() - channel_group_start

    # Write attachment index records to summary
    attachment_index_group_start = writer.tell()
    if attachment_indexes:
        attachment_count = 0
        for record_list in attachment_indexes.values():
            for record in record_list:
                attachment_count += 1
                McapRecordWriter.write_attachment_index(writer, record)
        logging.debug(f'Writing {attachment_count} attachment index records')
    attachment_index_group_length = writer.tell() - attachment_index_group_start

    # Write metadata index records to summary
    metadata_index_group_start = writer.tell()
    if metadata_indexes:
        metadata_count = 0
        for record_list in metadata_indexes.values():
            for record in record_list:
                metadata_count += 1
                McapRecordWriter.write_metadata_index(writer, record)
        logging.debug(f'Writing {metadata_count} metadata index records')
    metadata_index_group_length = writer.tell() - metadata_index_group_start

    # Write chunk index records to summary (only for chunked writers)
    chunk_index_group_start = writer.tell()
    if chunk_indexes:
        logging.debug(f'Writing {len(chunk_indexes)} chunk index records')
        for record in chunk_indexes:
            McapRecordWriter.write_chunk_index(writer, record)
    chunk_index_group_length = writer.tell() - chunk_index_group_start

    # Write statistics record
    statistics_group_start = writer.tell()
    if statistics_record is not None:
        logging.debug(f'Writing statistics record')
        McapRecordWriter.write_statistics(writer, statistics_record)
    statistics_group_length = writer.tell() - statistics_group_start

    # Write summary offsets
    summary_offset_start = writer.tell()
    if schema_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.SCHEMA,
                group_start=schema_group_start,
                group_length=schema_group_length,
            ),
        )
    if channel_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.CHANNEL,
                group_start=channel_group_start,
                group_length=channel_group_length,
            ),
        )
    if attachment_index_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.ATTACHMENT_INDEX,
                group_start=attachment_index_group_start,
                group_length=attachment_index_group_length,
            ),
        )
    if metadata_index_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.METADATA_INDEX,
                group_start=metadata_index_group_start,
                group_length=metadata_index_group_length,
            ),
        )
    if chunk_index_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.CHUNK_INDEX,
                group_start=chunk_index_group_start,
                group_length=chunk_index_group_length,
            ),
        )
    if statistics_group_length > 0:
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.STATISTICS,
                group_start=statistics_group_start,
                group_length=statistics_group_length,
            ),
        )

    return summary_start, summary_offset_start
