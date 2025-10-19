import logging
from collections import defaultdict
from typing import Literal, TypeAlias

from pybag.io.raw_reader import BaseReader, BytesReader
from pybag.mcap.error import (
    McapNoChunkIndexError,
    McapNoSummaryIndexError,
    McapNoSummarySectionError
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
    FooterRecord,
    MessageIndexRecord,
    SchemaRecord,
    StatisticsRecord,
    decompress_chunk
)

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


class McapChunkedSummary:
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
        file: BaseReader,
        *,
        enable_crc_check: bool = False,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:
        self._file = file
        self._enable_reconstruction = enable_reconstruction
        self._check_crc = enable_crc_check

        # Initialize cache variables
        self._summary_offset: dict[RecordId, Offset] = {}
        self._cached_schemas: dict[SchemaId, SchemaRecord] | None = None
        self._cached_channels: dict[ChannelId, ChannelRecord] | None = None
        self._cached_chunk_indexes: list[ChunkIndexRecord] | None = None
        self._cached_statistics: StatisticsRecord | None = None
        self._message_indexes: dict[Offset, dict[ChannelId, MessageIndexRecord]] = {}

        # Read footer to determine if summary sections exist
        _ = self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        self._footer = McapRecordParser.parse_footer(self._file)

        self._has_summary = self._footer.summary_start != 0
        self._has_summary_offset = self._footer.summary_offset_start != 0

        if enable_reconstruction == 'never':
            if not self._has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)
            if not self._has_summary_offset:
                error_msg = 'No summary offset section detected in MCAP'
                raise McapNoSummaryIndexError(error_msg)

        if not self._has_summary or enable_reconstruction == 'always':
            self._build_summary()
        elif self._has_summary and self._has_summary_offset:
            # Load summary offset into memory
            self._load_summary_offset()
        else:
            # Construct offset from existing summary section
            self._build_summary_offset()

        # If not chunk index records are found, then we cannot use this class
        # TODO: For MCAP files that do not have chunks, this could be slow
        if not self.get_chunk_indexes():
            raise McapNoChunkIndexError("No ChunkIndex records found!")

    def _load_summary_offset(self) -> None:
        """Load summary offset mapping from the summary offset section."""
        _ = self._file.seek_from_start(self._footer.summary_offset_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordParser.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = record.group_start

    def _build_summary_offset(self) -> None:
        """Build summary offset mapping by scanning the summary section.

        This method scans through the summary section and records the file offset
        for each new record type encountered.
        """
        current_record: RecordId | None = None
        _ = self._file.seek_from_start(self._footer.summary_start)
        while McapRecordParser.peek_record(self._file) != McapRecordType.FOOTER:
            next_record = McapRecordParser.peek_record(self._file)
            if current_record is None or current_record != next_record:
                self._summary_offset[next_record] = self._file.tell()
                current_record = next_record
            McapRecordParser.skip_record(self._file)

    def _build_summary(self) -> None:
        """Build summary information by scanning the data section.

        This method populates instance variables directly and is only called
        when no summary section exists.
        """
        # Track message statistics
        message_count = 0
        chunk_count = 0
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
            else:
                McapRecordParser.skip_record(self._file)

        # Sort chunk indexes by message start time
        # TODO: Is this the best place to do this?
        found_chunk_indexes.sort(key=lambda ci: ci.message_start_time)

        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(found_schemas),
            channel_count=len(found_channels),
            attachment_count=0,  # Not tracked during reconstruction
            metadata_count=0,  # Not tracked during reconstruction
            chunk_count=chunk_count,
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )
        self._cached_schemas = found_schemas
        self._cached_channels = found_channels
        self._cached_chunk_indexes = found_chunk_indexes

    def get_schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            Dictionary mapping schema ID to SchemaRecord
        """
        # Note _cached_schemas can be an empty dict!
        if self._cached_schemas is not None:
            return self._cached_schemas

        schemas: dict[SchemaId, SchemaRecord] = {}

        # If schema is in offset, then assume complete and return all
        if self._has_summary and McapRecordType.SCHEMA in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA])
            while McapRecordParser.peek_record(self._file) == McapRecordType.SCHEMA:
                if record := McapRecordParser.parse_schema(self._file):
                    schemas[record.id] = record
            self._cached_schemas = schemas
            return schemas

        # If schema is not in summary section, must search data section
        elif self._has_summary and McapRecordType.SCHEMA not in self._summary_offset:
            # If reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No schema records found in summary and searching is disabled.')
                self._cached_schemas = schemas
                return self._cached_schemas

            # Search for schema records in the summary section
            logging.warning('No schema records found in summary offset. Searching through file!')
            _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
            while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
                next_record = McapRecordParser.peek_record(self._file)
                if next_record == McapRecordType.SCHEMA:
                    if record := McapRecordParser.parse_schema(self._file):
                        schemas[record.id] = record
                elif next_record == McapRecordType.CHUNK:
                    chunk = McapRecordParser.parse_chunk(self._file)
                    reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                    while chunk_record_type := McapRecordParser.peek_record(reader):
                        if chunk_record_type == McapRecordType.SCHEMA:
                            if schema := McapRecordParser.parse_schema(reader):
                                schemas[schema.id] = schema
                        else:
                            McapRecordParser.skip_record(reader)
                else:
                    McapRecordParser.skip_record(self._file)
            self._cached_schemas = schemas
            return schemas

        # Either we built self._cached_schemas
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    def get_channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            Dictionary mapping channel ID to ChannelRecord
        """
        # Note _cached_channels could be an empty dict!
        if self._cached_channels is not None:
            return self._cached_channels

        channels: dict[ChannelId, ChannelRecord] = {}

        # If channel is in offset, then assume complete and return all
        if self._has_summary and McapRecordType.CHANNEL in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL])
            while McapRecordParser.peek_record(self._file) == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
            self._cached_channels = channels
            return channels

        # If channel is not in summary section, must search data section
        elif self._has_summary and McapRecordType.CHANNEL not in self._summary_offset:
            # If reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No channel records found in summary and reconstruction is disabled.')
                self._cached_channels = {}
                return self._cached_channels

            # Search for channel records in the summary section
            logging.warning('No channel records found in summary offset. Searching through file!')
            _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
            while McapRecordParser.peek_record(self._file) != McapRecordType.DATA_END:
                next_record = McapRecordParser.peek_record(self._file)
                if next_record == McapRecordType.CHANNEL:
                    channel = McapRecordParser.parse_channel(self._file)
                    channels[channel.id] = channel
                elif next_record == McapRecordType.CHUNK:
                    chunk = McapRecordParser.parse_chunk(self._file)
                    reader = BytesReader(decompress_chunk(chunk, check_crc=self._check_crc))
                    while chunk_record_type := McapRecordParser.peek_record(reader):
                        if chunk_record_type == McapRecordType.CHANNEL:
                            channel = McapRecordParser.parse_channel(reader)
                            channels[channel.id] = channel
                        else:
                            McapRecordParser.skip_record(reader)
                else:
                    McapRecordParser.skip_record(self._file)
            self._cached_channels = channels
            return channels

        # Either we built self._cached_channels
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    def get_chunk_indexes(self) -> list[ChunkIndexRecord]:
        """Get all chunk indexes, sorted by message_start_time.

        Uses lazy loading: checks cache first, then loads from summary if available,
        otherwise returns pre-built data from _build_summary().

        Returns:
            List of ChunkIndexRecords sorted by message_start_time
        """
        if self._cached_chunk_indexes is not None:
            return self._cached_chunk_indexes

        chunk_indexes: list[ChunkIndexRecord] = []

        # If chunk index is in summary offset, load from summary section
        if self._has_summary and McapRecordType.CHUNK_INDEX in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.CHUNK_INDEX])
            while McapRecordParser.peek_record(self._file) == McapRecordType.CHUNK_INDEX:
                chunk_index = McapRecordParser.parse_chunk_index(self._file)
                chunk_indexes.append(chunk_index)
            # Sort chunk indexes by message start time
            # TODO: Is this the best place to do this?
            chunk_indexes.sort(key=lambda ci: ci.message_start_time)
            self._cached_chunk_indexes = chunk_indexes
            return chunk_indexes

        # If chunk index is not in summary section, must search data section
        elif self._has_summary and McapRecordType.CHUNK_INDEX not in self._summary_offset:
            # If reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No chunk index records found in summary and reconstruction is disabled.')
                self._cached_chunk_indexes = []
                return self._cached_chunk_indexes

            # Search for chunk index records in the summary section
            logging.warning('No chunk index records found in summary offset. Searching through file!')
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

                    chunk_indexes.append(
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
            chunk_indexes.sort(key=lambda ci: ci.message_start_time)
            self._cached_chunk_indexes = chunk_indexes
            return chunk_indexes

        # Either we built self._cached_chunked_indexes
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    def get_message_indexes(
        self,
        chunk_index: ChunkIndexRecord,
    ) -> dict[ChannelId, MessageIndexRecord]:
        """Get reconstructed message indexes for a chunk if available."""
        chunk_offset = chunk_index.chunk_start_offset
        if chunk_offset in self._message_indexes:
            return self._message_indexes[chunk_offset]

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
        if self._cached_statistics is not None:
            return self._cached_statistics

        # If statistics is in summary offset, load from summary section
        if self._has_summary and McapRecordType.STATISTICS in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS])
            statistics = McapRecordParser.parse_statistics(self._file)
            self._cached_statistics = statistics
            return statistics

        # If statistics is not in summary section, then we must recreate from data section
        # TODO: Can we use the summary section instead of reading through the file?
        if self._has_summary and McapRecordType.STATISTICS not in self._summary_offset:
            # If reconstruction not allowed, create default statistics
            if self._enable_reconstruction == 'never':
                logging.warning('No statistics record found in summary and reconstruction is disabled.')
                return None

            # Track message statistics
            chunk_count = 0
            message_count = 0
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
                else:
                    McapRecordParser.skip_record(self._file)

            # Create statistics record from collected data
            self._cached_statistics = StatisticsRecord(
                message_count=message_count,
                schema_count=len(schema_ids),
                channel_count=len(channel_ids),
                attachment_count=0,  # Not tracked during reconstruction
                metadata_count=0,  # Not tracked during reconstruction
                chunk_count=chunk_count,
                message_start_time=message_start_time or 0,
                message_end_time=message_end_time or 0,
                channel_message_counts=dict(channel_message_counts),
            )
            return self._cached_statistics

        # Either we built self._cached_statistics
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    # TODO: Implement attachment index
    # TODO: Implement metadata index


class McapNonChunkedSummary:
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
        file: BaseReader,
        *,
        enable_crc_check: bool = False,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:
        self._file = file
        self._enable_reconstruction = enable_reconstruction
        self._check_crc = enable_crc_check

        # Initialize cache variables for lazy loading
        self._summary_offset: dict[RecordId, Offset] = {}
        self._cached_schemas: dict[SchemaId, SchemaRecord] | None = None
        self._cached_channels: dict[ChannelId, ChannelRecord] | None = None
        self._cached_statistics: StatisticsRecord | None = None

        _ = self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        self._footer: FooterRecord = McapRecordParser.parse_footer(self._file)

        self._has_summary: bool = self._footer.summary_start != 0
        self._has_summary_offset: bool = self._footer.summary_offset_start != 0

        if enable_reconstruction == 'never':
            if not self._has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)
            if not self._has_summary_offset:
                error_msg = 'No summary offset section detected in MCAP'
                raise McapNoSummaryIndexError(error_msg)

        if not self._has_summary or enable_reconstruction == 'always':
            logging.debug('Building summary from data section')
            self._build_summary()
        elif self._has_summary and self._has_summary_offset:
            logging.debug('Loading summary offset into memory')
            self._load_summary_offset()
        else:
            logging.debug('Building summary offset from summary section')
            self._build_summary_offset()

    def _load_summary_offset(self):
        _ = self._file.seek_from_start(self._footer.summary_offset_start)
        while McapRecordParser.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordParser.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = record.group_start

    def _build_summary_offset(self) -> None:
        """Build summary offset mapping by scanning the summary section."""
        current_record: RecordId | None = None
        _ = self._file.seek_from_start(self._footer.summary_start)
        while McapRecordParser.peek_record(self._file) != McapRecordType.FOOTER:
            next_record = McapRecordParser.peek_record(self._file)
            if current_record is None or current_record != next_record:
                self._summary_offset[next_record] = self._file.tell()
                current_record = next_record
            McapRecordParser.skip_record(self._file)

    def _build_summary(self) -> None:
        """Build summary information by scanning the data section."""
        # Track message statistics
        message_count = 0
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
            else:
                McapRecordParser.skip_record(self._file)

        # Create statistics record from collected data
        self._cached_statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(found_schemas),
            channel_count=len(found_channels),
            attachment_count=0,  # Not tracked during reconstruction
            metadata_count=0,  # Not tracked during reconstruction
            chunk_count=0,  # Non-chunked files have no chunks
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )
        self._cached_schemas = found_schemas
        self._cached_channels = found_channels

    def get_schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        # Note _cached_schemas could be an empty dict!
        if self._cached_schemas is not None:
            return self._cached_schemas

        schemas: dict[SchemaId, SchemaRecord] = {}

        # If schema is in offset, then assume complete and return all
        if self._has_summary and McapRecordType.SCHEMA in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA])
            while McapRecordParser.peek_record(self._file) == McapRecordType.SCHEMA:
                if record := McapRecordParser.parse_schema(self._file):
                    schemas[record.id] = record
            self._cached_schemas = schemas
            return schemas

        # If schema is not in summary section, then we must search data section
        elif self._has_summary and McapRecordType.SCHEMA not in self._summary_offset:
            # If reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No schema records found in summary and reconstruction is disabled.')
                self._cached_schemas = schemas
                return schemas

            # Search for schema records in the data section
            logging.warning('No schema records found in summary. Searching through file!')
            _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
            while (next_record := McapRecordParser.peek_record(self._file)) != McapRecordType.DATA_END:
                if next_record != McapRecordType.SCHEMA:
                    McapRecordParser.skip_record(self._file)
                    continue
                if record := McapRecordParser.parse_schema(self._file):
                    schemas[record.id] = record
            self._cached_schemas = schemas
            return schemas

        # Either we built self._cached_schemas
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    def get_channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file."""
        # Note _cached_channels could be an empty dict!
        if self._cached_channels is not None:
            return self._cached_channels

        channels: dict[ChannelId, ChannelRecord] = {}

        # If channel is in offset, then assume complete and return all
        if self._has_summary and McapRecordType.CHANNEL in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL])
            while McapRecordParser.peek_record(self._file) == McapRecordType.CHANNEL:
                if record := McapRecordParser.parse_channel(self._file):
                    channels[record.id] = record
            self._cached_channels = channels
            return channels

        # If channel is not in summary section, then we must search data section
        elif self._has_summary and McapRecordType.CHANNEL not in self._summary_offset:
            # If we reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No channel records found in summary and reconstruction is disabled.')
                self._cached_channels = channels
                return channels

            # Search for channel records in the data section
            logging.warning('No channel records found in summary. Searching through file!')
            _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
            while (next_record := McapRecordParser.peek_record(self._file)) != McapRecordType.DATA_END:
                if next_record != McapRecordType.CHANNEL:
                    McapRecordParser.skip_record(self._file)
                    continue
                if record := McapRecordParser.parse_channel(self._file):
                    channels[record.id] = record
            self._cached_channels = channels
            return channels

        # Either we built self._cached_channels
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    def get_statistics(self) -> StatisticsRecord | None:
        """Get statistics about the MCAP file."""
        if self._cached_statistics is not None:
            return self._cached_statistics

        # If statistics is in offset, then assume complete and return all
        if self._has_summary and McapRecordType.STATISTICS in self._summary_offset:
            _ = self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS])
            record = McapRecordParser.parse_statistics(self._file)
            self._cached_statistics = record
            return record

        # If statistics is not in summary section, then we must recreate from data section
        # TODO: Can we use the summary section instead of reading through the file?
        elif self._has_summary and McapRecordType.STATISTICS not in self._summary_offset:
            # If we reconstruction not allowed, return empty
            if self._enable_reconstruction == 'never':
                logging.warning('No statistics record found in summary and reconstruction is disabled.')
                return None

            # Track message statistics
            schema_count = 0
            channel_count = 0
            message_count = 0
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
                else:
                    McapRecordParser.skip_record(self._file)

            # Create statistics record from collected data
            self._cached_statistics = StatisticsRecord(
                message_count=message_count,
                schema_count=schema_count,
                channel_count=channel_count,
                attachment_count=0,  # Not tracked during reconstruction
                metadata_count=0,  # Not tracked during reconstruction
                chunk_count=0,  # Non-chunked files have no chunks
                message_start_time=message_start_time or 0,
                message_end_time=message_end_time or 0,
                channel_message_counts=channel_message_counts,
            )
            return self._cached_statistics

        # Either we built self._cached_statistics
        # or we hit one of the if statements above
        raise RuntimeError("Impossible.")

    # TODO: Maybe build message index here?
