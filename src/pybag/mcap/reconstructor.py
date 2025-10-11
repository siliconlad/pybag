from typing import Literal, TypeAlias

from pybag.io.raw_reader import BaseReader
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
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
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
        enable_reconstruction: Controls reconstruction behavior:
            - 'never': Raise error if summary sections are missing
            - 'missing': Load from summary if present, otherwise reconstruct
            - 'always': Always reconstruct even if summary exists
    """

    def __init__(
        self,
        file: BaseReader,
        *,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:
        self._file = file

        # Read footer to determine if summary sections exist
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer = McapRecordParser.parse_footer(self._file)

        has_summary = footer.summary_start != 0
        has_summary_offset = footer.summary_offset_start != 0

        # Determine whether to load from summary or reconstruct
        if enable_reconstruction == 'never':
            if not has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)
            if not has_summary_offset:
                error_msg = 'No summary offset section detected in MCAP'
                raise McapNoSummaryIndexError(error_msg)
            should_reconstruct = False
        elif enable_reconstruction == 'always':
            should_reconstruct = True
        else:  # 'missing'
            should_reconstruct = not (has_summary and has_summary_offset)

        # Load or reconstruct summary data
        if should_reconstruct:
            (
                self._schemas,
                self._channels,
                self._chunk_indexes,
                self._statistics,
                self._offsets,
            ) = self._reconstruct_summary()
        else:
            (
                self._schemas,
                self._channels,
                self._chunk_indexes,
                self._statistics,
                self._offsets,
            ) = self._load_from_summary(footer.summary_start, footer.summary_offset_start)

        # Check if this is a chunked file
        if len(self._chunk_indexes) == 0:
            error_msg = 'No chunk indexes found in MCAP file'
            raise McapNoChunkIndexError(error_msg)

        # Reset file position to start of data section for consistent state
        self._file.seek_from_start(MAGIC_BYTES_SIZE)

    def _load_from_summary(
        self,
        summary_start: int,
        summary_offset_start: int,
    ) -> tuple[
        dict[SchemaId, SchemaRecord],
        dict[ChannelId, ChannelRecord],
        list[ChunkIndexRecord],
        StatisticsRecord,
        dict[RecordId, SummaryOffsetRecord],
    ]:
        """Load summary information from existing summary sections.

        Args:
            summary_start: Offset to the summary section
            summary_offset_start: Offset to the summary offset section

        Returns:
            Tuple of (schemas, channels, chunk_indexes, statistics, offsets)
        """
        # First, read all summary offset records to know where each record type group is
        self._file.seek_from_start(summary_offset_start)
        offsets: dict[RecordId, SummaryOffsetRecord] = {}

        # Calculate footer position for bounds checking
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer_position = self._file.tell()
        self._file.seek_from_start(summary_offset_start)

        # Read summary offset records until we hit the footer
        while self._file.tell() < footer_position:
            record_type = McapRecordParser.peek_record(self._file)
            if record_type == McapRecordType.FOOTER:
                break
            if record_type == McapRecordType.SUMMARY_OFFSET:
                offset_record = McapRecordParser.parse_summary_offset(self._file)
                offsets[offset_record.group_opcode] = offset_record
            else:
                McapRecordParser.skip_record(self._file)

        # Now read each record type group using the offsets
        schemas: dict[SchemaId, SchemaRecord] = {}
        channels: dict[ChannelId, ChannelRecord] = {}
        chunk_indexes: list[ChunkIndexRecord] = []
        statistics: StatisticsRecord | None = None

        # Read schemas
        if McapRecordType.SCHEMA in offsets:
            schema_offset = offsets[McapRecordType.SCHEMA]
            self._file.seek_from_start(schema_offset.group_start)
            bytes_read = 0
            while bytes_read < schema_offset.group_length:
                pos_before = self._file.tell()
                schema = McapRecordParser.parse_schema(self._file)
                if schema is not None:  # Skip schemas with id == 0
                    schemas[schema.id] = schema
                bytes_read += self._file.tell() - pos_before

        # Read channels
        if McapRecordType.CHANNEL in offsets:
            channel_offset = offsets[McapRecordType.CHANNEL]
            self._file.seek_from_start(channel_offset.group_start)
            bytes_read = 0
            while bytes_read < channel_offset.group_length:
                pos_before = self._file.tell()
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
                bytes_read += self._file.tell() - pos_before

        # Read chunk indexes
        if McapRecordType.CHUNK_INDEX in offsets:
            chunk_index_offset = offsets[McapRecordType.CHUNK_INDEX]
            self._file.seek_from_start(chunk_index_offset.group_start)
            bytes_read = 0
            while bytes_read < chunk_index_offset.group_length:
                pos_before = self._file.tell()
                chunk_index = McapRecordParser.parse_chunk_index(self._file)
                chunk_indexes.append(chunk_index)
                bytes_read += self._file.tell() - pos_before

        # Read statistics
        if McapRecordType.STATISTICS in offsets:
            statistics_offset = offsets[McapRecordType.STATISTICS]
            self._file.seek_from_start(statistics_offset.group_start)
            statistics = McapRecordParser.parse_statistics(self._file)

        # Sort chunk indexes by message start time for efficient access
        chunk_indexes.sort(key=lambda ci: ci.message_start_time)

        # If no statistics record found, create a default one
        if statistics is None:
            statistics = StatisticsRecord(
                message_count=0,
                schema_count=0,
                channel_count=0,
                attachment_count=0,
                metadata_count=0,
                chunk_count=0,
                message_start_time=0,
                message_end_time=0,
                channel_message_counts={},
            )

        return schemas, channels, chunk_indexes, statistics, offsets

    def _reconstruct_summary(
        self,
    ) -> tuple[
        dict[SchemaId, SchemaRecord],
        dict[ChannelId, ChannelRecord],
        list[ChunkIndexRecord],
        StatisticsRecord,
        dict[RecordId, SummaryOffsetRecord],
    ]:
        """Reconstruct summary information by scanning the data section.

        Returns:
            Tuple of (schemas, channels, chunk_indexes, statistics, offsets)
        """
        schemas: dict[SchemaId, SchemaRecord] = {}
        channels: dict[ChannelId, ChannelRecord] = {}
        chunk_indexes: list[ChunkIndexRecord] = []

        # Seek to start of data section (after magic bytes)
        self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Calculate footer position by seeking to end and back
        current_pos = self._file.tell()
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer_position = self._file.tell()
        self._file.seek_from_start(current_pos)

        # Iterate through records until we reach the footer
        while self._file.tell() < footer_position:
            record_type = McapRecordParser.peek_record(self._file)

            if record_type == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(self._file)
                if schema is not None:  # Skip schemas with id == 0
                    schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
            elif record_type == McapRecordType.CHUNK_INDEX:
                chunk_index = McapRecordParser.parse_chunk_index(self._file)
                chunk_indexes.append(chunk_index)
            elif record_type == McapRecordType.FOOTER:
                # Reached footer, stop
                break
            else:
                # Skip other record types (header, chunk, message, etc.)
                McapRecordParser.skip_record(self._file)

        # Sort chunk indexes by message start time
        chunk_indexes.sort(key=lambda ci: ci.message_start_time)

        # Calculate statistics from collected data
        if chunk_indexes:
            message_count = sum(ci.message_index_length for ci in chunk_indexes)

            # Calculate per-channel message counts and accurate start/end times by reading message indexes
            channel_message_counts: dict[int, int] = {}
            all_log_times: list[int] = []
            for ci in chunk_indexes:
                for channel_id, message_index_offset in ci.message_index_offsets.items():
                    # Read the message index record to count messages for this channel in this chunk
                    self._file.seek_from_start(message_index_offset)
                    message_index = McapRecordParser.parse_message_index(self._file)
                    # Each record in the message index represents one message
                    message_count_in_chunk = len(message_index.records)
                    channel_message_counts[channel_id] = channel_message_counts.get(channel_id, 0) + message_count_in_chunk
                    # Collect log times for accurate start/end time calculation
                    all_log_times.extend(log_time for log_time, _ in message_index.records)

            # Use actual message log times for start/end time
            message_start_time = min(all_log_times) if all_log_times else 0
            message_end_time = max(all_log_times) if all_log_times else 0
            statistics = StatisticsRecord(
                message_count=message_count,
                schema_count=len(schemas),
                channel_count=len(channels),
                attachment_count=0,  # Not tracked during reconstruction
                metadata_count=0,  # Not tracked during reconstruction
                chunk_count=len(chunk_indexes),
                message_start_time=message_start_time,
                message_end_time=message_end_time,
                channel_message_counts=channel_message_counts,
            )
        else:
            # No chunks, create empty statistics
            statistics = StatisticsRecord(
                message_count=0,
                schema_count=len(schemas),
                channel_count=len(channels),
                attachment_count=0,
                metadata_count=0,
                chunk_count=0,
                message_start_time=0,
                message_end_time=0,
                channel_message_counts={},
            )

        # No offsets available when reconstructing
        offsets: dict[RecordId, SummaryOffsetRecord] = {}

        return schemas, channels, chunk_indexes, statistics, offsets

    @property
    def schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        return self._schemas

    @property
    def channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file."""
        return self._channels

    @property
    def chunk_indexes(self) -> list[ChunkIndexRecord]:
        """Get all chunk indexes, sorted by message_start_time."""
        return self._chunk_indexes

    @property
    def statistics(self) -> StatisticsRecord:
        """Get statistics about the MCAP file."""
        return self._statistics

    @property
    def offsets(self) -> dict[RecordId, SummaryOffsetRecord]:
        """Get summary offset records (empty if reconstructed)."""
        return self._offsets

    # TODO: Implement attachment index
    # TODO: Implement metadata index


class McapNonChunkedSummary:
    """Summary information for a non-chunked MCAP file.

    This class handles loading summary information from a non-chunked MCAP file,
    either from existing summary sections or by reconstructing it from the data section.

    Args:
        file: The file reader to read from.
        enable_reconstruction: Controls reconstruction behavior:
            - 'never': Raise error if summary sections are missing
            - 'missing': Load from summary if present, otherwise reconstruct
            - 'always': Always reconstruct even if summary exists
    """

    def __init__(
        self,
        file: BaseReader,
        *,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:
        self._file = file

        # Read footer to determine if summary sections exist
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer = McapRecordParser.parse_footer(self._file)

        has_summary = footer.summary_start != 0
        has_summary_offset = footer.summary_offset_start != 0

        # Determine whether to load from summary or reconstruct
        if enable_reconstruction == 'never':
            if not has_summary:
                error_msg = 'No summary section detected in MCAP'
                raise McapNoSummarySectionError(error_msg)
            if not has_summary_offset:
                error_msg = 'No summary offset section detected in MCAP'
                raise McapNoSummaryIndexError(error_msg)
            should_reconstruct = False
        elif enable_reconstruction == 'always':
            should_reconstruct = True
        else:  # 'missing'
            should_reconstruct = not (has_summary and has_summary_offset)

        # Load or reconstruct summary data
        if should_reconstruct:
            (
                self._schemas,
                self._channels,
                self._statistics,
                self._offsets,
                self._message_indexes,
            ) = self._reconstruct_summary()
        else:
            (
                self._schemas,
                self._channels,
                self._statistics,
                self._offsets,
                self._message_indexes,
            ) = self._load_from_summary(footer.summary_start, footer.summary_offset_start)

        # Reset file position to start of data section for consistent state
        self._file.seek_from_start(MAGIC_BYTES_SIZE)

    def _scan_data_section_for_messages(
        self,
        summary_start: int,
    ) -> dict[ChannelId, dict[LogTime, list[Offset]]]:
        """Scan the data section to build message indexes.

        Args:
            summary_start: Offset where the summary section starts

        Returns:
            Message indexes mapping channel IDs to log times to offsets
        """
        message_indexes: dict[ChannelId, dict[LogTime, list[Offset]]] = {}

        # Seek to start of data section (after magic bytes)
        self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Iterate through records until we reach the summary section
        while self._file.tell() < summary_start:
            record_type = McapRecordParser.peek_record(self._file)

            if record_type == McapRecordType.MESSAGE:
                # Get offset before parsing
                message_offset = self._file.tell()
                message = McapRecordParser.parse_message(self._file)

                # Track message in index
                channel_id = message.channel_id
                log_time = message.log_time

                if channel_id not in message_indexes:
                    message_indexes[channel_id] = {}
                if log_time not in message_indexes[channel_id]:
                    message_indexes[channel_id][log_time] = []
                message_indexes[channel_id][log_time].append(message_offset)
            else:
                # Skip other record types
                McapRecordParser.skip_record(self._file)

        return message_indexes

    def _load_from_summary(
        self,
        summary_start: int,
        summary_offset_start: int,
    ) -> tuple[
        dict[SchemaId, SchemaRecord],
        dict[ChannelId, ChannelRecord],
        StatisticsRecord,
        dict[RecordId, SummaryOffsetRecord],
        dict[ChannelId, dict[LogTime, list[Offset]]],
    ]:
        """Load summary information from existing summary sections.

        Args:
            summary_start: Offset to the summary section
            summary_offset_start: Offset to the summary offset section

        Returns:
            Tuple of (schemas, channels, statistics, offsets, message_indexes)
        """
        # First, read all summary offset records to know where each record type group is
        self._file.seek_from_start(summary_offset_start)
        offsets: dict[RecordId, SummaryOffsetRecord] = {}

        # Calculate footer position for bounds checking
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer_position = self._file.tell()
        self._file.seek_from_start(summary_offset_start)

        # Read summary offset records until we hit the footer
        while self._file.tell() < footer_position:
            record_type = McapRecordParser.peek_record(self._file)
            if record_type == McapRecordType.FOOTER:
                break
            if record_type == McapRecordType.SUMMARY_OFFSET:
                offset_record = McapRecordParser.parse_summary_offset(self._file)
                offsets[offset_record.group_opcode] = offset_record
            else:
                McapRecordParser.skip_record(self._file)

        # Now read each record type group using the offsets
        schemas: dict[SchemaId, SchemaRecord] = {}
        channels: dict[ChannelId, ChannelRecord] = {}
        message_indexes: dict[ChannelId, dict[LogTime, list[Offset]]] = {}
        statistics: StatisticsRecord | None = None

        # Read schemas
        if McapRecordType.SCHEMA in offsets:
            schema_offset = offsets[McapRecordType.SCHEMA]
            self._file.seek_from_start(schema_offset.group_start)
            bytes_read = 0
            while bytes_read < schema_offset.group_length:
                pos_before = self._file.tell()
                schema = McapRecordParser.parse_schema(self._file)
                if schema is not None:  # Skip schemas with id == 0
                    schemas[schema.id] = schema
                bytes_read += self._file.tell() - pos_before

        # Read channels
        if McapRecordType.CHANNEL in offsets:
            channel_offset = offsets[McapRecordType.CHANNEL]
            self._file.seek_from_start(channel_offset.group_start)
            bytes_read = 0
            while bytes_read < channel_offset.group_length:
                pos_before = self._file.tell()
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
                bytes_read += self._file.tell() - pos_before

        # Read message indexes
        if McapRecordType.MESSAGE_INDEX in offsets:
            message_index_offset = offsets[McapRecordType.MESSAGE_INDEX]
            self._file.seek_from_start(message_index_offset.group_start)
            bytes_read = 0
            while bytes_read < message_index_offset.group_length:
                pos_before = self._file.tell()
                message_index = McapRecordParser.parse_message_index(self._file)

                # Build the message_indexes structure
                channel_id = message_index.channel_id
                if channel_id not in message_indexes:
                    message_indexes[channel_id] = {}

                # Each record in the message index is a tuple of (log_time, offset)
                for log_time, offset in message_index.records:
                    if log_time not in message_indexes[channel_id]:
                        message_indexes[channel_id][log_time] = []
                    message_indexes[channel_id][log_time].append(offset)

                bytes_read += self._file.tell() - pos_before

        # Read statistics
        if McapRecordType.STATISTICS in offsets:
            statistics_offset = offsets[McapRecordType.STATISTICS]
            self._file.seek_from_start(statistics_offset.group_start)
            statistics = McapRecordParser.parse_statistics(self._file)

        # If no statistics record found, create a default one
        if statistics is None:
            statistics = StatisticsRecord(
                message_count=0,
                schema_count=0,
                channel_count=0,
                attachment_count=0,
                metadata_count=0,
                chunk_count=0,
                message_start_time=0,
                message_end_time=0,
                channel_message_counts={},
            )

        # If no message indexes found in summary, scan data section for messages
        # This happens when the writer creates a summary without message index records
        if not message_indexes:
            message_indexes = self._scan_data_section_for_messages(summary_start)

        return schemas, channels, statistics, offsets, message_indexes

    def _reconstruct_summary(
        self,
    ) -> tuple[
        dict[SchemaId, SchemaRecord],
        dict[ChannelId, ChannelRecord],
        StatisticsRecord,
        dict[RecordId, SummaryOffsetRecord],
        dict[ChannelId, dict[LogTime, list[Offset]]],
    ]:
        """Reconstruct summary information by scanning the data section.

        Returns:
            Tuple of (schemas, channels, statistics, offsets, message_indexes)
        """
        schemas: dict[SchemaId, SchemaRecord] = {}
        channels: dict[ChannelId, ChannelRecord] = {}
        message_indexes: dict[ChannelId, dict[LogTime, list[Offset]]] = {}

        # Track message statistics
        message_count = 0
        message_start_time: int | None = None
        message_end_time: int | None = None
        channel_message_counts: dict[int, int] = {}

        # Seek to start of data section (after magic bytes)
        self._file.seek_from_start(MAGIC_BYTES_SIZE)

        # Calculate footer position by seeking to end and back
        current_pos = self._file.tell()
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        footer_position = self._file.tell()
        self._file.seek_from_start(current_pos)

        # Iterate through records until we reach the footer
        while self._file.tell() < footer_position:
            record_type = McapRecordParser.peek_record(self._file)

            if record_type == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(self._file)
                if schema is not None:  # Skip schemas with id == 0
                    schemas[schema.id] = schema
            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(self._file)
                channels[channel.id] = channel
            elif record_type == McapRecordType.MESSAGE:
                # Get offset before parsing (after record type byte)
                message_offset = self._file.tell()
                message = McapRecordParser.parse_message(self._file)

                # Track message in index
                channel_id = message.channel_id
                log_time = message.log_time

                if channel_id not in message_indexes:
                    message_indexes[channel_id] = {}
                if log_time not in message_indexes[channel_id]:
                    message_indexes[channel_id][log_time] = []
                message_indexes[channel_id][log_time].append(message_offset)

                # Update statistics
                message_count += 1
                channel_message_counts[channel_id] = channel_message_counts.get(channel_id, 0) + 1

                if message_start_time is None or log_time < message_start_time:
                    message_start_time = log_time
                if message_end_time is None or log_time > message_end_time:
                    message_end_time = log_time
            elif record_type == McapRecordType.FOOTER:
                # Reached footer, stop
                break
            else:
                # Skip other record types (header, attachment, metadata, etc.)
                McapRecordParser.skip_record(self._file)

        # Create statistics record from collected data
        statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(schemas),
            channel_count=len(channels),
            attachment_count=0,  # Not tracked during reconstruction
            metadata_count=0,  # Not tracked during reconstruction
            chunk_count=0,  # Non-chunked files have no chunks
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=channel_message_counts,
        )

        # No offsets available when reconstructing
        offsets: dict[RecordId, SummaryOffsetRecord] = {}

        return schemas, channels, statistics, offsets, message_indexes

    @property
    def schemas(self) -> dict[SchemaId, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        return self._schemas

    @property
    def channels(self) -> dict[ChannelId, ChannelRecord]:
        """Get all channels defined in the MCAP file."""
        return self._channels

    @property
    def statistics(self) -> StatisticsRecord:
        """Get statistics about the MCAP file."""
        return self._statistics

    @property
    def offsets(self) -> dict[RecordId, SummaryOffsetRecord]:
        """Get summary offset records (empty if reconstructed)."""
        return self._offsets

    @property
    def message_indexes(self) -> dict[ChannelId, dict[LogTime, list[Offset]]]:
        """Get message indexes mapping channel IDs to log times to message offsets."""
        return self._message_indexes
