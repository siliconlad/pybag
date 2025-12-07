import heapq
import logging
from abc import ABC, abstractmethod
from functools import lru_cache
from pathlib import Path
from typing import Generator, Iterator, Literal

from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.mcap.chunk import decompress_chunk
from pybag.mcap.crc import assert_crc
from pybag.mcap.error import (
    McapNoChunkError,
    McapNoChunkIndexError,
    McapNoStatisticsError,
    McapNoSummaryIndexError,
    McapNoSummarySectionError,
    McapUnexpectedChunkIndexError,
    McapUnknownSchemaError
)
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser,
    McapRecordType
)
from pybag.mcap.records import (
    AttachmentIndexRecord,
    AttachmentRecord,
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    FooterRecord,
    HeaderRecord,
    MessageIndexRecord,
    MessageRecord,
    MetadataIndexRecord,
    MetadataRecord,
    SchemaRecord,
    StatisticsRecord
)
from pybag.mcap.summary import McapChunkedSummary, McapNonChunkedSummary

logger = logging.getLogger(__name__)


# TODO: Is this the minimal set of methods needed?
class BaseMcapRecordReader(ABC):
    @abstractmethod
    def __enter__(self) -> 'BaseMcapRecordReader':
        ...  # pragma: no cover

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        ...  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        ...  # pragma: no cover

    @abstractmethod
    def get_header(self) -> HeaderRecord:
        """Get the header record from the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        ...  # pragma: no cover

    # Schema Management

    @abstractmethod
    def get_schemas(self) -> dict[int, SchemaRecord]:
        """Get all schemas defined in the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """Get a schema by its ID."""
        ...  # pragma: no cover

    @abstractmethod
    def get_channel_schema(self, channel_id: int) -> SchemaRecord | None:
        """Get the schema for a given channel ID."""
        ...  # pragma: no cover

    @abstractmethod
    def get_message_schema(self, message: MessageRecord) -> SchemaRecord:
        """Get the schema for a given message."""
        ...  # pragma: no cover

    # Channel Management

    @abstractmethod
    def get_channels(self) -> dict[int, ChannelRecord]:
        """Get all channels/topics in the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_channel(self, channel_id: int) -> ChannelRecord | None:
        """Get a channel by its ID."""
        ...  # pragma: no cover

    @abstractmethod
    def get_channel_id(self, topic: str) -> int | None:
        """Get a channel ID by its topic."""
        ...  # pragma: no cover

    # Message Index Management

    @abstractmethod
    def get_message_indexes(self, chunk_index: ChunkIndexRecord) -> dict[int, MessageIndexRecord]:
        """Get all message indexes from the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_message_index(self, chunk_index: ChunkIndexRecord, channel_id: int) -> MessageIndexRecord | None:
        """Get a message index for a given channel ID."""
        ...  # pragma: no cover

    # Chunk Management

    @abstractmethod
    def get_chunk_indexes(self, channel_id: int | list[int] | None = None) -> list[ChunkIndexRecord]:
        """Get all chunk indexes from the MCAP file."""
        ...  # pragma: no cover

    @abstractmethod
    def get_chunk(self, chunk_index: ChunkIndexRecord) -> ChunkRecord:
        """Get a chunk by its index."""
        ...  # pragma: no cover

    # Message Management

    @abstractmethod
    def get_message(
        self,
        channel_id: int,
        timestamp: int | None = None,
    ) -> MessageRecord | None:
        ...  # pragma: no cover

    @abstractmethod
    def get_messages(
        self,
        channel_id: int | list[int] | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        ...  # pragma: no cover

    # Attachment Management

    @abstractmethod
    def get_attachments(
        self,
        name: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[AttachmentRecord]:
        """Get attachments from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all attachments.
                  If provided, returns only attachments with matching name.
            start_time: Optional start time filter in nanoseconds. If provided,
                        only attachments with log_time >= start_time are returned.
            end_time: Optional end time filter in nanoseconds. If provided,
                      only attachments with log_time <= end_time are returned.

        Returns:
            List of AttachmentRecord objects.
        """
        ...  # pragma: no cover

    # Metadata Management

    @abstractmethod
    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all metadata records.
                  If provided, returns only metadata records with matching name.

        Returns:
            List of MetadataRecord objects.
        """
        ...  # pragma: no cover


class McapChunkedReader(BaseMcapRecordReader):
    """Class to efficiently get records from a chunked MCAP file.

    Args:
        file: The file to read from.
        enable_crc_check: Whether to validate the crc values in the mcap (slow!)
        enable_summary_reconstruction:
            - 'missing' allows reconstruction if the summary section is missing.
            - 'never' throws an exception if the summary (or summary offset) section is missing.
            - 'always' forces reconstruction even if the summary section is present.
        chunk_cache_size: The number of decompressed chunks to store in memory at a time.
    """

    def __init__(
        self,
        file: BaseReader,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        chunk_cache_size: int = 1,
    ):
        self._file = file
        self._check_crc = enable_crc_check

        self._version = McapRecordParser.parse_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        # Mcap summary abstraction
        self._summary = McapChunkedSummary(
            self._file,
            enable_crc_check=self._check_crc,
            enable_reconstruction=enable_summary_reconstruction,
            load_summary_eagerly=False,
        )

        # Caches for message indexes
        self._message_indexes: dict[int, dict[int, MessageIndexRecord]] = {}

        # LRU cache for decompressed chunks (key: chunk_start_offset)
        self._decompress_chunk_cached = lru_cache(maxsize=chunk_cache_size)(self._decompress_chunk_impl)

    # Helpful Constructors

    @staticmethod
    def from_file(
        file_path: Path | str,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        chunk_cache_size: int = 1,
    ) -> 'McapChunkedReader':
        """Create a new MCAP reader from a file.

        Args:
            file_path: Path to the MCAP file
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists
            chunk_cache_size: The number of decompressed chunks to store in memory at a time.

        Returns:
            A McapChunkedReader instance
        """
        logging.debug('Creating McapChunkedReader')
        return McapChunkedReader(
            FileReader(file_path),
            enable_crc_check=enable_crc_check,
            enable_summary_reconstruction=enable_summary_reconstruction,
            chunk_cache_size=chunk_cache_size,
        )

    @staticmethod
    def from_bytes(
        data: bytes,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        chunk_cache_size: int = 1,
    ) -> 'McapChunkedReader':
        """Create a new MCAP reader from a bytes object.

        Args:
            data: Bytes containing the MCAP file data
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists
            chunk_cache_size: The number of decompressed chunks to store in memory at a time.

        Returns:
            A McapChunkedReader instance
        """
        logging.debug('Creating McapChunkedReader')
        return McapChunkedReader(
            BytesReader(data),
            enable_crc_check=enable_crc_check,
            enable_summary_reconstruction=enable_summary_reconstruction,
            chunk_cache_size=chunk_cache_size,
        )

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
        if (record := self._summary.get_statistics()) is None:
            raise McapNoStatisticsError("No statistics record found in MCAP")
        return record

    # Schema Management

    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        return self._summary.get_schemas()

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
        return self._summary.get_channels()

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

        if chunk_index.message_index_offsets:
            message_index: dict[int, MessageIndexRecord] = {}
            for channel_id, message_index_offset in chunk_index.message_index_offsets.items():
                _ = self._file.seek_from_start(message_index_offset)
                message_index[channel_id] = McapRecordParser.parse_message_index(self._file)
                message_index[channel_id].records.sort(key=lambda x: (x[0], x[1]))
        else:
            message_index = self._summary.get_message_indexes(chunk_index)
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

    def get_chunk_indexes(self, channel_id: int | list[int] | None = None) -> list[ChunkIndexRecord]:
        """
        Get all chunk indexes from the MCAP file.

        Args:
            channel_id: The ID of the channel(s) to get the chunk indexes for.
                        Can be a single int, a list of ints, or None for all channels.

        Returns:
            A list of ChunkIndexRecord objects.
        """
        if channel_id is None:
            return self._summary.get_chunk_indexes()

        # Normalize to list for consistent processing
        channel_ids = [channel_id] if isinstance(channel_id, int) else channel_id

        chunk_indexes: list[ChunkIndexRecord] = []
        for chunk_index in self._summary.get_chunk_indexes():
            # Check if any of the requested channels are in this chunk
            if any(cid in chunk_index.message_index_offsets for cid in channel_ids):
                chunk_indexes.append(chunk_index)
                continue
            if chunk_index.message_index_offsets:
                continue
            # Fall back to checking message indexes
            message_indexes = self._summary.get_message_indexes(chunk_index)
            if any(cid in message_indexes for cid in channel_ids):
                chunk_indexes.append(chunk_index)
        return chunk_indexes

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

    def _decompress_chunk_impl(self, chunk_offset: int) -> bytes:
        """Internal implementation for chunk decompression (cached).

        Args:
            chunk_offset: Chunk start offset (used as cache key)

        Returns:
            Decompressed chunk data
        """
        # Seek to the chunk and read it
        self._file.seek_from_start(chunk_offset)
        chunk = McapRecordParser.parse_chunk(self._file)
        return decompress_chunk(chunk, check_crc=self._check_crc)

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
                    continue  # TODO: What happens for overlapping chunks?

                if not message_index.records:
                    continue

                if timestamp is None:
                    offset = message_index.records[0][1]
                else:
                    offset = next((r[1] for r in message_index.records if r[0] == timestamp), None)

                if offset is None:
                    continue

                # Read data from chunk (using cache)
                reader = BytesReader(self._decompress_chunk_cached(chunk_index.chunk_start_offset))
                _ = reader.seek_from_start(offset)
                return McapRecordParser.parse_message(reader)
        return None

    def get_messages(
        self,
        channel_id: int | list[int] | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        """
        Get messages from the MCAP file.

        If no channel is provided, messages from all channels are returned.
        If the start and end timestamps are not provided, the entire available range is returned.

        Args:
            channel_id: Optional channel ID(s) to filter by. Can be:
                - int: Single channel ID
                - list[int]: Multiple channel IDs (reads chunks once, filters to these channels)
                - None: All channels
            start_timestamp: The start timestamp to filter by. If None, no filtering is done.
            end_timestamp: The end timestamp to filter by. If None, no filtering is done.
            in_log_time_order: Return messages in log time order if True, otherwise in write order.
            in_reverse: Return messages in reverse order (last first) if True.

        Returns:
            A generator of MessageRecord objects.
        """
        # Normalize channel_id to a set for efficient filtering
        if channel_id is None:
            channel_id_set = None  # All channels
        elif isinstance(channel_id, list):
            if not channel_id:  # Empty list
                return
            channel_id_set = set(channel_id)  # Multiple specific channels
        else:
            channel_id_set = {channel_id}  # Single channel

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

        if not in_log_time_order:
            yield from self._get_messages_write_order(
                relevant_chunks,
                channel_id_set,
                start_timestamp,
                end_timestamp,
                in_reverse=in_reverse,
            )
            return

        if self._has_overlapping_chunks(relevant_chunks):
            logger.warning("Detected time-overlapping chunks. Reading performance is affected!")
            yield from self._get_messages_with_overlaps(
                relevant_chunks,
                channel_id_set,
                start_timestamp,
                end_timestamp,
                in_reverse=in_reverse,
            )
        else:
            yield from self._get_messages_sequential(
                relevant_chunks,
                channel_id_set,
                start_timestamp,
                end_timestamp,
                in_reverse=in_reverse,
            )

    def _has_overlapping_chunks(self, chunks: list[ChunkIndexRecord]) -> bool:
        """Check if chunks have overlapping time ranges."""
        if len(chunks) <= 1:
            return False

        # Chunks should already be sorted by message_start_time
        for i in range(len(chunks) - 1):
            current_chunk_end_time = chunks[i].message_end_time
            next_chunk_start_time = chunks[i + 1].message_start_time
            if current_chunk_end_time >= next_chunk_start_time:
                return True

        return False

    def _get_messages_sequential(
        self,
        chunks: list[ChunkIndexRecord],
        channel_id_set: set[int] | None,
        start_timestamp: int | None,
        end_timestamp: int | None,
        *,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        """Fast sequential reading for non-overlapping chunks.

        Args:
            chunks: List of chunk indexes to read from
            channel_id_set: Set of channel IDs to filter by, or None for all channels
            start_timestamp: Start timestamp filter
            end_timestamp: End timestamp filter
            in_reverse: If True, yield messages in reverse time order (latest first)

        Yields:
            MessageRecord objects matching the filters
        """
        # For reverse iteration, process chunks in reverse order (by end time descending)
        chunks_to_process = reversed(chunks) if in_reverse else chunks

        for chunk_index in chunks_to_process:
            if channel_id_set is None:
                # All channels in this chunk
                message_indexes = self.get_message_indexes(chunk_index).values()
            else:
                # Get message indexes for requested channels only
                message_indexes = []
                for ch_id in channel_id_set:
                    if message_index := self.get_message_index(chunk_index, ch_id):
                        message_indexes.append(message_index)

            if not message_indexes:
                continue

            # Collect (timestamp, offset) tuples for sorting
            entries: list[tuple[int, int]] = []
            for message_index in message_indexes:
                for timestamp, offset in message_index.records:
                    if start_timestamp is not None and timestamp < start_timestamp:
                        continue
                    if end_timestamp is not None and timestamp > end_timestamp:
                        continue
                    entries.append((timestamp, offset))
            if not entries:
                continue

            # Sort by timestamp (and offset for determinism), reverse if needed
            entries.sort(key=lambda x: (x[0], x[1]), reverse=in_reverse)

            reader = BytesReader(self._decompress_chunk_cached(chunk_index.chunk_start_offset))
            for _, offset in entries:
                reader.seek_from_start(offset)
                yield McapRecordParser.parse_message(reader)

    def _get_messages_with_overlaps(
        self,
        chunks: list[ChunkIndexRecord],
        channel_id_set: set[int] | None,
        start_timestamp: int | None,
        end_timestamp: int | None,
        *,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        """Streaming overlap-safe reading using heap-based merge of per-chunk iterators.

        Args:
            chunks: List of chunk indexes to read from
            channel_id_set: Set of channel IDs to filter by, or None for all channels
            start_timestamp: Start timestamp filter
            end_timestamp: End timestamp filter
            in_reverse: If True, yield messages in reverse time order (latest first)

        Yields:
            MessageRecord objects in log time order (or reverse if specified)
        """

        def chunk_message_iterator(
            chunk_index_id: int,
            chunk_index: ChunkIndexRecord
        ) -> Iterator[tuple[int, int, MessageRecord]]:
            """Create an iterator that yields (timestamp, chunk_id, message) tuples for a chunk."""
            if channel_id_set is None:
                # All channels in this chunk
                message_indexes = self.get_message_indexes(chunk_index).values()
            else:
                # Get message indexes for requested channels only
                message_indexes = []
                for ch_id in channel_id_set:
                    if message_index := self.get_message_index(chunk_index, ch_id):
                        message_indexes.append(message_index)

            if not message_indexes:
                return

            # Collect and sort message references for this chunk
            # The records should already by sorted by timestamp + offset
            message_refs = []
            for message_index in message_indexes:
                for timestamp, offset in message_index.records:
                    if start_timestamp is not None and timestamp < start_timestamp:
                        continue
                    if end_timestamp is not None and timestamp > end_timestamp:
                        continue
                    message_refs.append((timestamp, offset))
            message_refs.sort()  # Sort to make sure timestamps are in correct order

            if not message_refs:
                return

            # Load the chunk once and parse messages as needed (using cache)
            reader = BytesReader(self._decompress_chunk_cached(chunk_index.chunk_start_offset))
            message_refs_it = reversed(message_refs) if in_reverse else iter(message_refs)
            for timestamp, offset in message_refs_it:
                reader.seek_from_start(offset)
                message = McapRecordParser.parse_message(reader)
                yield timestamp, chunk_index_id, message

        chunk_iterators = [
            iterator
            for i, chunk_index in enumerate(chunks)
            if (iterator := chunk_message_iterator(i, chunk_index)) is not None
        ]
        # Sort by the timestamp and break ties with the order of the chunk
        # For reverse, negate both timestamp and chunk_index_id to get descending order from heapq.merge
        heapq_key = (lambda x: (-x[0], -x[1])) if in_reverse else (lambda x: (x[0], x[1]))
        for _, _, message in heapq.merge(*chunk_iterators, key=heapq_key):
            yield message

    def _get_messages_write_order(
        self,
        chunks: list[ChunkIndexRecord],
        channel_id_set: set[int] | None,
        start_timestamp: int | None,
        end_timestamp: int | None,
        *,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        """Read messages preserving the order they were written to the file.

        Args:
            chunks: List of chunk indexes to read from
            channel_id_set: Set of channel IDs to filter by, or None for all channels
            start_timestamp: Start timestamp filter
            end_timestamp: End timestamp filter
            in_reverse: If True, yield messages in reverse write order (last written first)

        Yields:
            MessageRecord objects in write order (or reverse write order if in_reverse)
        """

        for chunk_index in sorted(chunks, key=lambda ci: ci.chunk_start_offset, reverse=in_reverse):
            if channel_id_set is None:
                # All channels in this chunk
                message_indexes = self.get_message_indexes(chunk_index).values()
            else:
                # Get message indexes for requested channels only
                message_indexes = []
                for ch_id in channel_id_set:
                    if message_index := self.get_message_index(chunk_index, ch_id):
                        message_indexes.append(message_index)

            if not message_indexes:
                continue

            entries: list[tuple[int, int]] = []
            for message_index in message_indexes:
                for timestamp, offset in message_index.records:
                    if start_timestamp is not None and timestamp < start_timestamp:
                        continue
                    if end_timestamp is not None and timestamp > end_timestamp:
                        continue
                    entries.append((timestamp, offset))

            if not entries:
                continue

            entries.sort(key=lambda x: x[1], reverse=in_reverse)

            reader = BytesReader(self._decompress_chunk_cached(chunk_index.chunk_start_offset))
            for _, offset in entries:
                reader.seek_from_start(offset)
                yield McapRecordParser.parse_message(reader)

    def get_attachments(
        self,
        name: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[AttachmentRecord]:
        """Get attachments from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all attachments.
                  If provided, returns only attachments with matching name.
            start_time: Optional start time filter in nanoseconds. If provided,
                        only attachments with log_time >= start_time are returned.
            end_time: Optional end time filter in nanoseconds. If provided,
                      only attachments with log_time <= end_time are returned.

        Returns:
            List of AttachmentRecord objects.
        """
        attachment_indexes = self._summary.get_attachment_indexes()
        if name and name not in attachment_indexes:
            logging.warning(f'{name} not found in attachments!')
            return []

        attachment_indexes_flat: list[AttachmentIndexRecord] = []
        if name is None:
            for i in attachment_indexes.values():
                attachment_indexes_flat.extend(i)
            # Preserve order in which they were written to mcap
            attachment_indexes_flat.sort(key=lambda x: x.offset)
        else:
            attachment_indexes_flat = attachment_indexes.get(name, [])
        logging.debug(f'Found {len(attachment_indexes_flat)} attachment indexes')

        # Filter by time using the index's log_time field
        if start_time is not None:
            attachment_indexes_flat = [a for a in attachment_indexes_flat if a.log_time >= start_time]
        if end_time is not None:
            attachment_indexes_flat = [a for a in attachment_indexes_flat if a.log_time <= end_time]

        current_pos = self._file.tell()
        attachments: list[AttachmentRecord] = []
        for attachment_index in attachment_indexes_flat:
            _ = self._file.seek_from_start(attachment_index.offset)
            attachment = McapRecordParser.parse_attachment(self._file)
            if self._check_crc and attachment.crc:  # If crc is 0, do not check
                assert_crc(attachment.data, attachment.crc)
            attachments.append(attachment)
        else:
            _ = self._file.seek_from_start(current_pos)

        return attachments

    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all metadata records.
                  If provided, returns only metadata records with matching name.

        Returns:
            List of MetadataRecord objects.
        """
        metadata_indexes = self._summary.get_metadata_indexes()
        if name and name not in metadata_indexes:
            logging.warning(f'{name} not found in metadata!')
            return []

        metadata_indexes_flat: list[MetadataIndexRecord] = []
        if name is None:
            for i in metadata_indexes.values():
                metadata_indexes_flat.extend(i)
            # Preserve order in which they were written to mcap
            metadata_indexes_flat.sort(key=lambda x: x.offset)
        else:
            metadata_indexes_flat = metadata_indexes.get(name, [])

        current_pos = self._file.tell()
        metadata_records: list[MetadataRecord] = []
        for metadata_index in metadata_indexes_flat:
            _ = self._file.seek_from_start(metadata_index.offset)
            metadata = McapRecordParser.parse_metadata(self._file)
            metadata_records.append(metadata)
        else:
            _ = self._file.seek_from_start(current_pos)

        return metadata_records


class McapNonChunkedReader(BaseMcapRecordReader):
    """Class to efficiently get records from an mcap file with no chunks.

    This reader handles MCAP files that don't contain chunks but have a proper
    summary section. It builds an index of message locations during initialization
    to enable efficient random access without loading the entire file into memory.

    Args:
        file: The file to read from.
        enable_crc_check: Whether to validate the crc values in the mcap
        enable_summary_reconstruction:
            - 'missing' allows reconstruction if the summary section is missing.
            - 'never' throws an exception if the summary (or summary offset) section is missing.
            - 'always' forces reconstruction even if the summary section is present.
    """

    def __init__(
        self,
        file: BaseReader,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ):
        self._file = file
        self._check_crc = enable_crc_check

        self._schemas: dict[int, SchemaRecord] | None = None
        self._channels: dict[int, ChannelRecord] | None = None

        # Parse file structure
        self._version = McapRecordParser.parse_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        self._summary: McapNonChunkedSummary = McapNonChunkedSummary(
            self._file,
            enable_crc_check=self._check_crc,
            enable_reconstruction=enable_summary_reconstruction,
            load_summary_eagerly=False,
        )
        self._message_indexes = self._build_message_index()

        # Check if this is indeed a non-chunked file
        self._statistics = self._summary.get_statistics()
        if self._statistics and self._statistics.chunk_count > 0:
            error_msg = 'MCAP file contains chunks, use McapChunkedReader instead'
            raise McapUnexpectedChunkIndexError(error_msg)

    def _build_message_index(self) -> dict[int, dict[int, list[int]]]:
            """Build an index of all messages in the file by scanning the data section."""
            logger.debug('Building message index for non-chunked MCAP')

            # Start after header, end before summary section
            _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
            _ = McapRecordParser.parse_header(self._file)

            message_count = 0
            message_index: dict[int, dict[int, list[int]]] = {}

            while (record_type := McapRecordParser.peek_record(self._file)) != McapRecordType.DATA_END:
                current_pos = self._file.tell()
                try:
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
    def from_file(
        file_path: Path | str,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> 'McapNonChunkedReader':
        """Create a new MCAP reader from a file.

        Args:
            file_path: Path to the MCAP file
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists

        Returns:
            A McapNonChunkedReader instance
        """
        logging.debug('Creating McapNonChunkedReader')
        return McapNonChunkedReader(
            FileReader(file_path),
            enable_crc_check=enable_crc_check,
            enable_summary_reconstruction=enable_summary_reconstruction,
        )

    @staticmethod
    def from_bytes(
        data: bytes,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> 'McapNonChunkedReader':
        """Create a new MCAP reader from a bytes object.

        Args:
            data: Bytes containing the MCAP file data
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists

        Returns:
            A McapNonChunkedReader instance
        """
        logging.debug('Creating McapNonChunkedReader')
        return McapNonChunkedReader(
            BytesReader(data),
            enable_crc_check=enable_crc_check,
            enable_summary_reconstruction=enable_summary_reconstruction,
        )

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
        _ = self._file.seek_from_start(MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_header(self._file)

    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        _ = self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_footer(self._file)

    def get_statistics(self) -> StatisticsRecord:  # TODO: Also return None here?
        """Get the statistics record from the MCAP file."""
        if self._statistics is None:
            raise McapNoStatisticsError('No statistics record!')
        return self._statistics

    # Schema Management

    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        if self._schemas is None:
            self._schemas = self._summary.get_schemas()
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
            self._channels = self._summary.get_channels()
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

    def get_chunk_indexes(self, channel_id: int | list[int] | None = None) -> list[ChunkIndexRecord]:
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
        if channel_id not in self._message_indexes:
            return None

        messages = self._message_indexes.get(channel_id)
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
        _ = self._file.seek_from_start(offsets[0])
        return McapRecordParser.parse_message(self._file)

    def get_messages(
        self,
        channel_id: int | list[int] | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[MessageRecord, None, None]:
        """
        Get messages from the MCAP file.

        If no channel is provided, messages from all channels are returned.
        If the start and end timestamps are not provided, the entire available range is returned.

        Args:
            channel_id: Optional channel ID(s) to filter by. Can be:
                - int: Single channel ID
                - list[int]: Multiple channel IDs
                - None: All channels
            start_timestamp: The start timestamp to filter by. If None, no filtering is done.
            end_timestamp: The end timestamp to filter by. If None, no filtering is done.
            in_log_time_order: Return records in log time order if true, else in the order they appear in the file
            in_reverse: Return messages in reverse order (last first) if True.

        Returns:
            A generator of MessageRecord objects.
        """
        # Determine which channels to process
        if channel_id is not None:
            channel_id = channel_id if isinstance(channel_id, list) else [channel_id]
            if not channel_id:  # Empty list
                return
            channels_to_process = [cid for cid in channel_id if cid in self._message_indexes]
            if not channels_to_process:
                logger.warning('None of the requested channel IDs are in MCAP!')
                return
        else:
            channels_to_process = list(self._message_indexes.keys())
        logger.debug(f'Channels requested: {channels_to_process}')

        # Collect all matching message offsets with timestamps
        entries: list[tuple[int, int]] = []
        for cid in channels_to_process:
            logger.debug(f'{len(self._message_indexes[cid])} messages for channel {cid}')
            for timestamp, offsets in self._message_indexes[cid].items():
                # Apply timestamp filtering
                if start_timestamp is not None and timestamp < start_timestamp:
                    continue
                if end_timestamp is not None and timestamp > end_timestamp:
                    continue
                for offset in offsets:
                    entries.append((timestamp, offset))

        if in_log_time_order:
            entries.sort(key=lambda x: (x[0], x[1]), reverse=in_reverse)
        else:
            entries.sort(key=lambda x: x[1], reverse=in_reverse)

        logger.debug(f'Found {len(entries)} messages')

        for _, offset in entries:
            _ = self._file.seek_from_start(offset)
            yield McapRecordParser.parse_message(self._file)

    def get_attachments(
        self,
        name: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[AttachmentRecord]:
        """Get attachments from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all attachments.
                  If provided, returns only attachments with matching name.
            start_time: Optional start time filter in nanoseconds. If provided,
                        only attachments with log_time >= start_time are returned.
            end_time: Optional end time filter in nanoseconds. If provided,
                      only attachments with log_time <= end_time are returned.

        Returns:
            List of AttachmentRecord objects.
        """
        attachment_indexes = self._summary.get_attachment_indexes()
        if name and name not in attachment_indexes:
            logging.warning(f'{name} not found in attachments!')
            return []

        attachment_indexes_flat: list[AttachmentIndexRecord] = []
        if name is None:
            for i in attachment_indexes.values():
                attachment_indexes_flat.extend(i)
        else:
            attachment_indexes_flat = attachment_indexes.get(name, [])

        # Filter by time using the index's log_time field
        if start_time is not None:
            attachment_indexes_flat = [a for a in attachment_indexes_flat if a.log_time >= start_time]
        if end_time is not None:
            attachment_indexes_flat = [a for a in attachment_indexes_flat if a.log_time <= end_time]

        current_pos = self._file.tell()
        attachments: list[AttachmentRecord] = []
        for attachment_index in attachment_indexes_flat:
            _ = self._file.seek_from_start(attachment_index.offset)
            attachment = McapRecordParser.parse_attachment(self._file)
            if self._check_crc and attachment.crc:  # If crc is 0, do not check
                assert_crc(attachment.data, attachment.crc)
            attachments.append(attachment)
        else:
            _ = self._file.seek_from_start(current_pos)

        return attachments

    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the MCAP file.

        Args:
            name: Optional name filter. If None, returns all metadata records.
                  If provided, returns only metadata records with matching name.

        Returns:
            List of MetadataRecord objects.
        """
        metadata_indexes = self._summary.get_metadata_indexes()
        if name and name not in metadata_indexes:
            logging.warning(f'{name} not found in metadata!')
            return []

        metadata_indexes_flat: list[MetadataIndexRecord] = []
        if name is None:
            for i in metadata_indexes.values():
                metadata_indexes_flat.extend(i)
        else:
            metadata_indexes_flat = metadata_indexes.get(name, [])

        current_pos = self._file.tell()
        metadata_records: list[MetadataRecord] = []
        for metadata_index in metadata_indexes_flat:
            _ = self._file.seek_from_start(metadata_index.offset)
            metadata = McapRecordParser.parse_metadata(self._file)
            metadata_records.append(metadata)
        else:
            _ = self._file.seek_from_start(current_pos)

        return metadata_records


class McapRecordReaderFactory:
    """Factory to create a McapFileSequentialReader or McapFileRandomAccessReader."""

    @staticmethod
    def from_file(
        file_path: Path | str,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        chunk_cache_size: int = 1,
    ) -> BaseMcapRecordReader:
        """Create a new MCAP reader from a file.

        Args:
            file_path: Path to the MCAP file
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists
            chunk_cache_size: Maximum number of decompressed chunks to cache (default: 8)

        Returns:
            Appropriate reader instance (chunked or non-chunked)

        Raises:
            NotImplementedError: If summary is missing and reconstruction is disabled
        """
        try:
            # Try to create a chunked reader first
            return McapChunkedReader.from_file(
                file_path,
                enable_crc_check=enable_crc_check,
                enable_summary_reconstruction=enable_summary_reconstruction,
                chunk_cache_size=chunk_cache_size,
            )
        except McapNoChunkIndexError:
            # If no chunks exist, use the non-chunked reader
            # TODO: Handle chunked MCAP files that lack chunk indexes by decoding CHUNK records directly.
            logger.warning('No chunk indexes detected, using non-chunked reader')
            return McapNonChunkedReader.from_file(
                file_path,
                enable_crc_check=enable_crc_check,
                enable_summary_reconstruction=enable_summary_reconstruction,
            )
        except (McapNoSummarySectionError, McapNoSummaryIndexError) as e:
            # Only raise if reconstruction is explicitly disabled
            if enable_summary_reconstruction == 'never':
                logger.error('Summary section missing and reconstruction is disabled')
                raise NotImplementedError(
                    'Sequential readers are not implemented yet. '
                    'Use enable_summary_reconstruction="missing" to reconstruct summaries.'
                ) from e
            # This should never happen since 'missing' mode should reconstruct
            # But if it does, provide helpful error message
            logger.error(f'Unexpected error with reconstruction mode "{enable_summary_reconstruction}": {e}')
            raise

    @staticmethod
    def from_bytes(
        data: bytes,
        *,
        enable_crc_check: bool = False,
        enable_summary_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
        chunk_cache_size: int = 1,
    ) -> BaseMcapRecordReader:
        """Create a new MCAP reader from a bytes object.

        Args:
            data: Bytes containing the MCAP file data
            enable_crc_check: Whether to validate CRC values
            enable_summary_reconstruction: Controls summary reconstruction behavior:
                - 'missing': Reconstruct if summary is missing (default)
                - 'never': Raise error if summary is missing
                - 'always': Always reconstruct even if summary exists
            chunk_cache_size: The number of decompressed chunks to store in memory at a time.

        Returns:
            Appropriate reader instance (chunked or non-chunked)

        Raises:
            NotImplementedError: If summary is missing and reconstruction is disabled
        """
        try:
            # Try to create a chunked reader first
            return McapChunkedReader.from_bytes(
                data,
                enable_crc_check=enable_crc_check,
                enable_summary_reconstruction=enable_summary_reconstruction,
                chunk_cache_size=chunk_cache_size,
            )
        except McapNoChunkIndexError:
            # If no chunks exist, use the non-chunked reader
            logger.warning('No chunk indexes detected, using non-chunked reader')
            return McapNonChunkedReader.from_bytes(
                data,
                enable_crc_check=enable_crc_check,
                enable_summary_reconstruction=enable_summary_reconstruction,
            )
        except (McapNoSummarySectionError, McapNoSummaryIndexError) as e:
            # Only raise if reconstruction is explicitly disabled
            if enable_summary_reconstruction == 'never':
                logger.error('Summary section missing and reconstruction is disabled')
                raise NotImplementedError(
                    'Sequential readers are not implemented yet. '
                    'Use enable_summary_reconstruction="missing" to reconstruct summaries.'
                ) from e
            # This should never happen since 'missing' mode should reconstruct
            # But if it does, provide helpful error message
            logger.error(f'Unexpected error with reconstruction mode "{enable_summary_reconstruction}": {e}')
            raise
