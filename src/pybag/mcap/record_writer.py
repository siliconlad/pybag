import logging
import zlib
from abc import ABC, abstractmethod
from typing import Callable, Literal

import lz4.frame
import zstandard as zstd

from pybag import __version__
from pybag.io.raw_writer import BaseWriter, BytesWriter, CrcWriter
from pybag.mcap.record_encoder import McapRecordWriter
from pybag.mcap.record_parser import (
    DATA_END_SIZE,
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)
from pybag.mcap.records import (
    AttachmentIndexRecord,
    AttachmentRecord,
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    HeaderRecord,
    MessageIndexRecord,
    MessageRecord,
    MetadataIndexRecord,
    MetadataRecord,
    RecordType,
    SchemaRecord
)
from pybag.mcap.summary import (
    McapChunkedSummary,
    McapNonChunkedSummary,
    McapSummary,
    McapSummaryFactory
)


def _prepare_append_writer(writer: BaseWriter) -> CrcWriter:
    """Seek to the start of the existing DATA_END and seed CRC from prior data."""
    # Read footer to locate the existing data end record
    _ = writer.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
    footer = McapRecordParser.parse_footer(writer)

    if footer.summary_start != 0:
        data_end_offset = footer.summary_start - DATA_END_SIZE
        _ = writer.seek_from_start(data_end_offset)
    else:
        _ = writer.seek_from_end(FOOTER_SIZE + DATA_END_SIZE + MAGIC_BYTES_SIZE)

    data_end = McapRecordParser.parse_data_end(writer)
    # Rewind to the start of the DataEnd record so new writes overwrite it
    writer.seek_from_current(-DATA_END_SIZE)
    return CrcWriter(writer, initial_crc=data_end.data_section_crc)


class BaseMcapRecordWriter(ABC):
    """Abstract base class for low-level MCAP record writers.

    Low-level writers accept pre-constructed MCAP record dataclasses and handle
    binary serialization, chunking, compression, and summary section management.
    """

    @abstractmethod
    def write_schema(self, schema: SchemaRecord) -> None:
        """Write a schema record to the MCAP file.

        Args:
            schema: The schema record to write.
        """
        ...  # pragma: no cover

    @abstractmethod
    def write_channel(self, channel: ChannelRecord) -> None:
        """Write a channel record to the MCAP file.

        Args:
            channel: The channel record to write.
        """
        ...  # pragma: no cover

    @abstractmethod
    def write_message(self, message: MessageRecord) -> None:
        """Write a message record to the MCAP file.

        Args:
            message: The message record to write.
        """
        ...  # pragma: no cover

    @abstractmethod
    def write_attachment(self, attachment: AttachmentRecord) -> None:
        """Write an attachment record to the MCAP file.

        Args:
            attachment: The attachment record to write.
        """
        ...  # pragma: no cover

    @abstractmethod
    def write_metadata(self, metadata: MetadataRecord) -> None:
        """Write a metadata record to the MCAP file.

        Args:
            metadata: The metadata record to write.
        """
        ...  # pragma: no cover

    @abstractmethod
    def close(self) -> None:
        """Finalize the MCAP file by writing summary section and footer."""
        ...  # pragma: no cover

    @abstractmethod
    def flush_chunk(self) -> None:
        """Flush the current chunk if applicable.

        For chunked writers, this forces the current chunk to be written even if
        it hasn't reached the size threshold. For non-chunked writers, this is a no-op.
        """
        ...  # pragma: no cover

    @abstractmethod
    def __enter__(self) -> 'BaseMcapRecordWriter':
        """Context manager entry."""
        ...  # pragma: no cover

    @abstractmethod
    def __exit__(self, exc_type, exc, tb) -> None:
        """Context manager exit."""
        ...  # pragma: no cover


class McapNonChunkedWriter(BaseMcapRecordWriter):
    """Low-level MCAP writer for non-chunked files.

    Writes schemas, channels, and messages directly to the data section without
    chunking. Tracks all records and statistics for the summary section.
    """

    def __init__(
        self,
        writer: BaseWriter,
        *,
        mode: Literal['w', 'a'] = 'w',
        summary: McapNonChunkedSummary,
        profile: str = "ros2",
    ) -> None:
        """Initialize a non-chunked MCAP writer.

        Args:
            writer: The underlying writer to write binary data to.
            summary: Existing summary
            profile: The MCAP profile to use (default: "ros2").
            has_file_start: File already contains magic bytes + header
        """

        self._writer = CrcWriter(writer) if mode == 'w' else _prepare_append_writer(writer)
        self._summary = summary
        self._profile = profile

        # Write file header
        if mode == 'w':
            McapRecordWriter.write_magic_bytes(self._writer)
            header = HeaderRecord(profile=profile, library=f"pybag {__version__}")
            McapRecordWriter.write_header(self._writer, header)

    def __enter__(self) -> 'McapNonChunkedWriter':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def write_schema(self, schema: SchemaRecord) -> None:
        """Write a schema record immediately to the data section."""
        self._summary.add_schema(schema)
        McapRecordWriter.write_schema(self._writer, schema)

    def write_channel(self, channel: ChannelRecord) -> None:
        """Write a channel record immediately to the data section."""
        self._summary.add_channel(channel)
        McapRecordWriter.write_channel(self._writer, channel)

    def write_message(self, message: MessageRecord) -> None:
        """Write a message record immediately to the data section."""
        self._summary.add_message(message)
        McapRecordWriter.write_message(self._writer, message)

    def flush_chunk(self) -> None:
        """No-op for non-chunked writer."""
        pass

    def write_attachment(self, attachment: AttachmentRecord) -> None:
        """Write an attachment record immediately to the data section."""
        offset = self._writer.tell()
        # TODO: maybe write should return length
        McapRecordWriter.write_attachment(self._writer, attachment)
        length = self._writer.tell() - offset

        self._summary.add_attachment_index(
            AttachmentIndexRecord(
                offset=offset,
                length=length,
                log_time=attachment.log_time,
                create_time=attachment.create_time,
                data_size=len(attachment.data),
                name=attachment.name,
                media_type=attachment.media_type,
            )
        )

    def write_metadata(self, metadata: MetadataRecord) -> None:
        """Write a metadata record immediately to the data section."""
        offset = self._writer.tell()
        # TODO: maybe write should return length
        McapRecordWriter.write_metadata(self._writer, metadata)
        length = self._writer.tell() - offset

        self._summary.add_metadata_index(
            MetadataIndexRecord(
                offset=offset,
                length=length,
                name=metadata.name,
            )
        )

    def close(self) -> None:
        """Finalize the file by writing summary section and footer."""
        self._summary.write_summary(self._writer)
        # Close the underlying writer
        self._writer.close()


class McapChunkedWriter(BaseMcapRecordWriter):
    """Low-level MCAP writer for chunked files with compression.

    Writes schemas and channels directly to the file, but accumulates messages
    in chunks. When a chunk reaches the size threshold, it is compressed and
    written along with message indexes.
    """

    def __init__(
        self,
        writer: BaseWriter,
        summary: McapChunkedSummary,
        *,
        mode: Literal['w', 'a'] = 'w',
        chunk_size: int,
        chunk_compression: Literal["none", "lz4", "zstd"] | None = "none",
        profile: str = "ros2",
    ) -> None:
        """Initialize a chunked MCAP writer.

        Args:
            writer: The underlying writer to write binary data to.
            summary: Existing summary
            chunk_size: The size threshold for flushing chunks (in bytes).
            chunk_compression: Compression algorithm ("lz4" or "zstd").
            profile: The MCAP profile to use (default: "ros2").
            has_file_start: File already contains magic bytes + header
        """
        self._writer = CrcWriter(writer) if mode == 'w' else _prepare_append_writer(writer)
        self._summary = summary
        self._profile = profile
        self._chunk_size = chunk_size
        self._chunk_compression = "" if chunk_compression in ("none", None)  else chunk_compression
        self._compress_chunk = self._create_chunk_compressor()

        # Current chunk buffering
        self._current_chunk_buffer: BytesWriter = BytesWriter()
        self._current_chunk_start_time: int | None = None
        self._current_chunk_end_time: int | None = None
        self._current_message_index: dict[int, list[tuple[int, int]]] = {}

        # Write file header
        if mode == 'w':
            McapRecordWriter.write_magic_bytes(self._writer)
            header = HeaderRecord(profile=profile, library=f"pybag {__version__}")
            McapRecordWriter.write_header(self._writer, header)

    def __enter__(self) -> 'McapChunkedWriter':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _create_chunk_compressor(self) -> Callable[[bytes], bytes]:
        """Create a compression function based on the configured algorithm."""
        if self._chunk_compression == "lz4":
            return lz4.frame.compress
        elif self._chunk_compression == "zstd":
            return zstd.ZstdCompressor().compress
        elif self._chunk_compression == "":
            return lambda x: x
        else:
            raise ValueError(f"Unsupported chunk compression: {self._chunk_compression}")

    def write_schema(self, schema: SchemaRecord) -> None:
        """Write a schema record immediately to the data section (not buffered)."""
        self._summary.add_schema(schema)
        McapRecordWriter.write_schema(self._writer, schema)

    def write_channel(self, channel: ChannelRecord) -> None:
        """Write a channel record immediately to the data section (not buffered)."""
        self._summary.add_channel(channel)
        McapRecordWriter.write_channel(self._writer, channel)

    def write_message(self, message: MessageRecord) -> None:
        """Write a message record to the current chunk buffer.

        If the buffer size exceeds the chunk size threshold, flush the chunk.
        """
        # Update chunk timing
        self._current_chunk_start_time = min(
            message.log_time if self._current_chunk_start_time is None else self._current_chunk_start_time,
            message.log_time,
        )
        self._current_chunk_end_time = max(
            message.log_time if self._current_chunk_end_time is None else self._current_chunk_end_time,
            message.log_time,
        )

        # Write message to chunk buffer and track offset
        offset = self._current_chunk_buffer.size()
        self._summary.add_message(message)
        McapRecordWriter.write_message(self._current_chunk_buffer, message)
        self._current_message_index.setdefault(message.channel_id, []).append((message.log_time, offset))

        # Flush chunk if size threshold reached
        if self._current_chunk_buffer.size() >= self._chunk_size:
            self._flush_chunk()

    def write_attachment(self, attachment: AttachmentRecord) -> None:
        """Write an attachment record immediately to the data section (not buffered).

        Attachments are written directly to the file, not buffered in chunks.
        """
        offset = self._writer.tell()
        McapRecordWriter.write_attachment(self._writer, attachment)
        length = self._writer.tell() - offset

        self._summary.add_attachment_index(
            AttachmentIndexRecord(
                offset=offset,
                length=length,
                log_time=attachment.log_time,
                create_time=attachment.create_time,
                data_size=len(attachment.data),
                name=attachment.name,
                media_type=attachment.media_type,
            )
        )

    def write_metadata(self, metadata: MetadataRecord) -> None:
        """Write a metadata record immediately to the data section (not buffered).

        Metadata is written directly to the file, not buffered in chunks.
        """
        offset = self._writer.tell()
        McapRecordWriter.write_metadata(self._writer, metadata)
        length = self._writer.tell() - offset

        self._summary.add_metadata_index(
            MetadataIndexRecord(
                offset=offset,
                length=length,
                name=metadata.name,
            )
        )

    def _flush_chunk(self) -> None:
        """Compress and write the current chunk buffer to the file."""
        records = self._current_chunk_buffer.as_bytes()

        # Create and write chunk record
        chunk = ChunkRecord(
            message_start_time=self._current_chunk_start_time or 0,
            message_end_time=self._current_chunk_end_time or 0,
            uncompressed_size=len(records),
            uncompressed_crc=zlib.crc32(records),
            compression=self._chunk_compression,
            records=self._compress_chunk(records),
        )
        chunk_start_offset = self._writer.tell()
        McapRecordWriter.write_chunk(self._writer, chunk)
        chunk_length = self._writer.tell() - chunk_start_offset

        # Write message index records for the chunk
        message_index_offsets = {}
        message_index_start_offset = self._writer.tell()
        for cid, records_list in self._current_message_index.items():
            message_index_offsets[cid] = self._writer.tell()
            message_index_record = MessageIndexRecord(
                channel_id=cid,
                records=records_list
            )
            McapRecordWriter.write_message_index(self._writer, message_index_record)
        message_index_length = self._writer.tell() - message_index_start_offset

        chunk_index = ChunkIndexRecord(
            message_start_time=chunk.message_start_time,
            message_end_time=chunk.message_end_time,
            chunk_start_offset=chunk_start_offset,
            chunk_length=chunk_length,
            message_index_offsets=message_index_offsets,
            message_index_length=message_index_length,
            compression=chunk.compression,
            compressed_size=len(chunk.records),
            uncompressed_size=chunk.uncompressed_size,
        )
        self._summary.add_chunk_index(chunk_index, message_index_length)

        self._current_chunk_buffer.clear()
        self._current_chunk_start_time = None
        self._current_chunk_end_time = None
        self._current_message_index = {}

    def flush_chunk(self) -> None:
        """Flush the current chunk buffer to the file.

        Forces the current chunk to be written even if it hasn't reached the
        size threshold. This is useful when switching topics to ensure that
        each chunk only contains messages from one topic.
        """
        if self._current_chunk_buffer.size() > 0:
            self._flush_chunk()

    def close(self) -> None:
        """Finalize the file by flushing remaining chunk and writing summary."""
        # Flush any remaining buffered messages
        if self._current_chunk_buffer.size() > 0:
            self._flush_chunk()
        self._summary.write_summary(self._writer)
        # Close the underlying writer
        self._writer.close()


class McapRecordWriterFactory:
    """Factory for creating appropriate MCAP record writers."""

    @staticmethod
    def create_writer(
        writer: BaseWriter,
        summary: McapSummary,
        *,
        mode: Literal['w', 'a'] = 'w',
        chunk_size: int | None = None,
        chunk_compression: Literal["none", "lz4", "zstd"] | None = "none",
        profile: str = "ros2",
    ) -> BaseMcapRecordWriter:
        """Create an appropriate MCAP record writer based on configuration.

        Args:
            writer: The underlying writer to write binary data to.
            chunk_size: If provided, creates a chunked writer with this size threshold.
                       If None, creates a non-chunked writer.
            chunk_compression: Compression algorithm for chunks ("lz4" or "zstd").
            profile: The MCAP profile to use (default: "ros2").

        Returns:
            A BaseMcapRecordWriter instance (either chunked or non-chunked).
        """
        # Choose writer based on summary type (not chunk_size)
        # This ensures append mode respects the existing file's chunking mode
        if isinstance(summary, McapNonChunkedSummary):
            return McapNonChunkedWriter(
                writer,
                mode=mode,
                profile=profile,
                summary=summary,
            )
        elif isinstance(summary, McapChunkedSummary):
            # For chunked writer, use provided chunk_size or default
            # In append mode, this may differ from the user's request but maintains file consistency
            assert chunk_size is not None, "Chunk size cannot be None"
            return McapChunkedWriter(
                writer,
                mode=mode,
                summary=summary,
                chunk_size=chunk_size,
                chunk_compression=chunk_compression,
                profile=profile,
            )
        else:
            raise ValueError(f"Unknown summary type: {type(summary)}")
