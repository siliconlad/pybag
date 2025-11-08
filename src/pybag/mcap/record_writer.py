import logging
import struct
import zlib
from abc import ABC, abstractmethod
from typing import Any, Callable, Literal

from pybag.io.raw_writer import BaseWriter, BytesWriter, CrcWriter
from pybag.mcap.records import (
    AttachmentIndexRecord,
    AttachmentRecord,
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    DataEndRecord,
    FooterRecord,
    HeaderRecord,
    MessageIndexRecord,
    MessageRecord,
    MetadataIndexRecord,
    MetadataRecord,
    RecordType,
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)

# Footer payload size: 8 bytes summary_start + 8 bytes summary_offset_start + 4 bytes summary_crc
FOOTER_PAYLOAD_SIZE = 20


class McapRecordWriter:
    """Utilities for writing MCAP records."""

    @classmethod
    def write_magic_bytes(cls, writer: BaseWriter, version: str = "0") -> int:
        """Write the MCAP magic bytes."""
        magic = b"\x89MCAP" + version.encode() + b"\r\n"
        return writer.write(magic)

    # Primitive encoders -------------------------------------------------

    @classmethod
    def _encode_uint8(cls, value: int) -> bytes:
        return struct.pack("<B", value)

    @classmethod
    def _encode_uint16(cls, value: int) -> bytes:
        return struct.pack("<H", value)

    @classmethod
    def _encode_uint32(cls, value: int) -> bytes:
        return struct.pack("<I", value)

    @classmethod
    def _encode_uint64(cls, value: int) -> bytes:
        return struct.pack("<Q", value)

    @classmethod
    def _encode_timestamp(cls, value: int) -> bytes:
        return cls._encode_uint64(value)

    @classmethod
    def _encode_record_type(cls, value: int) -> bytes:
        return cls._encode_uint8(value)

    @classmethod
    def _encode_string(cls, value: str) -> bytes:
        encoded = value.encode()
        return cls._encode_uint32(len(encoded)) + encoded

    # Container encoders -------------------------------------------------

    @classmethod
    def _encode_tuple(cls, data: tuple, first_type: str, second_type: str) -> bytes:
        first_value, second_value = data
        first_value_encoded = getattr(cls, f'_encode_{first_type}')(first_value)
        second_value_encoded = getattr(cls, f'_encode_{second_type}')(second_value)
        return first_value_encoded + second_value_encoded

    @classmethod
    def _encode_map(cls, data: dict, key_type: str, value_type: str) -> bytes:
        parts: list[bytes] = []
        for k, v in data.items():
            parts.append(getattr(cls, f'_encode_{key_type}')(k))
            parts.append(getattr(cls, f'_encode_{value_type}')(v))
        payload = b"".join(parts)
        return cls._encode_uint32(len(payload)) + payload

    @classmethod
    def _encode_array(cls, data: list, array_type_parser: Callable[[Any], bytes]) -> bytes:
        parts = [array_type_parser(v) for v in data]
        payload = b"".join(parts)
        return cls._encode_uint32(len(payload)) + payload

    # MCAP Record Writers -----------------------------------------------

    @classmethod
    def _write_record(cls, writer: BaseWriter, record_type: int, payload: bytes) -> None:
        writer.write(cls._encode_record_type(record_type))
        writer.write(cls._encode_uint64(len(payload)))
        writer.write(payload)

    @classmethod
    def write_header(cls, writer: BaseWriter, record: HeaderRecord) -> None:
        payload = (
              cls._encode_string(record.profile)
            + cls._encode_string(record.library)
        )
        cls._write_record(writer, RecordType.HEADER, payload)


    @classmethod
    def write_footer(cls, writer: BaseWriter, record: FooterRecord) -> None:
        payload = (
              cls._encode_uint64(record.summary_start)
            + cls._encode_uint64(record.summary_offset_start)
            + cls._encode_uint32(record.summary_crc)
        )
        cls._write_record(writer, RecordType.FOOTER, payload)

    @classmethod
    def write_schema(cls, writer: BaseWriter, record: SchemaRecord) -> None:
        payload = (
              cls._encode_uint16(record.id)
            + cls._encode_string(record.name)
            + cls._encode_string(record.encoding)
            + cls._encode_uint32(len(record.data))
            + record.data  # just bytes
        )
        cls._write_record(writer, RecordType.SCHEMA, payload)

    @classmethod
    def write_channel(cls, writer: BaseWriter, record: ChannelRecord) -> None:
        payload = (
              cls._encode_uint16(record.id)
            + cls._encode_uint16(record.schema_id)
            + cls._encode_string(record.topic)
            + cls._encode_string(record.message_encoding)
            + cls._encode_map(record.metadata, "string", "string")
        )
        cls._write_record(writer, RecordType.CHANNEL, payload)


    @classmethod
    def write_message(cls, writer: BaseWriter, record: MessageRecord) -> None:
        payload = (
              cls._encode_uint16(record.channel_id)
            + cls._encode_uint32(record.sequence)
            + cls._encode_timestamp(record.log_time)
            + cls._encode_timestamp(record.publish_time)
            + record.data
        )
        cls._write_record(writer, RecordType.MESSAGE, payload)

    @classmethod
    def write_chunk(cls, writer: BaseWriter, record: ChunkRecord) -> None:
        payload = (
              cls._encode_timestamp(record.message_start_time)
            + cls._encode_timestamp(record.message_end_time)
            + cls._encode_uint64(record.uncompressed_size)
            + cls._encode_uint32(record.uncompressed_crc)
            + cls._encode_string(record.compression)
            + cls._encode_uint64(len(record.records))
            + record.records
        )
        cls._write_record(writer, RecordType.CHUNK, payload)


    @classmethod
    def write_message_index(cls, writer: BaseWriter, record: MessageIndexRecord) -> None:
        payload = (
              cls._encode_uint16(record.channel_id)
            + cls._encode_array(record.records, lambda x: cls._encode_tuple(x, "timestamp", "uint64"))
        )
        cls._write_record(writer, RecordType.MESSAGE_INDEX, payload)

    @classmethod
    def write_chunk_index(cls, writer: BaseWriter, record: ChunkIndexRecord) -> None:
        payload = (
              cls._encode_timestamp(record.message_start_time)
            + cls._encode_timestamp(record.message_end_time)
            + cls._encode_uint64(record.chunk_start_offset)
            + cls._encode_uint64(record.chunk_length)
            + cls._encode_map(record.message_index_offsets, "uint16", "uint64")
            + cls._encode_uint64(record.message_index_length)
            + cls._encode_string(record.compression)
            + cls._encode_uint64(record.compressed_size)
            + cls._encode_uint64(record.uncompressed_size)
        )
        cls._write_record(writer, RecordType.CHUNK_INDEX, payload)

    @classmethod
    def write_attachment(cls, writer: BaseWriter, record: AttachmentRecord) -> None:
        payload = (
              cls._encode_timestamp(record.log_time)
            + cls._encode_timestamp(record.create_time)
            + cls._encode_string(record.name)
            + cls._encode_string(record.media_type)
            + cls._encode_uint64(len(record.data))
            + record.data
            + cls._encode_uint32(record.crc)
        )
        cls._write_record(writer, RecordType.ATTACHMENT, payload)

    @classmethod
    def write_metadata(cls, writer: BaseWriter, record: MetadataRecord) -> None:
        payload = (
              cls._encode_string(record.name)
            + cls._encode_map(record.metadata, "string", "string")
        )
        cls._write_record(writer, RecordType.METADATA, payload)

    @classmethod
    def write_data_end(cls, writer: BaseWriter, record: DataEndRecord) -> None:
        payload = cls._encode_uint32(record.data_section_crc)
        cls._write_record(writer, RecordType.DATA_END, payload)

    @classmethod
    def write_attachment_index(cls, writer: BaseWriter, record: AttachmentIndexRecord) -> None:
        payload = (
              cls._encode_uint64(record.offset)
            + cls._encode_uint64(record.length)
            + cls._encode_timestamp(record.log_time)
            + cls._encode_timestamp(record.create_time)
            + cls._encode_uint64(record.data_size)
            + cls._encode_string(record.name)
            + cls._encode_string(record.media_type)
        )
        cls._write_record(writer, RecordType.ATTACHMENT_INDEX, payload)

    @classmethod
    def write_metadata_index(cls, writer: BaseWriter, record: MetadataIndexRecord) -> None:
        payload = (
              cls._encode_uint64(record.offset)
            + cls._encode_uint64(record.length)
            + cls._encode_string(record.name)
        )
        cls._write_record(writer, RecordType.METADATA_INDEX, payload)

    @classmethod
    def write_statistics(cls, writer: BaseWriter, record: StatisticsRecord) -> None:
        payload = (
              cls._encode_uint64(record.message_count)
            + cls._encode_uint16(record.schema_count)
            + cls._encode_uint32(record.channel_count)
            + cls._encode_uint32(record.attachment_count)
            + cls._encode_uint32(record.metadata_count)
            + cls._encode_uint32(record.chunk_count)
            + cls._encode_timestamp(record.message_start_time)
            + cls._encode_timestamp(record.message_end_time)
            + cls._encode_map(record.channel_message_counts, "uint16", "uint64")
        )
        cls._write_record(writer, RecordType.STATISTICS, payload)

    @classmethod
    def write_summary_offset(cls, writer: BaseWriter, record: SummaryOffsetRecord) -> None:
        payload = (
              cls._encode_uint8(record.group_opcode)
            + cls._encode_uint64(record.group_start)
            + cls._encode_uint64(record.group_length)
        )
        cls._write_record(writer, RecordType.SUMMARY_OFFSET, payload)


# Low-level MCAP Writer Classes ------------------------------------------


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
    def close(self) -> None:
        """Finalize the MCAP file by writing summary section and footer."""
        ...  # pragma: no cover

    @abstractmethod
    def __enter__(self) -> 'BaseMcapRecordWriter':
        """Context manager entry."""
        ...  # pragma: no cover

    @abstractmethod
    def __exit__(self, exc_type, exc, tb) -> None:
        """Context manager exit."""
        ...  # pragma: no cover

    def _write_summary_section(
        self,
        writer: CrcWriter,
        schema_records: list[SchemaRecord] | None = None,
        channel_records: list[ChannelRecord] | None = None,
        statistics_record: StatisticsRecord | None = None,
        chunk_indexes: list[ChunkIndexRecord] | None = None
    ) -> tuple[int, int]:
        """Write the summary section and return (summary_start, summary_offset_start).

        Args:
            chunk_indexes: Optional list of chunk index records (for chunked writers).

        Returns:
            Tuple of (summary_start, summary_offset_start) positions.
        """
        # Start summary section
        summary_start = writer.tell()
        writer.clear_crc()

        # Write schema records to summary
        schema_group_start = summary_start
        if schema_records:
            for record in schema_records:
                McapRecordWriter.write_schema(writer, record)
        schema_group_length = writer.tell() - schema_group_start

        # Write channel records to summary
        channel_group_start = writer.tell()
        if channel_records:
            for record in channel_records:
                McapRecordWriter.write_channel(writer, record)
        channel_group_length = writer.tell() - channel_group_start

        # Write chunk index records to summary (only for chunked writers)
        chunk_index_group_start = writer.tell()
        chunk_index_group_length = 0
        if chunk_indexes:
            for record in chunk_indexes:
                McapRecordWriter.write_chunk_index(writer, record)
            chunk_index_group_length = writer.tell() - chunk_index_group_start

        # Write statistics record
        statistics_group_start = writer.tell()
        if statistics_record is not None:
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


class McapNonChunkedWriter(BaseMcapRecordWriter):
    """Low-level MCAP writer for non-chunked files.

    Writes schemas, channels, and messages directly to the data section without
    chunking. Tracks all records and statistics for the summary section.
    """

    def __init__(self, writer: BaseWriter, *, profile: str = "ros2") -> None:
        """Initialize a non-chunked MCAP writer.

        Args:
            writer: The underlying writer to write binary data to.
            profile: The MCAP profile to use (default: "ros2").
        """
        self._writer = CrcWriter(writer)
        self._profile = profile

        # Tracking for summary section
        self._schema_records: dict[int, SchemaRecord] = {}
        self._channel_records: dict[int, ChannelRecord] = {}

        # Statistics tracking
        self._message_count = 0
        self._chunk_count = 0
        self._message_start_time: int | None = None
        self._message_end_time: int | None = None
        self._channel_message_counts: dict[int, int] = {}

        # Write file header
        from pybag import __version__
        McapRecordWriter.write_magic_bytes(self._writer)
        header = HeaderRecord(profile=profile, library=f"pybag {__version__}")
        McapRecordWriter.write_header(self._writer, header)

    def __enter__(self) -> 'McapNonChunkedWriter':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def write_schema(self, schema: SchemaRecord) -> None:
        """Write a schema record immediately to the data section."""
        McapRecordWriter.write_schema(self._writer, schema)
        if schema.id in self._schema_records:
            logging.warning(f'Schema (id {schema.id}) already written to file')
        else:
            self._schema_records[schema.id] = schema

    def write_channel(self, channel: ChannelRecord) -> None:
        """Write a channel record immediately to the data section."""
        McapRecordWriter.write_channel(self._writer, channel)
        if channel.id in self._channel_records:
            logging.warning(f'Channel (id {channel.id}) already written to file')
        else:
            self._channel_records[channel.id] = channel
            self._channel_message_counts[channel.id] = 0

    def write_message(self, message: MessageRecord) -> None:
        """Write a message record immediately to the data section."""
        McapRecordWriter.write_message(self._writer, message)

        # Update statistics
        self._message_count += 1
        self._channel_message_counts[message.channel_id] += 1
        self._message_start_time = min(
            self._message_start_time or message.log_time,
            message.log_time
        )
        self._message_end_time = max(
            self._message_end_time or message.log_time,
            message.log_time
        )

    def close(self) -> None:
        """Finalize the file by writing summary section and footer."""
        # Write DataEnd record
        data_end = DataEndRecord(data_section_crc=self._writer.get_crc())
        McapRecordWriter.write_data_end(self._writer, data_end)

        # Write summary section using shared helper
        summary_start, summary_offset_start = self._write_summary_section(
            self._writer,
            schema_records=list(self._schema_records.values()),
            channel_records=list(self._channel_records.values()),
            statistics_record=StatisticsRecord(
                message_count=self._message_count,
                schema_count=len(self._schema_records),
                channel_count=len(self._channel_records),
                attachment_count=0,  # TODO: Implement
                metadata_count=0,    # TODO: Implement
                chunk_count=self._chunk_count,
                message_start_time=self._message_start_time or 0,
                message_end_time=self._message_end_time or 0,
                channel_message_counts=self._channel_message_counts,
            )
        )

        # Write footer record manually for CRC calculation
        self._writer.write(McapRecordWriter._encode_record_type(RecordType.FOOTER))
        self._writer.write(McapRecordWriter._encode_uint64(FOOTER_PAYLOAD_SIZE))
        self._writer.write(McapRecordWriter._encode_uint64(summary_start))
        self._writer.write(McapRecordWriter._encode_uint64(summary_offset_start))
        self._writer.write(McapRecordWriter._encode_uint32(self._writer.get_crc()))

        # Write magic bytes again
        McapRecordWriter.write_magic_bytes(self._writer)

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
        *,
        chunk_size: int,
        chunk_compression: Literal["lz4", "zstd"] | None = None,
        profile: str = "ros2",
    ) -> None:
        """Initialize a chunked MCAP writer.

        Args:
            writer: The underlying writer to write binary data to.
            chunk_size: The size threshold for flushing chunks (in bytes).
            chunk_compression: Compression algorithm ("lz4" or "zstd").
            profile: The MCAP profile to use (default: "ros2").
        """
        self._writer = CrcWriter(writer)
        self._profile = profile
        self._chunk_size = chunk_size
        self._chunk_compression = chunk_compression or ""
        self._compress_chunk = self._create_chunk_compressor()

        # Tracking for summary section
        self._schema_records: dict[int, SchemaRecord] = {}
        self._channel_records: dict[ int, ChannelRecord] = {}
        self._chunk_indexes: list[ChunkIndexRecord] = []

        # Statistics tracking
        self._message_count = 0
        self._chunk_count = 0
        self._message_start_time: int | None = None
        self._message_end_time: int | None = None
        self._channel_message_counts: dict[int, int] = {}

        # Current chunk buffering
        self._current_chunk_buffer: BytesWriter = BytesWriter()
        self._current_chunk_start_time: int | None = None
        self._current_chunk_end_time: int | None = None
        self._current_message_index: dict[int, list[tuple[int, int]]] = {}

        # Write file header
        from pybag import __version__
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
            import lz4.frame
            return lz4.frame.compress
        elif self._chunk_compression == "zstd":
            import zstandard as zstd
            return zstd.ZstdCompressor().compress
        elif self._chunk_compression == "":
            return lambda x: x
        else:
            raise ValueError(f"Unsupported chunk compression: {self._chunk_compression}")

    def write_schema(self, schema: SchemaRecord) -> None:
        """Write a schema record immediately to the data section (not buffered)."""
        McapRecordWriter.write_schema(self._writer, schema)
        if schema.id in self._schema_records:
            logging.warning(f'Schema (id {schema.id}) already written to file')
        else:
            self._schema_records[schema.id] = schema

    def write_channel(self, channel: ChannelRecord) -> None:
        """Write a channel record immediately to the data section (not buffered)."""
        McapRecordWriter.write_channel(self._writer, channel)
        if channel.id in self._channel_records:
            logging.warning(f'Channel (id {channel.id}) already written to file')
        else:
            self._channel_records[channel.id] = channel
            self._channel_message_counts[channel.id] = 0

    def write_message(self, message: MessageRecord) -> None:
        """Write a message record to the current chunk buffer.

        If the buffer size exceeds the chunk size threshold, flush the chunk.
        """
        # Update chunk timing
        self._current_chunk_start_time = min(
            self._current_chunk_start_time or message.log_time,
            message.log_time
        )
        self._current_chunk_end_time = max(
            self._current_chunk_end_time or message.log_time,
            message.log_time
        )

        # Write message to chunk buffer and track offset
        offset = self._current_chunk_buffer.size()
        McapRecordWriter.write_message(self._current_chunk_buffer, message)
        self._current_message_index.setdefault(message.channel_id, []).append(
            (message.log_time, offset)
        )

        # Flush chunk if size threshold reached
        if self._current_chunk_buffer.size() >= self._chunk_size:
            self._flush_chunk()

        # Update statistics
        self._message_count += 1
        self._channel_message_counts[message.channel_id] += 1
        self._message_start_time = min(
            self._message_start_time or message.log_time,
            message.log_time
        )
        self._message_end_time = max(
            self._message_end_time or message.log_time,
            message.log_time
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

        # Track chunk index for summary
        self._chunk_indexes.append(
            ChunkIndexRecord(
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
        )

        # Increment chunk count and clear buffer
        self._chunk_count += 1
        self._current_chunk_buffer.clear()
        self._current_chunk_start_time = None
        self._current_chunk_end_time = None
        self._current_message_index = {}

    def close(self) -> None:
        """Finalize the file by flushing remaining chunk and writing summary."""
        # Flush any remaining buffered messages
        if self._current_chunk_buffer.size() > 0:
            self._flush_chunk()

        # Write DataEnd record
        data_end = DataEndRecord(data_section_crc=self._writer.get_crc())
        McapRecordWriter.write_data_end(self._writer, data_end)

        # Write summary section using shared helper (passing chunk indexes)
        summary_start, summary_offset_start = self._write_summary_section(
            self._writer,
            schema_records=list(self._schema_records.values()),
            channel_records=list(self._channel_records.values()),
            statistics_record=StatisticsRecord(
                message_count=self._message_count,
                schema_count=len(self._schema_records),
                channel_count=len(self._channel_records),
                attachment_count=0,  # TODO: Implement
                metadata_count=0,    # TODO: Implement
                chunk_count=self._chunk_count,
                message_start_time=self._message_start_time or 0,
                message_end_time=self._message_end_time or 0,
                channel_message_counts=self._channel_message_counts,
            ),
            chunk_indexes=self._chunk_indexes
        )

        # Write footer record manually for CRC calculation
        self._writer.write(McapRecordWriter._encode_record_type(RecordType.FOOTER))
        self._writer.write(McapRecordWriter._encode_uint64(FOOTER_PAYLOAD_SIZE))
        self._writer.write(McapRecordWriter._encode_uint64(summary_start))
        self._writer.write(McapRecordWriter._encode_uint64(summary_offset_start))
        self._writer.write(McapRecordWriter._encode_uint32(self._writer.get_crc()))

        # Write magic bytes again
        McapRecordWriter.write_magic_bytes(self._writer)

        # Close the underlying writer
        self._writer.close()


class McapRecordWriterFactory:
    """Factory for creating appropriate MCAP record writers."""

    @staticmethod
    def create_writer(
        writer: BaseWriter,
        *,
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = None,
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
        if chunk_size is None:
            return McapNonChunkedWriter(writer, profile=profile)
        else:
            return McapChunkedWriter(
                writer,
                chunk_size=chunk_size,
                chunk_compression=chunk_compression,
                profile=profile,
            )
