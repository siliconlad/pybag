"""Utilities for writing MCAP files."""

import logging
import zlib
from pathlib import Path
from typing import Any, Callable, Literal

from pybag import __version__
from pybag.io.raw_writer import BaseWriter, BytesWriter, CrcWriter, FileWriter
from pybag.mcap.record_writer import McapRecordWriter
from pybag.mcap.records import (
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    DataEndRecord,
    HeaderRecord,
    MessageIndexRecord,
    MessageRecord,
    RecordType,
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)
from pybag.serialize import MessageSerializerFactory
from pybag.types import Message

logger = logging.getLogger(__name__)


class McapFileWriter:
    """High level writer for producing MCAP files."""

    def __init__(
        self,
        writer: BaseWriter,
        *,
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = None,
    ) -> None:
        self._writer = CrcWriter(writer)
        self._chunk_count = 0
        self._chunk_size = chunk_size
        self._chunk_compression = chunk_compression or ""
        self._compress_chunk = self._create_chunk_compressor()

        self._next_schema_id = 1  # Schema ID must be non-zero
        self._next_channel_id = 1

        self._topics: dict[str, int] = {}  # topic -> channel_id
        self._schemas: dict[type[Message], int] = {}  # type -> schema_id
        self._schema_records: list[SchemaRecord] = []
        self._channel_records: list[ChannelRecord] = []
        self._sequences: dict[int, int] = {}

        self._message_count = 0
        self._message_start_time: int | None = None
        self._message_end_time: int | None = None
        self._channel_message_counts: dict[int, int] = {}

        self._current_chunk_buffer: BytesWriter = BytesWriter()
        self._current_chunk_start_time: int | None = None
        self._current_chunk_end_time: int | None = None
        self._current_message_index: dict[int, list[tuple[int, int]]] = {}
        self._chunk_indexes: list[ChunkIndexRecord] = []

        # Write the start of the file
        McapRecordWriter.write_magic_bytes(self._writer)
        header = HeaderRecord(profile=profile, library=f"pybag {__version__}")
        McapRecordWriter.write_header(self._writer, header)

        self._profile = profile
        self._message_serializer = MessageSerializerFactory.from_profile(self._profile)
        if self._message_serializer is None:
            raise ValueError(f"Unknown encoding type: {self._profile}")

    def __enter__(self) -> "McapFileWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @classmethod
    def open(
        cls,
        file_path: str | Path,
        *,
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = "lz4",
    ) -> "McapFileWriter":
        """Create a writer backed by a file on disk.

        Args:
            file_path: The path to the file to write to.
            profile: The profile to use for the MCAP file.
            chunk_size: The size of the chunk to write to in bytes.
            chunk_compression: The compression to use for the chunk.

        Returns:
            A writer backed by a file on disk.
        """

        return cls(
            FileWriter(file_path),
            profile=profile,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
        )

    def add_channel(self, topic: str, channel_type: type[Message]) -> int:
        """Add a channel to the MCAP output.

        Args:
            topic: The topic name.
            channel_type: The type of the messages to be written to the channel.

        Returns:
            The channel ID.
        """
        if (channel_id := self._topics.get(topic)) is None:
            # Register the schema if it's not already registered
            if (schema_id := self._schemas.get(channel_type)) is None:
                schema_id = self._next_schema_id
                self._next_schema_id += 1

                # Check that the channel type has a __msg_name__ attribute
                if not isinstance(channel_type, Message):
                    raise ValueError(f"Channel type {channel_type} needs a __msg_name__ attribute")

                schema_record = SchemaRecord(
                    id=schema_id,
                    name=channel_type.__msg_name__,
                    encoding=self._message_serializer.schema_encoding,
                    data=self._message_serializer.serialize_schema(channel_type),
                )

                McapRecordWriter.write_schema(self._writer, schema_record)
                self._schemas[channel_type] = schema_id
                self._schema_records.append(schema_record)

            # Register the channel if it's not already registered
            channel_id = self._next_channel_id
            self._next_channel_id += 1
            channel_record = ChannelRecord(
                id=channel_id,
                schema_id=schema_id,
                topic=topic,
                message_encoding=self._message_serializer.message_encoding,
                metadata={},
            )
            McapRecordWriter.write_channel(self._writer, channel_record)
            self._topics[topic] = channel_id
            self._channel_records.append(channel_record)
            self._sequences[channel_id] = 0
            self._channel_message_counts[channel_id] = 0

        return channel_id

    def write_message(self, topic: str, timestamp: int, message: Message) -> None:
        """Write a message to a topic at a given timestamp.

        Args:
            topic: The topic name.
            timestamp: The timestamp of the message.
            message: The message to write.
        """

        channel_id = self.add_channel(topic, type(message))
        sequence = self._sequences[channel_id]
        self._sequences[channel_id] = sequence + 1

        record = MessageRecord(
            channel_id=channel_id,
            sequence=sequence,
            log_time=timestamp,
            publish_time=timestamp,
            data=self._message_serializer.serialize_message(message),
        )

        if self._chunk_size is not None:
            if self._current_chunk_start_time is None:
                self._current_chunk_start_time = timestamp
            self._current_chunk_end_time = max(self._current_chunk_end_time or timestamp, timestamp)
            offset = self._current_chunk_buffer.size()
            McapRecordWriter.write_message(self._current_chunk_buffer, record)
            self._current_message_index.setdefault(channel_id, []).append((timestamp, offset))
            if self._current_chunk_buffer.size() >= self._chunk_size:
                self._flush_chunk()
        else:
            McapRecordWriter.write_message(self._writer, record)

        # Keep track of statistics
        self._message_count += 1
        self._channel_message_counts[channel_id] += 1
        self._message_start_time = min(self._message_start_time or timestamp, timestamp)
        self._message_end_time = max(self._message_end_time or timestamp, timestamp)

    def _create_chunk_compressor(self) -> Callable[[bytes], bytes]:
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

    def _flush_chunk(self) -> None:
        records = self._current_chunk_buffer.as_bytes()
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

        # Write the message index records for the chunk
        message_index_offsets = {}
        message_index_start_offset = self._writer.tell()
        for cid, records in self._current_message_index.items():
            message_index_offsets[cid] = self._writer.tell()
            message_index_record = MessageIndexRecord(channel_id=cid, records=records)
            McapRecordWriter.write_message_index(self._writer, message_index_record)
        message_index_length = self._writer.tell() - message_index_start_offset

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
        self._chunk_count += 1
        self._current_chunk_buffer.clear()
        self._current_chunk_start_time = None
        self._current_chunk_end_time = None
        self._current_message_index = {}

    def close(self) -> None:
        if self._current_chunk_buffer.size() > 0:
            self._flush_chunk()

        # Data end record
        data_end = DataEndRecord(data_section_crc=self._writer.get_crc())
        McapRecordWriter.write_data_end(self._writer, data_end)

        summary_start = self._writer.tell()
        self._writer.clear_crc()

        # Schema records
        schema_group_start = summary_start
        for record in self._schema_records:
            McapRecordWriter.write_schema(self._writer, record)
        schema_group_length = self._writer.tell() - schema_group_start

        # Channel records
        channel_group_start = self._writer.tell()
        for record in self._channel_records:
            McapRecordWriter.write_channel(self._writer, record)
        channel_group_length = self._writer.tell() - channel_group_start

        # Message index and chunk index records
        chunk_index_group_start = self._writer.tell()
        for record in self._chunk_indexes:
            McapRecordWriter.write_chunk_index(self._writer, record)
        chunk_index_group_length = self._writer.tell() - chunk_index_group_start

        # Statistics record
        statistics_group_start = self._writer.tell()
        stats = StatisticsRecord(
            message_count=self._message_count,
            schema_count=len(self._schema_records),
            channel_count=len(self._channel_records),
            attachment_count=0,
            metadata_count=0,
            chunk_count=self._chunk_count,
            message_start_time=self._message_start_time or 0,
            message_end_time=self._message_end_time or 0,
            channel_message_counts=self._channel_message_counts,
        )
        McapRecordWriter.write_statistics(self._writer, stats)
        statistics_group_length = self._writer.tell() - statistics_group_start

        # Summary offsets
        summary_offset_start = self._writer.tell()
        if schema_group_length > 0:
            McapRecordWriter.write_summary_offset(
                self._writer,
                SummaryOffsetRecord(
                    group_opcode=RecordType.SCHEMA,
                    group_start=schema_group_start,
                    group_length=schema_group_length,
                ),
            )
        if channel_group_length > 0:
            McapRecordWriter.write_summary_offset(
                self._writer,
                SummaryOffsetRecord(
                    group_opcode=RecordType.CHANNEL,
                    group_start=channel_group_start,
                    group_length=channel_group_length,
                ),
            )
        if chunk_index_group_length > 0:
            McapRecordWriter.write_summary_offset(
                self._writer,
                SummaryOffsetRecord(
                    group_opcode=RecordType.CHUNK_INDEX,
                    group_start=chunk_index_group_start,
                    group_length=chunk_index_group_length,
                ),
            )
        McapRecordWriter.write_summary_offset(
            self._writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.STATISTICS,
                group_start=statistics_group_start,
                group_length=statistics_group_length,
            ),
        )

        # Write footer record manually for CRC calculation
        self._writer.write(McapRecordWriter._encode_record_type(RecordType.FOOTER))
        self._writer.write(McapRecordWriter._encode_uint64(20))
        self._writer.write(McapRecordWriter._encode_uint64(summary_start))
        self._writer.write(McapRecordWriter._encode_uint64(summary_offset_start))
        self._writer.write(McapRecordWriter._encode_uint32(self._writer.get_crc()))

        # Write the magic bytes again
        McapRecordWriter.write_magic_bytes(self._writer)

        # Close the file
        self._writer.close()
