"""Utilities for writing MCAP files."""

import logging
import zlib
from pathlib import Path
from typing import Any

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

logger = logging.getLogger(__name__)


class McapFileWriter:
    """High level writer for producing MCAP files."""

    def __init__(self, writer: BaseWriter, *, chunk_size: int | None = None) -> None:
        self._writer = CrcWriter(writer)
        self._chunk_size = chunk_size
        self._chunk_buffer: BytesWriter | None = None
        self._chunk_start_time: int | None = None
        self._chunk_end_time: int | None = None
        self._chunk_count = 0
        self._chunk_indexes: list[ChunkIndexRecord] = []
        self._message_index_records: list[dict[int, MessageIndexRecord]] = []
        self._current_message_index: dict[int, list[tuple[int, int]]] = {}

        self._next_schema_id = 1  # Schema ID must be non-zero
        self._next_channel_id = 1

        self._topics: dict[str, int] = {}    # topic -> channel_id
        self._schemas: dict[type, int] = {}  # type -> schema_id
        self._schema_records: list[SchemaRecord] = []
        self._channel_records: list[ChannelRecord] = []
        self._sequences: dict[int, int] = {}

        self._message_count = 0
        self._message_start_time: int | None = None
        self._message_end_time: int | None = None
        self._channel_message_counts: dict[int, int] = {}

        # Write the start of the file
        # TODO: Support different profiles
        McapRecordWriter.write_magic_bytes(self._writer)
        header = HeaderRecord(profile="ros2", library=f"pybag {__version__}")
        McapRecordWriter.write_header(self._writer, header)

        self._profile = header.profile
        self._message_serializer = MessageSerializerFactory.from_profile(self._profile)
        if self._message_serializer is None:
            raise ValueError(f"Unknown encoding type: {self._profile}")

    @classmethod
    def open(cls, file_path: str | Path, *, chunk_size: int | None = None) -> "McapFileWriter":
        """Create a writer backed by a file on disk."""

        return cls(FileWriter(file_path), chunk_size=chunk_size)

    def add_channel(self, topic: str, channel_type: type) -> int:
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
                # TODO: Replace with a protocol
                if not hasattr(channel_type, '__msg_name__'):
                    raise ValueError(f"Channel type {channel_type} needs a __msg_name__ attribute")
                if not isinstance(channel_type.__msg_name__, str):
                    raise ValueError(f"Channel type {channel_type} __msg_name__ must be a string")

                schema_record = SchemaRecord(
                    id=schema_id,
                    name=channel_type.__msg_name__,
                    encoding="ros2msg",
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
                message_encoding="cdr",
                metadata={},
            )
            McapRecordWriter.write_channel(self._writer, channel_record)
            self._topics[topic] = channel_id
            self._channel_records.append(channel_record)
            self._sequences[channel_id] = 0
            self._channel_message_counts[channel_id] = 0

        return channel_id

    def write_message(self, topic: str, timestamp: int, message: Any) -> None:
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

        if self._chunk_size is None:
            McapRecordWriter.write_message(self._writer, record)
        else:
            if self._chunk_buffer is None:
                self._chunk_buffer = BytesWriter()
                self._chunk_start_time = timestamp
                self._current_message_index = {}
            self._chunk_end_time = timestamp
            offset = self._chunk_buffer.size()
            McapRecordWriter.write_message(self._chunk_buffer, record)
            self._current_message_index.setdefault(channel_id, []).append((timestamp, offset))
            if self._chunk_buffer.size() >= self._chunk_size:
                self._flush_chunk()

        self._message_count += 1
        self._channel_message_counts[channel_id] += 1
        self._message_start_time = min(self._message_start_time or timestamp, timestamp)
        self._message_end_time = max(self._message_end_time or timestamp, timestamp)

    def _flush_chunk(self) -> None:
        if self._chunk_buffer is None or self._chunk_buffer.size() == 0:
            return
        records = self._chunk_buffer.as_bytes()
        chunk = ChunkRecord(
            message_start_time=self._chunk_start_time or 0,
            message_end_time=self._chunk_end_time or 0,
            uncompressed_size=len(records),
            uncompressed_crc=zlib.crc32(records),
            compression="",
            records=records,
        )
        chunk_start_offset = self._writer.tell()
        McapRecordWriter.write_chunk(self._writer, chunk)
        chunk_length = self._writer.tell() - chunk_start_offset
        self._message_index_records.append(
            {
                cid: MessageIndexRecord(channel_id=cid, records=idx)
                for cid, idx in self._current_message_index.items()
            }
        )
        self._chunk_indexes.append(
            ChunkIndexRecord(
                message_start_time=chunk.message_start_time,
                message_end_time=chunk.message_end_time,
                chunk_start_offset=chunk_start_offset,
                chunk_length=chunk_length,
                message_index_offsets={},
                message_index_length=0,
                compression=chunk.compression,
                compressed_size=len(chunk.records),
                uncompressed_size=chunk.uncompressed_size,
            )
        )
        self._chunk_count += 1
        self._chunk_buffer = None
        self._chunk_start_time = None
        self._chunk_end_time = None
        self._current_message_index = {}

    def close(self) -> None:
        if self._chunk_size is not None:
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
        for chunk_index, message_indexes in zip(self._chunk_indexes, self._message_index_records):
            chunk_message_index_start = self._writer.tell()
            offsets: dict[int, int] = {}
            for channel_id, record in message_indexes.items():
                offsets[channel_id] = self._writer.tell()
                McapRecordWriter.write_message_index(self._writer, record)
            chunk_index.message_index_offsets = offsets
            chunk_index.message_index_length = self._writer.tell() - chunk_message_index_start
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

    def __enter__(self) -> "McapFileWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
