from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict

from pybag.crc import assert_crc
from pybag.io.raw_reader import BytesReader
from pybag.mcap.error import McapUnknownCompressionError
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap.records import (
    ChannelRecord,
    ChunkIndexRecord,
    ChunkRecord,
    MessageIndexRecord,
    SchemaRecord,
    StatisticsRecord,
)


@dataclass(slots=True)
class ReconstructedSummary:
    """Container for reconstructed MCAP summary data."""

    summary_start: int = 0
    summary_offset_start: int = 0
    summary_offsets: Dict[int, tuple[int, int]] = field(default_factory=dict)
    schemas: Dict[int, SchemaRecord] = field(default_factory=dict)
    channels: Dict[int, ChannelRecord] = field(default_factory=dict)
    statistics: StatisticsRecord | None = None
    chunk_indexes: list[ChunkIndexRecord] = field(default_factory=list)
    chunk_message_indexes: Dict[int, Dict[int, MessageIndexRecord]] = field(default_factory=dict)
    non_chunked_message_index: Dict[int, Dict[int, list[int]]] = field(default_factory=dict)

    @property
    def is_chunked(self) -> bool:
        return len(self.chunk_indexes) > 0


class SummaryReconstructor:
    """Utility to rebuild MCAP summary information when missing."""

    def __init__(self, data: bytes):
        self._data = data

    @classmethod
    def from_bytes(cls, data: bytes) -> ReconstructedSummary:
        return cls(data)._build()

    def _build(self) -> ReconstructedSummary:
        reader = BytesReader(self._data)

        # Parse magic bytes and header to position reader at start of records.
        _ = McapRecordParser.parse_magic_bytes(reader)
        _ = McapRecordParser.parse_header(reader)

        schemas: dict[int, SchemaRecord] = {}
        channels: dict[int, ChannelRecord] = {}
        chunk_indexes: dict[int, ChunkIndexRecord] = {}
        chunk_message_indexes: dict[int, dict[int, MessageIndexRecord]] = {}
        non_chunked_index: dict[int, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))

        message_count = 0
        channel_message_counts: dict[int, int] = defaultdict(int)
        message_start_time: int | None = None
        message_end_time: int | None = None
        attachment_count = 0
        metadata_count = 0

        # Track the last chunk we processed to associate message index records.
        last_chunk_start: int | None = None

        while True:
            record_start = reader.tell()
            record_type = McapRecordParser.peek_record(reader)
            if record_type == 0:
                break
            if record_type == McapRecordType.FOOTER:
                break

            if record_type == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(reader)
                if schema is not None:
                    schemas[schema.id] = schema
                continue

            if record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(reader)
                channels[channel.id] = channel
                continue

            if record_type == McapRecordType.MESSAGE:
                message = McapRecordParser.parse_message(reader)
                non_chunked_index[message.channel_id][message.log_time].append(record_start)
                message_count, message_start_time, message_end_time = self._update_statistics(
                    message.log_time,
                    channel_message_counts,
                    message.channel_id,
                    message_count,
                    message_start_time,
                    message_end_time,
                )
                continue

            if record_type == McapRecordType.CHUNK:
                chunk = McapRecordParser.parse_chunk(reader)
                chunk_end = reader.tell()
                chunk_start = record_start
                last_chunk_start = chunk_start

                reconstructed_index = ChunkIndexRecord(
                    message_start_time=chunk.message_start_time,
                    message_end_time=chunk.message_end_time,
                    chunk_start_offset=chunk_start,
                    chunk_length=chunk_end - chunk_start,
                    message_index_offsets={},
                    message_index_length=0,
                    compression=chunk.compression,
                    compressed_size=len(chunk.records),
                    uncompressed_size=chunk.uncompressed_size,
                )
                chunk_indexes[chunk_start] = reconstructed_index

                # Reconstruct message indexes by decoding the chunk contents.
                chunk_data = self._decompress_chunk(chunk, check_crc=False)
                chunk_reader = BytesReader(chunk_data)
                channel_indexes: dict[int, MessageIndexRecord] = {}

                while True:
                    chunk_offset = chunk_reader.tell()
                    inner_type = McapRecordParser.peek_record(chunk_reader)
                    if inner_type == 0:
                        break
                    if inner_type == McapRecordType.MESSAGE:
                        message = McapRecordParser.parse_message(chunk_reader)
                        channel_indexes.setdefault(
                            message.channel_id,
                            MessageIndexRecord(message.channel_id, []),
                        ).records.append((message.log_time, chunk_offset))
                        message_count, message_start_time, message_end_time = self._update_statistics(
                            message.log_time,
                            channel_message_counts,
                            message.channel_id,
                            message_count,
                            message_start_time,
                            message_end_time,
                        )
                        continue

                    # Skip other record types within the chunk payload.
                    McapRecordParser.skip_record(chunk_reader)

                chunk_message_indexes[chunk_start] = channel_indexes
                continue

            if record_type == McapRecordType.MESSAGE_INDEX:
                index_start = reader.tell()
                message_index = McapRecordParser.parse_message_index(reader)
                if last_chunk_start is not None:
                    chunk_indexes.setdefault(
                        last_chunk_start,
                        ChunkIndexRecord(
                            message_start_time=0,
                            message_end_time=0,
                            chunk_start_offset=last_chunk_start,
                            chunk_length=0,
                            message_index_offsets={},
                            message_index_length=0,
                            compression="",
                            compressed_size=0,
                            uncompressed_size=0,
                        ),
                    )
                    chunk = chunk_indexes[last_chunk_start]
                    chunk.message_index_offsets[message_index.channel_id] = index_start
                    chunk.message_index_length += reader.tell() - index_start
                    chunk_message_indexes.setdefault(last_chunk_start, {})[
                        message_index.channel_id
                    ] = message_index
                continue

            if record_type == McapRecordType.CHUNK_INDEX:
                chunk_index = McapRecordParser.parse_chunk_index(reader)
                chunk_indexes[chunk_index.chunk_start_offset] = chunk_index
                chunk_message_indexes.setdefault(chunk_index.chunk_start_offset, {})
                continue

            if record_type == McapRecordType.STATISTICS:
                _ = McapRecordParser.parse_statistics(reader)
                continue

            if record_type == McapRecordType.ATTACHMENT:
                _ = McapRecordParser.parse_attachment(reader)
                attachment_count += 1
                continue

            if record_type == McapRecordType.METADATA:
                _ = McapRecordParser.parse_metadata(reader)
                metadata_count += 1
                continue

            if record_type == McapRecordType.DATA_END:
                _ = McapRecordParser.parse_data_end(reader)
                continue

            if record_type == McapRecordType.ATTACHMENT_INDEX:
                _ = McapRecordParser.parse_attachment_index(reader)
                continue

            if record_type == McapRecordType.METADATA_INDEX:
                _ = McapRecordParser.parse_metadata_index(reader)
                continue

            # Skip any unknown or unsupported record types.
            McapRecordParser.skip_record(reader)

        # Parse footer to advance reader but we don't rely on summary info.
        if McapRecordParser.peek_record(reader) == McapRecordType.FOOTER:
            _ = McapRecordParser.parse_footer(reader)
            try:
                _ = McapRecordParser.parse_magic_bytes(reader)
            except Exception:  # pragma: no cover - tolerates trailing padding
                pass

        statistics = StatisticsRecord(
            message_count=message_count,
            schema_count=len(schemas),
            channel_count=len(channels),
            attachment_count=attachment_count,
            metadata_count=metadata_count,
            chunk_count=len(chunk_indexes),
            message_start_time=message_start_time or 0,
            message_end_time=message_end_time or 0,
            channel_message_counts=dict(channel_message_counts),
        )

        return ReconstructedSummary(
            schemas=schemas,
            channels=channels,
            statistics=statistics,
            chunk_indexes=sorted(chunk_indexes.values(), key=lambda idx: idx.message_start_time),
            chunk_message_indexes=chunk_message_indexes,
            non_chunked_message_index={
                channel_id: dict(timestamp_map)
                for channel_id, timestamp_map in non_chunked_index.items()
            },
        )

    @staticmethod
    def _update_statistics(
        log_time: int,
        channel_message_counts: dict[int, int],
        channel_id: int,
        message_count: int,
        message_start_time: int | None,
        message_end_time: int | None,
    ) -> tuple[int, int | None, int | None]:
        channel_message_counts[channel_id] += 1
        message_count += 1
        if message_start_time is None or log_time < message_start_time:
            message_start_time = log_time
        if message_end_time is None or log_time > message_end_time:
            message_end_time = log_time
        return message_count, message_start_time, message_end_time

    @staticmethod
    def _decompress_chunk(chunk: ChunkRecord, *, check_crc: bool) -> bytes:
        if chunk.compression == "zstd":
            import zstandard as zstd

            chunk_data = zstd.ZstdDecompressor().decompress(chunk.records)
        elif chunk.compression == "lz4":
            import lz4.frame

            chunk_data = lz4.frame.decompress(chunk.records)
        elif chunk.compression == "":
            chunk_data = chunk.records
        else:  # pragma: no cover - handled by caller
            msg = f"Unknown compression type: {chunk.compression}"
            raise McapUnknownCompressionError(msg)

        if check_crc and chunk.uncompressed_crc != 0:
            assert_crc(chunk_data, chunk.uncompressed_crc)
        return chunk_data
