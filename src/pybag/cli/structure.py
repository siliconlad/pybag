"""Utilities for rendering MCAP file structure for the CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.mcap.error import McapUnknownCompressionError
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.record_reader import decompress_chunk
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


def describe_mcap_structure(file_path: Path) -> str:
    """Return a human readable description of the MCAP structure."""

    reader = FileReader(file_path)
    try:
        formatter = _McapStructureFormatter()
        return formatter.describe(reader)
    finally:
        reader.close()


def structure_command(args: Any) -> None:
    """Entry point for the ``pybag structure`` CLI command."""

    file_path: Path = args.mcap
    if not file_path.exists():
        raise SystemExit(f"MCAP file not found: {file_path}")

    print(describe_mcap_structure(file_path))


class _McapStructureFormatter:
    """Format MCAP records into an ASCII box representation."""

    def __init__(self) -> None:
        self._lines: list[str] = []
        self._schemas: dict[int, SchemaRecord] = {}
        self._channels: dict[int, ChannelRecord] = {}
        self._initial_magic_consumed = False

    def describe(self, reader: BaseReader) -> str:
        version = McapRecordParser.parse_magic_bytes(reader)
        self._initial_magic_consumed = True
        self._append_box("MAGIC_BYTES", [f"version: {version}"], indent=0)

        for record_type_value, record in McapRecordParser.parse_record(reader):
            try:
                record_type = RecordType(record_type_value)
            except ValueError:
                self._append_box(
                    f"UNKNOWN_RECORD ({record_type_value})",
                    [],
                    indent=0,
                )
                continue

            self._format_record(record_type, record, indent=0)

        return "\n".join(self._lines)

    # ------------------------------------------------------------------
    # Formatting helpers

    def _format_record(self, record_type: RecordType, record: Any, indent: int) -> None:
        if record_type == RecordType.HEADER:
            self._append_box(
                "HEADER",
                [
                    f"profile: {record.profile}",
                    f"library: {record.library}",
                ],
                indent,
            )
        elif record_type == RecordType.FOOTER:
            footer: FooterRecord = record
            self._append_box(
                "FOOTER",
                [
                    f"summary_start: {footer.summary_start}",
                    f"summary_offset_start: {footer.summary_offset_start}",
                    f"summary_crc: {footer.summary_crc}",
                ],
                indent,
            )
        elif record_type == RecordType.SCHEMA:
            schema: SchemaRecord | None = record
            if schema is None:
                return
            self._schemas[schema.id] = schema
            self._append_box(
                f"SCHEMA (id={schema.id})",
                [
                    f"name: {schema.name}",
                    f"encoding: {schema.encoding}",
                    f"data_size: {len(schema.data)} bytes",
                ],
                indent,
            )
        elif record_type == RecordType.CHANNEL:
            channel: ChannelRecord = record
            self._channels[channel.id] = channel
            schema_label = self._schema_label(channel.schema_id)
            self._append_box(
                f"CHANNEL (id={channel.id})",
                [
                    f"topic: {channel.topic}",
                    f"schema: {schema_label}",
                    f"message_encoding: {channel.message_encoding}",
                    f"metadata: {len(channel.metadata)} entries",
                ],
                indent,
            )
        elif record_type == RecordType.MESSAGE:
            message: MessageRecord = record
            channel_label = self._channel_label(message.channel_id)
            schema_label = self._schema_for_channel(message.channel_id)
            self._append_box(
                f"MESSAGE (channel={message.channel_id})",
                [
                    f"channel: {channel_label}",
                    f"sequence: {message.sequence}",
                    f"schema: {schema_label}",
                    f"log_time: {message.log_time}",
                    f"publish_time: {message.publish_time}",
                    f"size: {len(message.data)} bytes",
                ],
                indent,
            )
        elif record_type == RecordType.CHUNK:
            chunk: ChunkRecord = record
            compression = chunk.compression or "none"
            self._append_box(
                "CHUNK",
                [
                    f"message_start: {chunk.message_start_time}",
                    f"message_end: {chunk.message_end_time}",
                    f"compression: {compression}",
                    f"uncompressed_size: {chunk.uncompressed_size} bytes",
                    f"uncompressed_crc: {chunk.uncompressed_crc}",
                    f"records_length: {len(chunk.records)} bytes",
                ],
                indent,
            )

            try:
                chunk_data = decompress_chunk(chunk)
            except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover - defensive
                raise RuntimeError(
                    "Unable to decompress chunk. Optional compression dependency missing."
                ) from exc
            except McapUnknownCompressionError as exc:  # pragma: no cover - defensive
                raise RuntimeError(str(exc)) from exc

            chunk_reader = BytesReader(chunk_data)
            for inner_type_value, inner_record in McapRecordParser.parse_record(chunk_reader):
                inner_type = RecordType(inner_type_value)
                self._format_record(inner_type, inner_record, indent + 1)
        elif record_type == RecordType.MESSAGE_INDEX:
            message_index: MessageIndexRecord = record
            channel_label = self._channel_label(message_index.channel_id)
            entries = len(message_index.records)
            body = [
                f"channel: {channel_label}",
                f"entries: {entries}",
            ]
            if entries:
                start = message_index.records[0][0]
                end = message_index.records[-1][0]
                body.append(f"time_range: {start} -> {end}")
            self._append_box("MESSAGE_INDEX", body, indent)
        elif record_type == RecordType.CHUNK_INDEX:
            chunk_index: ChunkIndexRecord = record
            compression = chunk_index.compression or "none"
            self._append_box(
                "CHUNK_INDEX",
                [
                    f"message_start: {chunk_index.message_start_time}",
                    f"message_end: {chunk_index.message_end_time}",
                    f"chunk_start_offset: {chunk_index.chunk_start_offset}",
                    f"chunk_length: {chunk_index.chunk_length}",
                    f"message_index_length: {chunk_index.message_index_length}",
                    f"message_index_offsets: {len(chunk_index.message_index_offsets)} entries",
                    f"compression: {compression}",
                    f"compressed_size: {chunk_index.compressed_size} bytes",
                    f"uncompressed_size: {chunk_index.uncompressed_size} bytes",
                ],
                indent,
            )
        elif record_type == RecordType.ATTACHMENT:
            attachment: AttachmentRecord = record
            self._append_box(
                f"ATTACHMENT ({attachment.name})",
                [
                    f"log_time: {attachment.log_time}",
                    f"create_time: {attachment.create_time}",
                    f"media_type: {attachment.media_type}",
                    f"size: {len(attachment.data)} bytes",
                ],
                indent,
            )
        elif record_type == RecordType.METADATA:
            metadata: MetadataRecord = record
            self._append_box(
                f"METADATA ({metadata.name})",
                [f"entries: {len(metadata.metadata)}"],
                indent,
            )
        elif record_type == RecordType.DATA_END:
            data_end: DataEndRecord = record
            self._append_box(
                "DATA_END",
                [f"data_section_crc: {data_end.data_section_crc}"],
                indent,
            )
        elif record_type == RecordType.ATTACHMENT_INDEX:
            attachment_index: AttachmentIndexRecord = record
            self._append_box(
                f"ATTACHMENT_INDEX ({attachment_index.name})",
                [
                    f"offset: {attachment_index.offset}",
                    f"length: {attachment_index.length}",
                    f"log_time: {attachment_index.log_time}",
                    f"create_time: {attachment_index.create_time}",
                    f"data_size: {attachment_index.data_size} bytes",
                    f"media_type: {attachment_index.media_type}",
                ],
                indent,
            )
        elif record_type == RecordType.METADATA_INDEX:
            metadata_index: MetadataIndexRecord = record
            self._append_box(
                f"METADATA_INDEX ({metadata_index.name})",
                [
                    f"offset: {metadata_index.offset}",
                    f"length: {metadata_index.length}",
                ],
                indent,
            )
        elif record_type == RecordType.STATISTICS:
            stats: StatisticsRecord = record
            self._append_box(
                "STATISTICS",
                [
                    f"message_count: {stats.message_count}",
                    f"schema_count: {stats.schema_count}",
                    f"channel_count: {stats.channel_count}",
                    f"attachment_count: {stats.attachment_count}",
                    f"metadata_count: {stats.metadata_count}",
                    f"chunk_count: {stats.chunk_count}",
                    f"message_start: {stats.message_start_time}",
                    f"message_end: {stats.message_end_time}",
                ],
                indent,
            )
        elif record_type == RecordType.SUMMARY_OFFSET:
            summary_offset: SummaryOffsetRecord = record
            try:
                group_name = RecordType(summary_offset.group_opcode).name
            except ValueError:
                group_name = str(summary_offset.group_opcode)
            self._append_box(
                "SUMMARY_OFFSET",
                [
                    f"group: {group_name}",
                    f"group_start: {summary_offset.group_start}",
                    f"group_length: {summary_offset.group_length}",
                ],
                indent,
            )
        elif record_type == RecordType.MAGIC_BYTES:
            label = "MAGIC_BYTES (end)" if self._initial_magic_consumed else "MAGIC_BYTES"
            self._append_box(label, [f"version: {record}"], indent)
        else:  # pragma: no cover - defensive
            self._append_box(record_type.name, [], indent)

    def _append_box(self, title: str, body_lines: Iterable[str], indent: int) -> None:
        body = list(body_lines)
        width = max([len(title), *(len(line) for line in body)] or [len(title)])
        padding = "    " * indent

        top = f"{padding}┌{'─' * (width + 2)}┐"
        header = f"{padding}│ {title.ljust(width)} │"
        content = [f"{padding}│ {line.ljust(width)} │" for line in body]
        bottom = f"{padding}└{'─' * (width + 2)}┘"

        self._lines.extend([top, header, *content, bottom])

    def _channel_label(self, channel_id: int) -> str:
        channel = self._channels.get(channel_id)
        if channel is None:
            return str(channel_id)
        return f"{channel_id} ({channel.topic})"

    def _schema_label(self, schema_id: int) -> str:
        schema = self._schemas.get(schema_id)
        if schema is None:
            return str(schema_id)
        return f"{schema_id} ({schema.name})"

    def _schema_for_channel(self, channel_id: int) -> str:
        channel = self._channels.get(channel_id)
        if channel is None:
            return "unknown"
        return self._schema_label(channel.schema_id)

