"""Utilities for writing MCAP files."""

from __future__ import annotations

import logging
import math
from dataclasses import is_dataclass
from pathlib import Path
from typing import Any

from pybag import __version__
from pybag.encoding.cdr import CdrEncoder
from pybag.io.raw_writer import BaseWriter, CrcWriter, FileWriter
from pybag.mcap.record_writer import McapRecordWriter
from pybag.mcap.records import (
    ChannelRecord,
    DataEndRecord,
    FooterRecord,
    HeaderRecord,
    MessageRecord,
    RecordType,
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)
from pybag.schema.ros2msg import (
    Array,
    Complex,
    Primitive,
    Ros2MsgSchemaEncoder,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String
)

logger = logging.getLogger(__name__)


def serialize_message(message: Any, little_endian: bool = True) -> bytes:
    """Serialize a dataclass instance into a CDR byte stream."""

    if not is_dataclass(message):  # pragma: no cover - defensive programming
        raise TypeError("Expected a dataclass instance")

    encoder = CdrEncoder(little_endian=little_endian)
    schema, sub_schemas = Ros2MsgSchemaEncoder().encode(message)

    def _encode_field(message: Any, schema_field: SchemaField, sub_schemas: dict[str, Schema]) -> None:
        if isinstance(schema_field.type, Primitive):
            primitive_type = schema_field.type
            encoder.encode(primitive_type.type, message)

        if isinstance(schema_field.type, String):
            string_type = schema_field.type
            encoder.encode(string_type.type, message)

        if isinstance(schema_field.type, Array):
            array_type = schema_field.type
            if isinstance(array_type.type, (Primitive, String)):
                encoder.array(array_type.type.type, message)
            elif isinstance(array_type.type, Complex):
                for item in message:
                    _encode_message(item, array_type.type, sub_schemas)
            else:
                raise ValueError(f"Unknown array type: {array_type.type}")

        if isinstance(schema_field.type, Sequence):
            sequence_type = schema_field.type
            if isinstance(sequence_type.type, (Primitive, String)):
                encoder.sequence(sequence_type.type.type, message)
            elif isinstance(sequence_type.type, Complex):
                encoder.uint32(len(message))
                for item in message:
                    _encode_message(item, sequence_type.type, sub_schemas)
            else:
                raise ValueError(f"Unknown sequence type: {sequence_type.type}")

        if isinstance(schema_field.type, Complex):
            complex_type = schema_field.type
            if complex_type.type not in sub_schemas:
                raise ValueError(f"Complex type {complex_type.type} not found in sub_schemas")
            _encode_message(message, sub_schemas[complex_type.type], sub_schemas)

    def _encode_message(message: Any, schema: Schema, sub_schemas: dict[str, Schema]) -> None:
        if isinstance(schema, Complex):
            _encode_message(message, sub_schemas[schema.type], sub_schemas)
        else:
            for field_name, schema_field in schema.fields.items():
                if isinstance(schema_field, SchemaConstant):
                    continue  # Nothing to do for constants
                _encode_field(getattr(message, field_name), schema_field, sub_schemas)

    if isinstance(schema, Complex):
        schema = sub_schemas[schema.type]
    _encode_message(message, schema, sub_schemas)

    return encoder.save()


class McapFileWriter:
    """High level writer for producing MCAP files."""

    def __init__(self, writer: BaseWriter) -> None:
        self._writer = CrcWriter(writer)

        self._next_schema_id = 1  # Schema ID must be non-zero
        self._next_channel_id = 1

        self._topics: dict[str, int] = {}    # topic -> channel_id
        self._schemas: dict[type, int] = {}  # type -> schema_id
        self._schema_records: list[SchemaRecord] = []
        self._channel_records: list[ChannelRecord] = []
        self._sequences: dict[int, int] = {}

        self._message_count = 0
        self._message_start_time: int = math.inf
        self._message_end_time: int = -math.inf
        self._channel_message_counts: dict[int, int] = {}

        # Write the start of the file
        McapRecordWriter.write_magic_bytes(self._writer)
        header = HeaderRecord(profile="ros2", library=f"pybag {__version__}")
        McapRecordWriter.write_header(self._writer, header)

    @classmethod
    def open(cls, file_path: str | Path) -> "McapFileWriter":
        """Create a writer backed by a file on disk."""

        return cls(FileWriter(file_path))

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

                schema_record = SchemaRecord(
                    id=schema_id,
                    name=channel_type.__name__,
                    encoding="ros2msg",
                    data=Ros2MsgSchemaEncoder().encode(channel_type),
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
            data=serialize_message(message),
        )
        McapRecordWriter.write_message(self._writer, record)

        self._message_count += 1
        self._channel_message_counts[channel_id] += 1
        self._message_start_time = min(self._message_start_time or timestamp, timestamp)
        self._message_end_time = max(self._message_end_time or timestamp, timestamp)

    def close(self) -> None:
        # Data end record
        data_end = DataEndRecord(data_section_crc=self._writer.get_crc())
        McapRecordWriter.write_data_end(self._writer, data_end)

        summary_start = self._writer.tell()

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

        # Statistics record
        statistics_group_start = self._writer.tell()
        stats = StatisticsRecord(
            message_count=self._message_count,
            schema_count=len(self._schema_records),
            channel_count=len(self._channel_records),
            attachment_count=0,
            metadata_count=0,
            chunk_count=0,
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
        McapRecordWriter.write_summary_offset(
            self._writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.STATISTICS,
                group_start=statistics_group_start,
                group_length=statistics_group_length,
            ),
        )

        # Footer record
        summary_crc = self._writer.get_crc()
        footer = FooterRecord(
            summary_start=summary_start,
            summary_offset_start=summary_offset_start,
            summary_crc=summary_crc,
        )
        McapRecordWriter.write_footer(self._writer, footer)
        McapRecordWriter.write_magic_bytes(self._writer)

        # Close the file
        self._writer.close()

    def __enter__(self) -> "McapFileWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
