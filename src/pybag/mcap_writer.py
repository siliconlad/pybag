"""Utilities for writing MCAP files."""

from __future__ import annotations

import logging
import zlib
from dataclasses import fields, is_dataclass
from typing import Annotated, Any, get_args, get_origin

from pybag.encoding.cdr import CdrEncoder
from pybag.io.raw_writer import BaseWriter, FileWriter
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
from pybag.schema.ros2msg import PRIMITIVE_TYPE_MAP, STRING_TYPE_MAP

logger = logging.getLogger(__name__)


def _annotation_to_ros_type(annotation: Any) -> tuple[str, tuple[Any, ...]]:
    """Extract the ROS type string from an ``Annotated`` field annotation."""

    # ``types.Array`` returns ``Annotated[list[T], ("array", ...)]``.  The
    # metadata lives on the ``Annotated`` instance, so unwrap the ``list`` first.
    if get_origin(annotation) is list:
        annotation = get_args(annotation)[0]

    if get_origin(annotation) is not Annotated:
        raise TypeError("Fields must use pybag.types annotations")

    args = get_args(annotation)[1]
    return args[0], args


def serialize_message(message: Any, little_endian: bool = True) -> bytes:
    """Serialize a dataclass instance into a CDR byte stream."""

    if not is_dataclass(message):  # pragma: no cover - defensive programming
        raise TypeError("Expected a dataclass instance")

    encoder = CdrEncoder(little_endian=little_endian)

    for field in fields(message):
        value = getattr(message, field.name)
        ros_type, args = _annotation_to_ros_type(field.type)

        if ros_type in PRIMITIVE_TYPE_MAP:
            encoder.encode(ros_type, value)
        elif ros_type in STRING_TYPE_MAP:
            encoder.string(value)
        elif ros_type == "array":
            sub_ros_type, _ = _annotation_to_ros_type(args[1])
            if args[2] is None:
                encoder.sequence(sub_ros_type, value)
            else:
                encoder.array(sub_ros_type, value)
        else:  # pragma: no cover - complex types are not yet supported
            raise ValueError(f"Unsupported field type: {ros_type}")

    return encoder.save()


class _CRCWriter(BaseWriter):
    """Writer wrapper that tracks position and CRC32."""

    def __init__(self, writer: BaseWriter):
        self._writer = writer
        self.crc = 0
        self.position = 0

    def write(self, data: bytes) -> int:  # pragma: no cover - thin wrapper
        self.crc = zlib.crc32(data, self.crc)
        self.position += len(data)
        return self._writer.write(data)

    def close(self) -> None:  # pragma: no cover - thin wrapper
        self._writer.close()


class McapFileWriter:
    """High level writer for producing MCAP files."""

    def __init__(self, writer: BaseWriter) -> None:
        self._writer = _CRCWriter(writer)
        self._next_schema_id = 1
        self._next_channel_id = 1
        self._schemas: dict[type, int] = {}
        self._schema_records: list[SchemaRecord] = []
        self._channel_records: list[ChannelRecord] = []
        self._sequences: dict[int, int] = {}
        self._message_count = 0
        self._message_start_time: int | None = None
        self._message_end_time: int | None = None
        self._channel_message_counts: dict[int, int] = {}

        McapRecordWriter.write_magic_bytes(self._writer)
        header = HeaderRecord(profile="ros2", library="pybag")
        McapRecordWriter.write_header(self._writer, header)

    @classmethod
    def open(cls, file_path: str | bytes | Any) -> "McapFileWriter":
        """Create a writer backed by a file on disk."""

        return cls(FileWriter(file_path))

    def add_channel(self, topic: str, channel_type: type) -> int:
        """Add a channel to the MCAP output.

        A corresponding :class:`SchemaRecord` and :class:`ChannelRecord` are
        written to the underlying writer.
        """

        if channel_type not in self._schemas:
            schema_id = self._next_schema_id
            self._next_schema_id += 1

            schema_record = SchemaRecord(
                id=schema_id,
                name=channel_type.__name__,
                encoding="ros2msg",
                data=b"",
            )
            McapRecordWriter.write_schema(self._writer, schema_record)
            self._schemas[channel_type] = schema_id
            self._schema_records.append(schema_record)
        else:
            schema_id = self._schemas[channel_type]

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
        self._channel_records.append(channel_record)
        self._sequences[channel_id] = 0
        self._channel_message_counts[channel_id] = 0
        return channel_id

    def write_message(self, channel_id: int, timestamp: int, message: Any) -> None:
        """Write ``message`` to ``channel_id`` at ``timestamp``."""

        data = serialize_message(message)
        sequence = self._sequences[channel_id]
        record = MessageRecord(
            channel_id=channel_id,
            sequence=sequence,
            log_time=timestamp,
            publish_time=timestamp,
            data=data,
        )
        McapRecordWriter.write_message(self._writer, record)
        self._sequences[channel_id] = sequence + 1
        self._message_count += 1
        self._channel_message_counts[channel_id] += 1
        if self._message_start_time is None or timestamp < self._message_start_time:
            self._message_start_time = timestamp
        if self._message_end_time is None or timestamp > self._message_end_time:
            self._message_end_time = timestamp

    def close(self) -> None:
        # Data end -------------------------------------------------------
        data_end = DataEndRecord(data_section_crc=0)
        McapRecordWriter.write_data_end(self._writer, data_end)

        summary_start = self._writer.position

        # Schema group ---------------------------------------------------
        schema_group_start = self._writer.position
        for record in self._schema_records:
            McapRecordWriter.write_schema(self._writer, record)
        schema_group_length = self._writer.position - schema_group_start

        # Channel group --------------------------------------------------
        channel_group_start = self._writer.position
        for record in self._channel_records:
            McapRecordWriter.write_channel(self._writer, record)
        channel_group_length = self._writer.position - channel_group_start

        # Statistics group ----------------------------------------------
        statistics_group_start = self._writer.position
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
        statistics_group_length = self._writer.position - statistics_group_start

        summary_offset_start = self._writer.position

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

        summary_crc = self._writer.crc
        footer = FooterRecord(
            summary_start=summary_start,
            summary_offset_start=summary_offset_start,
            summary_crc=summary_crc,
        )
        McapRecordWriter.write_footer(self._writer, footer)
        McapRecordWriter.write_magic_bytes(self._writer)
        self._writer.close()

    # Context manager support -------------------------------------------
    def __enter__(self) -> "McapFileWriter":  # pragma: no cover - thin wrapper
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - thin wrapper
        self.close()

