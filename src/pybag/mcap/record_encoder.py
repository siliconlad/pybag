import struct
from typing import Any, Callable

from pybag import __version__
from pybag.io.raw_writer import BaseWriter
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
