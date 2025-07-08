import struct

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
    SummaryOffsetRecord,
)


class McapRecordWriter:
    """Utilities for writing MCAP records."""

    @classmethod
    def write_magic_bytes(cls, writer: BaseWriter, version: str = "0") -> None:
        """Write the MCAP magic bytes."""
        magic = b"\x89MCAP" + version.encode() + b"\r\n"
        writer.write(magic)

    # Primitive encoders -------------------------------------------------
    @staticmethod
    def _write_uint8(writer: BaseWriter, value: int) -> None:
        writer.write(struct.pack("<B", value))

    @staticmethod
    def _write_uint16(writer: BaseWriter, value: int) -> None:
        writer.write(struct.pack("<H", value))

    @staticmethod
    def _write_uint32(writer: BaseWriter, value: int) -> None:
        writer.write(struct.pack("<I", value))

    @staticmethod
    def _write_uint64(writer: BaseWriter, value: int) -> None:
        writer.write(struct.pack("<Q", value))

    @classmethod
    def _write_timestamp(cls, writer: BaseWriter, value: int) -> None:
        cls._write_uint64(writer, value)

    @classmethod
    def _encode_string(cls, value: str) -> bytes:
        encoded = value.encode()
        return struct.pack("<I", len(encoded)) + encoded

    @classmethod
    def _write_string(cls, writer: BaseWriter, value: str) -> None:
        writer.write(cls._encode_string(value))

    @classmethod
    def _encode_bytes(cls, value: bytes) -> bytes:
        return value

    @classmethod
    def _write_bytes(cls, writer: BaseWriter, value: bytes) -> None:
        writer.write(value)

    # Container encoders -------------------------------------------------
    @classmethod
    def _encode_tuple_timestamp_uint64(cls, data: tuple[int, int]) -> bytes:
        timestamp, offset = data
        return struct.pack("<QQ", timestamp, offset)

    @classmethod
    def _encode_array_tuple_timestamp_uint64(
        cls, values: list[tuple[int, int]]
    ) -> bytes:
        parts = [cls._encode_tuple_timestamp_uint64(v) for v in values]
        payload = b"".join(parts)
        return struct.pack("<I", len(payload)) + payload

    @classmethod
    def _encode_map_string_string(cls, data: dict[str, str]) -> bytes:
        parts: list[bytes] = []
        for k, v in data.items():
            parts.append(cls._encode_string(k))
            parts.append(cls._encode_string(v))
        payload = b"".join(parts)
        return struct.pack("<I", len(payload)) + payload

    @classmethod
    def _encode_map_uint16_uint64(cls, data: dict[int, int]) -> bytes:
        parts = [struct.pack("<H", k) + struct.pack("<Q", v) for k, v in data.items()]
        payload = b"".join(parts)
        return struct.pack("<I", len(payload)) + payload

    # MCAP Record Writers -----------------------------------------------
    @classmethod
    def write_header(cls, writer: BaseWriter, record: HeaderRecord) -> None:
        payload = cls._encode_string(record.profile) + cls._encode_string(record.library)
        writer.write(bytes([RecordType.HEADER]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_footer(cls, writer: BaseWriter, record: FooterRecord) -> None:
        writer.write(bytes([RecordType.FOOTER]))
        cls._write_uint64(writer, 20)
        cls._write_uint64(writer, record.summary_start)
        cls._write_uint64(writer, record.summary_offset_start)
        cls._write_uint32(writer, record.summary_crc)

    @classmethod
    def write_schema(cls, writer: BaseWriter, record: SchemaRecord) -> None:
        payload = (
            struct.pack("<H", record.id)
            + cls._encode_string(record.name)
            + cls._encode_string(record.encoding)
            + struct.pack("<I", len(record.data))
            + record.data
        )
        writer.write(bytes([RecordType.SCHEMA]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_channel(cls, writer: BaseWriter, record: ChannelRecord) -> None:
        metadata_bytes = cls._encode_map_string_string(record.metadata)
        payload = (
            struct.pack("<H", record.id)
            + struct.pack("<H", record.schema_id)
            + cls._encode_string(record.topic)
            + cls._encode_string(record.message_encoding)
            + metadata_bytes
        )
        writer.write(bytes([RecordType.CHANNEL]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_message(cls, writer: BaseWriter, record: MessageRecord) -> None:
        payload = (
            struct.pack("<H", record.channel_id)
            + struct.pack("<I", record.sequence)
            + struct.pack("<Q", record.log_time)
            + struct.pack("<Q", record.publish_time)
            + record.data
        )
        writer.write(bytes([RecordType.MESSAGE]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_chunk(cls, writer: BaseWriter, record: ChunkRecord) -> None:
        payload = (
            struct.pack("<Q", record.message_start_time)
            + struct.pack("<Q", record.message_end_time)
            + struct.pack("<Q", record.uncompressed_size)
            + struct.pack("<I", record.uncompressed_crc)
            + cls._encode_string(record.compression)
            + struct.pack("<Q", len(record.records))
            + record.records
        )
        writer.write(bytes([RecordType.CHUNK]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_message_index(cls, writer: BaseWriter, record: MessageIndexRecord) -> None:
        records_bytes = cls._encode_array_tuple_timestamp_uint64(record.records)
        payload = struct.pack("<H", record.channel_id) + records_bytes
        writer.write(bytes([RecordType.MESSAGE_INDEX]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_chunk_index(cls, writer: BaseWriter, record: ChunkIndexRecord) -> None:
        message_index_offsets = cls._encode_map_uint16_uint64(record.message_index_offsets)
        payload = (
            struct.pack("<Q", record.message_start_time)
            + struct.pack("<Q", record.message_end_time)
            + struct.pack("<Q", record.chunk_start_offset)
            + struct.pack("<Q", record.chunk_length)
            + message_index_offsets
            + struct.pack("<Q", record.message_index_length)
            + cls._encode_string(record.compression)
            + struct.pack("<Q", record.compressed_size)
            + struct.pack("<Q", record.uncompressed_size)
        )
        writer.write(bytes([RecordType.CHUNK_INDEX]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_attachment(cls, writer: BaseWriter, record: AttachmentRecord) -> None:
        payload = (
            struct.pack("<Q", record.log_time)
            + struct.pack("<Q", record.create_time)
            + cls._encode_string(record.name)
            + cls._encode_string(record.media_type)
            + struct.pack("<Q", len(record.data))
            + record.data
            + struct.pack("<I", record.crc)
        )
        writer.write(bytes([RecordType.ATTACHMENT]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_metadata(cls, writer: BaseWriter, record: MetadataRecord) -> None:
        metadata_bytes = cls._encode_map_string_string(record.metadata)
        payload = cls._encode_string(record.name) + metadata_bytes
        writer.write(bytes([RecordType.METADATA]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_data_end(cls, writer: BaseWriter, record: DataEndRecord) -> None:
        payload = struct.pack("<I", record.data_section_crc)
        writer.write(bytes([RecordType.DATA_END]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_attachment_index(
        cls, writer: BaseWriter, record: AttachmentIndexRecord
    ) -> None:
        payload = (
            struct.pack("<Q", record.offset)
            + struct.pack("<Q", record.length)
            + struct.pack("<Q", record.log_time)
            + struct.pack("<Q", record.create_time)
            + struct.pack("<Q", record.data_size)
            + cls._encode_string(record.name)
            + cls._encode_string(record.media_type)
        )
        writer.write(bytes([RecordType.ATTACHMENT_INDEX]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_metadata_index(cls, writer: BaseWriter, record: MetadataIndexRecord) -> None:
        payload = (
            struct.pack("<Q", record.offset)
            + struct.pack("<Q", record.length)
            + cls._encode_string(record.name)
        )
        writer.write(bytes([RecordType.METADATA_INDEX]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_statistics(cls, writer: BaseWriter, record: StatisticsRecord) -> None:
        channel_counts = cls._encode_map_uint16_uint64(record.channel_message_counts)
        payload = (
            struct.pack("<Q", record.message_count)
            + struct.pack("<H", record.schema_count)
            + struct.pack("<I", record.channel_count)
            + struct.pack("<I", record.attachment_count)
            + struct.pack("<I", record.metadata_count)
            + struct.pack("<I", record.chunk_count)
            + struct.pack("<Q", record.message_start_time)
            + struct.pack("<Q", record.message_end_time)
            + channel_counts
        )
        writer.write(bytes([RecordType.STATISTICS]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)

    @classmethod
    def write_summary_offset(cls, writer: BaseWriter, record: SummaryOffsetRecord) -> None:
        payload = (
            struct.pack("<B", record.group_opcode)
            + struct.pack("<Q", record.group_start)
            + struct.pack("<Q", record.group_length)
        )
        writer.write(bytes([RecordType.SUMMARY_OFFSET]))
        cls._write_uint64(writer, len(payload))
        cls._write_bytes(writer, payload)
