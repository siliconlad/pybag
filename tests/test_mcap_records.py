from pybag.io.raw_reader import BytesReader
from pybag.io.raw_writer import BytesWriter
from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.record_writer import McapRecordWriter
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
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)


def test_header_encode_decode():
    record = HeaderRecord(profile="test_profile", library="pybag")
    writer = BytesWriter()
    McapRecordWriter.write_header(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_header(reader)
    assert parsed == record


def test_footer_encode_decode():
    record = FooterRecord(summary_start=1, summary_offset_start=2, summary_crc=3)
    writer = BytesWriter()
    McapRecordWriter.write_footer(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_footer(reader)
    assert parsed == record


def test_channel_encode_decode():
    record = ChannelRecord(id=1, schema_id=2, topic="topic", message_encoding="encoding", metadata={"a": "b"})
    writer = BytesWriter()
    McapRecordWriter.write_channel(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_channel(reader)
    assert parsed == record


def test_schema_encode_decode():
    record = SchemaRecord(id=1, name="name", encoding="enc", data=b"data")
    writer = BytesWriter()
    McapRecordWriter.write_schema(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_schema(reader)
    assert parsed == record


def test_message_encode_decode():
    record = MessageRecord(channel_id=1, sequence=2, log_time=3, publish_time=4, data=b"msg")
    writer = BytesWriter()
    McapRecordWriter.write_message(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_message(reader)
    assert parsed == record


def test_chunk_encode_decode():
    record = ChunkRecord(
        message_start_time=1,
        message_end_time=2,
        uncompressed_size=3,
        uncompressed_crc=4,
        compression="",
        records=b"records",
    )
    writer = BytesWriter()
    McapRecordWriter.write_chunk(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_chunk(reader)
    assert parsed == record


def test_message_index_encode_decode():
    record = MessageIndexRecord(channel_id=1, records=[(1, 2), (3, 4)])
    writer = BytesWriter()
    McapRecordWriter.write_message_index(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_message_index(reader)
    assert parsed == record


def test_chunk_index_encode_decode():
    record = ChunkIndexRecord(
        message_start_time=1,
        message_end_time=2,
        chunk_start_offset=3,
        chunk_length=4,
        message_index_offsets={1: 10, 2: 20},
        message_index_length=5,
        compression="",
        compressed_size=6,
        uncompressed_size=7,
    )
    writer = BytesWriter()
    McapRecordWriter.write_chunk_index(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_chunk_index(reader)
    assert parsed == record


def test_attachment_encode_decode():
    record = AttachmentRecord(
        log_time=1,
        create_time=2,
        name="file",
        media_type="text/plain",
        data=b"payload",
        crc=3,
    )
    writer = BytesWriter()
    McapRecordWriter.write_attachment(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_attachment(reader)
    assert parsed == record


def test_metadata_encode_decode():
    record = MetadataRecord(name="meta", metadata={"k": "v"})
    writer = BytesWriter()
    McapRecordWriter.write_metadata(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_metadata(reader)
    assert parsed == record


def test_data_end_encode_decode():
    record = DataEndRecord(data_section_crc=1)
    writer = BytesWriter()
    McapRecordWriter.write_data_end(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_data_end(reader)
    assert parsed == record


def test_attachment_index_encode_decode():
    record = AttachmentIndexRecord(
        offset=1,
        length=2,
        log_time=3,
        create_time=4,
        data_size=5,
        name="file",
        media_type="text/plain",
    )
    writer = BytesWriter()
    McapRecordWriter.write_attachment_index(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_attachment_index(reader)
    assert parsed == record


def test_metadata_index_encode_decode():
    record = MetadataIndexRecord(offset=1, length=2, name="meta")
    writer = BytesWriter()
    McapRecordWriter.write_metadata_index(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_metadata_index(reader)
    assert parsed == record


def test_statistics_encode_decode():
    record = StatisticsRecord(
        message_count=1,
        schema_count=2,
        channel_count=3,
        attachment_count=4,
        metadata_count=5,
        chunk_count=6,
        message_start_time=7,
        message_end_time=8,
        channel_message_counts={1: 9},
    )
    writer = BytesWriter()
    McapRecordWriter.write_statistics(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_statistics(reader)
    assert parsed == record


def test_summary_offset_encode_decode():
    record = SummaryOffsetRecord(group_opcode=1, group_start=2, group_length=3)
    writer = BytesWriter()
    McapRecordWriter.write_summary_offset(writer, record)
    reader = BytesReader(writer.as_bytes())
    parsed = McapRecordParser.parse_summary_offset(reader)
    assert parsed == record
