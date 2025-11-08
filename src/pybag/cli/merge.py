import argparse
from collections.abc import Sequence

from pybag.io.raw_reader import BytesReader, FileReader
from pybag.io.raw_writer import CrcWriter, FileWriter
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap.record_reader import decompress_chunk
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


def merge(inputs: Sequence[str], output: str) -> None:
    """Merge multiple MCAP files into a single file."""
    writer = CrcWriter(FileWriter(output))
    try:
        McapRecordWriter.write_magic_bytes(writer)
        header = HeaderRecord(profile="ros2", library="pybag")
        McapRecordWriter.write_header(writer, header)

        next_schema_id = 1
        next_channel_id = 1
        schemas: dict[tuple[str, str, bytes], SchemaRecord] = {}
        channels: list[ChannelRecord] = []
        channel_sequences: dict[int, int] = {}
        channel_message_counts: dict[int, int] = {}
        message_count = 0
        message_start: int | None = None
        message_end: int | None = None

        for file_index, path in enumerate(inputs):
            reader = FileReader(path)
            try:
                McapRecordParser.parse_magic_bytes(reader)
                schema_map: dict[int, int] = {}
                channel_map_local: dict[int, int] = {}
                for record_type, record in McapRecordParser.parse_record(reader):
                    if record_type == McapRecordType.SCHEMA:
                        key = (record.name, record.encoding, record.data)
                        existing = schemas.get(key)
                        if existing is None:
                            new_schema = SchemaRecord(
                                id=next_schema_id,
                                name=record.name,
                                encoding=record.encoding,
                                data=record.data,
                            )
                            schemas[key] = new_schema
                            McapRecordWriter.write_schema(writer, new_schema)
                            next_schema_id += 1
                        schema_map[record.id] = schemas[key].id
                    elif record_type == McapRecordType.CHANNEL:
                        new_channel = ChannelRecord(
                            id=next_channel_id,
                            schema_id=schema_map[record.schema_id],
                            topic=record.topic,
                            message_encoding=record.message_encoding,
                            metadata=record.metadata,
                        )
                        McapRecordWriter.write_channel(writer, new_channel)
                        channels.append(new_channel)
                        channel_map_local[record.id] = new_channel.id
                        channel_sequences[new_channel.id] = 0
                        channel_message_counts[new_channel.id] = 0
                        next_channel_id += 1
                    elif record_type == McapRecordType.MESSAGE:
                        new_channel_id = channel_map_local[record.channel_id]
                        sequence = channel_sequences[new_channel_id]
                        new_record = MessageRecord(
                            channel_id=new_channel_id,
                            sequence=sequence,
                            log_time=record.log_time,
                            publish_time=record.publish_time,
                            data=record.data,
                        )
                        McapRecordWriter.write_message(writer, new_record)
                        channel_sequences[new_channel_id] = sequence + 1
                        message_count += 1
                        channel_message_counts[new_channel_id] += 1
                        message_start = (
                            record.log_time
                            if message_start is None
                            else min(message_start, record.log_time)
                        )
                        message_end = (
                            record.log_time
                            if message_end is None
                            else max(message_end, record.log_time)
                        )
                    elif record_type == McapRecordType.CHUNK:
                        chunk_reader = BytesReader(decompress_chunk(record))
                        for sub_type, sub_record in McapRecordParser.parse_record(chunk_reader):
                            if sub_type != McapRecordType.MESSAGE:
                                continue
                            new_channel_id = channel_map_local[sub_record.channel_id]
                            sequence = channel_sequences[new_channel_id]
                            new_record = MessageRecord(
                                channel_id=new_channel_id,
                                sequence=sequence,
                                log_time=sub_record.log_time,
                                publish_time=sub_record.publish_time,
                                data=sub_record.data,
                            )
                            McapRecordWriter.write_message(writer, new_record)
                            channel_sequences[new_channel_id] = sequence + 1
                            message_count += 1
                            channel_message_counts[new_channel_id] += 1
                            message_start = (
                                sub_record.log_time
                                if message_start is None
                                else min(message_start, sub_record.log_time)
                            )
                            message_end = (
                                sub_record.log_time
                                if message_end is None
                                else max(message_end, sub_record.log_time)
                            )
                    elif record_type == McapRecordType.DATA_END:
                        break
            finally:
                reader.close()

        data_end = DataEndRecord(data_section_crc=writer.get_crc())
        McapRecordWriter.write_data_end(writer, data_end)

        summary_start = writer.tell()
        writer.clear_crc()

        schema_group_start = summary_start
        for record in schemas.values():
            McapRecordWriter.write_schema(writer, record)
        schema_group_length = writer.tell() - schema_group_start

        channel_group_start = writer.tell()
        for record in channels:
            McapRecordWriter.write_channel(writer, record)
        channel_group_length = writer.tell() - channel_group_start

        chunk_index_group_start = writer.tell()
        chunk_index_group_length = writer.tell() - chunk_index_group_start

        statistics_group_start = writer.tell()
        stats = StatisticsRecord(
            message_count=message_count,
            schema_count=len(schemas),
            channel_count=len(channels),
            attachment_count=0,
            metadata_count=0,
            chunk_count=0,
            message_start_time=message_start or 0,
            message_end_time=message_end or 0,
            channel_message_counts=channel_message_counts,
        )
        McapRecordWriter.write_statistics(writer, stats)
        statistics_group_length = writer.tell() - statistics_group_start

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
        McapRecordWriter.write_summary_offset(
            writer,
            SummaryOffsetRecord(
                group_opcode=RecordType.STATISTICS,
                group_start=statistics_group_start,
                group_length=statistics_group_length,
            ),
        )

        summary_crc = writer.get_crc()
        footer = FooterRecord(
            summary_start=summary_start,
            summary_offset_start=summary_offset_start,
            summary_crc=summary_crc,
        )
        McapRecordWriter.write_footer(writer, footer)
        McapRecordWriter.write_magic_bytes(writer)
    finally:
        writer.close()


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("merge", help="Merge MCAP files into one.")
    parser.add_argument("input", nargs="+", help="Input MCAP files to merge.")
    parser.add_argument(
        "-o", "--output", required=True, help="Path to the merged MCAP file."
    )
    parser.set_defaults(func=lambda args: merge(args.input, args.output))
