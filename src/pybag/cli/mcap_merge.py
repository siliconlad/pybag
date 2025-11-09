import argparse
from collections.abc import Sequence
from typing import Literal

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord


def merge_mcap(
    inputs: Sequence[str],
    output: str,
    chunk_size: int | None = None,
    chunk_compression: Literal["lz4", "zstd"] | None = None,
) -> None:
    """Merge multiple MCAP files into a single file.

    Args:
        inputs: List of input MCAP file paths
        output: Path to output merged MCAP file
        chunk_size: Optional chunk size in bytes for output file
        chunk_compression: Optional compression algorithm (lz4 or zstd)

    The merge process:
    1. Deduplicates schemas across all input files (same name/encoding/data → same ID)
    2. Assigns new channel IDs to avoid conflicts between input files
    3. Renumbers message sequences per channel starting from 0
    4. Writes a valid MCAP with proper statistics and summary sections
    """
    # Track schemas globally using (name, encoding, data) as key
    next_schema_id = 1
    schemas: dict[tuple[str, str, bytes], SchemaRecord] = {}

    # Track channels and sequence counters globally
    next_channel_id = 1
    sequence_counters: dict[int, int] = {}

    with McapRecordWriterFactory.create_writer(
        FileWriter(output),
        chunk_size=chunk_size,
        chunk_compression=chunk_compression,
        profile="ros2"
    ) as writer:
        # Process each input file
        for path in inputs:
            with McapRecordReaderFactory.from_file(path) as reader:
                # Per-file mapping of old IDs to new IDs
                schema_id_map: dict[int, int] = {}
                channel_id_map: dict[int, int] = {}

                # Process schemas from this file
                for schema_id, schema in reader.get_schemas().items():
                    key = (schema.name, schema.encoding, schema.data)
                    if key not in schemas:
                        # New schema - write it and track it
                        new_schema = SchemaRecord(
                            id=next_schema_id,
                            name=schema.name,
                            encoding=schema.encoding,
                            data=schema.data
                        )
                        schemas[key] = new_schema
                        writer.write_schema(new_schema)
                        next_schema_id += 1
                    # Map old schema ID to deduplicated schema ID
                    schema_id_map[schema_id] = schemas[key].id

                # Process channels from this file
                for channel_id, channel in reader.get_channels().items():
                    # Create new channel with remapped schema ID
                    new_channel = ChannelRecord(
                        id=next_channel_id,
                        schema_id=schema_id_map[channel.schema_id],
                        topic=channel.topic,
                        message_encoding=channel.message_encoding,
                        metadata=channel.metadata
                    )
                    writer.write_channel(new_channel)
                    channel_id_map[channel_id] = next_channel_id
                    sequence_counters[next_channel_id] = 0
                    next_channel_id += 1

                # Process messages from this file
                for message in reader.get_messages():
                    new_channel_id = channel_id_map[message.channel_id]
                    new_message = MessageRecord(
                        channel_id=new_channel_id,
                        sequence=sequence_counters[new_channel_id],
                        log_time=message.log_time,
                        publish_time=message.publish_time,
                        data=message.data
                    )
                    writer.write_message(new_message)
                    sequence_counters[new_channel_id] += 1


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("merge", help="Merge MCAP files into one.")
    parser.add_argument("input", nargs="+", help="Input MCAP files to merge.")
    parser.add_argument("-o", "--output", required=True, help="Path to the merged MCAP file.")
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Optional chunk size in bytes for output file."
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd", None],
        help="Optional compression algorithm for chunks (lz4 or zstd)."
    )
    parser.set_defaults(
        func=lambda args: merge_mcap(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
            chunk_compression=args.chunk_compression
        )
    )
