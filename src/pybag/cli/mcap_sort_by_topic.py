"""MCAP topic-sorting CLI command."""

import argparse
import logging
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import MessageRecord

logger = logging.getLogger(__name__)


def sort_by_topic(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False
) -> Path:
    """Sort an MCAP file so that messages from each topic are grouped together in chunks.

    This reads all messages, groups them by topic, and writes them out one topic at a time,
    flushing chunks between topics to ensure each chunk only contains messages from one topic.

    Args:
        input_path: Path to the input MCAP file.
        output_path: Path to the output MCAP file. If None, uses input_sorted.mcap.
        chunk_size: The size of chunks to write in bytes. If None, uses default chunking.
        chunk_compression: The compression to use for chunks ("lz4" or "zstd").
        overwrite: Whether to overwrite the output file if it exists.

    Returns:
        The path to the output MCAP file.
    """
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_sorted.mcap")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output mcap exists. Please set `overwrite` to True.')

    with McapRecordReaderFactory.from_file(input_path) as reader:
        # Get all channels and schemas
        all_channels = reader.get_channels()
        all_schemas = {}
        for channel_id, channel in all_channels.items():
            schema_id = channel.schema_id
            if schema_id != 0 and schema_id not in all_schemas:
                if (schema := reader.get_schema(schema_id)) is not None:
                    all_schemas[schema_id] = schema

        # Read and group messages by topic
        logger.info("Reading and grouping messages by topic...")
        messages_by_topic: dict[str, list[MessageRecord]] = defaultdict(list)
        for msg_record in reader.get_messages(in_log_time_order=False):
            topic = all_channels[msg_record.channel_id].topic
            messages_by_topic[topic].append(msg_record)

        # Write the sorted MCAP
        logger.info(f"Writing sorted MCAP with {len(messages_by_topic)} topics...")
        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=reader.get_header().profile
        ) as writer:
            # Write all schemas first
            for schema in all_schemas.values():
                writer.write_schema(schema)

            # Write messages grouped by topic
            written_channel_ids = set()
            sequence_counters = defaultdict(int)

            for topic, messages in messages_by_topic.items():
                logger.info(f"Writing {len(messages)} messages for topic {topic}")

                # Write messages for this topic
                for msg_record in messages:
                    # Write the channel record the first time
                    if msg_record.channel_id not in written_channel_ids:
                        writer.write_channel(all_channels[msg_record.channel_id])
                        written_channel_ids.add(msg_record.channel_id)

                    # Write message with updated sequence number
                    new_record = MessageRecord(
                        channel_id=msg_record.channel_id,
                        sequence=sequence_counters[msg_record.channel_id],
                        log_time=msg_record.log_time,
                        publish_time=msg_record.publish_time,
                        data=msg_record.data
                    )
                    sequence_counters[msg_record.channel_id] += 1
                    writer.write_message(new_record)

                # Flush the chunk to ensure topic separation
                writer.flush_chunk()

    logger.info(f"Sorted MCAP written to {output_path}")
    return output_path


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "sort-by-topic",
        help="Sort messages in an MCAP file by topic",
        description=dedent("""
            Sorts messages in an MCAP file so that messages from each topic are
            grouped together in chunks. This ensures that each chunk only contains
            messages from one topic, which can improve read performance when
            accessing specific topics.

            The messages are read from the input file, grouped by topic, and written
            to a new MCAP file one topic at a time. Chunks are flushed between topics
            to maintain topic separation.
        """)
    )
    parser.add_argument("input", help="Path to input MCAP file (*.mcap)")
    parser.add_argument("-o", "--output", help="Output MCAP file path")
    parser.add_argument(
        "--chunk-size",
        type=int,
        help=dedent("""
            Chunk size of the output MCAP in bytes. If not specified,
            uses the default chunk size.
        """)
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=['lz4', 'zstd'],
        help=dedent("""Compression used for the chunk records.""")
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists"
    )
    parser.set_defaults(
        func=lambda args: sort_by_topic(
            args.input,
            output_path=args.output,
            chunk_size=args.chunk_size,
            chunk_compression=args.chunk_compression,
            overwrite=args.overwrite,
        )
    )
