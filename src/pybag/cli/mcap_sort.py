"""MCAP sorting CLI command."""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.summary import McapSummaryFactory

logger = logging.getLogger(__name__)


def sort_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
    sort_by_topic: bool = False,
    sort_by_log_time: bool = False,
) -> Path:
    """Sort an MCAP file by topic and/or log time.

    This function supports multiple sorting modes:
    - No flags: no-op, returns input path without creating output
    - by_topic only: group messages by topic, keep write order within topics
    - log_time only: sort all messages by log time (mixed topics)
    - by_topic and log_time: group by topic, sort by log time within each topic

    Args:
        input_path: Path to the input MCAP file.
        output_path: Path to the output MCAP file. If None, uses input_sorted.mcap.
        chunk_size: The size of chunks to write in bytes. If None, uses default chunking.
        chunk_compression: The compression to use for chunks ("lz4" or "zstd").
        overwrite: Whether to overwrite the output file if it exists.
        sort_by_topic: If True, group messages by topic in chunks.
        sort_by_log_time: If True, sort messages by log time.

    Returns:
        The path to the output MCAP file, or input path if no sorting requested.
    """
    logging.debug('Sorting mcap...')

    # Resolve input path
    input_path = Path(input_path).resolve()

    # Early return if no sorting is requested
    if not sort_by_topic and not sort_by_log_time:
        logger.info("No sorting flags specified, returning input path unchanged.")
        return input_path

    # Resolve output path
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_sorted.mcap")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError("Input path cannot be same as output.")

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError("Output mcap exists. Please set `overwrite` to True.")

    with McapRecordReaderFactory.from_file(input_path) as reader:
        # Get all channels and schemas
        all_schemas = reader.get_schemas()
        all_channels = reader.get_channels()

        # Read all attachments and metadata to preserve them
        all_attachments = reader.get_attachments()
        logging.debug(f'Found {len(all_attachments)} attachments')
        all_metadata = reader.get_metadata()
        logging.debug(f'Found {len(all_metadata)} metadata')

        # Write the sorted MCAP
        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            McapSummaryFactory.create_summary(chunk_size=chunk_size),
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=reader.get_header().profile,
        ) as writer:
            # Write all schemas first
            for schema in all_schemas.values():
                writer.write_schema(schema)

            # Write all channels
            for channel in all_channels.values():
                writer.write_channel(channel)

            if sort_by_topic:  # by_topic: group messages by topic
                for channel_id in reader.get_channels().keys():
                    for msg_record in reader.get_messages(channel_id, in_log_time_order=sort_by_log_time):
                        writer.write_message(msg_record)
                    # Flush the chunk to ensure topic separation
                    writer.flush_chunk()
            else:  # log_time only: sort all messages by log time without grouping
                for msg_record in reader.get_messages(in_log_time_order=True):
                    writer.write_message(msg_record)

            # Write all attachments to preserve them
            for attachment in all_attachments:
                writer.write_attachment(attachment)

            # Write all metadata to preserve them
            for metadata in all_metadata:
                writer.write_metadata(metadata)

    logger.info(f"Sorted MCAP written to {output_path}")
    return output_path


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "sort",
        help="Sort messages in an MCAP file by topic and/or log time",
        description=dedent("""
            Sorts messages in an MCAP file by topic and/or log time.

            Sorting modes:
            - --by-topic: Group messages by topic in chunks. Each chunk will only
              contain messages from one topic, which can improve read performance
              when accessing specific topics.
            - --log-time: Sort all messages by their log time.
            - --by-topic --log-time: Group by topic and sort by log time within
              each topic group.

            If no sorting flags are specified, no output file is created.
        """),
    )
    parser.add_argument("input", help="Path to input MCAP file (*.mcap)")
    parser.add_argument("-o", "--output", help="Output MCAP file path")
    parser.add_argument(
        "--chunk-size",
        type=int,
        help=dedent("""
            Chunk size of the output MCAP in bytes. If not specified,
            uses the default chunk size.
        """),
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd"],
        help=dedent("""Compression used for the chunk records."""),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    parser.add_argument(
        "--by-topic",
        action="store_true",
        help="Group messages by topic in chunks",
    )
    parser.add_argument(
        "--log-time",
        action="store_true",
        help="Sort messages by log time",
    )
    parser.set_defaults(
        func=lambda args: sort_mcap(
            args.input,
            output_path=args.output,
            chunk_size=args.chunk_size,
            chunk_compression=args.chunk_compression,
            overwrite=args.overwrite,
            sort_by_topic=args.by_topic,
            sort_by_log_time=args.log_time,
        )
    )
