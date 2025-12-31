"""MCAP and bag file sorting CLI command."""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.cli.utils import (
    get_file_format,
    validate_compression_for_bag,
    validate_compression_for_mcap
)
from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.summary import McapSummaryFactory
from pybag.types import SchemaText

logger = logging.getLogger(__name__)


def sort_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
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
    logger.debug('Sorting mcap...')

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
        logger.debug(f'Found {len(all_attachments)} attachments')
        all_metadata = reader.get_metadata()
        logger.debug(f'Found {len(all_metadata)} metadata')

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


def sort_bag(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    compression: Literal["none", "bz2"] | None = None,
    *,
    overwrite: bool = False,
    sort_by_topic: bool = False,
    sort_by_log_time: bool = False,
) -> Path:
    """Sort a ROS 1 bag file by topic and/or log time.

    This function supports multiple sorting modes:
    - No flags: no-op, returns input path without creating output
    - by_topic only: group messages by topic, keep write order within topics
    - log_time only: sort all messages by log time (mixed topics)
    - by_topic and log_time: group by topic, sort by log time within each topic

    Args:
        input_path: Path to the input bag file.
        output_path: Path to the output bag file. If None, uses input_sorted.bag.
        chunk_size: The size of chunks to write in bytes. If None, uses default.
        compression: The compression to use for chunks ("none" or "bz2").
        overwrite: Whether to overwrite the output file if it exists.
        sort_by_topic: If True, group messages by topic.
        sort_by_log_time: If True, sort messages by log time.

    Returns:
        The path to the output bag file, or input path if no sorting requested.
    """
    logger.debug('Sorting bag...')

    # Resolve input path
    input_path = Path(input_path).resolve()

    # Early return if no sorting is requested
    if not sort_by_topic and not sort_by_log_time:
        logger.info("No sorting flags specified, returning input path unchanged.")
        return input_path

    # Resolve output path
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_sorted.bag")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError("Input path cannot be same as output.")

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError("Output bag exists. Please set `overwrite` to True.")

    with BagFileReader.from_file(input_path) as reader:
        # Get all connections for schema info
        all_connections = reader.get_connections()

        # Build a mapping from topic to connection
        topic_to_connection = {conn.topic: conn for conn in all_connections}

        # Prepare writer kwargs
        writer_kwargs = {}
        if chunk_size is not None:
            writer_kwargs['chunk_size'] = chunk_size
        if compression is not None:
            writer_kwargs['compression'] = compression

        with BagFileWriter.open(output_path, **writer_kwargs) as writer:
            # Register all connections with their schemas first
            for topic, conn in topic_to_connection.items():
                conn_header = conn.connection_header
                schema = SchemaText(
                    name=conn_header.type,
                    text=conn_header.message_definition,
                )
                writer.add_connection(topic, schema=schema)

            if sort_by_topic:  # by_topic: group messages by topic
                for topic in reader.get_topics():
                    for msg in reader.messages(
                        topic,
                        in_log_time_order=sort_by_log_time,
                    ):
                        writer.write_message(topic, msg.log_time, msg.data)
            else:  # log_time only: sort all messages by log time without grouping
                all_topics = reader.get_topics()
                for msg in reader.messages(
                    all_topics,
                    in_log_time_order=True,
                ):
                    writer.write_message(msg.topic, msg.log_time, msg.data)

    logger.info(f"Sorted bag written to {output_path}")
    return output_path


def _run_sort(args) -> Path:
    """Run the sort command based on file format."""
    input_path = Path(args.input).resolve()
    file_format = get_file_format(input_path)

    if file_format == 'mcap':
        # Validate compression for MCAP files
        chunk_compression = validate_compression_for_mcap(args.chunk_compression)
        return sort_mcap(
            input_path,
            output_path=args.output,
            chunk_size=args.chunk_size,
            chunk_compression=chunk_compression,
            overwrite=args.overwrite,
            sort_by_topic=args.by_topic,
            sort_by_log_time=args.log_time,
        )
    else:
        # Map compression for bag files
        compression = validate_compression_for_bag(args.chunk_compression)
        return sort_bag(
            input_path,
            output_path=args.output,
            chunk_size=args.chunk_size,
            compression=compression,
            overwrite=args.overwrite,
            sort_by_topic=args.by_topic,
            sort_by_log_time=args.log_time,
        )


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "sort",
        help="Sort messages in an MCAP or bag file by topic and/or log time",
        description=dedent("""
            Sorts messages in an MCAP or ROS 1 bag file by topic and/or log time.
            The output format matches the input format (MCAP in -> MCAP out,
            bag in -> bag out).

            Sorting modes:
            - --by-topic: Group messages by topic in chunks. Each chunk will only
              contain messages from one topic, which can improve read performance
              when accessing specific topics.
            - --log-time: Sort all messages by their log time.
            - --by-topic --log-time: Group by topic and sort by log time within
              each topic group.

            If no sorting flags are specified, no output file is created.

            Note: MCAP files support 'none', 'lz4', and 'zstd' compression;
            using 'bz2' with MCAP will raise an error. Bag files support 'none'
            and 'bz2'; using 'lz4' or 'zstd' will raise an error.
        """),
    )
    parser.add_argument(
        "input",
        help="Path to MCAP file (*.mcap) or ROS 1 bag file (*.bag)"
    )
    parser.add_argument("-o", "--output", help="Output file path (same format as input)")
    parser.add_argument(
        "--chunk-size",
        type=int,
        help=dedent("""
            Chunk size of the output file in bytes. If not specified,
            uses the default chunk size.
        """),
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd", "none", "bz2"],
        help=dedent("""
            Compression used for chunk records. MCAP files support 'none',
            'lz4', and 'zstd'; using 'bz2' with MCAP will raise an error.
            Bag files support 'none' and 'bz2'; using 'lz4' or 'zstd' with
            bag files will raise an error.
        """),
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
    parser.set_defaults(func=_run_sort)
