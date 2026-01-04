"""MCAP and bag file filtering CLI command."""

import fnmatch
import logging
from collections import defaultdict
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
from pybag.mcap.records import MessageRecord
from pybag.mcap.summary import McapSummaryFactory
from pybag.types import SchemaText

logger = logging.getLogger(__name__)


def _to_ns(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return int(seconds * 1_000_000_000)


def filter_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    include_topics: list[str] | None = None,
    exclude_topics: list[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False
) -> Path:
    """Filter an MCAP file based on topics and time."""
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_filtered.mcap")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output mcap exists. Please set `overwrite` to True.')

    start_ns, end_ns = _to_ns(start_time), _to_ns(end_time)
    with McapRecordReaderFactory.from_file(input_path) as reader:
        # Build topic -> channel_ids mapping
        all_channels = reader.get_channels()
        topic_to_channel_ids: dict[str, set[int]] = defaultdict(set)
        for channel_id, channel in all_channels.items():
            topic_to_channel_ids[channel.topic].add(channel_id)
        all_topics = set(topic_to_channel_ids.keys())

        # Determine which topics to include
        if include_topics is None:  # Include all topics
            topics_to_include = set(topic_to_channel_ids.keys())
        else:  # Expand glob patterns and filter
            topics_to_include = set()
            for pattern in include_topics:
                matched = fnmatch.filter(all_topics, pattern)
                topics_to_include.update(matched)

        # Remove excluded topics
        if exclude_topics:
            topics_to_exclude = set()
            for pattern in exclude_topics:
                matched = fnmatch.filter(all_topics, pattern)
                topics_to_exclude.update(matched)
            topics_to_include -= topics_to_exclude

        channel_ids_to_include = set()
        for topic in topics_to_include:
            channel_ids_to_include.update(topic_to_channel_ids[topic])

        # Read attachments (filtered by time) and metadata
        all_attachments = reader.get_attachments(start_time=start_ns, end_time=end_ns)
        all_metadata = reader.get_metadata()

        # Step 2: Write the filtered MCAP using factory
        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            McapSummaryFactory.create_summary(chunk_size=chunk_size),
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=reader.get_header().profile
        ) as writer:
            # Write message records as we read them
            written_schema_ids = set()
            written_channel_ids = set()
            sequence_counters = defaultdict(int)

            # If no topics match the filters, create an empty MCAP (no messages)
            if not channel_ids_to_include:
                logger.warning("No topics match filter.")
                # Still write attachments and metadata even if no messages match
                for attachment in all_attachments:
                    writer.write_attachment(attachment)
                for metadata in all_metadata:
                    writer.write_metadata(metadata)
                return output_path

            for msg_record in reader.get_messages(
                channel_id=list(channel_ids_to_include),
                start_timestamp=start_ns,
                end_timestamp=end_ns,
                in_log_time_order=False
            ):
                # Write the schema record to the mcap the first time (if it has one)
                # Note: schema_id == 0 means "no schema" and is valid in MCAP
                schema_id = all_channels[msg_record.channel_id].schema_id
                if schema_id != 0 and schema_id not in written_schema_ids:
                    if (schema := reader.get_schema(schema_id)) is not None:
                        writer.write_schema(schema)
                        written_schema_ids.add(schema_id)
                    else:
                        channel_id = msg_record.channel_id
                        logger.warning(f'Schema {schema_id} not found for channel {channel_id}')

                # Write the channel record to the mcap the first time
                if msg_record.channel_id not in written_channel_ids:
                    writer.write_channel(all_channels[msg_record.channel_id])
                    written_channel_ids.add(msg_record.channel_id)

                # Write message immediately with updated sequence number
                new_record = MessageRecord(
                    channel_id=msg_record.channel_id,
                    sequence=sequence_counters[msg_record.channel_id],
                    log_time=msg_record.log_time,
                    publish_time=msg_record.publish_time,
                    data=msg_record.data
                )
                sequence_counters[msg_record.channel_id] += 1
                writer.write_message(new_record)

            # Write attachments (already filtered by time)
            for attachment in all_attachments:
                writer.write_attachment(attachment)

            # Write all metadata to preserve them
            for metadata in all_metadata:
                writer.write_metadata(metadata)

    return output_path


def filter_bag(
    input_path: str | Path,
    output_path: str | Path | None = None,
    include_topics: list[str] | None = None,
    exclude_topics: list[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    chunk_size: int | None = None,
    compression: Literal["none", "bz2"] | None = None,
    *,
    overwrite: bool = False
) -> Path:
    """Filter a ROS 1 bag file based on topics and time.

    Args:
        input_path: Path to input bag file.
        output_path: Path to output bag file. If None, defaults to
            <input_stem>_filtered.bag.
        include_topics: List of topic patterns to include (glob patterns supported).
            If None, all topics are included.
        exclude_topics: List of topic patterns to exclude (glob patterns supported).
            Exclusion takes precedence over inclusion.
        start_time: Start time in seconds. Messages before this time are excluded.
        end_time: End time in seconds. Messages after this time are excluded.
        chunk_size: Target chunk size in bytes for the output bag file.
        compression: Compression algorithm for chunks ('none' or 'bz2').
        overwrite: Whether to overwrite the output file if it exists.

    Returns:
        Path to the output bag file.

    Raises:
        ValueError: If input and output paths are the same, or if output exists
            and overwrite is False.
    """
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_filtered.bag")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output bag exists. Please set `overwrite` to True.')

    start_ns, end_ns = _to_ns(start_time), _to_ns(end_time)

    with BagFileReader.from_file(input_path) as reader:
        # Get all topics and connections
        all_topics = set(reader.get_topics())
        all_connections = reader.get_connections()

        # Build topic -> connection mapping
        topic_to_connections: dict[str, list] = defaultdict(list)
        for conn in all_connections:
            topic_to_connections[conn.topic].append(conn)

        # Determine which topics to include
        if include_topics is None:  # Include all topics
            topics_to_include = set(all_topics)
        else:  # Expand glob patterns and filter
            topics_to_include = set()
            for pattern in include_topics:
                matched = fnmatch.filter(all_topics, pattern)
                topics_to_include.update(matched)

        # Remove excluded topics
        if exclude_topics:
            topics_to_exclude = set()
            for pattern in exclude_topics:
                matched = fnmatch.filter(all_topics, pattern)
                topics_to_exclude.update(matched)
            topics_to_include -= topics_to_exclude

        # Prepare writer kwargs
        writer_kwargs = {}
        if chunk_size is not None:
            writer_kwargs['chunk_size'] = chunk_size
        if compression is not None:
            writer_kwargs['compression'] = compression

        # If no topics match, create an empty bag
        if not topics_to_include:
            logger.warning("No topics match filter.")
            # Create an empty bag file with no messages
            with BagFileWriter.open(output_path, **writer_kwargs):
                pass  # Empty bag file
            return output_path

        with BagFileWriter.open(output_path, **writer_kwargs) as writer:
            # Track which connections we've registered
            registered_topics: set[str] = set()

            # Iterate over messages for this topic with time filtering
            for msg in reader.messages(
                list(topics_to_include),
                start_time=start_ns,
                end_time=end_ns,
                in_log_time_order=False,
            ):
                # Get connection for this topic to extract schema
                connections = topic_to_connections[msg.topic]
                if not connections:
                    continue
                # Use the first connection for schema info
                # TODO: Should we do something more sophisticated?
                conn = connections[0]
                conn_header = conn.connection_header

                # Register connection with schema if not already done
                if msg.topic not in registered_topics:
                    schema = SchemaText(
                        name=conn_header.type,
                        text=conn_header.message_definition,
                    )
                    writer.add_connection(msg.topic, schema=schema)
                    registered_topics.add(msg.topic)

                # Write the message
                writer.write_message(msg.topic, msg.log_time, msg.data)

    return output_path


def _run_filter(args) -> Path:
    """Run the filter command based on file format."""
    input_path = Path(args.input).resolve()
    file_format = get_file_format(input_path)

    if file_format == 'mcap':
        # Validate compression for MCAP files
        chunk_compression = validate_compression_for_mcap(args.chunk_compression)
        return filter_mcap(
            input_path,
            output_path=args.output,
            include_topics=args.include_topic,
            exclude_topics=args.exclude_topic,
            start_time=args.start_time,
            end_time=args.end_time,
            chunk_size=args.chunk_size,
            chunk_compression=chunk_compression,
            overwrite=args.overwrite,
        )
    else:
        # Map compression for bag files
        compression = validate_compression_for_bag(args.chunk_compression)
        return filter_bag(
            input_path,
            output_path=args.output,
            include_topics=args.include_topic,
            exclude_topics=args.exclude_topic,
            start_time=args.start_time,
            end_time=args.end_time,
            chunk_size=args.chunk_size,
            compression=compression,
            overwrite=args.overwrite,
        )


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "filter",
        help="Extract data from an MCAP or bag file.",
        description=dedent("""
            Filter messages from an MCAP or ROS 1 bag file based on topic patterns
            and time range. The messages that satisfy the given constraints are
            written into a new file of the same format.

            The new file is written from scratch, so the way messages are stored
            may differ from the input (e.g., messages may no longer be in the
            same chunk).

            Topic patterns support glob syntax (e.g., '/camera/*', '/sensor/?/data').
        """)
    )
    parser.add_argument(
        "input",
        help="Path to MCAP file (*.mcap) or ROS 1 bag file (*.bag)."
    )
    parser.add_argument("-o", "--output", help="Output file path (same format as input)")
    parser.add_argument(
        "--include-topic",
        action="append",
        help=dedent("""
            Topics to include (supports glob patterns). If not specified, defaults
            to all topics. If specified, only the matching topics are included.
            Excluded topics are ignored (i.e. exclusion takes precedence).
        """)
    )
    parser.add_argument(
        "--exclude-topic",
        action="append",
        help=dedent("""
            Topics to exclude (supports glob patterns). If not specified, defaults
            to no topics. If specified, matching topics are excluded from the output.
        """)
    )
    parser.add_argument(
        "--start-time",
        type=float,
        help=dedent("""
            Start time in seconds. All messages with a log time less than
            the start time are ignored and not included in the output.
            By default it is set to the smallest log time in the input file.
        """)
    )
    parser.add_argument(
        "--end-time",
        type=float,
        help=dedent("""
            End time in seconds. All messages with a log time greater than
            the end time are ignored and not included in the output.
            By default it is set to the largest log time in the input file.
        """)
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the output file in bytes."
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=['lz4', 'zstd', 'none', 'bz2'],
        help=dedent("""
            Compression used for chunk records. MCAP files support 'none',
            'lz4', and 'zstd'; using 'bz2' with MCAP will raise an error.
            Bag files support 'none' and 'bz2'; using 'lz4' or 'zstd' with
            bag files will raise an error.
        """)
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists."
    )
    parser.set_defaults(func=_run_filter)
