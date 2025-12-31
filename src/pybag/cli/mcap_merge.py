import argparse
import heapq
import logging
from collections.abc import Sequence
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
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.mcap.summary import McapSummaryFactory
from pybag.types import SchemaText

logger = logging.getLogger(__name__)

SUPPORTED_PROFILES = {"ros1", "ros2"}


def merge_mcap(
    inputs: Sequence[str],
    output: str,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
) -> None:
    """Merge multiple MCAP files into a single file.

    Args:
        inputs: List of input MCAP file paths
        output: Path to output merged MCAP file
        chunk_size: Optional chunk size in bytes for output file
        chunk_compression: Optional compression algorithm (lz4 or zstd)
    """
    # Validate that all input files have the same profile
    profile: str | None = None
    for path in inputs:
        with McapRecordReaderFactory.from_file(path) as reader:
            header = reader.get_header()
            if profile is None:
                profile = header.profile
                if profile not in SUPPORTED_PROFILES:
                    raise ValueError(
                        f"Unsupported profile '{profile}' in {path}. "
                        f"Supported profiles: {sorted(SUPPORTED_PROFILES)}"
                    )
            elif header.profile != profile:
                raise ValueError(
                    f"All input files must have the same profile. "
                    f"Expected '{profile}' but got '{header.profile}' in {path}"
                )

    if profile is None:
        raise ValueError("No input files provided")

    # Track schemas globally using (name, encoding, data) as key
    next_schema_id = 1
    schemas: dict[tuple[str, str, bytes], SchemaRecord] = {}
    channels: dict[tuple[int, str, str], ChannelRecord] = {}

    # Track channels globally: (file_index, old_channel_id) -> new_channel_id
    next_channel_id = 1
    channel_id_map: dict[tuple[int, int], int] = {}
    sequence_counters: dict[int, int] = {}

    # Track schema ID mappings per file: (file_index, old_schema_id) -> new_schema_id
    schema_id_map: dict[tuple[int, int], int] = {}

    with McapRecordWriterFactory.create_writer(
        FileWriter(output),
        McapSummaryFactory.create_summary(chunk_size=chunk_size),
        chunk_size=chunk_size,
        chunk_compression=chunk_compression,
        profile=profile,
    ) as writer:
        # Collect all attachments and metadata from all files
        all_attachments = []
        all_metadata = []

        # First pass: Write all schemas and channels from all files
        # This ensures channels without messages are preserved
        for file_index, path in enumerate(inputs):
            with McapRecordReaderFactory.from_file(path) as reader:
                # Process all schemas from this file
                for old_schema_id, old_schema in reader.get_schemas().items():
                    schema_key = (file_index, old_schema_id)

                    # Check if this schema content already exists (deduplication)
                    schema_content_key = (old_schema.name, old_schema.encoding, old_schema.data)
                    if schema_content_key not in schemas:
                        # New unique schema - create and write it
                        new_schema = SchemaRecord(
                            id=next_schema_id,
                            name=old_schema.name,
                            encoding=old_schema.encoding,
                            data=old_schema.data
                        )
                        schemas[schema_content_key] = new_schema
                        writer.write_schema(new_schema)
                        next_schema_id += 1

                    # Map this file's schema ID to the deduplicated schema ID
                    schema_id_map[schema_key] = schemas[schema_content_key].id

                # Process all channels from this file
                for old_channel_id, old_channel in reader.get_channels().items():
                    channel_key = (file_index, old_channel_id)

                    # Get the new schema ID for this channel
                    old_schema_id = old_channel.schema_id
                    if old_schema_id != 0:
                        new_schema_id = schema_id_map[(file_index, old_schema_id)]
                    else:
                        new_schema_id = 0

                    # Assign new channel ID and write channel
                    # TODO: Currently ignores channel metadata
                    channel_content_key = (new_schema_id, old_channel.topic, old_channel.message_encoding)
                    if channel_content_key not in channels:
                        new_channel = ChannelRecord(
                            id=next_channel_id,
                            schema_id=new_schema_id,
                            topic=old_channel.topic,
                            message_encoding=old_channel.message_encoding,
                            metadata=old_channel.metadata
                        )
                        writer.write_channel(new_channel)
                        channels[channel_content_key] = new_channel
                        sequence_counters[next_channel_id] = 0
                        next_channel_id += 1
                    channel_id_map[channel_key] = channels[channel_content_key].id

                # Collect attachments and metadata from this file
                all_attachments.extend(reader.get_attachments())
                all_metadata.extend(reader.get_metadata())
            # File is closed here by context manager

        # Second pass: Merge messages in log time order using heap-based merge
        def lazy_message_iterator(file_index: int, path: str):
            """Lazy iterator that opens file, yields messages, then closes when exhausted."""
            with McapRecordReaderFactory.from_file(path) as reader:
                for message in reader.get_messages():
                    yield (message.log_time, file_index, message)

        # Create lazy iterators for each file (files opened on-demand)
        iterators = [lazy_message_iterator(i, path) for i, path in enumerate(inputs)]

        # Merge messages in log time order, breaking ties with file order
        for _, file_index, message in heapq.merge(*iterators, key=lambda x: (x[0], x[1])):
            # Look up the new channel ID for this message
            channel_key = (file_index, message.channel_id)
            new_channel_id = channel_id_map[channel_key]

            # Write message with remapped IDs and sequence number
            new_message = MessageRecord(
                channel_id=new_channel_id,
                sequence=sequence_counters[new_channel_id],
                log_time=message.log_time,
                publish_time=message.publish_time,
                data=message.data
            )
            writer.write_message(new_message)
            sequence_counters[new_channel_id] += 1

        # Write all attachments to preserve them
        for attachment in all_attachments:
            writer.write_attachment(attachment)

        # Write all metadata to preserve them
        for metadata in all_metadata:
            writer.write_metadata(metadata)


def merge_bag(
    inputs: Sequence[str],
    output: str,
    chunk_size: int | None = None,
    compression: Literal["none", "bz2"] | None = None,
) -> None:
    """Merge multiple ROS 1 bag files into a single file.

    Args:
        inputs: List of input bag file paths.
        output: Path to output merged bag file.
        chunk_size: Optional chunk size in bytes for output file.
        compression: Optional compression algorithm ('none' or 'bz2').
    """
    if not inputs:
        raise ValueError("No input files provided")

    # Track connections globally using (topic, type, md5sum) as key
    # This deduplicates connections that appear in multiple files
    connections: dict[tuple[str, str, str], tuple[str, str]] = {}
    # Maps (topic, type, md5sum) -> (msg_type, msg_def)

    # First pass: Collect all unique connections from all files
    for path in inputs:
        with BagFileReader.from_file(path) as reader:
            for conn in reader.get_connections():
                conn_header = conn.connection_header
                conn_key = (conn.topic, conn_header.type, conn_header.md5sum)
                if conn_key not in connections:
                    connections[conn_key] = (
                        conn_header.type,
                        conn_header.message_definition,
                    )

    # Prepare writer kwargs
    writer_kwargs: dict = {}
    if chunk_size is not None:
        writer_kwargs['chunk_size'] = chunk_size
    if compression is not None:
        writer_kwargs['compression'] = compression

    with BagFileWriter.open(output, **writer_kwargs) as writer:
        # Register all unique connections
        for (topic, msg_type, _md5sum), (type_name, msg_def) in connections.items():
            schema = SchemaText(name=type_name, text=msg_def)
            writer.add_connection(topic, schema=schema)

        # Second pass: Merge messages in log time order using heap-based merge
        def lazy_message_iterator(file_index: int, path: str):
            """Lazy iterator that opens file, yields messages, then closes when exhausted."""
            with BagFileReader.from_file(path) as reader:
                # Get all topics from this file
                topics = reader.get_topics()
                if not topics:
                    return

                # Iterate over all messages from all topics
                for msg in reader.messages(topics, in_log_time_order=True):
                    yield (msg.log_time, file_index, msg)

        # Create lazy iterators for each file (files opened on-demand)
        iterators = [lazy_message_iterator(i, path) for i, path in enumerate(inputs)]

        # Merge messages in log time order, breaking ties with file order
        for _, _file_index, msg in heapq.merge(*iterators, key=lambda x: (x[0], x[1])):
            writer.write_message(msg.topic, msg.log_time, msg.data)


def _run_merge(args) -> None:
    """Run the merge command based on file format."""
    input_paths = [Path(p).resolve() for p in args.input]

    # Validate all input files have the same format
    formats = set()
    for path in input_paths:
        formats.add(get_file_format(path))

    if len(formats) > 1:
        raise ValueError(
            "All input files must have the same format. "
            f"Found mixed formats: {sorted(formats)}"
        )

    file_format = formats.pop()

    if file_format == 'mcap':
        # Validate compression for MCAP files
        chunk_compression = validate_compression_for_mcap(args.chunk_compression)
        merge_mcap(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
            chunk_compression=chunk_compression,
        )
    else:
        # Map compression for bag files
        compression = validate_compression_for_bag(args.chunk_compression)
        merge_bag(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
            compression=compression,
        )


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "merge",
        help="Merge MCAP or bag files into one.",
        description=dedent("""
            Merge multiple MCAP or ROS 1 bag files into a single file.

            All input files must be of the same format (all MCAP or all bag).
            The output file will be the same format as the input files.

            Messages are merged in log time order. Connections/channels that
            appear in multiple input files are deduplicated based on:
            - For MCAP: (schema_id, topic, message_encoding)
            - For bag: (topic, message_type, md5sum)

            Note: MCAP files support 'none', 'lz4', and 'zstd' compression;
            using 'bz2' with MCAP will raise an error. Bag files support 'none'
            and 'bz2'; using 'lz4' or 'zstd' will raise an error.
        """)
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="Input files to merge (*.mcap or *.bag). All must be the same format."
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path to the merged output file."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Optional chunk size in bytes for output file."
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
        """)
    )
    parser.set_defaults(func=_run_merge)
