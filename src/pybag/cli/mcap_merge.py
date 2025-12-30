import argparse
import heapq
from collections.abc import Sequence
from typing import Literal

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.mcap.summary import McapSummaryFactory

SUPPORTED_PROFILES = {"ros1", "ros2"}


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
        choices=["lz4", "zstd"],
        help="Optional compression algorithm for chunks."
    )
    parser.set_defaults(
        func=lambda args: merge_mcap(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
            chunk_compression=args.chunk_compression
        )
    )
