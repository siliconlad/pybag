"""MCAP filtering CLI command."""

import argparse
import fnmatch
import logging
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Iterable

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import MessageRecord

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
    *,
    overwrite: bool = False
) -> Path:
    """Filter an MCAP file based on topics and time.

    This function preserves exact schema and channel records from the input file,
    rather than regenerating them based on message types.
    """
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
        # Build topic -> channel_id mapping and filter topics
        all_channels = reader.get_channels()
        topic_to_channel_id: dict[str, int] = {
            channel.topic: channel_id
            for channel_id, channel in all_channels.items()
        }
        all_topics = set(topic_to_channel_id.keys())

        # Determine which topics to include
        if include_topics is None:  # Include all topics
            topics_to_include = set(topic_to_channel_id.keys())
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

        # Get channel IDs for included topics
        channel_ids_to_include = {topic_to_channel_id[topic] for topic in topics_to_include}

        # Step 2: Write the filtered MCAP using factory
        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            chunk_size=None,  # TODO: What is the best way to set this?
            chunk_compression="lz4",
            profile=reader.get_header().profile
        ) as writer:
            # Write message records as we read them
            written_schema_ids = set()
            written_channel_ids = set()
            sequence_counters = defaultdict(int)

            for msg_record in reader.get_messages(
                channel_id=list(channel_ids_to_include) if channel_ids_to_include else None,
                start_timestamp=start_ns,
                end_timestamp=end_ns,
                in_log_time_order=False
            ):
                # Write the schema record to the mcap the first time
                schema_id = all_channels[msg_record.channel_id].schema_id
                if schema_id not in written_schema_ids:
                    if (schema := reader.get_schema(schema_id)) is not None:
                        writer.write_schema(schema)
                        written_schema_ids.add(schema_id)
                    else:
                        channel_id = msg_record.channel_id
                        logging.warning(f'Found no schema {schema_id} (channel {channel_id})')
                        continue

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

    return output_path


def _filter_mcap_from_args(args: argparse.Namespace) -> None:
    filter_mcap(
        args.input,
        output_path=args.output,
        include_topics=args.include_topic,
        exclude_topics=args.exclude_topic,
        start_time=args.start_time,
        end_time=args.end_time,
    )


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "filter",
        help="Extract data from an mcap.",
        description=dedent("""
            The messages that satisfy the given constraints are written into
            a new mcap file. The new mcap is written from scratch, so the way
            the messages are stored in the file compared to the input mcap may
            be different (e.g messages may no longer be in the same chunk).
        """)
    )
    parser.add_argument("input", help="Path to mcap file (*.mcap).")
    parser.add_argument("-o", "--output", help="Output MCAP file path")
    parser.add_argument(
        "--include-topic",
        action="append",
        help=dedent("""
            Topics to include. If not specified, defaults to all topics.
            If specified, only the topics listed are included.
            Excluded topics are ignored (i.e. exclusion takes precedent).
        """)
    )
    parser.add_argument(
        "--exclude-topic",
        action="append",
        help=dedent("""
            Topics to exclude. If not specified, defaults to no topics.
            If specified, the specified topics are excluded from the output.
        """)
    )
    parser.add_argument(
        "--start-time",
        type=float,
        help=dedent("""
            Start time in seconds. All messages with a log time less than
            the start time is ignored and not included in the output mcap.
            By default it is set to the smallest log time in the input mcap.
        """)
    )
    parser.add_argument(
        "--end-time",
        type=float,
        help=dedent("""
            End time in seconds. All messages with a log time greater than
            the start time is ignored and not included in the output mcap.
            By default it is set to the largest log time in the input mcap.
        """)
    )
    parser.set_defaults(func=_filter_mcap_from_args)
