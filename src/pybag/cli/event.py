"""MCAP event CLI command for managing events in MCAP files.

Events are stored as metadata records with a special name prefix to distinguish
them from regular metadata. Each event has a timestamp, name, and optional
description and custom key-value pairs.
"""

import json
import logging
import shutil
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from tkinter.constants import FALSE
from typing import Literal

from pybag.cli.filter import filter_mcap
from pybag.cli.utils import (
    get_file_format_from_magic,
    validate_compression_for_mcap
)
from pybag.io.raw_reader import FileReader
from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import MessageRecord, MetadataRecord
from pybag.mcap.summary import McapSummaryFactory

logger = logging.getLogger(__name__)

# Event metadata record name - used to identify event records
EVENT_METADATA_NAME = "pybag.event"


def _ns_to_seconds(ns: int) -> float:
    """Convert nanoseconds to seconds."""
    return ns / 1_000_000_000


def _seconds_to_ns(seconds: float) -> int:
    """Convert seconds to nanoseconds."""
    return int(seconds * 1_000_000_000)


def _to_s(ns: int | None) -> float | None:
    if ns is None:
        return None
    return ns / 1_000_000_000


def _to_ns(seconds: float | None) -> int | None:
    """Convert seconds to nanoseconds."""
    if seconds is None:
        return None
    return int(seconds * 1_000_000_000)


def _is_event_metadata(metadata: MetadataRecord) -> bool:
    """Check if a metadata record is an event."""
    return metadata.name == EVENT_METADATA_NAME


def _get_event_timestamp(metadata: MetadataRecord) -> int | None:
    """Extract timestamp from event metadata."""
    ts_str = metadata.metadata.get("timestamp")
    if ts_str is None:
        return None
    try:
        return int(ts_str)
    except ValueError:
        return None


def _get_event_name(metadata: MetadataRecord) -> str:
    """Extract event name from event metadata."""
    return metadata.metadata.get("name", "")


def _get_event_description(metadata: MetadataRecord) -> str:
    """Extract event description from event metadata."""
    return metadata.metadata.get("description", "")


def _event_matches_name(metadata: MetadataRecord, name: str | None) -> bool:
    event_name = _get_event_name(metadata)
    if name is not None:
        return event_name == name
    return True


def _event_matches_timestamp(
    metadata: MetadataRecord,
    start_time_ns: int | None = None,
    end_time_ns: int | None = None,
) -> bool:
    """Check if an event matches the given filters."""
    timestamp = _get_event_timestamp(metadata)
    if timestamp is not None:
        if start_time_ns is not None and timestamp < start_time_ns:
            return False
        if end_time_ns is not None and timestamp > end_time_ns:
            return False
    return True


#####################
# Event Listing     #
#####################

def _print_event_table(events: list[MetadataRecord]) -> None:
    """Print events in a table format."""
    if not events:
        print("No events found.")
        return

    # Sort by timestamp
    events = sorted(events, key=lambda e: _get_event_timestamp(e) or 0)

    # Calculate column widths
    max_name_len = max(len(_get_event_name(e)) for e in events) if events else 10
    max_name_len = max(max_name_len, 4)  # Min width for "Name" header

    # Print header
    print(f"  {'Timestamp (s)':>16}  {'Name':<{max_name_len}}  Description")
    print(f"  {'-' * 16}  {'-' * max_name_len}  {'-' * 40}")

    # Print rows
    for event in events:
        timestamp = _get_event_timestamp(event)
        timestamp_s = f"{_ns_to_seconds(timestamp):.6f}" if timestamp else "N/A"
        name = _get_event_name(event)
        description = _get_event_description(event)
        if len(description) > 50:
            description = description[:47] + "..."
        print(f"  {timestamp_s:>16}  {name:<{max_name_len}}  {description}")


def _event_to_json(event: MetadataRecord) -> dict:
    """Convert event metadata to JSON-serializable dict."""
    result = {
        "timestamp": _get_event_timestamp(event),
        "name": _get_event_name(event),
        "description": _get_event_description(event)
    }

    # Add any extra custom fields
    for key, value in event.metadata.items():
        if key not in ("timestamp", "name", "description"):
            result[key] = value

    return result


def list_events_mcap(
    input_path: Path,
    *,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    output_json: bool = False,
) -> None:
    """List events in an MCAP file."""
    start_ns = _to_ns(start_time)
    end_ns = _to_ns(end_time)

    with McapRecordReaderFactory.from_file(input_path) as reader:
        all_metadata = reader.get_metadata(name=EVENT_METADATA_NAME)

        # Filter events - also check if event identity is in deleted set
        events: list[MetadataRecord] = [
            m for m in all_metadata
            if _is_event_metadata(m)
            and _event_matches_name(m, name)
            and _event_matches_timestamp(m, start_ns, end_ns)
        ]

        if output_json:
            print(json.dumps([_event_to_json(e) for e in events], indent=2))
        else:
            print(f"Events ({len(events)}):\n")
            _print_event_table(events)


def list_events(
    input_path: str | Path,
    *,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    output_json: bool = False,
) -> None:
    """List events in an MCAP or bag file."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        list_events_mcap(
            input_path,
            name=name,
            start_time=start_time,
            end_time=end_time,
            output_json=output_json,
        )
    else:
        raise ValueError("Events are not supported in bag format.")


#####################
# Event Adding      #
#####################

def add_event_mcap(
    input_path: str | Path,
    event_name: str,
    timestamp: float,
    description: str | None = None,
    extra_fields: dict[str, str] | None = None,
) -> Path:
    """Add an event to an MCAP file by appending in place.

    This function appends a new event metadata record to an existing MCAP file
    without rewriting the entire file. The event is written at the end of the
    data section and the summary section is updated accordingly.

    Args:
        input_path: Path to the MCAP file to modify.
        event_name: Name of the event.
        timestamp: Event timestamp in seconds.
        description: Optional event description.
        extra_fields: Optional extra key-value pairs to include in the event.

    Returns:
        Path to the modified MCAP file (same as input_path).
    """
    input_path = Path(input_path).resolve()
    timestamp_ns = _to_ns(timestamp)

    # Build the event metadata
    event_metadata: dict[str, str] = {
        "timestamp": str(timestamp_ns),
        "name": event_name,
    }
    if description:
        event_metadata["description"] = description
    if extra_fields:
        event_metadata.update(extra_fields)

    # TODO: Replace this with the public writer append api
    # Load existing summary from the file using FileReader (for peek support)
    new_event = MetadataRecord(name=EVENT_METADATA_NAME, metadata=event_metadata)
    summary = McapSummaryFactory.create_summary(
        file=FileReader(input_path),
        load_summary_eagerly=True,
   )
    # Open file for reading and writing (append mode)
    file_writer = FileWriter(input_path, mode="r+b")
    # Create writer in append mode - this will seek to before the data end record
    # and set up the CRC writer with the existing CRC
    with McapRecordWriterFactory.create_writer(
        file_writer,
        summary,
        mode='a',
        chunk_size=1024 * 1024,  # Default chunk size (not used for metadata)
    ) as writer:
        # Only write the new event metadata - all other records are preserved
        writer.write_metadata(new_event)

    return input_path


def add_event(
    input_path: str | Path,
    event_name: str,
    timestamp: int,
    output_path: str | Path | None = None,
    description: str | None = None,
    extra_fields: dict[str, str] | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Add an event to an MCAP file.

    Args:
        input_path: Path to the input MCAP file.
        event_name: Name of the event.
        timestamp: Event timestamp in nanoseconds.
        output_path: If provided, copy the MCAP and add event to the copy. If None, append the event in place.
        description: Optional event description.
        extra_fields: Optional extra key-value pairs to include in the event.
        chunk_size: Target chunk size for output (only used with output_path).
        chunk_compression: Compression algorithm (only used with output_path).
        overwrite: Whether to overwrite output file if it exists.

    Returns:
        Path to the file with the event added.
    """
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format != "mcap":
        raise ValueError("Events are not supported in bag format.")
    _ = validate_compression_for_mcap(chunk_compression)

    if output_path is None:
        # Append in place
        return add_event_mcap(
            input_path,
            event_name,
            timestamp,
            description=description,
            extra_fields=extra_fields,
        )
    else:
        # Copy MCAP and add event to the copy
        output_path = Path(output_path).resolve()

        if output_path == input_path:
            raise ValueError('Input path cannot be same as output.')
        if not overwrite and output_path.exists():
            raise ValueError('Output mcap exists. Please set `overwrite` to True.')

        # Append the event to the copy
        shutil.copyfile(input_path, output_path)
        return add_event_mcap(
            output_path,
            event_name,
            timestamp,
            description=description,
            extra_fields=extra_fields,
        )


#####################
# Event Deletion    #
#####################

def delete_events_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Delete events from an MCAP file.

    Args:
        input_path: Path to input MCAP file.
        output_path: Path to output MCAP file. If None, defaults to <input_stem>_filtered.mcap.
        name: Filter events to delete by name. If None, all events matching other filters will be deleted.
        start_time: Filter events to delete with timestamp >= start_time (seconds).
        end_time: Filter events to delete with timestamp <= end_time (seconds).
        chunk_size: Target chunk size in bytes for the output file.
        chunk_compression: Compression algorithm for chunks.
        overwrite: Whether to overwrite the output file if it exists.

    Returns:
        Path to the output MCAP file.

    Raises:
        ValueError: If input and output paths are the same, or if output exists and overwrite is False.
    """
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_filtered.mcap")
    output_path = Path(output_path).resolve()

    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    if not overwrite and output_path.exists():
        raise ValueError('Output mcap exists. Please set `overwrite` to True.')

    start_ns = _to_ns(start_time)
    end_ns = _to_ns(end_time)

    # TODO: Simplify because we onlly need to delete relevant metadata records
    with McapRecordReaderFactory.from_file(input_path) as reader:
        all_channels = reader.get_channels()
        topic_to_channel_ids: dict[str, set[int]] = defaultdict(set)
        for channel_id, channel in all_channels.items():
            topic_to_channel_ids[channel.topic].add(channel_id)

        all_attachments = reader.get_attachments()
        all_metadata = reader.get_metadata()

        # Count events to be deleted
        events_to_delete = 0
        for metadata in all_metadata:
            if _is_event_metadata(metadata) and _event_matches_filters(
                metadata, name, start_ns, end_ns
            ):
                events_to_delete += 1

        if events_to_delete == 0:
            logger.warning("No events match the deletion criteria.")

        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            McapSummaryFactory.create_summary(chunk_size=chunk_size),
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=reader.get_header().profile,
        ) as writer:
            # Write message records
            written_schema_ids: set[int] = set()
            written_channel_ids: set[int] = set()
            sequence_counters: dict[int, int] = defaultdict(int)

            for msg_record in reader.get_messages(in_log_time_order=False):
                # Write the schema record the first time
                schema_id = all_channels[msg_record.channel_id].schema_id
                if schema_id != 0 and schema_id not in written_schema_ids:
                    if (schema := reader.get_schema(schema_id)) is not None:
                        writer.write_schema(schema)
                        written_schema_ids.add(schema_id)

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
                    data=msg_record.data,
                )
                sequence_counters[msg_record.channel_id] += 1
                writer.write_message(new_record)

            # Write attachments
            for attachment in all_attachments:
                writer.write_attachment(attachment)

            # Write metadata, filtering out deleted events
            deleted_count = 0
            for metadata in all_metadata:
                if _is_event_metadata(metadata) and _event_matches_filters(
                    metadata, name, start_ns, end_ns
                ):
                    # Skip this event (delete it)
                    deleted_count += 1
                    continue
                writer.write_metadata(metadata)

            logger.info(f"Deleted {deleted_count} event(s).")

    return output_path


def delete_events(
    input_path: str | Path,
    output_path: str | Path | None = None,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Delete events from an MCAP or bag file.

    Args:
        input_path: Path to input file.
        output_path: Path to output file (only used for hard delete).
        name: Filter events to delete by name.
        start_time: Filter events with timestamp >= start_time (seconds).
        end_time: Filter events with timestamp <= end_time (seconds).
        chunk_size: Target chunk size for output (only used for hard delete).
        chunk_compression: Compression algorithm (only used for hard delete).
        overwrite: Whether to overwrite output file if it exists.

    Returns:
        Tuple of (path to output file, number of events deleted).
    """
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format != "mcap":
        raise ValueError("Events are not supported in bag format.")
    _ = validate_compression_for_mcap(chunk_compression)

    # Hard delete: rewrite the file without matching events
    return delete_events_mcap(
        input_path,
        output_path=output_path,
        name=name,
        start_time=start_time,
        end_time=end_time,
        chunk_size=chunk_size,
        chunk_compression=chunk_compression,
        overwrite=overwrite,
    )


#####################
# Event Clipping    #
#####################

def clip_event_mcap(
    input_path: str | Path,
    event_name: str,
    output_path: str | Path | None = None,
    before_timestamp: float = 5.0,
    after_timestamp: float = 5.0,
    include_topics: list[str] | None = None,
    exclude_topics: list[str] | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Clip an MCAP file around an event.

    Extracts a portion of an MCAP file centered on an event's timestamp,
    including a specified time margin before and/or after the event.

    Args:
        input_path: Path to input MCAP file.
        event_name: Name of the event to clip around.
        output_path: Path to output MCAP file. If None, defaults to <input_stem>_clip_<event_name>.mcap.
        before_timestamp: Time in seconds to include before the event. Clips from (event_time - before) to (event_time + after).
        after_timestamp: Time in seconds to include after the event. Clips from (event_time - before) to (event_time + after).
        include_topics: List of topic patterns to include (glob patterns supported). If None, all topics are included.
        exclude_topics: List of topic patterns to exclude (glob patterns supported).
        chunk_size: Target chunk size in bytes for the output file.
        chunk_compression: Compression algorithm for chunks.
        overwrite: Whether to overwrite the output file if it exists.

    Returns:
        Path to the output MCAP file.

    Raises:
        ValueError: If no event with the given name is found.
    """
    input_path = Path(input_path).resolve()
    _ = validate_compression_for_mcap(chunk_compression)

    # Get all the relevant metadata records
    with McapRecordReaderFactory.from_file(input_path) as reader:
        all_metadata = reader.get_metadata(name=EVENT_METADATA_NAME)

        matching_events = [
            m for m in all_metadata
            if _is_event_metadata(m)
            and _get_event_name(m) == event_name
        ]
        if not matching_events:
            raise ValueError(f"No event found with name '{event_name}'")

        event = matching_events[0]
        event_timestamp_ns = _get_event_timestamp(event)
        if event_timestamp_ns is None:
            raise ValueError(f"Event '{event_name}' has no timestamp")
        if len(matching_events) > 1:
            # Use the first event and warn
            logger.warning(
                f"Multiple events found with name '{event_name}'. "
                f"Using the first one (timestamp: {_to_s(event_timestamp_ns):.6f}s)"
            )

    # Calculate clip time range
    event_timestamp_s = _ns_to_seconds(event_timestamp_ns)
    start_time = event_timestamp_s - before_timestamp
    end_time = event_timestamp_s + after_timestamp

    # Default output path
    if output_path is None:
        safe_name = event_name.replace("/", "_").replace(" ", "_")
        output_path = input_path.with_name(f"{input_path.stem}_clip_{safe_name}.mcap")

    # Use the filter function to do the actual clipping
    return filter_mcap(
        input_path,
        output_path=output_path,
        include_topics=include_topics,
        exclude_topics=exclude_topics,
        start_time=start_time,
        end_time=end_time,
        chunk_size=chunk_size,
        chunk_compression=chunk_compression,
        overwrite=overwrite,
    )


def clip_event(
    input_path: str | Path,
    event_name: str,
    output_path: str | Path | None = None,
    before_timestamp: float = 5.0,
    after_timestamp: float = 5.0,
    include_topics: list[str] | None = None,
    exclude_topics: list[str] | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Clip an MCAP or bag file around an event."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        return clip_event_mcap(
            input_path,
            event_name,
            output_path=output_path,
            before_timestamp=before_timestamp,
            after_timestamp=after_timestamp,
            include_topics=include_topics,
            exclude_topics=exclude_topics,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            overwrite=overwrite,
        )
    else:
        raise ValueError("Events are not supported in bag format.")


#####################
# CLI Parser Setup  #
#####################

def _run_list(args) -> None:
    """Run the event list command."""
    list_events(
        args.input,
        name=args.name,
        start_time=args.start_time,
        end_time=args.end_time,
        output_json=args.output_json,
    )


def _run_add(args) -> None:
    """Run the event add command."""
    # Parse extra key-value pairs
    extra_fields: dict[str, str] = {}
    for item in args.extra:
        if "=" not in item:
            raise ValueError(f"Invalid extra field format: {item}. Expected 'key=value'")
        key, value = item.split("=", 1)
        extra_fields[key] = value

    file_path = add_event(
        args.input,
        args.name,
        _seconds_to_ns(args.time * 1_000_000_000),
        output_path=args.output,
        description=args.description,
        extra_fields=extra_fields,
        chunk_size=getattr(args, 'chunk_size', None),
        chunk_compression=args.chunk_compression,
        overwrite=getattr(args, 'overwrite', False),
    )
    print(f"Event added to: {file_path}")


def _run_delete(args) -> None:
    """Run the event delete command."""
    # Hard delete: copy MCAP and remove events from the copy
    output_path = delete_events(
        args.input,
        output_path=args.output,
        name=args.name,
        start_time=args.start_time,
        end_time=args.end_time,
        chunk_size=args.chunk_size,
        chunk_compression=args.chunk_compression,
        overwrite=args.overwrite,
    )
    print(f"Events deleted. Output written to: {output_path}")


def _run_clip(args) -> None:
    """Run the event clip command."""
    # Handle --margin flag: it's a CLI convenience that sets both before and after
    if args.margin is not None:
        if args.before is not None or args.after is not None:
            raise ValueError("Cannot use --margin together with --before or --after")
        before, after = args.margin
    else:
        # Use 0 for unspecified values (clip up to or from event time)
        before = args.before if args.before is not None else 0.0
        after = args.after if args.after is not None else 0.0

    output_path = clip_event(
        args.input,
        args.name,
        output_path=args.output,
        before_timestamp=before,
        after_timestamp=after,
        include_topics=args.include_topic,
        exclude_topics=args.exclude_topic,
        chunk_size=args.chunk_size,
        chunk_compression=args.chunk_compression,
        overwrite=args.overwrite,
    )
    print(f"Clipped around event '{args.name}'. Output written to: {output_path}")


def add_parser(subparsers) -> None:
    """Add the event command and its subcommands to the argument parser."""
    event_parser = subparsers.add_parser(
        "event",
        help="Manage events in MCAP files.",
        description=dedent("""
            Manage events in MCAP files. Events are markers that indicate when
            something happens in the recording, such as the start of a maneuver,
            a collision, or any other significant occurrence.

            Events are stored as metadata records with a special name. Each event
            has a timestamp, a name, and an optional description.

            Note: Events are only supported in MCAP format, not bag files.
        """),
    )
    event_subparsers = event_parser.add_subparsers(dest="event_command")
    event_parser.set_defaults(func=lambda args: event_parser.print_help())

    # List subcommand
    list_parser = event_subparsers.add_parser(
        "list",
        help="List events in an MCAP file.",
        description="List all events in an MCAP file. Use filters to narrow down the results.",
    )
    list_parser.add_argument("input", help="Path to MCAP file (*.mcap)")
    list_parser.add_argument(
        "--name",
        help="Filter events by name",
    )
    list_parser.add_argument(
        "--start-time",
        type=float,
        help="Filter events with timestamp >= start_time (in seconds)",
    )
    list_parser.add_argument(
        "--end-time",
        type=float,
        help="Filter events with timestamp <= end_time (in seconds)",
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output in JSON format",
    )
    list_parser.set_defaults(func=_run_list)

    # Add subcommand
    add_parser_cmd = event_subparsers.add_parser(
        "add",
        help="Add an event to an MCAP file.",
        description=dedent("""
            Add a new event to an MCAP file. Events are markers with a timestamp
            and name that indicate when something significant happened.

            By default, the event is appended directly to the input file.
            Use -o to copy the MCAP and add the event to the copy instead.
        """),
    )
    add_parser_cmd.add_argument("input", help="Path to MCAP file (*.mcap)")
    add_parser_cmd.add_argument("name", help="Name of the event (e.g., 'start', 'collision')")
    add_parser_cmd.add_argument(
        "time",
        required=True,
        type=float,
        help="Timestamp of the event in seconds",
    )
    add_parser_cmd.add_argument(
        "-o", "--output",
        help="Output file path. If specified, copies the MCAP and adds the event to the copy.",
    )
    add_parser_cmd.add_argument(
        "--description",
        help="Optional description of the event",
    )
    add_parser_cmd.add_argument(
        "--extra",
        action="append",
        metavar="KEY=VALUE",
        help="Extra key-value pair to include in the event (can be used multiple times)",
    )
    add_parser_cmd.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the output file in bytes (only used with -o)",
    )
    add_parser_cmd.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd", "none"],
        help="Compression used for chunk records (only used with -o)",
    )
    add_parser_cmd.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists (only used with -o)",
    )
    add_parser_cmd.set_defaults(func=_run_add)

    # Delete subcommand
    delete_parser = event_subparsers.add_parser(
        "delete",
        help="Delete events from an MCAP file.",
        description=dedent("""
            Delete events from an MCAP file. Use filters to select which events
            to delete. If no filters are provided, all events will be deleted.
        """),
    )
    delete_parser.add_argument("input", help="Path to input MCAP file (*.mcap)")
    delete_parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output file path. If specified, copies MCAP and removes events from copy.",
    )
    delete_parser.add_argument(
        "--name",
        help="Delete only events with this name",
    )
    delete_parser.add_argument(
        "--start-time",
        type=float,
        help="Delete events with timestamp >= start_time (in seconds)",
    )
    delete_parser.add_argument(
        "--end-time",
        type=float,
        help="Delete events with timestamp <= end_time (in seconds)",
    )
    delete_parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the output file in bytes (only used with -o)",
    )
    delete_parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd", "none"],
        help="Compression used for chunk records (only used with -o)",
    )
    delete_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists (only used with -o)",
    )
    delete_parser.set_defaults(func=_run_delete)

    # Clip subcommand
    clip_parser = event_subparsers.add_parser(
        "clip",
        help="Extract a portion of an MCAP file around an event.",
        description=dedent("""
            Clip an MCAP file around an event, extracting messages within a time
            window relative to the event's timestamp. This is useful for extracting
            specific incidents or occurrences from a larger recording.

            Time range options:
              --margin X      Symmetric: X seconds before AND after the event
              --before X      Clip from (event_time - X) to event_time
              --after X       Clip from event_time to (event_time + X)
              --before X --after Y  Clip from (event_time - X) to (event_time + Y)
        """),
    )
    clip_parser.add_argument("input", help="Path to input MCAP file (*.mcap)")
    clip_parser.add_argument("name", help="Name of the event to clip around")
    clip_parser.add_argument(
        "-o", "--output",
        help="Output file path. If not specified, creates <input>_clip_<event_name>.mcap",
    )
    clip_parser.add_argument(
        "--margin",
        type=float,
        help="Symmetric margin: seconds to include before AND after the event",
    )
    clip_parser.add_argument(
        "--before",
        type=float,
        help="Seconds to include before the event (clips to event_time if --after not set)",
    )
    clip_parser.add_argument(
        "--after",
        type=float,
        help="Seconds to include after the event (clips from event_time if --before not set)",
    )
    clip_parser.add_argument(
        "--include-topic",
        action="append",
        help="Topics to include (supports glob patterns). Can be used multiple times.",
    )
    clip_parser.add_argument(
        "--exclude-topic",
        action="append",
        help="Topics to exclude (supports glob patterns). Can be used multiple times.",
    )
    clip_parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the output file in bytes",
    )
    clip_parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=["lz4", "zstd", "none"],
        help="Compression used for chunk records",
    )
    clip_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    clip_parser.set_defaults(func=_run_clip)
