"""MCAP and ROS bag inspect CLI command for examining record contents."""

import fnmatch
import json
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.bag.records import ConnectionRecord
from pybag.bag_reader import BagFileReader
from pybag.cli.utils import get_file_format_from_magic
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.records import (
    AttachmentRecord,
    ChannelRecord,
    MetadataRecord,
    SchemaRecord
)


def _ns_to_seconds(ns: int) -> float:
    """Convert nanoseconds to seconds."""
    return ns / 1_000_000_000


def _to_ns(seconds: float | None) -> int | None:
    """Convert seconds to nanoseconds."""
    if seconds is None:
        return None
    return int(seconds * 1_000_000_000)


def _format_bytes(data: bytes, max_length: int = 100) -> str:
    """Format bytes for display, truncating if necessary."""
    if len(data) <= max_length:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return repr(data)
    else:
        try:
            preview = data[:max_length].decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            preview = repr(data[:max_length])
        return f"{preview}... ({len(data)} bytes total)"

#####################
# Schema inspection #
#####################

def _print_schema_table(schemas: dict[int, SchemaRecord]) -> None:
    """Print schemas in a table format."""
    if not schemas:
        print("No schemas found.")
        return

    schema_list = sorted(schemas.values(), key=lambda s: s.id)

    # Calculate column widths
    max_id_len = max(len(str(s.id)) for s in schema_list)
    max_name_len = max(len(s.name) for s in schema_list)
    max_encoding_len = max(len(s.encoding) for s in schema_list)

    # Print header
    print(f"  {'ID':>{max_id_len}}  {'Name':<{max_name_len}}  {'Encoding':<{max_encoding_len}}")
    print(f"  {'-' * max_id_len}  {'-' * max_name_len}  {'-' * max_encoding_len}")

    # Print rows
    for schema in schema_list:
        print(f"  {schema.id:>{max_id_len}}  {schema.name:<{max_name_len}}  {schema.encoding:<{max_encoding_len}}")


def _print_schema_detail(schema: SchemaRecord) -> None:
    """Print detailed schema information."""
    print(f"Schema ID: {schema.id}")
    print(f"Name:      {schema.name}")
    print(f"Encoding:  {schema.encoding}")
    print(f"Data Size: {len(schema.data):,} bytes")
    print()
    print("Data:")
    print("-" * 40)
    try:
        print(schema.data.decode("utf-8"))
    except UnicodeDecodeError:
        print(repr(schema.data))


def _schema_to_json(schema: SchemaRecord) -> dict:
    """Convert schema to JSON-serializable dict."""
    return {
        "id": schema.id,
        "name": schema.name,
        "encoding": schema.encoding,
        "data_size": len(schema.data),
        "data": schema.data.decode("utf-8", errors="replace"),
    }


def _print_schema_json(schema: SchemaRecord) -> None:
    print(json.dumps(_schema_to_json(schema), indent=2))


def _print_schemas_json(schemas: list[SchemaRecord]) -> None:
    print(json.dumps([_schema_to_json(s) for s in schemas], indent=2))


def inspect_schemas_mcap(
    input_path: Path,
    schema_id: int | None = None,
    name_pattern: str | None = None,
    *,
    output_json: bool = False,
) -> None:
    """Inspect schemas in an MCAP file."""
    with McapRecordReaderFactory.from_file(input_path) as reader:
        schemas = reader.get_schemas()

        # Filter by name pattern if provided
        if name_pattern:
            schemas = {
                sid: s for sid, s in schemas.items()
                if fnmatch.fnmatch(s.name, name_pattern)
            }

        # Single schema detail view
        if schema_id is not None:
            schema = schemas.get(schema_id)
            if schema is None:
                print(f"Schema with ID {schema_id} not found.")
                return
            if output_json:
                _print_schema_json(schema)
            else:
                _print_schema_detail(schema)
            return

        # List view
        if output_json:
            _print_schemas_json(list(schemas.values()))
        else:
            print(f"Schemas ({len(schemas)}):\n")
            _print_schema_table(schemas)


def _bag_connection_to_schema(conn: ConnectionRecord) -> SchemaRecord:
    """Convert a bag ConnectionRecord to a SchemaRecord.

    In bag files, schemas are stored in the connection's message_definition field.
    """
    header = conn.connection_header
    return SchemaRecord(
        id=conn.conn,
        name=header.type,
        encoding="ros1msg",
        data=header.message_definition.encode("utf-8"),
    )


def inspect_schemas_bag(
    input_path: Path,
    schema_id: int | None = None,
    name_pattern: str | None = None,
    *,
    output_json: bool = False,
) -> None:
    """Inspect schemas in a ROS bag file.

    Bag files store schema information in connection records.
    """
    with BagFileReader.from_file(input_path) as reader:
        connections = reader.get_connections()

        # Convert connections to schemas
        schemas: dict[int, SchemaRecord] = {}
        for conn in connections:
            schema = _bag_connection_to_schema(conn)
            # Only add unique schemas (by name) to avoid duplicates
            # when multiple connections use the same message type
            if schema.id not in schemas:
                schemas[schema.id] = schema

        # Filter by name pattern if provided
        if name_pattern:
            schemas = {
                sid: s for sid, s in schemas.items()
                if fnmatch.fnmatch(s.name, name_pattern)
            }

        # Single schema detail view
        if schema_id is not None:
            schema = schemas.get(schema_id)
            if schema is None:
                print(f"Schema with ID {schema_id} not found.")
                return
            if output_json:
                _print_schema_json(schema)
            else:
                _print_schema_detail(schema)
            return

        # List view
        if output_json:
            _print_schemas_json(list(schemas.values()))
        else:
            print(f"Schemas ({len(schemas)}):\n")
            _print_schema_table(schemas)


def inspect_schemas(
    input_path: str | Path,
    schema_id: int | None = None,
    name_pattern: str | None = None,
    *,
    output_json: bool = False,
) -> None:
    """Inspect schemas in an MCAP or bag file."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        inspect_schemas_mcap(
            input_path,
            schema_id=schema_id,
            name_pattern=name_pattern,
            output_json=output_json,
        )
    else:
        inspect_schemas_bag(
            input_path,
            schema_id=schema_id,
            name_pattern=name_pattern,
            output_json=output_json,
        )


######################
# Channel inspection #
######################

def _print_channel_table(channels: dict[int, ChannelRecord], schemas: dict[int, SchemaRecord]) -> None:
    """Print channels in a table format."""
    if not channels:
        print("No channels found.")
        return

    channel_list = sorted(channels.values(), key=lambda c: c.topic)

    # Calculate column widths
    max_id_len = max(len(str(c.id)) for c in channel_list)
    max_topic_len = max(len(c.topic) for c in channel_list)
    max_encoding_len = max(len(c.message_encoding) for c in channel_list)

    # Print header
    print(f"  {'ID':>{max_id_len}}  {'Topic':<{max_topic_len}}  {'Encoding':<{max_encoding_len}}  Schema")
    print(f"  {'-' * max_id_len}  {'-' * max_topic_len}  {'-' * max_encoding_len}  {'-' * 30}")

    # Print rows
    for channel in channel_list:
        schema_name = "N/A"
        if channel.schema_id in schemas:
            schema_name = schemas[channel.schema_id].name
        print(f"  {channel.id:>{max_id_len}}  {channel.topic:<{max_topic_len}}  {channel.message_encoding:<{max_encoding_len}}  {schema_name}")


def _print_channel_detail(channel: ChannelRecord, schema: SchemaRecord | None) -> None:
    """Print detailed channel information."""
    print(f"Channel ID:       {channel.id}")
    print(f"Topic:            {channel.topic}")
    print(f"Message Encoding: {channel.message_encoding}")
    print(f"Schema ID:        {channel.schema_id}")
    if schema:
        print(f"Schema Name:      {schema.name}")
        print(f"Schema Encoding:  {schema.encoding}")
    if channel.metadata:
        print()
        print("Metadata:")
        for key, value in channel.metadata.items():
            print(f"  {key}: {value}")


def _channel_to_json(channel: ChannelRecord, schema: SchemaRecord | None) -> dict:
    """Convert channel to JSON-serializable dict."""
    result = {
        "id": channel.id,
        "topic": channel.topic,
        "message_encoding": channel.message_encoding,
        "schema_id": channel.schema_id,
        "metadata": channel.metadata,
    }
    if schema:
        result["schema_name"] = schema.name
        result["schema_encoding"] = schema.encoding
    return result


def _print_channel_json(channel: ChannelRecord, schema: SchemaRecord | None):
    print(json.dumps(_channel_to_json(channel, schema), indent=2))


def _bag_connection_to_channel(conn: ConnectionRecord) -> ChannelRecord:
    """Convert a bag ConnectionRecord to a ChannelRecord.

    In bag files, connections are equivalent to MCAP channels.
    """
    header = conn.connection_header
    return ChannelRecord(
        id=conn.conn,
        schema_id=conn.conn,  # In bag, schema is per-connection
        topic=conn.topic,
        message_encoding="ros1msg",
        metadata={},  # Bag format doesn't have channel metadata
    )


def inspect_channels_mcap(
    input_path: Path,
    *,
    channel_id: int | None = None,
    topic_pattern: str | None = None,
    output_json: bool = False,
) -> None:
    """Inspect channels in an MCAP file."""
    with McapRecordReaderFactory.from_file(input_path) as reader:
        channels = reader.get_channels()
        schemas = reader.get_schemas()

        # Filter by topic pattern if provided
        if topic_pattern:
            channels = {
                cid: c for cid, c in channels.items()
                if fnmatch.fnmatch(c.topic, topic_pattern)
            }

        # Single channel detail view
        if channel_id is not None:
            channel = channels.get(channel_id)
            if channel is None:
                print(f"Channel with ID {channel_id} not found.")
                return
            schema = schemas.get(channel.schema_id)
            if output_json:
                _print_channel_json(channel, schema)
            else:
                _print_channel_detail(channel, schema)
            return

        # List view
        if output_json:
            print(json.dumps([
                _channel_to_json(c, schemas.get(c.schema_id))
                for c in channels.values()
            ], indent=2))
        else:
            print(f"Channels ({len(channels)}):\n")
            _print_channel_table(channels, schemas)


def inspect_channels_bag(
    input_path: Path,
    *,
    channel_id: int | None = None,
    topic_pattern: str | None = None,
    output_json: bool = False,
) -> None:
    """Inspect channels (connections) in a ROS bag file."""
    with BagFileReader.from_file(input_path) as reader:
        connections = reader.get_connections()

        # Convert connections to channels and schemas
        channels: dict[int, ChannelRecord] = {}
        schemas: dict[int, SchemaRecord] = {}
        for conn in connections:
            channels[conn.conn] = _bag_connection_to_channel(conn)
            schemas[conn.conn] = _bag_connection_to_schema(conn)

        # Filter by topic pattern if provided
        if topic_pattern:
            channels = {
                cid: c for cid, c in channels.items()
                if fnmatch.fnmatch(c.topic, topic_pattern)
            }

        # Single channel detail view
        if channel_id is not None:
            channel = channels.get(channel_id)
            if channel is None:
                print(f"Channel with ID {channel_id} not found.")
                return
            schema = schemas.get(channel.schema_id)
            if output_json:
                _print_channel_json(channel, schema)
            else:
                _print_channel_detail(channel, schema)
            return

        # List view
        if output_json:
            print(json.dumps([
                _channel_to_json(c, schemas.get(c.schema_id))
                for c in channels.values()
            ], indent=2))
        else:
            print(f"Channels ({len(channels)}):\n")
            _print_channel_table(channels, schemas)


def inspect_channels(
    input_path: str | Path,
    *,
    channel_id: int | None = None,
    topic_pattern: str | None = None,
    output_json: bool = False,
) -> None:
    """Inspect channels in an MCAP or bag file."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        inspect_channels_mcap(
            input_path,
            channel_id=channel_id,
            topic_pattern=topic_pattern,
            output_json=output_json,
        )
    else:
        inspect_channels_bag(
            input_path,
            channel_id=channel_id,
            topic_pattern=topic_pattern,
            output_json=output_json,
        )


#######################
# Metadata inspection #
#######################

def _print_metadata_table(metadata_list: list[MetadataRecord]) -> None:
    """Print metadata in a table format."""
    if not metadata_list:
        print("No metadata found.")
        return

    metadata_list = sorted(metadata_list, key=lambda m: m.name)

    # Calculate column widths
    max_name_len = max(len(m.name) for m in metadata_list)

    # Print header
    print(f"  {'Name':<{max_name_len}}  Keys")
    print(f"  {'-' * max_name_len}  {'-' * 40}")

    # Print rows
    for metadata in metadata_list:
        keys = ", ".join(metadata.metadata.keys())
        if len(keys) > 60:
            keys = keys[:57] + "..."
        print(f"  {metadata.name:<{max_name_len}}  {keys}")


def _print_metadata_detail(metadata: MetadataRecord) -> None:
    """Print detailed metadata information."""
    print(f"Name: {metadata.name}")
    print()
    print("Values:")
    print("-" * 40)
    for key, value in metadata.metadata.items():
        print(f"  {key}: {value}")


def _metadata_to_json(metadata: MetadataRecord) -> dict:
    """Convert metadata to JSON-serializable dict."""
    return {
        "name": metadata.name,
        "metadata": metadata.metadata,
    }


def inspect_metadata_mcap(
    input_path: Path,
    *,
    name: str | None = None,
    output_json: bool = False,
) -> None:
    """Inspect metadata in an MCAP file."""
    with McapRecordReaderFactory.from_file(input_path) as reader:
        metadata_list = reader.get_metadata(name=name)

        # Single metadata detail view (when name matches exactly one)
        if name is not None and len(metadata_list) == 1:
            metadata = metadata_list[0]
            if output_json:
                print(json.dumps(_metadata_to_json(metadata), indent=2))
            else:
                _print_metadata_detail(metadata)
            return

        # List view
        if output_json:
            print(json.dumps([_metadata_to_json(m) for m in metadata_list], indent=2))
        else:
            print(f"Metadata ({len(metadata_list)}):\n")
            _print_metadata_table(metadata_list)


def inspect_metadata_bag(
    input_path: Path,
    *,
    name: str | None = None,
    output_json: bool = False,
) -> None:
    """Show message that metadata is not supported in bag format."""
    print("Metadata not supported in bag format.")


def inspect_metadata(
    input_path: str | Path,
    *,
    name: str | None = None,
    output_json: bool = False,
) -> None:
    """Inspect metadata in an MCAP or bag file."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        inspect_metadata_mcap(
            input_path,
            name=name,
            output_json=output_json,
        )
    else:
        inspect_metadata_bag(
            input_path,
            name=name,
            output_json=output_json,
        )


#########################
# Attachment inspection #
#########################

def _print_attachment_table(attachments: list[AttachmentRecord]) -> None:
    """Print attachments in a table format."""
    if not attachments:
        print("No attachments found.")
        return

    attachments = sorted(attachments, key=lambda a: a.log_time)

    # Calculate column widths
    max_name_len = max(len(a.name) for a in attachments)
    max_type_len = max(len(a.media_type) for a in attachments)

    # Print header
    print(f"  {'Name':<{max_name_len}}  {'Media Type':<{max_type_len}}  {'Log Time':>14}  {'Size':>12}")
    print(f"  {'-' * max_name_len}  {'-' * max_type_len}  {'-' * 14}  {'-' * 12}")

    # Print rows
    for attachment in attachments:
        log_time_s = f"{_ns_to_seconds(attachment.log_time):.6f}"
        size = f"{len(attachment.data):,} bytes"
        print(f"  {attachment.name:<{max_name_len}}  {attachment.media_type:<{max_type_len}}  {log_time_s:>14}  {size:>12}")


def _print_attachment_detail(attachment: AttachmentRecord, include_data: bool = False) -> None:
    """Print detailed attachment information."""
    print(f"Name:        {attachment.name}")
    print(f"Media Type:  {attachment.media_type}")
    print(f"Log Time:    {_ns_to_seconds(attachment.log_time):.6f} s ({attachment.log_time} ns)")
    print(f"Create Time: {_ns_to_seconds(attachment.create_time):.6f} s ({attachment.create_time} ns)")
    print(f"Data Size:   {len(attachment.data):,} bytes")
    print(f"CRC:         {attachment.crc}")
    if include_data:
        print()
        print("Data:")
        print("-" * 40)
        print(_format_bytes(attachment.data, max_length=1000))


def _attachment_to_json(attachment: AttachmentRecord, include_data: bool = False) -> dict:
    """Convert attachment to JSON-serializable dict."""
    result = {
        "name": attachment.name,
        "media_type": attachment.media_type,
        "log_time": attachment.log_time,
        "log_time_seconds": _ns_to_seconds(attachment.log_time),
        "create_time": attachment.create_time,
        "create_time_seconds": _ns_to_seconds(attachment.create_time),
        "data_size": len(attachment.data),
        "crc": attachment.crc,
    }
    if include_data:
        # Try to decode as UTF-8, fall back to base64
        try:
            result["data"] = attachment.data.decode("utf-8")
        except UnicodeDecodeError:
            import base64
            result["data_base64"] = base64.b64encode(attachment.data).decode("ascii")
    return result


def inspect_attachments_mcap(
    input_path: Path,
    *,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    include_data: bool = False,
    output_json: bool = False,
) -> None:
    """Inspect attachments in an MCAP file."""
    with McapRecordReaderFactory.from_file(input_path) as reader:
        attachments = reader.get_attachments(
            name=name,
            start_time=_to_ns(start_time),
            end_time=_to_ns(end_time),
        )

        # Single attachment detail view (when name matches exactly one)
        if name is not None and len(attachments) == 1:
            attachment = attachments[0]
            if output_json:
                print(json.dumps(_attachment_to_json(attachment, include_data), indent=2))
            else:
                _print_attachment_detail(attachment, include_data)
            return

        # List view
        if output_json:
            print(json.dumps([_attachment_to_json(a, include_data) for a in attachments], indent=2))
        else:
            print(f"Attachments ({len(attachments)}):")
            print()
            _print_attachment_table(attachments)


def inspect_attachments_bag(
    input_path: Path,
    *,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    include_data: bool = False,
    output_json: bool = False,
) -> None:
    """Show message that attachments are not supported in bag format."""
    print("Attachments not supported in bag format.")


def inspect_attachments(
    input_path: str | Path,
    *,
    name: str | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    include_data: bool = False,
    output_json: bool = False,
) -> None:
    """Inspect attachments in an MCAP or bag file."""
    input_path = Path(input_path).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        inspect_attachments_mcap(
            input_path,
            name=name,
            start_time=start_time,
            end_time=end_time,
            include_data=include_data,
            output_json=output_json,
        )
    else:
        inspect_attachments_bag(
            input_path,
            name=name,
            start_time=start_time,
            end_time=end_time,
            include_data=include_data,
            output_json=output_json,
        )


# CLI argument parser setup


def add_parser(subparsers) -> None:
    """Add the inspect command and its subcommands to the argument parser."""
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect MCAP or bag record contents.",
        description=dedent("""
            Inspect the contents of MCAP or ROS bag records. Supports examining
            schemas, channels, metadata, and attachments.

            The file format is automatically detected from magic bytes or extension.

            Use subcommands to inspect specific record types:
              pybag inspect schemas <file>      - List all schemas
              pybag inspect channels <file>     - List all channels
              pybag inspect metadata <file>     - List all metadata (MCAP only)
              pybag inspect attachments <file>  - List all attachments (MCAP only)

            Use --id or --name to show details for a specific record.
            Use --json for machine-readable output.

            Note: Bag files store schema info in connection records. Metadata and
            attachments are not supported in the bag format.
        """),
    )
    inspect_subparsers = inspect_parser.add_subparsers(dest="record_type")
    inspect_parser.set_defaults(func=lambda args: inspect_parser.print_help())

    # Schemas subcommand
    schema_parser = inspect_subparsers.add_parser(
        "schemas",
        help="Inspect schemas in an MCAP or bag file.",
        description="List or inspect schema records. For bag files, schemas are extracted from connection records.",
    )
    schema_parser.add_argument("input", help="Path to MCAP (*.mcap) or bag (*.bag) file")
    schema_parser.add_argument(
        "--id",
        type=int,
        dest="schema_id",
        help="Show details for a specific schema by ID",
    )
    schema_parser.add_argument(
        "--name",
        dest="name_pattern",
        help="Filter schemas by name (supports glob patterns like 'sensor_msgs/*')",
    )
    schema_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output in JSON format",
    )
    schema_parser.set_defaults(
        func=lambda args: inspect_schemas(
            args.input,
            schema_id=args.schema_id,
            name_pattern=args.name_pattern,
            output_json=args.output_json,
        )
    )

    # Channels subcommand
    channel_parser = inspect_subparsers.add_parser(
        "channels",
        help="Inspect channels in an MCAP or bag file.",
        description="List or inspect channel records. For bag files, channels correspond to connection records.",
    )
    channel_parser.add_argument("input", help="Path to MCAP (*.mcap) or bag (*.bag) file")
    channel_parser.add_argument(
        "--id",
        type=int,
        dest="channel_id",
        help="Show details for a specific channel by ID",
    )
    channel_parser.add_argument(
        "--topic",
        dest="topic_pattern",
        help="Filter channels by topic (supports glob patterns like '/sensor/*')",
    )
    channel_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output in JSON format",
    )
    channel_parser.set_defaults(
        func=lambda args: inspect_channels(
            args.input,
            channel_id=args.channel_id,
            topic_pattern=args.topic_pattern,
            output_json=args.output_json,
        )
    )

    # Metadata subcommand
    metadata_parser = inspect_subparsers.add_parser(
        "metadata",
        help="Inspect metadata in an MCAP file (not supported for bag files).",
        description="List or inspect metadata records. Metadata stores custom key-value pairs. Not supported in bag format.",
    )
    metadata_parser.add_argument("input", help="Path to MCAP (*.mcap) or bag (*.bag) file")
    metadata_parser.add_argument(
        "--name",
        help="Filter or select metadata by name",
    )
    metadata_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output in JSON format",
    )
    metadata_parser.set_defaults(
        func=lambda args: inspect_metadata(
            args.input,
            name=args.name,
            output_json=args.output_json,
        )
    )

    # Attachments subcommand
    attachment_parser = inspect_subparsers.add_parser(
        "attachments",
        help="Inspect attachments in an MCAP file (not supported for bag files).",
        description="List or inspect attachment records. Attachments are arbitrary file data. Not supported in bag format.",
    )
    attachment_parser.add_argument("input", help="Path to MCAP (*.mcap) or bag (*.bag) file")
    attachment_parser.add_argument(
        "--name",
        help="Filter or select attachments by name",
    )
    attachment_parser.add_argument(
        "--start-time",
        type=float,
        help="Filter attachments with log_time >= start_time (in seconds)",
    )
    attachment_parser.add_argument(
        "--end-time",
        type=float,
        help="Filter attachments with log_time <= end_time (in seconds)",
    )
    attachment_parser.add_argument(
        "--data",
        action="store_true",
        dest="include_data",
        help="Include attachment data in output (only when viewing single attachment)",
    )
    attachment_parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output in JSON format",
    )
    attachment_parser.set_defaults(
        func=lambda args: inspect_attachments(
            args.input,
            name=args.name,
            start_time=args.start_time,
            end_time=args.end_time,
            include_data=args.include_data,
            output_json=args.output_json,
        )
    )
