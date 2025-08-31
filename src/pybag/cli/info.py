import argparse
from datetime import datetime
from pathlib import Path

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from pybag.mcap.record_reader import (
    BaseMcapRecordReader,
    McapRecordReaderFactory
)


def format_duration(nanoseconds: int) -> str:
    """Format duration from nanoseconds to human readable format."""
    seconds = nanoseconds / 1_000_000_000

    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def format_timestamp(nanoseconds: int) -> str:
    """Format timestamp from nanoseconds since epoch to human readable format."""
    seconds = nanoseconds / 1_000_000_000
    return datetime.fromtimestamp(seconds).strftime("%Y-%m-%d %H:%M:%S")


def format_file_size(bytes_size: int) -> str:
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}" if bytes_size != int(bytes_size) else f"{int(bytes_size)} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} PB"


def format_frequency(message_count: int, duration_ns: int) -> str:
    """Calculate and format message frequency."""
    if duration_ns == 0:
        return "N/A"

    duration_seconds = duration_ns / 1_000_000_000
    frequency = message_count / duration_seconds

    if frequency >= 1:
        return f"{frequency:.1f} Hz"
    else:
        period = 1 / frequency
        return f"1/{period:.1f} Hz"


def create_file_info_panel(reader: BaseMcapRecordReader, file_path: Path) -> Panel:
    """Create a panel with basic file information."""
    stats = reader.get_statistics()
    duration = stats.message_end_time - stats.message_start_time
    file_size = file_path.stat().st_size

    info_text = Text()
    info_text.append(f"File: {file_path.name}\n", style="bold")
    info_text.append(f"Size: {format_file_size(file_size)}\n")
    info_text.append(f"Duration: {format_duration(duration)}\n")
    info_text.append(f"Start: {format_timestamp(stats.message_start_time)}\n")
    info_text.append(f"End: {format_timestamp(stats.message_end_time)}")

    return Panel(info_text, title="File Information", border_style="dim")


def create_summary_panel(reader: BaseMcapRecordReader) -> Panel:
    """Create a panel with summary statistics."""
    stats = reader.get_statistics()

    summary_text = Text()
    summary_text.append(f"Messages: {stats.message_count:,}   ", style="bold")
    summary_text.append(f"Topics: {stats.channel_count}   ")
    summary_text.append(f"Schemas: {stats.schema_count}\n")
    summary_text.append(f"Chunks: {stats.chunk_count}   ")
    summary_text.append(f"Attachments: {stats.attachment_count}   ")
    summary_text.append(f"Metadata: {stats.metadata_count}")

    return Panel(summary_text, title="Summary", border_style="dim")


def create_topics_table(reader: BaseMcapRecordReader) -> Table:
    """Create a table with topic information."""
    from rich.box import SIMPLE
    table = Table(show_header=True, header_style="bold", box=SIMPLE, border_style="dim")
    table.add_column("Topic", no_wrap=True, justify="center")
    table.add_column("Messages", justify="center")
    table.add_column("Frequency", justify="center")
    table.add_column("Type", no_wrap=True, justify="center")

    stats = reader.get_statistics()
    duration = stats.message_end_time - stats.message_start_time
    topics = reader.get_channels()
    schemas = reader.get_schemas()

    # Sort topics by message count (descending)
    sorted_topics = sorted(
        topics.items(),
        key=lambda x: stats.channel_message_counts.get(x[0], 0),
        reverse=True
    )

    for channel_id, channel in sorted_topics:
        message_count = stats.channel_message_counts.get(channel_id, 0)
        frequency = format_frequency(message_count, duration)

        # Get schema name if available
        schema_name = "Unknown"
        if channel.schema_id in schemas:
            schema_name = schemas[channel.schema_id].name

        table.add_row(
            channel.topic,
            f"{message_count:,}",
            frequency,
            schema_name
        )

    return table


def create_schemas_table(reader: BaseMcapRecordReader) -> Table:
    """Create a table with schema information."""
    table = Table(show_header=True, header_style="bold", box=None, border_style="dim")
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Encoding")
    table.add_column("Size", justify="right")
    table.add_column("Topics", justify="right")

    schemas = reader.get_schemas()
    channels = reader.get_channels()

    # Count how many channels use each schema
    schema_usage = {}
    for channel in channels.values():
        schema_id = channel.schema_id
        schema_usage[schema_id] = schema_usage.get(schema_id, 0) + 1

    # Sort schemas by usage (descending), then by name
    sorted_schemas = sorted(
        schemas.items(),
        key=lambda x: (schema_usage.get(x[0], 0), x[1].name),
        reverse=True
    )

    for schema_id, schema in sorted_schemas:
        usage_count = schema_usage.get(schema_id, 0)
        size_formatted = format_file_size(len(schema.data))
        usage_text = str(usage_count)

        table.add_row(
            str(schema_id),
            schema.name,
            schema.encoding,
            size_formatted,
            usage_text
        )

    return table


def info_command(args: argparse.Namespace) -> None:
    """Execute the info command."""
    console = Console()
    file_path = Path(args.mcap_path)

    if not file_path.exists():
        console.print(f"[bold red]Error:[/bold red] File '{file_path}' does not exist")
        return

    try:
        reader = McapRecordReaderFactory.from_file(file_path)
        stats = reader.get_statistics()
        header = reader.get_header()
        duration = stats.message_end_time - stats.message_start_time
        file_size = file_path.stat().st_size

        # Print clean title
        console.print(f"\n[bold blue]{file_path.name}[/bold blue]\n")

        # File properties
        file_info = Text()
        file_info.append("Size: ", style="dim")
        file_info.append(f"{format_file_size(file_size)}")
        file_info.append("  Duration: ", style="dim")
        file_info.append(f"{format_duration(duration)}")
        if header.profile:
            file_info.append("  Profile: ", style="dim")
            file_info.append(header.profile, style="cyan")
        console.print(file_info)

        # Content summary
        content_info = Text()
        content_info.append("Messages: ", style="dim")
        content_info.append(f"{stats.message_count:,}", style="bold")
        content_info.append("  Topics: ", style="dim")
        content_info.append(f"{stats.channel_count}")
        content_info.append("  Schemas: ", style="dim")
        content_info.append(f"{stats.schema_count}")
        content_info.append("  Chunks: ", style="dim")
        content_info.append(f"{stats.chunk_count}")
        console.print(content_info)

        # Time range section
        console.print()
        console.print("[dim]Time Range:[/dim]")
        console.print(f"  {format_timestamp(stats.message_start_time)} → {format_timestamp(stats.message_end_time)}")
        console.print(f"  [dim]{stats.message_start_time} → {stats.message_end_time} ns[/dim]")

        # Topics section
        console.print()
        if stats.channel_count == 0:
            console.print("[dim]Topics: none[/dim]")
        else:
            console.print("[dim]Topics:[/dim]")
            topics_table = create_topics_table(reader)
            console.print(topics_table)

    except Exception as e:
        console.print(f"[bold red]Error reading MCAP file:[/bold red] {e}")


def add_info_parser(subparsers) -> None:
    """Add the info command parser."""
    info_parser = subparsers.add_parser(
        "info",
        help="Display information about an MCAP file"
    )
    info_parser.add_argument(
        "mcap_path",
        help="Path to the MCAP file"
    )
    info_parser.set_defaults(func=info_command)
