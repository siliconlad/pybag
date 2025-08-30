import argparse
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.columns import Columns

from pybag.mcap.record_reader import McapRecordReaderFactory, BaseMcapRecordReader


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

    info_text = Text()
    info_text.append(f"File: {file_path.name}\n", style="bold")
    info_text.append(f"Size: {file_path.stat().st_size:,} bytes\n")
    info_text.append(f"Duration: {format_duration(duration)}\n")
    info_text.append(f"Start: {format_timestamp(stats.message_start_time)}\n")
    info_text.append(f"End: {format_timestamp(stats.message_end_time)}")

    return Panel(info_text, title="File Information", border_style="blue")


def create_summary_panel(reader: BaseMcapRecordReader) -> Panel:
    """Create a panel with summary statistics."""
    stats = reader.get_statistics()

    summary_text = Text()
    summary_text.append(f"Messages: {stats.message_count:,}\n", style="bold green")
    summary_text.append(f"Topics: {stats.channel_count}\n", style="bold cyan")
    summary_text.append(f"Schemas: {stats.schema_count}\n", style="bold yellow")
    summary_text.append(f"Chunks: {stats.chunk_count}\n")
    summary_text.append(f"Attachments: {stats.attachment_count}\n")
    summary_text.append(f"Metadata: {stats.metadata_count}")

    return Panel(summary_text, title="Summary", border_style="green")


def create_topics_table(reader: BaseMcapRecordReader) -> Table:
    """Create a table with topic information."""
    table = Table(title="Topics")
    table.add_column("Topic", style="cyan", no_wrap=True)
    table.add_column("Messages", justify="right", style="green")
    table.add_column("Frequency", justify="right", style="yellow")

    stats = reader.get_statistics()
    duration = stats.message_end_time - stats.message_start_time
    topics = reader.get_channels()

    for channel_id, channel in topics.items():
        message_count = stats.channel_message_counts[channel_id]
        frequency = format_frequency(message_count, duration)
        table.add_row(channel.topic, f"{message_count:,}", frequency)

    return table


def create_schemas_table(reader: BaseMcapRecordReader) -> Table:
    """Create a table with schema information."""
    table = Table(title="Schemas")
    table.add_column("ID", justify="right", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Encoding", style="yellow")
    table.add_column("Size", justify="right", style="magenta")

    schemas = reader.get_schemas()

    for schema_id, schema in schemas.items():
        table.add_row(
            str(schema_id),
            schema.name,
            schema.encoding,
            f"{len(schema.data)} bytes"
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

        # Create layout
        layout = Layout()
        layout.split_column(
            Layout(name="top", size=6),
            Layout(name="middle", size=8),
            Layout(name="bottom")
        )

        # Split top row for file info and summary
        layout["top"].split_row(
            Layout(create_file_info_panel(reader, file_path), name="file_info"),
            Layout(create_summary_panel(reader), name="summary")
        )

        # Split bottom for topics and schemas
        layout["bottom"].split_row(
            Layout(create_topics_table(reader), name="topics"),
            Layout(create_schemas_table(reader), name="schemas")
        )

        console.print(layout)

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
