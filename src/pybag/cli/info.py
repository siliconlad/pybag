import argparse
from pathlib import Path

from rich.console import Console
from rich.table import Table

from pybag.mcap.record_reader import McapRecordReaderFactory


def _run(args: argparse.Namespace) -> None:
    reader = McapRecordReaderFactory.from_file(args.mcap_file_path)
    try:
        stats = reader.get_statistics()
        channels = reader.get_channels()
    finally:
        reader.close()

    console = Console()

    stats_table = Table(title="MCAP Statistics")
    stats_table.add_column("Field")
    stats_table.add_column("Value")
    stats_table.add_row("message_count", str(stats.message_count))
    stats_table.add_row("schema_count", str(stats.schema_count))
    stats_table.add_row("channel_count", str(stats.channel_count))
    stats_table.add_row("attachment_count", str(stats.attachment_count))
    stats_table.add_row("metadata_count", str(stats.metadata_count))
    stats_table.add_row("chunk_count", str(stats.chunk_count))
    stats_table.add_row("message_start_time", str(stats.message_start_time))
    stats_table.add_row("message_end_time", str(stats.message_end_time))
    console.print(stats_table)

    channel_table = Table(title="Channel Message Counts")
    channel_table.add_column("Channel ID")
    channel_table.add_column("Topic")
    channel_table.add_column("Count")
    for channel_id, count in stats.channel_message_counts.items():
        topic = channels.get(channel_id).topic if channel_id in channels else ""
        channel_table.add_row(str(channel_id), topic, str(count))
    if stats.channel_message_counts:
        console.print(channel_table)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("info", help="Show statistics for an MCAP file")
    parser.add_argument("mcap_file_path", type=Path, help="Path to MCAP file")
    parser.set_defaults(func=_run)
