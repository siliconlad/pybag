import argparse
from pathlib import Path

from rich.console import Console
from rich.table import Table

from pybag.mcap.record_reader import McapRecordReaderFactory


def _display_info(mcap_file: Path) -> None:
    reader = McapRecordReaderFactory.from_file(mcap_file)
    stats = reader.get_statistics()

    console = Console()

    # Display MCAP Statistics
    console.print('\n[bold green]MCAP Info[/bold green]')
    time_table = Table(show_header=False)
    time_table.add_row('length', f'{stats.message_end_time - stats.message_start_time}')
    time_table.add_row('message_start_time', f'{stats.message_start_time}')
    time_table.add_row('message_end_time', f'{stats.message_end_time}')
    console.print(time_table)

    # Display statistics with aligned formatting
    console.print('\n[bold green]Record Counts[/bold green]')
    stats_table = Table(show_header=False)
    stats_table.add_row('message_count', f'{stats.message_count}')
    stats_table.add_row('schema_count', f'{stats.schema_count}')
    stats_table.add_row('channel_count', f'{stats.channel_count}')
    stats_table.add_row('attachment_count', f'{stats.attachment_count}')
    stats_table.add_row('metadata_count', f'{stats.metadata_count}')
    stats_table.add_row('chunk_count', f'{stats.chunk_count}')
    console.print(stats_table)

    # Display Channel Message Counts
    if stats.channel_message_counts:
        channels = reader.get_channels()
        schemas = reader.get_schemas()
        console.print('\n[bold green]Message Counts by Channel[/bold green]')
        channel_table = Table(show_header=False)
        for channel_id, count in stats.channel_message_counts.items():
            topic = channels[channel_id].topic if channel_id in channels else ""
            schema = schemas[channels[channel_id].schema_id].name if channel_id in channels else ""
            channel_table.add_row(topic, schema, str(count))
        console.print(channel_table)


def _run_info(args: argparse.Namespace) -> None:
    mcap_file = args.mcap_file_path
    _display_info(mcap_file)


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("info", help="Show statistics for an MCAP file")
    parser.add_argument("mcap_file_path", type=Path, help="Path to MCAP file")
    parser.set_defaults(func=_run_info)
