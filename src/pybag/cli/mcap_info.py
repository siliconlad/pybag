"""MCAP info CLI command."""

from pathlib import Path
from textwrap import dedent

from pybag.mcap.record_reader import McapRecordReaderFactory


def _ns_to_seconds(ns: int) -> float:
    """Convert nanoseconds to seconds."""
    return ns / 1_000_000_000


def _format_duration(duration_ns: int) -> str:
    """Format duration in a human-readable way."""
    duration_s = _ns_to_seconds(duration_ns)

    if duration_s < 1:
        return f"{duration_s * 1000:.2f} ms"
    elif duration_s < 60:
        return f"{duration_s:.2f} s"
    elif duration_s < 3600:
        minutes = int(duration_s // 60)
        seconds = duration_s % 60
        return f"{minutes}m {seconds:.2f}s"
    else:
        hours = int(duration_s // 3600)
        minutes = int((duration_s % 3600) // 60)
        seconds = duration_s % 60
        return f"{hours}h {minutes}m {seconds:.2f}s"


def info_mcap(input_path: str | Path) -> None:
    """Display information about an MCAP file."""
    input_path = Path(input_path).resolve()

    with McapRecordReaderFactory.from_file(input_path) as reader:
        # Get header information
        header = reader.get_header()

        # Get statistics
        stats = reader.get_statistics()

        # Get channels and schemas
        channels = reader.get_channels()
        schemas = reader.get_schemas()

        # Calculate duration
        duration_ns = stats.message_end_time - stats.message_start_time

        # Print file information
        print(f"File: {input_path.name}")
        print(f"Path: {input_path}")
        print()

        # Print time information
        print("Time Information:")
        print(f"  Start time:  {_ns_to_seconds(stats.message_start_time):.6f} s")
        print(f"  End time:    {_ns_to_seconds(stats.message_end_time):.6f} s")
        print(f"  Duration:    {_format_duration(duration_ns)}")
        print()

        # Print general statistics
        print("Statistics:")
        print(f"  Profile:        {header.profile}")
        print(f"  Library:        {header.library}")
        print(f"  Messages:       {stats.message_count:,}")
        print(f"  Channels:       {stats.channel_count}")
        print(f"  Schemas:        {stats.schema_count}")
        print(f"  Chunks:         {stats.chunk_count}")
        print(f"  Attachments:    {stats.attachment_count}")
        print(f"  Metadata:       {stats.metadata_count}")
        print()

        # Print topic information
        if channels:
            print("Topics:")

            # Build a list of topic info
            topic_info = []
            for channel_id, channel in channels.items():
                msg_count = stats.channel_message_counts.get(channel_id, 0)

                # Calculate frequency (messages per second)
                frequency = msg_count / _ns_to_seconds(duration_ns) if duration_ns > 0 else 0

                # Get schema encoding if available
                schema_encoding = "N/A"
                if channel.schema_id != 0 and channel.schema_id in schemas:
                    schema = schemas[channel.schema_id]
                    schema_encoding = schema.encoding

                topic_info.append({
                    'topic': channel.topic,
                    'messages': msg_count,
                    'encoding': channel.message_encoding,
                    'schema_encoding': schema_encoding,
                    'frequency': frequency,
                    'schema_name': schemas[channel.schema_id].name if channel.schema_id != 0 and channel.schema_id in schemas else "N/A"
                })

            # Sort by topic name
            topic_info.sort(key=lambda x: x['topic'])

            # Find max widths for alignment
            max_topic_len = max(len(t['topic']) for t in topic_info)
            max_msgs_len = max(len(f"{t['messages']:,}") for t in topic_info)

            # Print header
            print(f"  {'Topic':<{max_topic_len}}  {'Messages':>{max_msgs_len}}  {'Freq (Hz)':>10}  {'Encoding':<15}  Schema")
            print(f"  {'-' * max_topic_len}  {'-' * max_msgs_len}  {'-' * 10}  {'-' * 15}  {'-' * 20}")

            # Print each topic
            for info in topic_info:
                topic = info['topic']
                messages = f"{info['messages']:,}"
                freq = f"{info['frequency']:.2f}" if info['frequency'] < 1000 else f"{info['frequency']:.1f}"
                encoding = info['encoding']
                schema = info['schema_name']

                print(f"  {topic:<{max_topic_len}}  {messages:>{max_msgs_len}}  {freq:>10}  {encoding:<15}  {schema}")


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "info",
        help="Display information about an MCAP file.",
        description=dedent("""
            Display statistics and information about an MCAP file including:
            - File name and path
            - Start/end time and duration
            - Profile and library
            - Message, channel, and schema counts
            - Per-topic statistics (message count, encoding, frequency)
        """)
    )
    parser.add_argument("input", help="Path to MCAP file (*.mcap)")
    parser.set_defaults(func=lambda args: info_mcap(args.input))
