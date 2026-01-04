"""MCAP and bag file info CLI command."""

from pathlib import Path
from textwrap import dedent

from pybag.bag_reader import BagFileReader
from pybag.cli.utils import get_file_format
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


def info_bag(input_path: str | Path) -> None:
    """Display information about a ROS 1 bag file."""
    input_path = Path(input_path).resolve()

    with BagFileReader.from_file(input_path) as reader:
        # Get connections and topics
        connections = reader.get_connections()
        topics = reader.get_topics()

        # Get time information
        start_time = reader.start_time
        end_time = reader.end_time
        duration_ns = end_time - start_time

        # Calculate total message count
        total_messages = sum(reader.get_message_count(topic) for topic in topics)

        # Print file information
        print(f"File: {input_path.name}")
        print(f"Path: {input_path}")
        print()

        # Print time information
        print("Time Information:")
        print(f"  Start time:  {_ns_to_seconds(start_time):.6f} s")
        print(f"  End time:    {_ns_to_seconds(end_time):.6f} s")
        print(f"  Duration:    {_format_duration(duration_ns)}")
        print()

        # Print general statistics
        print("Statistics:")
        print(f"  Messages:       {total_messages:,}")
        print(f"  Topics:         {len(topics)}")
        print(f"  Connections:    {len(connections)}")
        print()

        # Print topic information
        if not topics:
            print("No topics found.")
        else:
            print("Topics:")

            # Build a mapping from topic to connection for message type lookup
            # Use first connection for each topic (they should have the same type)
            topic_to_conn = {}
            for conn in connections:
                if conn.topic not in topic_to_conn:
                    topic_to_conn[conn.topic] = conn

            # Build a list of topic info (iterate over unique topics, not connections)
            topic_info = []
            for topic in topics:
                msg_count = reader.get_message_count(topic)

                # Calculate frequency (messages per second)
                frequency = msg_count / _ns_to_seconds(duration_ns) if duration_ns > 0 else 0

                # Get message type from connection header
                conn = topic_to_conn[topic]
                conn_header = conn.connection_header
                msg_type = conn_header.type

                topic_info.append({
                    'topic': topic,
                    'messages': msg_count,
                    'frequency': frequency,
                    'msg_type': msg_type,
                })

            # Sort by topic name
            topic_info.sort(key=lambda x: x['topic'])

            # Find max widths for alignment
            max_topic_len = max(len(t['topic']) for t in topic_info)
            max_msgs_len = max(len(f"{t['messages']:,}") for t in topic_info)

            # Print header
            print(f"  {'Topic':<{max_topic_len}}  {'Messages':>{max_msgs_len}}  {'Freq (Hz)':>10}  Type")
            print(f"  {'-' * max_topic_len}  {'-' * max_msgs_len}  {'-' * 10}  {'-' * 20}")

            # Print each topic
            for info in topic_info:
                topic = info['topic']
                messages = f"{info['messages']:,}"
                freq = f"{info['frequency']:.2f}" if info['frequency'] < 1000 else f"{info['frequency']:.1f}"
                msg_type = info['msg_type']

                print(f"  {topic:<{max_topic_len}}  {messages:>{max_msgs_len}}  {freq:>10}  {msg_type}")


def _run_info(args) -> None:
    """Run the info command based on file format."""
    input_path = Path(args.input).resolve()
    file_format = get_file_format(input_path)

    if file_format == 'mcap':
        info_mcap(input_path)
    else:
        info_bag(input_path)


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "info",
        help="Display information about an MCAP or bag file.",
        description=dedent("""
            Display statistics and information about an MCAP or ROS 1 bag file including:
            - File name and path
            - Start/end time and duration
            - Message and topic/channel counts
            - Per-topic statistics (message count, frequency, message type)

            For MCAP files, additional information is displayed:
            - Profile and library
            - Schema counts, chunks, attachments, and metadata
            - Message encoding and schema encoding
        """)
    )
    parser.add_argument("input", help="Path to MCAP file (*.mcap) or ROS 1 bag file (*.bag)")
    parser.set_defaults(func=_run_info)
