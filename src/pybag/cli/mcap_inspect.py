"""MCAP inspect CLI command."""

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


def _format_bytes(size_bytes: int) -> str:
    """Format byte size in a human-readable way."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def _print_general_summary(reader) -> None:
    """Print general MCAP structure summary."""
    header = reader.get_header()
    footer = reader.get_footer()
    stats = reader.get_statistics()
    channels = reader.get_channels()
    schemas = reader.get_schemas()

    print("MCAP File Structure")
    print("=" * 80)
    print()

    # Header information
    print("Header:")
    print(f"  Profile:        {header.profile}")
    print(f"  Library:        {header.library}")
    print()

    # Record counts
    print("Record Counts:")
    print(f"  Messages:       {stats.message_count:,}")
    print(f"  Channels:       {stats.channel_count}")
    print(f"  Schemas:        {stats.schema_count}")
    print(f"  Chunks:         {stats.chunk_count}")
    print(f"  Attachments:    {stats.attachment_count}")
    print(f"  Metadata:       {stats.metadata_count}")
    print()

    # Time information
    if stats.message_count > 0:
        duration_ns = stats.message_end_time - stats.message_start_time
        print("Time Range:")
        print(f"  Start time:     {_ns_to_seconds(stats.message_start_time):.6f} s")
        print(f"  End time:       {_ns_to_seconds(stats.message_end_time):.6f} s")
        print(f"  Duration:       {_format_duration(duration_ns)}")
        print()

    # Footer/Summary information
    print("Summary Section:")
    has_summary = footer.summary_start > 0
    print(f"  Present:        {'Yes' if has_summary else 'No'}")
    if has_summary:
        print(f"  Start offset:   {footer.summary_start:,}")
        print(f"  Offset start:   {footer.summary_offset_start:,}")
        print(f"  CRC:            0x{footer.summary_crc:08x}")
    print()


def _print_chunk_details(reader) -> None:
    """Print detailed chunk information."""
    print("Chunk Details")
    print("=" * 80)
    print()

    chunk_indexes = reader.get_chunk_indexes()

    if not chunk_indexes:
        print("  No chunks found in this MCAP file.")
        print()
        return

    # Print header
    print(f"  {'Chunk':<8}  {'Start Time':<20}  {'End Time':<20}  {'Duration':<12}  {'Compression':<12}  {'Size (Compressed)':<20}  {'Size (Uncompressed)':<20}  {'Ratio':<8}  {'Msg Indexes':<12}")
    print(f"  {'-' * 8}  {'-' * 20}  {'-' * 20}  {'-' * 12}  {'-' * 12}  {'-' * 20}  {'-' * 20}  {'-' * 8}  {'-' * 12}")

    for idx, chunk_index in enumerate(chunk_indexes, 1):
        start_time = _ns_to_seconds(chunk_index.message_start_time)
        end_time = _ns_to_seconds(chunk_index.message_end_time)
        duration = chunk_index.message_end_time - chunk_index.message_start_time
        compression = chunk_index.compression if chunk_index.compression else "none"
        compressed_size = chunk_index.compressed_size
        uncompressed_size = chunk_index.uncompressed_size
        compression_ratio = compressed_size / uncompressed_size if uncompressed_size > 0 else 0
        num_msg_indexes = len(chunk_index.message_index_offsets)

        print(f"  {idx:<8}  {start_time:<20.6f}  {end_time:<20.6f}  {_format_duration(duration):<12}  {compression:<12}  {_format_bytes(compressed_size):<20}  {_format_bytes(uncompressed_size):<20}  {compression_ratio:<8.2f}  {num_msg_indexes:<12}")

    print()

    # Summary statistics
    total_compressed = sum(ci.compressed_size for ci in chunk_indexes)
    total_uncompressed = sum(ci.uncompressed_size for ci in chunk_indexes)
    overall_ratio = total_compressed / total_uncompressed if total_uncompressed > 0 else 0

    print("Chunk Summary:")
    print(f"  Total chunks:           {len(chunk_indexes)}")
    print(f"  Total compressed size:  {_format_bytes(total_compressed)}")
    print(f"  Total uncompressed size: {_format_bytes(total_uncompressed)}")
    print(f"  Overall compression:     {overall_ratio:.2f}x")
    print()


def _print_summary_section_details(reader) -> None:
    """Print detailed summary section information."""
    print("Summary Section Details")
    print("=" * 80)
    print()

    footer = reader.get_footer()

    if footer.summary_start == 0:
        print("  No summary section present in this MCAP file.")
        print()
        return

    schemas = reader.get_schemas()
    channels = reader.get_channels()
    chunk_indexes = reader.get_chunk_indexes()

    print("Summary Contents:")
    print(f"  Summary start offset:   {footer.summary_start:,}")
    print(f"  Summary offset start:   {footer.summary_offset_start:,}")
    print(f"  Summary CRC:            0x{footer.summary_crc:08x}")
    print()

    # Schema records in summary
    if schemas:
        print(f"Schemas ({len(schemas)}):")
        print(f"  {'ID':<8}  {'Name':<40}  {'Encoding':<20}  {'Size':<15}")
        print(f"  {'-' * 8}  {'-' * 40}  {'-' * 20}  {'-' * 15}")

        for schema_id, schema in sorted(schemas.items()):
            name = schema.name[:40] if len(schema.name) <= 40 else schema.name[:37] + "..."
            encoding = schema.encoding[:20] if len(schema.encoding) <= 20 else schema.encoding[:17] + "..."
            size = _format_bytes(len(schema.data))
            print(f"  {schema_id:<8}  {name:<40}  {encoding:<20}  {size:<15}")
        print()

    # Channel records in summary
    if channels:
        print(f"Channels ({len(channels)}):")
        print(f"  {'ID':<8}  {'Schema ID':<10}  {'Topic':<50}  {'Message Encoding':<20}")
        print(f"  {'-' * 8}  {'-' * 10}  {'-' * 50}  {'-' * 20}")

        for channel_id, channel in sorted(channels.items()):
            topic = channel.topic[:50] if len(channel.topic) <= 50 else channel.topic[:47] + "..."
            msg_encoding = channel.message_encoding[:20] if len(channel.message_encoding) <= 20 else channel.message_encoding[:17] + "..."
            print(f"  {channel_id:<8}  {channel.schema_id:<10}  {topic:<50}  {msg_encoding:<20}")
        print()

    # Chunk indexes in summary
    if chunk_indexes:
        print(f"Chunk Indexes ({len(chunk_indexes)}):")
        print(f"  Total chunk indexes: {len(chunk_indexes)}")
        print(f"  (Use --chunks for detailed chunk information)")
        print()


def inspect_mcap(
    input_path: str | Path,
    *,
    show_chunks: bool = False,
    show_summary: bool = False,
) -> None:
    """Inspect the structure of an MCAP file.

    Args:
        input_path: Path to the MCAP file
        show_chunks: Whether to show detailed chunk information
        show_summary: Whether to show summary section details
    """
    input_path = Path(input_path).resolve()

    with McapRecordReaderFactory.from_file(input_path) as reader:
        # Always show general summary
        _print_general_summary(reader)

        # Show chunk details if requested
        if show_chunks:
            _print_chunk_details(reader)

        # Show summary section details if requested
        if show_summary:
            _print_summary_section_details(reader)


def add_parser(subparsers) -> None:
    """Add the inspect command parser."""
    parser = subparsers.add_parser(
        "inspect",
        help="Inspect the structure of an MCAP file.",
        description=dedent("""
            Inspect the structure and records of an MCAP file.

            By default, shows a high-level summary of the MCAP file structure
            including record counts and time ranges.

            Optional flags allow inspection of specific record types:
            - --chunks: Show detailed information about each chunk
            - --summary: Show detailed information about the summary section
            - --all: Show all available details
        """)
    )
    parser.add_argument("input", help="Path to MCAP file (*.mcap)")
    parser.add_argument(
        "--chunks",
        action="store_true",
        help="Show detailed chunk information"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show detailed summary section information"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all available details (equivalent to --chunks --summary)"
    )
    parser.set_defaults(func=lambda args: inspect_mcap(
        args.input,
        show_chunks=args.chunks or args.all,
        show_summary=args.summary or args.all,
    ))
