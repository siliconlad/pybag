"""MCAP filtering CLI command."""

from __future__ import annotations

import argparse
from pathlib import Path
from textwrap import dedent
from typing import Iterable

from pybag import __version__
from pybag.io.raw_writer import CrcWriter, FileWriter
from pybag.mcap.record_writer import McapRecordWriter
from pybag.mcap.records import DataEndRecord, FooterRecord, HeaderRecord
from pybag.mcap_reader import McapFileReader


def _to_ns(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return int(seconds * 1_000_000_000)


def filter_mcap(
    input_path: str | Path,
    *,
    output_path: str | Path | None = None,
    include_topics: Iterable[str] | None = None,
    exclude_topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> None:
    """Filter an MCAP file based on topics and time."""
    input_path = Path(input_path)
    output_path = (
        Path(output_path)
        if output_path is not None
        else input_path.with_name(input_path.stem + "_filtered.mcap")
    )

    include_topics = set(include_topics or [])
    exclude_topics = set(exclude_topics or [])

    start_ns = _to_ns(start_time)
    end_ns = _to_ns(end_time)

    with McapFileReader.from_file(input_path) as reader:
        writer = CrcWriter(FileWriter(output_path))
        try:
            header = reader._reader.get_header()
            McapRecordWriter.write_magic_bytes(writer)
            McapRecordWriter.write_header(
                writer,
                HeaderRecord(profile=header.profile, library=f"pybag {__version__}"),
            )

            channels = reader._reader.get_channels()
            schemas = reader._reader.get_schemas()
            written_schemas: set[int] = set()

            for cid, channel in channels.items():
                topic = channel.topic
                if include_topics and topic not in include_topics:
                    continue
                if topic in exclude_topics:
                    continue

                schema_id = channel.schema_id
                if schema_id not in written_schemas:
                    McapRecordWriter.write_schema(writer, schemas[schema_id])
                    written_schemas.add(schema_id)
                McapRecordWriter.write_channel(writer, channel)

                for message in reader._reader.get_messages(cid, start_ns, end_ns):
                    McapRecordWriter.write_message(writer, message)

            McapRecordWriter.write_data_end(
                writer, DataEndRecord(data_section_crc=writer.get_crc())
            )
            McapRecordWriter.write_footer(
                writer, FooterRecord(summary_start=0, summary_offset_start=0, summary_crc=0)
            )
            McapRecordWriter.write_magic_bytes(writer)
        finally:
            writer.close()


def _filter_mcap_from_args(args: argparse.Namespace) -> None:
    filter_mcap(
        args.input,
        output_path=args.output,
        include_topics=args.include_topic,
        exclude_topics=args.exclude_topic,
        start_time=args.start_time,
        end_time=args.end_time,
    )


def add_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
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
