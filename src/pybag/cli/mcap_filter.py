"""MCAP filtering CLI command."""

import argparse
import logging
from pathlib import Path
from textwrap import dedent
from typing import Iterable

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter

logger = logging.getLogger(__name__)


def _to_ns(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return int(seconds * 1_000_000_000)


def filter_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    include_topics: list[str] | None = None,
    exclude_topics: list[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
    *,
    overwrite: bool = False
) -> Path:
    """Filter an MCAP file based on topics and time."""
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_filtered.mcap")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output mcap exists. Please set `overwrite` to True.')

    start_ns, end_ns = _to_ns(start_time), _to_ns(end_time)

    with (
        McapFileReader.from_file(input_path) as reader,
        McapFileWriter.open(
                output_path,
                profile=reader.profile,
                chunk_size=None,
                chunk_compression=None,
            ) as writer
    ):
        # If no topics specified, default to all topics
        if include_topics is None:
            topics_to_filter = set(reader.get_topics())
        else:
            topics_to_filter = set(include_topics)

        # Remove excluded topics
        if exclude_topics:
            topics_to_filter -= set(exclude_topics)

        for msg in reader.messages(
            topic=list(topics_to_filter),
            start_time=start_ns,
            end_time=end_ns,
            in_log_time_order=False
        ):
            writer.write_message(
                topic=msg.topic,
                timestamp=msg.log_time,
                message=msg.data,
            )
    return output_path


def _filter_mcap_from_args(args: argparse.Namespace) -> None:
    filter_mcap(
        args.input,
        output_path=args.output,
        include_topics=args.include_topic,
        exclude_topics=args.exclude_topic,
        start_time=args.start_time,
        end_time=args.end_time,
    )


def add_parser(subparsers) -> None:
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
