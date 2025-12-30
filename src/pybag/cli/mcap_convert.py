"""CLI command for converting between bag and mcap formats."""

import argparse
import logging
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.translate import (
    translate_ros1_to_ros2,
    translate_ros2_to_ros1,
    translate_schema_ros1_to_ros2,
    translate_schema_ros2_to_ros1
)
from pybag.types import SchemaText

logger = logging.getLogger(__name__)


def _detect_format(file_path: Path) -> Literal["bag", "mcap"] | None:
    """Detect the format of a file based on its extension.

    Args:
        file_path: Path to the file.

    Returns:
        The detected format or None if unknown.
    """
    suffix = file_path.suffix.lower()
    if suffix == ".bag":
        return "bag"
    elif suffix == ".mcap":
        return "mcap"
    return None


def convert(
    input_path: str | Path,
    output_path: str | Path,
    # Common options
    chunk_size: int | None = None,
    # MCAP output options
    mcap_compression: Literal["none", "lz4", "zstd"] = "lz4",
    mcap_profile: Literal["ros1", "ros2"] = "ros2",
    # Bag output options
    bag_compression: Literal["none", "bz2"] = "none",
    *,
    overwrite: bool = False,
) -> Path:
    """Convert a bag file to mcap or vice versa.

    This function converts between ROS 1 bag files and MCAP files.
    Messages are streamed through the conversion to minimize memory usage.

    Note: When converting MCAP to bag, attachments and metadata records are lost
    as the bag format does not support these features.

    Args:
        input_path: Path to the input file (bag or mcap).
        output_path: Path to the output file.
        chunk_size: Chunk size for MCAP output in bytes. If None, uses default chunking.
        mcap_compression: Compression for MCAP chunks ("lz4", "zstd", or None).
        mcap_profile: MCAP profile to use ("ros1" or "ros2").
        bag_compression: Compression for bag chunks ("none" or "bz2").
        overwrite: Whether to overwrite the output file if it exists.

    Returns:
        Path to the output file.

    Raises:
        ValueError: If input/output formats are the same, paths are the same,
                   or output file exists without overwrite flag.
        FileNotFoundError: If input file doesn't exist.
    """
    input_path = Path(input_path).resolve()
    output_path = Path(output_path).resolve()

    # Validate input file exists
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Validate paths are different
    if input_path == output_path:
        raise ValueError("Input and output paths cannot be the same.")

    # Detect input format
    input_format = _detect_format(input_path)
    if input_format is None:
        raise ValueError(
            f"Cannot detect input format from extension: {input_path.suffix}. "
            "Expected .bag or .mcap"
        )

    # Detect or validate output format
    output_format = _detect_format(output_path)
    if output_format is None:
        raise ValueError(
            f"Cannot detect output format from extension: {output_path.suffix}. "
            "Expected .bag or .mcap, or specify --output-format explicitly."
        )

    # Validate formats are different
    if input_format == output_format:
        raise ValueError(
            f"Input and output formats are the same ({input_format}). "
            "Use a different output format or file extension."
        )

    # Check if output exists
    if not overwrite and output_path.exists():
        raise ValueError(
            f"Output file exists: {output_path}. Use --overwrite to replace it."
        )

    # Perform conversion
    if input_format == "bag" and output_format == "mcap":
        return convert_bag_to_mcap(
            input_path,
            output_path,
            chunk_size=chunk_size,
            chunk_compression=mcap_compression,
            profile=mcap_profile,
        )
    else:  # mcap to bag
        return convert_mcap_to_bag(
            input_path,
            output_path,
            chunk_size=chunk_size,
            chunk_compression=bag_compression,
        )


def convert_bag_to_mcap(
    input_path: Path,
    output_path: Path,
    *,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] = "lz4",
    profile: Literal["ros1", "ros2"] = "ros2",
) -> Path:
    """Convert a bag file to mcap format.

    Args:
        input_path: Path to the input bag file.
        output_path: Path to the output mcap file.
        chunk_size: Chunk size in bytes. If None, uses default chunking.
        chunk_compression: Compression algorithm for chunks.
        profile: MCAP profile to use. "ros1" keeps messages in ROS1 format,
                "ros2" translates messages to ROS2/CDR format.

    Returns:
        Path to the output file.
    """
    logger.info(f"Converting bag to mcap (profile={profile}): {input_path} -> {output_path}")

    with BagFileReader.from_file(input_path) as reader:
        topics = reader.get_topics()
        if not topics:
            logger.warning("No topics found in bag file, creating empty mcap")

        with McapFileWriter.open(
            output_path,
            profile=profile,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
        ) as writer:
            # Pre-register all channels with schemas
            for conn in reader.get_connections():
                conn_header = conn.connection_header
                if profile == "ros1":
                    # Keep schema in ROS1 format
                    schema = SchemaText(
                        name=conn_header.type,
                        text=conn_header.message_definition,
                    )
                else:
                    # Translate schema to ROS2 format
                    schema = translate_schema_ros1_to_ros2(
                        conn_header.type,
                        conn_header.message_definition,
                    )
                writer.add_channel(conn.topic, schema=schema)

            message_count = 0
            for msg in reader.messages(topics, in_log_time_order=False):
                if profile == "ros1":
                    # Keep message in ROS1 format
                    message = msg.data
                else:
                    # Translate message to ROS2 format
                    message = translate_ros1_to_ros2(msg.data)
                writer.write_message(
                    topic=msg.topic,
                    timestamp=msg.log_time,
                    message=message,
                    publish_time=msg.log_time,
                )
                message_count += 1
            logger.info(f"Converted {message_count} messages to mcap")

    return output_path


def convert_mcap_to_bag(
    input_path: Path,
    output_path: Path,
    *,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "bz2"] = "none",
) -> Path:
    """Convert an mcap file to bag format.

    Note: Attachments and metadata records are lost in this conversion
    as the bag format does not support these features.

    Args:
        input_path: Path to the input mcap file.
        output_path: Path to the output bag file.
        chunk_size: Chunk size in bytes.
        chunk_compression: Compression algorithm for bag chunks.

    Returns:
        Path to the output file.
    """
    logger.info(f"Converting mcap to bag: {input_path} -> {output_path}")

    with McapFileReader.from_file(input_path) as reader:
        # Detect the profile from the MCAP file
        profile = reader.profile
        logger.info(f"Detected MCAP profile: {profile}")

        topics = reader.get_topics()
        logger.info(f"Found {len(topics)} topics in mcap file")

        # Warn about data loss
        if attachments := reader.get_attachments():
            logger.warning(
                f"MCAP contains {len(attachments)} attachment(s) which will be lost "
                "in conversion to bag format."
            )
        if metadata := reader.get_metadata():
            logger.warning(
                f"MCAP contains {len(metadata)} metadata record(s) which will be lost "
                "in conversion to bag format."
            )

        if not topics:
            logger.warning("No topics found in mcap file, creating empty bag")

        with BagFileWriter.open(
            output_path,
            compression=chunk_compression,
            chunk_size=chunk_size,
        ) as writer:
            # Pre-register all connections with schemas
            for channel in reader.get_channels():
                schema_record = reader.get_schema(channel.topic)
                if schema_record is None:
                    logger.warning(f"No schema found for channel {channel.topic}")
                    continue
                if profile == "ros1":
                    # Schema is already in ROS1 format
                    schema = SchemaText(
                        name=schema_record.name,
                        text=schema_record.data.decode('utf-8'),
                    )
                else:
                    # Translate schema from ROS2 to ROS1 format
                    schema = translate_schema_ros2_to_ros1(
                        schema_record.name,
                        schema_record.data.decode('utf-8'),
                    )
                writer.add_connection(channel.topic, schema=schema)

            message_count = 0
            for msg in reader.messages(topics, in_log_time_order=False):
                if profile == "ros1":
                    # Message is already in ROS1 format
                    message = msg.data
                else:
                    # Translate message from ROS2 to ROS1 format
                    message = translate_ros2_to_ros1(msg.data)
                writer.write_message(
                    topic=msg.topic,
                    timestamp=msg.log_time,
                    message=message,
                )
                message_count += 1
            logger.info(f"Converted {message_count} messages to bag")

    return output_path


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    """Add the convert command parser to the subparsers."""
    parser = subparsers.add_parser(
        "convert",
        help="Convert between bag and mcap formats.",
        description=dedent("""
            Convert ROS 1 bag files to MCAP format or vice versa.

            When converting bag to mcap, you can choose the MCAP profile:
            - ros1: Keeps messages in ROS1 serialization format (rosmsg encoding)
            - ros2: Translates messages to ROS2/CDR format (default)

            When converting mcap to bag, the profile is auto-detected and messages
            are translated if needed.

            The input and output formats are detected from file extensions (.bag, .mcap).

            Note: When converting MCAP to bag, attachments and metadata records
            are lost as the bag format does not support these features.
        """),
    )

    # Required arguments
    parser.add_argument(
        "input",
        help="Path to the input file (*.bag or *.mcap).",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path to the output file.",
    )

    # Common output options
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size in bytes.",
    )

    # MCAP output options
    mcap_group = parser.add_argument_group("MCAP output options")
    mcap_group.add_argument(
        "--mcap-compression",
        choices=["lz4", "zstd", "none"],
        default="lz4",
        help="Compression for MCAP chunks (default: lz4). Use 'none' for no compression.",
    )
    mcap_group.add_argument(
        "--profile",
        choices=["ros1", "ros2"],
        default="ros2",
        help="MCAP profile for bag->mcap (default: ros2). Ignored for mcap->bag (auto-detected).",
    )

    # Bag output options
    bag_group = parser.add_argument_group("Bag output options")
    bag_group.add_argument(
        "--bag-compression",
        choices=["none", "bz2"],
        default="none",
        help="Compression for bag chunks (default: none).",
    )

    # Common options
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists.",
    )

    parser.set_defaults(func=lambda args: convert(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
            mcap_compression=args.mcap_compression,
            mcap_profile=args.profile,
            bag_compression=args.bag_compression,
            overwrite=args.overwrite,
        )
    )
