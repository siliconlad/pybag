"""MCAP and ROS bag recovery CLI command."""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.bag.record_parser import BagRecordParser
from pybag.bag.records import BagRecordType, ChunkRecord
from pybag.bag.records import ConnectionRecord as BagConnectionRecord
from pybag.bag.records import MessageDataRecord
from pybag.bag_writer import BagFileWriter
from pybag.cli.utils import (
    get_file_format_from_magic,
    validate_compression_for_bag,
    validate_compression_for_mcap
)
from pybag.io.raw_reader import BytesReader, FileReader
from pybag.io.raw_writer import FileWriter
from pybag.mcap.chunk import decompress_chunk
from pybag.mcap.record_parser import McapRecordParser, McapRecordType
from pybag.mcap.record_writer import (
    BaseMcapRecordWriter,
    McapRecordWriterFactory
)
from pybag.mcap.records import ChannelRecord, SchemaRecord
from pybag.mcap.summary import McapSummaryFactory

logger = logging.getLogger(__name__)


def _process_bag_chunk_records(
    chunk_data: bytes,
    connections: dict[int, BagConnectionRecord],
    writer: BagFileWriter,
    *,
    verbose: bool = False
) -> tuple[int, dict[int, BagConnectionRecord]]:
    """Process records inside a decompressed bag chunk.

    Args:
        chunk_data: The decompressed chunk data.
        connections: Dictionary to store connection records (conn_id -> record).
        writer: The bag writer to write records to.
        verbose: Whether to log verbose progress.

    Returns:
        Number of messages recovered, and updated connections dict.
    """
    messages_recovered = 0
    chunk_reader = BytesReader(chunk_data)

    try:
        while chunk_reader.tell() < chunk_reader.size():
            result = BagRecordParser.parse_record(chunk_reader)
            if result is None:
                break

            record_type, record = result

            if record_type == BagRecordType.CONNECTION:
                conn_record: BagConnectionRecord = record
                if conn_record.conn not in connections:
                    connections[conn_record.conn] = conn_record
                    # Register connection with writer using the record directly
                    writer.add_connection_record(conn_record)

            elif record_type == BagRecordType.MSG_DATA:
                msg_record: MessageDataRecord = record
                # Ensure connection exists before writing message
                if msg_record.conn in connections:
                    # Write message record directly (no re-serialization)
                    writer.write_message_record(msg_record)
                    messages_recovered += 1
                elif verbose:
                    logger.warning(
                        f"Skipping message with unknown conn_id {msg_record.conn}"
                    )

            # Skip other record types (INDEX_DATA can appear in chunks)

    except Exception as e:
        if verbose:
            logger.warning(f"Error reading chunk records: {e}")
        # Stop parsing this chunk but return what we recovered

    return messages_recovered, connections


def recover_bag(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    compression: Literal["none", "bz2"] | None = None,
    *,
    overwrite: bool = False,
    verbose: bool = False
) -> Path:
    """Recover data from a potentially corrupted ROS bag file.

    This command reads records one at a time from the input bag and writes them
    to a new output bag. If corruption is encountered, it stops and saves all
    successfully recovered data up to that point.

    Args:
        input_path: Path to the potentially corrupted bag file.
        output_path: Path for the recovered output file.
        chunk_size: Chunk size for the output file in bytes.
        compression: Compression algorithm for chunks ('none' or 'bz2').
        overwrite: Whether to overwrite existing output file.
        verbose: Whether to print detailed recovery progress.

    Returns:
        Path to the recovered bag file.
    """
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_recovered.bag")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output bag exists. Please set `overwrite` to True.')

    messages_recovered = 0
    connections: dict[int, BagConnectionRecord] = {}

    with FileReader(input_path) as reader:
        # Parse version string
        version = BagRecordParser.parse_version(reader)
        if verbose:
            logger.info(f"Bag version: {version}")

        # Parse bag header record
        result = BagRecordParser.parse_record(reader)
        if result is None:
            raise ValueError("Failed to parse bag header record")

        record_type, bag_header = result
        if record_type != BagRecordType.BAG_HEADER:
            raise ValueError(f"Expected BAG_HEADER record, got {record_type}")

        if verbose:
            logger.info(
                f"Bag header: {bag_header.conn_count} connections, "
                f"{bag_header.chunk_count} chunks"
            )

        # Create output writer
        with BagFileWriter.open(
            output_path,
            compression=compression or 'none',
            chunk_size=chunk_size,
        ) as writer:
            try:
                # Iterate through all records in the file
                while True:
                    result = BagRecordParser.parse_record(reader)
                    if result is None:
                        break  # EOF

                    record_type, record = result

                    if record_type == BagRecordType.CONNECTION:
                        conn_record: BagConnectionRecord = record
                        if conn_record.conn not in connections:
                            connections[conn_record.conn] = conn_record
                            # Register connection with writer using the record directly
                            writer.add_connection_record(conn_record)

                    elif record_type == BagRecordType.MSG_DATA:
                        msg_record: MessageDataRecord = record
                        # Ensure connection exists before writing message
                        if msg_record.conn in connections:
                            # Write message record directly (no re-serialization)
                            writer.write_message_record(msg_record)
                            messages_recovered += 1
                        elif verbose:
                            logger.warning(
                                f"Skipping message with unknown conn_id "
                                f"{msg_record.conn}"
                            )

                    elif record_type == BagRecordType.CHUNK:
                        chunk_record: ChunkRecord = record
                        try:
                            chunk_data = BagRecordParser.decompress_chunk(chunk_record)
                            chunk_messages, connections = _process_bag_chunk_records(
                                chunk_data,
                                connections,
                                writer,
                                verbose=verbose
                            )
                            messages_recovered += chunk_messages
                        except Exception as e:
                            if verbose:
                                logger.warning(f"Error processing chunk: {e}")

                    elif record_type == BagRecordType.INDEX_DATA:
                        # Skip index data records
                        pass

                    elif record_type == BagRecordType.CHUNK_INFO:
                        # Skip chunk info records (summary section)
                        pass

                    else:
                        if verbose:
                            logger.warning(
                                f"Unknown record type {record_type}, skipping"
                            )

            except Exception as e:
                logger.warning(f"Encountered error while reading records: {e}")
                logger.info(
                    f"Recovered {messages_recovered} messages before corruption"
                )
            else:
                logger.info("\nFull recovery successful - no corruption detected!")

    # Print recovery summary
    logger.info(f"\n{'='*60}")
    logger.info("Recovery Summary:")
    logger.info(f"  Input file:  {input_path}")
    logger.info(f"  Output file: {output_path}")
    logger.info(f"  Messages recovered: {messages_recovered}")
    logger.info(f"  Connections recovered: {len(connections)}")

    return output_path


def _process_chunk_records(
    chunk_data: bytes,
    schemas: dict[int, SchemaRecord],
    channels: dict[int, ChannelRecord],
    writer: BaseMcapRecordWriter,
    *,
    verbose: bool = False
) -> tuple[int, dict[int, SchemaRecord], dict[int, ChannelRecord]]:
    """Process records inside a decompressed chunk.

    Args:
        chunk_data: The decompressed chunk data.
        schemas: Dictionary to store schema records (id -> record).
        channels: Dictionary to store channel records (id -> record).
        writer: The MCAP writer to write records to.
        verbose: Whether to log verbose progress.

    Returns:
        Number of messages recovered, and updated schemas and channels dicts
    """
    messages_recovered = 0
    chunk_reader = BytesReader(chunk_data)

    try:
        while record_type := McapRecordParser.peek_record(chunk_reader):
            if record_type == McapRecordType.SCHEMA:
                schema = McapRecordParser.parse_schema(chunk_reader)
                if schema is not None and schema.id not in schemas:
                    schemas[schema.id] = schema
                    writer.write_schema(schema)

            elif record_type == McapRecordType.CHANNEL:
                channel = McapRecordParser.parse_channel(chunk_reader)
                if channel.id not in channels:
                    channels[channel.id] = channel
                    writer.write_channel(channel)

            # TODO: Decode message to make sure it is valid?
            elif record_type == McapRecordType.MESSAGE:
                message = McapRecordParser.parse_message(chunk_reader)
                # Ensure channel exists and is written before message
                if message.channel_id in channels:
                    writer.write_message(message)
                    messages_recovered += 1
                elif verbose:
                    logger.warning(f"Skipping message with unknown channel_id {message.channel_id}")

            else:  # No other record types should be in chunks
                if verbose:
                    logger.warning(f"Skipping record: {record_type}")
                McapRecordParser.skip_record(chunk_reader)

    except Exception as e:
        if verbose:
            logger.warning(f"Error reading chunk records: {e}")
        # Stop parsing this chunk but return what we recovered

    return messages_recovered, schemas, channels


def recover_mcap(
    input_path: str | Path,
    output_path: str | Path | None = None,
    chunk_size: int | None = None,
    chunk_compression: Literal["none", "lz4", "zstd"] | None = None,
    *,
    overwrite: bool = False,
    verbose: bool = False
) -> Path:
    """Recover data from a potentially corrupted MCAP file.

    This command reads records one at a time from the input MCAP and writes them
    to a new output MCAP. If corruption is encountered, it stops and saves all
    successfully recovered data up to that point.

    Args:
        input_path: Path to the potentially corrupted MCAP file
        output_path: Path for the recovered output file
        chunk_size: Chunk size for the output file in bytes
        chunk_compression: Compression algorithm for chunks
        overwrite: Whether to overwrite existing output file
        verbose: Whether to print detailed recovery progress

    Returns:
        Path to the recovered MCAP file
    """
    # Resolve input and output paths
    input_path = Path(input_path).resolve()
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_recovered.mcap")
    output_path = Path(output_path).resolve()
    if output_path == input_path:
        raise ValueError('Input path cannot be same as output.')

    # Check if output path exists
    if not overwrite and output_path.exists():
        raise ValueError('Output mcap exists. Please set `overwrite` to True.')

    attachments_recovered = 0
    metadata_recovered = 0
    messages_recovered = 0

    schemas: dict[int, SchemaRecord] = {}
    channels: dict[int, ChannelRecord] = {}

    with FileReader(input_path) as reader:
        # Parse magic bytes
        McapRecordParser.parse_magic_bytes(reader)

        # Parse header record
        record_type = McapRecordParser.peek_record(reader)
        if record_type != McapRecordType.HEADER:
            raise ValueError(f"Expected HEADER record, got {record_type}")
        header = McapRecordParser.parse_header(reader)

        if verbose:
            logger.info(f"MCAP profile: {header.profile}, library: {header.library}")

        # Create output writer
        with McapRecordWriterFactory.create_writer(
            FileWriter(output_path),
            McapSummaryFactory.create_summary(chunk_size=chunk_size),
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=header.profile
        ) as writer:
            try:
                # Iterate through all records in the file
                while record_type := McapRecordParser.peek_record(reader):
                    if record_type == McapRecordType.DATA_END:
                        break

                    elif record_type == McapRecordType.SCHEMA:
                        schema = McapRecordParser.parse_schema(reader)
                        if schema is not None and schema.id not in schemas:
                            schemas[schema.id] = schema
                            writer.write_schema(schema)

                    elif record_type == McapRecordType.CHANNEL:
                        channel = McapRecordParser.parse_channel(reader)
                        if channel.id not in channels:
                            channels[channel.id] = channel
                            writer.write_channel(channel)

                    elif record_type == McapRecordType.MESSAGE:
                        # TODO: Decode message to make sure it is valid?
                        message = McapRecordParser.parse_message(reader)
                        if message.channel_id in channels:
                            writer.write_message(message)
                            messages_recovered += 1
                        elif verbose:
                            logger.warning(f"Skipping message with unknown channel_id {message.channel_id}")

                    elif record_type == McapRecordType.CHUNK:
                        # TODO: Use crc to check data integrity?
                        chunk = McapRecordParser.parse_chunk(reader)
                        try:
                            chunk_messages, schemas, channels = _process_chunk_records(
                                decompress_chunk(chunk),
                                schemas,
                                channels,
                                writer,
                                verbose=verbose
                            )
                            messages_recovered += chunk_messages
                        except Exception as e:
                            if verbose:
                                logger.warning(f"Error processing chunk: {e}")

                    elif record_type == McapRecordType.ATTACHMENT:
                        # TODO: Use crc to check data integrity?
                        attachment = McapRecordParser.parse_attachment(reader)
                        attachments_recovered += 1
                        writer.write_attachment(attachment)

                    elif record_type == McapRecordType.METADATA:
                        metadata = McapRecordParser.parse_metadata(reader)
                        metadata_recovered += 1
                        writer.write_metadata(metadata)

                    elif record_type == McapRecordType.MESSAGE_INDEX:
                        McapRecordParser.skip_record(reader)

                    elif record_type in (
                        McapRecordType.CHUNK_INDEX,
                        McapRecordType.FOOTER,
                        McapRecordType.STATISTICS,
                        McapRecordType.SUMMARY_OFFSET,
                        McapRecordType.ATTACHMENT_INDEX,
                        McapRecordType.METADATA_INDEX,
                    ):
                        # Skip summary section records
                        if verbose:
                            logger.warning(f"Unexpected summary record {record_type}, skipping")
                        McapRecordParser.skip_record(reader)

                    else:
                        # Unknown record type, skip it
                        if verbose:
                            logger.warning(f"Unknown record type {record_type}, skipping")
                        McapRecordParser.skip_record(reader)

            except Exception as e:
                logger.warning(f"Encountered error while reading records: {e}")
                logger.info(f"Recovered {messages_recovered} messages before corruption")
            else:
                logger.info("\nFull recovery successful - no corruption detected!")

    # Print recovery summary
    logger.info(f"\n{'='*60}")
    logger.info("Recovery Summary:")
    logger.info(f"  Input file:  {input_path}")
    logger.info(f"  Output file: {output_path}")
    logger.info(f"  Messages recovered: {messages_recovered}")
    logger.info(f"  Channels recovered: {len(channels)}")
    logger.info(f"  Schemas recovered:  {len(schemas)}")

    return output_path


def _run_recover(args) -> Path:
    """Run the recover command with format auto-detection.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Path to the recovered file.
    """
    input_path = Path(args.input).resolve()
    file_format = get_file_format_from_magic(input_path)

    if file_format == "mcap":
        # Validate compression for MCAP files
        chunk_compression = validate_compression_for_mcap(args.chunk_compression)
        return recover_mcap(
            input_path,
            output_path=args.output,
            chunk_size=args.chunk_size,
            chunk_compression=chunk_compression,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )
    else:  # bag format
        # Map compression for bag files
        compression = validate_compression_for_bag(args.chunk_compression)

        return recover_bag(
            input_path,
            output_path=args.output,
            chunk_size=args.chunk_size,
            compression=compression,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "recover",
        help="Recover data from a corrupted MCAP or ROS bag file.",
        description=dedent("""
            Attempt to recover data from a potentially corrupted MCAP or ROS bag file.
            The file format is automatically detected from the file header.

            This command reads messages one at a time and writes them to a new
            file. If corruption is encountered during reading, the command
            stops and saves all successfully recovered data up to that point.

            This is useful for extracting partial data from damaged files.

            Supported formats:
              - MCAP (.mcap) - ROS 2 recording format
              - ROS bag (.bag) - ROS 1 recording format
        """)
    )
    parser.add_argument(
        "input",
        help="Path to the potentially corrupted MCAP or bag file."
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path for recovered data (format matches input)."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the recovered file in bytes."
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=['lz4', 'zstd', 'bz2', 'none'],
        help=dedent("""
            Compression used for chunk records. MCAP files support 'none',
            'lz4', and 'zstd'; using 'bz2' with MCAP will raise an error.
            Bag files support 'none' and 'bz2'; using 'lz4' or 'zstd' with
            bag files will raise an error.
        """)
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print detailed recovery progress."
    )
    parser.set_defaults(func=_run_recover)
