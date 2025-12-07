"""MCAP recovery CLI command."""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Literal

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
    chunk_compression: Literal["lz4", "zstd"] | None = None,
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


def add_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "recover",
        help="Recover data from a corrupted MCAP file.",
        description=dedent("""
            Attempt to recover data from a potentially corrupted MCAP file.
            This command reads messages one at a time and writes them to a new
            MCAP file. If corruption is encountered during reading, the command
            stops and saves all successfully recovered data up to that point.

            This is useful for extracting partial data from damaged MCAP files.
        """)
    )
    parser.add_argument("input", help="Path to the potentially corrupted MCAP file.")
    parser.add_argument("-o", "--output", help="Output MCAP file path for recovered data")
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Chunk size of the recovered MCAP in bytes."
    )
    parser.add_argument(
        "--chunk-compression",
        type=str,
        choices=['lz4', 'zstd'],
        help="Compression used for the chunk records."
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
    parser.set_defaults(
        func=lambda args: recover_mcap(
            args.input,
            output_path=args.output,
            chunk_size=args.chunk_size,
            chunk_compression=args.chunk_compression,
            overwrite=args.overwrite,
            verbose=args.verbose,
        )
    )
