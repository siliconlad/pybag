"""MCAP recovery CLI command."""

import argparse
import logging
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Literal

from pybag.io.raw_writer import FileWriter
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import MessageRecord

logger = logging.getLogger(__name__)


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

    messages_recovered = 0
    channels_recovered = set()
    schemas_recovered = set()
    error_message = None

    try:
        with McapRecordReaderFactory.from_file(
            input_path,
            enable_summary_reconstruction='always'
        ) as reader:
            # Get all metadata from the file
            all_channels = reader.get_channels()
            all_schemas = reader.get_schemas()
            header = reader.get_header()

            if verbose:
                logger.info(f"Input file has {len(all_channels)} channels and {len(all_schemas)} schemas")

            # Create output writer
            with McapRecordWriterFactory.create_writer(
                FileWriter(output_path),
                chunk_size=chunk_size,
                chunk_compression=chunk_compression,
                profile=header.profile
            ) as writer:
                # Track what we've written
                written_schema_ids = set()
                written_channel_ids = set()
                sequence_counters = defaultdict(int)

                try:
                    # Iterate through all messages in the file
                    for msg_record in reader.get_messages(
                        channel_id=None,  # Get all channels
                        start_timestamp=None,
                        end_timestamp=None,
                        in_log_time_order=False  # Read in write order for recovery
                    ):
                        # Write the schema if we haven't yet
                        schema_id = all_channels[msg_record.channel_id].schema_id
                        if schema_id != 0 and schema_id not in written_schema_ids:
                            if (schema := all_schemas.get(schema_id)) is not None:
                                writer.write_schema(schema)
                                written_schema_ids.add(schema_id)
                                schemas_recovered.add(schema_id)
                            else:
                                logger.warning(f'Schema {schema_id} not found for channel {msg_record.channel_id}')

                        # Write the channel if we haven't yet
                        if msg_record.channel_id not in written_channel_ids:
                            writer.write_channel(all_channels[msg_record.channel_id])
                            written_channel_ids.add(msg_record.channel_id)
                            channels_recovered.add(msg_record.channel_id)

                        # Write the message with updated sequence number
                        new_record = MessageRecord(
                            channel_id=msg_record.channel_id,
                            sequence=sequence_counters[msg_record.channel_id],
                            log_time=msg_record.log_time,
                            publish_time=msg_record.publish_time,
                            data=msg_record.data
                        )
                        sequence_counters[msg_record.channel_id] += 1
                        writer.write_message(new_record)
                        messages_recovered += 1

                        if verbose and messages_recovered % 1000 == 0:
                            logger.info(f"Recovered {messages_recovered} messages so far...")

                except Exception as e:
                    error_message = str(e)
                    logger.warning(f"Encountered error while reading messages: {e}")
                    logger.info(f"Recovered {messages_recovered} messages before corruption")

    except Exception as e:
        error_message = str(e)
        logger.error(f"Failed to open or process MCAP file: {e}")
        raise

    # Print recovery summary
    logger.info(f"\n{'='*60}")
    logger.info("Recovery Summary:")
    logger.info(f"{'='*60}")
    logger.info(f"Input file:  {input_path}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"Messages recovered: {messages_recovered}")
    logger.info(f"Channels recovered: {len(channels_recovered)}")
    logger.info(f"Schemas recovered:  {len(schemas_recovered)}")

    if error_message:
        logger.info(f"\nRecovery stopped due to: {error_message}")
    else:
        logger.info("\nFull recovery successful - no corruption detected!")

    logger.info(f"{'='*60}\n")

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
