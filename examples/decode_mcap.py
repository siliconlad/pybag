"""
Decode H.264 compressed image topics back to raw Image messages.

This utility reads an existing MCAP file with H.264 CompressedImage messages,
decodes them back to raw Image messages, and writes a new MCAP file.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Add src to path so we can import pybag
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import sensor_msgs
from h264_compression import h264_compressed_to_image


def decode_compressed_topic(
    input_mcap: str,
    output_mcap: str,
    compressed_topic: str,
    output_encoding: str = 'rgb8',
    output_topic: Optional[str] = None,
    chunk_compression: Optional[str] = None,
    verbose: bool = True
) -> dict:
    """
    Decode an H.264 CompressedImage topic back to raw Image messages.

    Args:
        input_mcap: Path to input MCAP file
        output_mcap: Path to output MCAP file
        compressed_topic: Topic name to decode (must contain H.264 CompressedImage messages)
        output_encoding: Desired output encoding (rgb8, bgr8, mono8, etc.)
        output_topic: Topic name for decoded output (default: same as input_topic)
        chunk_compression: MCAP chunk compression ('lz4', 'zstd', or None)
        verbose: Print progress information

    Returns:
        Dictionary with statistics about the conversion
    """
    if output_topic is None:
        output_topic = compressed_topic

    stats = {
        'input_file': input_mcap,
        'output_file': output_mcap,
        'compressed_topic': compressed_topic,
        'output_topic': output_topic,
        'total_messages': 0,
        'decoded_messages': 0,
        'other_messages': 0,
        'compressed_size': 0,
        'decompressed_size': 0,
    }

    if verbose:
        print(f"Reading from: {input_mcap}")
        print(f"Writing to: {output_mcap}")
        print(f"Decoding topic: {compressed_topic} -> {output_topic}")
        print(f"Output encoding: {output_encoding}")
        if chunk_compression:
            print(f"MCAP chunk compression: {chunk_compression}")
        print()

    # Track if we found the topic
    topic_found = False

    with McapFileReader.from_file(input_mcap) as reader:
        with McapFileWriter.open(output_mcap, chunk_compression=chunk_compression) as writer:
            # Get all topics
            all_topics = reader.get_topics()

            if verbose:
                print(f"Found topics: {all_topics}\n")

            # Process each topic
            for topic_name in all_topics:
                if topic_name == compressed_topic:
                    topic_found = True

                    # Process compressed image topic
                    for msg in reader.messages(topic_name):
                        stats['total_messages'] += 1
                        message = msg.data
                        timestamp = msg.log_time

                        # Check message type
                        msg_type = getattr(message, '__msg_name__', None)

                        if msg_type == 'sensor_msgs/msg/CompressedImage' or isinstance(message, sensor_msgs.CompressedImage):
                            if stats['decoded_messages'] == 0 and verbose:
                                print(f"Found CompressedImage topic: {topic_name}")
                                print(f"  Format: {message.format}")

                            # Check if it's H.264
                            if message.format != 'h264':
                                raise ValueError(
                                    f"CompressedImage topic uses format '{message.format}', not 'h264'. "
                                    f"This utility only decodes H.264 compressed images."
                                )

                            # Track compressed size
                            stats['compressed_size'] += len(message.data)

                            # Decode H.264 to raw Image
                            decoded = h264_compressed_to_image(message, encoding=output_encoding)

                            # Track decompressed size
                            stats['decompressed_size'] += len(decoded.data)

                            # Write decoded message to output topic
                            writer.write_message(output_topic, timestamp, decoded)
                            stats['decoded_messages'] += 1

                            if verbose and stats['decoded_messages'] % 100 == 0:
                                print(f"  Decoded {stats['decoded_messages']} images...")

                        else:
                            raise TypeError(
                                f"Topic {topic_name} contains {type(message).__name__}, "
                                f"expected CompressedImage"
                            )
                else:
                    # Copy all other topics as-is
                    for msg in reader.messages(topic_name):
                        stats['total_messages'] += 1
                        writer.write_message(topic_name, msg.log_time, msg.data)
                        stats['other_messages'] += 1

    if not topic_found:
        raise ValueError(f"Topic '{compressed_topic}' not found in {input_mcap}")

    # Calculate statistics
    if stats['compressed_size'] > 0:
        stats['expansion_ratio'] = stats['decompressed_size'] / stats['compressed_size']
        stats['size_increase'] = stats['decompressed_size'] - stats['compressed_size']
    else:
        stats['expansion_ratio'] = 1.0
        stats['size_increase'] = 0

    if verbose:
        print(f"\nCompleted!")
        print(f"  Total messages: {stats['total_messages']}")
        print(f"  Decoded messages: {stats['decoded_messages']}")
        print(f"  Other messages: {stats['other_messages']}")
        print(f"  Compressed image data: {stats['compressed_size']:,} bytes ({stats['compressed_size'] / 1024 / 1024:.2f} MB)")
        print(f"  Decompressed image data: {stats['decompressed_size']:,} bytes ({stats['decompressed_size'] / 1024 / 1024:.2f} MB)")
        print(f"  Expansion ratio: {stats['expansion_ratio']:.2f}x")

    return stats


def main():
    """Command-line interface for decoding H.264 compressed MCAP files."""
    parser = argparse.ArgumentParser(
        description='Decode H.264 compressed image topics back to raw Image messages',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Decode a compressed image topic back to raw images
  python decode_mcap.py compressed.mcap decoded.mcap /camera/image_compressed

  # Decode with specific output encoding
  python decode_mcap.py compressed.mcap decoded.mcap /camera/h264 --encoding bgr8

  # Decode to a different topic name
  python decode_mcap.py compressed.mcap decoded.mcap /camera/h264 --output-topic /camera/image_raw

  # Decode with MCAP chunk compression
  python decode_mcap.py compressed.mcap decoded.mcap /camera/h264 --chunk-compression zstd

Supported output encodings:
  rgb8, bgr8, rgba8, bgra8, mono8
        """
    )

    parser.add_argument('input', help='Input MCAP file path (with H.264 compressed images)')
    parser.add_argument('output', help='Output MCAP file path')
    parser.add_argument('topic', help='CompressedImage topic to decode (must use H.264 format)')
    parser.add_argument(
        '--output-topic',
        help='Output topic name (default: same as input topic)'
    )
    parser.add_argument(
        '--encoding',
        default='rgb8',
        choices=['rgb8', 'bgr8', 'rgba8', 'bgra8', 'mono8'],
        help='Output image encoding (default: rgb8)'
    )
    parser.add_argument(
        '--chunk-compression',
        choices=['lz4', 'zstd'],
        help='MCAP chunk compression algorithm (default: none)'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress progress output'
    )

    args = parser.parse_args()

    # Check input file exists
    input_path = Path(args.input)
    if not input_path.exists():
        parser.error(f"Input file not found: {args.input}")

    # Check output file doesn't exist (safety check)
    output_path = Path(args.output)
    if output_path.exists():
        response = input(f"Output file {args.output} already exists. Overwrite? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return

    try:
        decode_compressed_topic(
            input_mcap=args.input,
            output_mcap=args.output,
            compressed_topic=args.topic,
            output_encoding=args.encoding,
            output_topic=args.output_topic,
            chunk_compression=args.chunk_compression,
            verbose=not args.quiet
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
