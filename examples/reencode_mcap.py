"""
Re-encode image topics in MCAP files with H.264 compression.

This utility reads an existing MCAP file, re-encodes specified image topics
with H.264 compression, and writes a new MCAP file with the compressed images
and all other topics preserved.
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
from h264_compression import image_to_h264_compressed, h264_compressed_to_image


def reencode_image_topic(
    input_mcap: str,
    output_mcap: str,
    image_topic: str,
    quality: int = 23,
    preset: str = 'medium',
    output_topic: Optional[str] = None,
    chunk_compression: Optional[str] = None,
    verbose: bool = True
) -> dict:
    """
    Re-encode an image topic with H.264 compression.

    Args:
        input_mcap: Path to input MCAP file
        output_mcap: Path to output MCAP file
        image_topic: Topic name to re-encode (must contain Image or CompressedImage messages)
        quality: H.264 CRF quality (0-51, lower is better)
        preset: H.264 encoding preset (ultrafast to veryslow)
        output_topic: Topic name for compressed output (default: same as input_topic)
        chunk_compression: MCAP chunk compression ('lz4', 'zstd', or None)
        verbose: Print progress information

    Returns:
        Dictionary with statistics about the conversion
    """
    if output_topic is None:
        output_topic = image_topic

    stats = {
        'input_file': input_mcap,
        'output_file': output_mcap,
        'image_topic': image_topic,
        'output_topic': output_topic,
        'total_messages': 0,
        'image_messages': 0,
        'other_messages': 0,
        'original_size': 0,
        'compressed_size': 0,
    }

    if verbose:
        print(f"Reading from: {input_mcap}")
        print(f"Writing to: {output_mcap}")
        print(f"Re-encoding topic: {image_topic} -> {output_topic}")
        print(f"H.264 settings: CRF={quality}, preset={preset}")
        if chunk_compression:
            print(f"MCAP chunk compression: {chunk_compression}")
        print()

    # Track if we found the topic
    topic_found = False
    topic_type = None

    with McapFileReader.from_file(input_mcap) as reader:
        with McapFileWriter.open(output_mcap, chunk_compression=chunk_compression) as writer:
            # Get all topics
            all_topics = reader.get_topics()

            if verbose:
                print(f"Found topics: {all_topics}\n")

            # Process each topic
            for topic_name in all_topics:
                if topic_name == image_topic:
                    topic_found = True

                    # Process image topic
                    for msg in reader.messages(topic_name):
                        stats['total_messages'] += 1
                        message = msg.data
                        timestamp = msg.log_time

                        # Determine message type
                        # Check message type by __msg_name__ attribute
                        msg_type = getattr(message, '__msg_name__', None)

                        if msg_type == 'sensor_msgs/msg/Image' or isinstance(message, sensor_msgs.Image):
                            if topic_type is None:
                                topic_type = 'Image'
                                if verbose:
                                    print(f"Found Image topic: {topic_name}")

                            # Track original size (approximate)
                            stats['original_size'] += len(message.data)

                            # Convert Image to H.264 CompressedImage
                            compressed = image_to_h264_compressed(
                                message,
                                quality=quality,
                                preset=preset
                            )

                            # Track compressed size
                            stats['compressed_size'] += len(compressed.data)

                            # Write compressed message to output topic
                            writer.write_message(output_topic, timestamp, compressed)
                            stats['image_messages'] += 1

                            if verbose and stats['image_messages'] % 100 == 0:
                                print(f"  Processed {stats['image_messages']} images...")

                        elif msg_type == 'sensor_msgs/msg/CompressedImage' or isinstance(message, sensor_msgs.CompressedImage):
                            if topic_type is None:
                                topic_type = 'CompressedImage'
                                if verbose:
                                    print(f"Found CompressedImage topic: {topic_name}")

                            # Track original size
                            stats['original_size'] += len(message.data)

                            # First decompress if it's not already H.264
                            if message.format == 'h264':
                                # Already H.264, just copy it
                                compressed = message
                                if verbose and stats['image_messages'] == 0:
                                    print(f"  Topic already uses H.264, copying as-is")
                            else:
                                # Decompress to raw image first
                                # Note: This assumes CompressedImage is JPEG/PNG which we'd need
                                # additional libraries to decode. For now, error out.
                                raise ValueError(
                                    f"CompressedImage topic uses format '{message.format}'. "
                                    f"Only 'h264' format is supported for CompressedImage. "
                                    f"To re-encode from other formats, first convert to raw Image messages."
                                )

                            # Track compressed size
                            stats['compressed_size'] += len(compressed.data)

                            # Write message to output topic
                            writer.write_message(output_topic, timestamp, compressed)
                            stats['image_messages'] += 1

                            if verbose and stats['image_messages'] % 100 == 0:
                                print(f"  Processed {stats['image_messages']} images...")

                        else:
                            raise TypeError(
                                f"Topic {topic_name} contains {type(message).__name__}, "
                                f"expected Image or CompressedImage"
                            )
                else:
                    # Copy all other topics as-is
                    for msg in reader.messages(topic_name):
                        stats['total_messages'] += 1
                        writer.write_message(topic_name, msg.log_time, msg.data)
                        stats['other_messages'] += 1

    if not topic_found:
        raise ValueError(f"Topic '{image_topic}' not found in {input_mcap}")

    # Calculate statistics
    if stats['original_size'] > 0:
        stats['compression_ratio'] = stats['original_size'] / stats['compressed_size']
        stats['space_saved'] = stats['original_size'] - stats['compressed_size']
        stats['percent_saved'] = (stats['space_saved'] / stats['original_size']) * 100
    else:
        stats['compression_ratio'] = 1.0
        stats['space_saved'] = 0
        stats['percent_saved'] = 0.0

    if verbose:
        print(f"\nCompleted!")
        print(f"  Total messages: {stats['total_messages']}")
        print(f"  Image messages: {stats['image_messages']}")
        print(f"  Other messages: {stats['other_messages']}")
        print(f"  Original image data: {stats['original_size']:,} bytes ({stats['original_size'] / 1024 / 1024:.2f} MB)")
        print(f"  Compressed image data: {stats['compressed_size']:,} bytes ({stats['compressed_size'] / 1024 / 1024:.2f} MB)")
        print(f"  Compression ratio: {stats['compression_ratio']:.2f}x")
        print(f"  Space saved: {stats['space_saved']:,} bytes ({stats['percent_saved']:.1f}%)")

    return stats


def main():
    """Command-line interface for re-encoding MCAP files."""
    parser = argparse.ArgumentParser(
        description='Re-encode image topics in MCAP files with H.264 compression',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Re-encode a single image topic
  python reencode_mcap.py input.mcap output.mcap /camera/image_raw

  # Re-encode with custom quality
  python reencode_mcap.py input.mcap output.mcap /camera/image_raw --quality 18

  # Re-encode to a different topic name
  python reencode_mcap.py input.mcap output.mcap /camera/image_raw --output-topic /camera/image_compressed

  # Re-encode with MCAP chunk compression
  python reencode_mcap.py input.mcap output.mcap /camera/image_raw --chunk-compression zstd

Quality (CRF) values:
  0-17:  Visually lossless to excellent quality (larger files)
  18-23: High quality (recommended for most use cases)
  24-28: Medium quality (good compression)
  29-51: Low quality (maximum compression)

Preset values (encoding speed):
  ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow
  Slower presets = better compression but slower encoding
        """
    )

    parser.add_argument('input', help='Input MCAP file path')
    parser.add_argument('output', help='Output MCAP file path')
    parser.add_argument('topic', help='Image topic to re-encode (must be Image or CompressedImage)')
    parser.add_argument(
        '--output-topic',
        help='Output topic name (default: same as input topic)'
    )
    parser.add_argument(
        '--quality',
        type=int,
        default=23,
        help='H.264 CRF quality value, 0-51 (default: 23, lower is better)'
    )
    parser.add_argument(
        '--preset',
        default='medium',
        choices=['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow'],
        help='H.264 encoding preset (default: medium)'
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

    # Validate quality
    if not 0 <= args.quality <= 51:
        parser.error('Quality must be between 0 and 51')

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
        reencode_image_topic(
            input_mcap=args.input,
            output_mcap=args.output,
            image_topic=args.topic,
            quality=args.quality,
            preset=args.preset,
            output_topic=args.output_topic,
            chunk_compression=args.chunk_compression,
            verbose=not args.quiet
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
