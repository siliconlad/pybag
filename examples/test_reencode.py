"""
Test script for the reencode_mcap utility.

Creates a test MCAP file with raw images, re-encodes it with H.264,
and verifies the output.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import sensor_msgs, std_msgs, builtin_interfaces
from h264_compression import create_test_image
from reencode_mcap import reencode_image_topic


def create_test_mcap(output_path: str, num_images: int = 20):
    """Create a test MCAP file with raw images and other topics."""
    print(f"Creating test MCAP file with {num_images} images...")

    with McapFileWriter.open(output_path) as writer:
        for i in range(num_images):
            # Create a test image
            image = create_test_image(
                width=640,
                height=480,
                encoding='rgb8',
                pattern='gradient'
            )
            image.header.stamp.sec = i
            image.header.stamp.nanosec = 0

            # Write image message
            writer.write_message('/camera/image_raw', i * 100_000_000, image)

            # Write some other messages (e.g., string messages)
            string_msg = std_msgs.String(data=f"Message {i}")
            writer.write_message('/other/topic', i * 100_000_000, string_msg)

    file_size = os.path.getsize(output_path)
    print(f"  Created: {output_path}")
    print(f"  File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)\n")
    return file_size


def verify_reencoded_mcap(mcap_path: str, expected_image_topic: str, num_images: int):
    """Verify the re-encoded MCAP file."""
    print(f"Verifying re-encoded MCAP file...")

    image_count = 0
    other_count = 0
    found_compressed = False

    with McapFileReader.from_file(mcap_path) as reader:
        all_topics = reader.get_topics()

        for topic_name in all_topics:
            for msg in reader.messages(topic_name):
                message = msg.data
                msg_type = getattr(message, '__msg_name__', None)

                if topic_name == expected_image_topic:
                    image_count += 1
                    if msg_type == 'sensor_msgs/msg/CompressedImage' or isinstance(message, sensor_msgs.CompressedImage):
                        found_compressed = True
                        if message.format != 'h264':
                            print(f"  ERROR: Expected h264 format, got {message.format}")
                            return False
                    else:
                        print(f"  ERROR: Expected CompressedImage, got {type(message).__name__}")
                        return False
                else:
                    other_count += 1

    if image_count != num_images:
        print(f"  ERROR: Expected {num_images} images, found {image_count}")
        return False

    if not found_compressed:
        print(f"  ERROR: No compressed images found")
        return False

    print(f"  ✓ Found {image_count} H.264 compressed images on topic {expected_image_topic}")
    print(f"  ✓ Found {other_count} other messages (preserved)")
    print(f"  ✓ All images use H.264 format")
    return True


def main():
    """Run the test."""
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)

    test_input = output_dir / 'test_input.mcap'
    test_output = output_dir / 'test_output.mcap'

    num_images = 20

    print("="*80)
    print("Testing reencode_mcap utility")
    print("="*80)
    print()

    # Step 1: Create test MCAP
    original_size = create_test_mcap(str(test_input), num_images)

    # Step 2: Re-encode with H.264
    print("Re-encoding image topic with H.264...")
    stats = reencode_image_topic(
        input_mcap=str(test_input),
        output_mcap=str(test_output),
        image_topic='/camera/image_raw',
        quality=23,
        preset='medium',
        verbose=True
    )
    print()

    # Step 3: Verify output
    if verify_reencoded_mcap(str(test_output), '/camera/image_raw', num_images):
        print()
        print("="*80)
        print("TEST PASSED ✓")
        print("="*80)
        print()

        # Print file size comparison
        output_size = os.path.getsize(test_output)
        print("File size comparison:")
        print(f"  Original: {original_size:,} bytes ({original_size / 1024 / 1024:.2f} MB)")
        print(f"  Re-encoded: {output_size:,} bytes ({output_size / 1024 / 1024:.2f} MB)")
        print(f"  File size reduction: {(1 - output_size / original_size) * 100:.1f}%")
    else:
        print()
        print("="*80)
        print("TEST FAILED ✗")
        print("="*80)
        sys.exit(1)


if __name__ == '__main__':
    main()
