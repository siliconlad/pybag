"""
Examples of decoding H.264 compressed images with pybag.

This script demonstrates various ways to read and decode H.264 CompressedImage messages.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import sensor_msgs
from h264_compression import h264_compressed_to_image, image_to_h264_compressed, create_test_image


def example1_decode_single_message():
    """Example 1: Decode a single H.264 CompressedImage message."""
    print("="*80)
    print("Example 1: Decode a single H.264 CompressedImage message")
    print("="*80)

    # Create a test image and compress it
    print("\n1. Creating test image...")
    image = create_test_image(width=640, height=480, encoding='rgb8', pattern='gradient')
    print(f"   Original size: {len(image.data):,} bytes")

    # Compress with H.264
    print("\n2. Compressing with H.264...")
    compressed = image_to_h264_compressed(image, quality=23)
    print(f"   Compressed size: {len(compressed.data):,} bytes")
    print(f"   Format: {compressed.format}")

    # Decode back to raw image
    print("\n3. Decoding back to raw image...")
    decoded = h264_compressed_to_image(compressed, encoding='rgb8')
    print(f"   Decoded size: {len(decoded.data):,} bytes")
    print(f"   Dimensions: {decoded.width}x{decoded.height}")
    print(f"   Encoding: {decoded.encoding}")

    print("\n✓ Single message decode successful!\n")


def example2_read_from_mcap():
    """Example 2: Read and decode H.264 images from an MCAP file."""
    print("="*80)
    print("Example 2: Read and decode H.264 images from MCAP file")
    print("="*80)

    # Create a test MCAP with compressed images
    test_file = Path(__file__).parent / 'output' / 'test_compressed.mcap'
    test_file.parent.mkdir(exist_ok=True)

    print("\n1. Creating test MCAP with 5 compressed images...")
    with McapFileWriter.open(str(test_file)) as writer:
        for i in range(5):
            image = create_test_image(width=320, height=240, encoding='rgb8', pattern='gradient')
            image.header.stamp.sec = i
            compressed = image_to_h264_compressed(image, quality=23)
            writer.write_message('/camera/compressed', i * 100_000_000, compressed)

    print(f"   Created: {test_file}")

    # Read and decode messages
    print("\n2. Reading and decoding messages...")
    with McapFileReader.from_file(str(test_file)) as reader:
        for msg in reader.messages('/camera/compressed'):
            compressed = msg.data
            timestamp = msg.log_time

            # Decode the compressed image
            decoded = h264_compressed_to_image(compressed, encoding='rgb8')

            print(f"   Message at {timestamp}ns:")
            print(f"     Compressed: {len(compressed.data):,} bytes ({compressed.format})")
            print(f"     Decoded: {len(decoded.data):,} bytes ({decoded.width}x{decoded.height} {decoded.encoding})")

    print("\n✓ MCAP reading and decoding successful!\n")


def example3_decode_to_different_encoding():
    """Example 3: Decode H.264 to different image encodings."""
    print("="*80)
    print("Example 3: Decode to different encodings")
    print("="*80)

    # Create and compress an image
    print("\n1. Creating and compressing test image...")
    image = create_test_image(width=320, height=240, encoding='rgb8', pattern='gradient')
    compressed = image_to_h264_compressed(image, quality=23)
    print(f"   Compressed size: {len(compressed.data):,} bytes")

    # Decode to different encodings
    print("\n2. Decoding to different encodings...")

    encodings = ['rgb8', 'bgr8', 'mono8']
    for encoding in encodings:
        decoded = h264_compressed_to_image(compressed, encoding=encoding)
        print(f"   {encoding}: {decoded.width}x{decoded.height}, {len(decoded.data):,} bytes, step={decoded.step}")

    print("\n✓ Multi-encoding decode successful!\n")


def example4_streaming_decode():
    """Example 4: Simulate streaming decode (process images as they arrive)."""
    print("="*80)
    print("Example 4: Streaming decode simulation")
    print("="*80)

    # Create a test MCAP
    test_file = Path(__file__).parent / 'output' / 'test_stream.mcap'
    test_file.parent.mkdir(exist_ok=True)

    print("\n1. Creating test stream with 10 frames...")
    with McapFileWriter.open(str(test_file)) as writer:
        for i in range(10):
            image = create_test_image(width=160, height=120, encoding='rgb8',
                                     pattern='gradient' if i % 2 == 0 else 'checkerboard')
            image.header.stamp.sec = i
            compressed = image_to_h264_compressed(image, quality=23)
            writer.write_message('/camera/stream', i * 33_333_333, compressed)  # ~30 FPS

    print(f"   Created: {test_file}")

    # Simulate real-time processing
    print("\n2. Processing stream (simulating real-time decode)...")
    frame_count = 0
    total_compressed = 0
    total_decompressed = 0

    with McapFileReader.from_file(str(test_file)) as reader:
        for msg in reader.messages('/camera/stream'):
            compressed = msg.data

            # Decode frame
            decoded = h264_compressed_to_image(compressed, encoding='rgb8')

            # Update statistics
            frame_count += 1
            total_compressed += len(compressed.data)
            total_decompressed += len(decoded.data)

            # Simulate processing
            if frame_count % 3 == 0:
                print(f"   Frame {frame_count}: {decoded.width}x{decoded.height} @ {msg.log_time}ns")

    print(f"\n   Processed {frame_count} frames")
    print(f"   Total compressed: {total_compressed:,} bytes")
    print(f"   Total decompressed: {total_decompressed:,} bytes")
    print(f"   Compression ratio: {total_decompressed / total_compressed:.2f}x")

    print("\n✓ Streaming decode successful!\n")


def example5_selective_decode():
    """Example 5: Selectively decode only certain frames."""
    print("="*80)
    print("Example 5: Selective decoding (every 5th frame)")
    print("="*80)

    # Create a test MCAP
    test_file = Path(__file__).parent / 'output' / 'test_selective.mcap'
    test_file.parent.mkdir(exist_ok=True)

    print("\n1. Creating test MCAP with 20 frames...")
    with McapFileWriter.open(str(test_file)) as writer:
        for i in range(20):
            image = create_test_image(width=160, height=120, encoding='rgb8', pattern='gradient')
            image.header.stamp.sec = i
            compressed = image_to_h264_compressed(image, quality=23)
            writer.write_message('/camera/all_frames', i * 100_000_000, compressed)

    # Decode only every 5th frame
    print("\n2. Decoding every 5th frame...")
    with McapFileReader.from_file(str(test_file)) as reader:
        frame_num = 0
        for msg in reader.messages('/camera/all_frames'):
            compressed = msg.data

            # Only decode every 5th frame
            if frame_num % 5 == 0:
                decoded = h264_compressed_to_image(compressed, encoding='rgb8')
                print(f"   Frame {frame_num}: Decoded to {decoded.width}x{decoded.height}")
            else:
                # Skip decoding, just read metadata
                print(f"   Frame {frame_num}: Skipped (compressed size: {len(compressed.data)} bytes)")

            frame_num += 1

    print("\n✓ Selective decode successful!\n")


def main():
    """Run all examples."""
    print("\n")
    print("█"*80)
    print("H.264 Decoding Examples for pybag")
    print("█"*80)
    print("\n")

    example1_decode_single_message()
    example2_read_from_mcap()
    example3_decode_to_different_encoding()
    example4_streaming_decode()
    example5_selective_decode()

    print("="*80)
    print("All examples completed successfully!")
    print("="*80)
    print()


if __name__ == '__main__':
    main()
