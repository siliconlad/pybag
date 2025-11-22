"""
Comparison of Image vs CompressedImage with H.264 encoding

This script demonstrates the space savings achieved by using H.264 compression
for image messages in MCAP files.
"""

import os
import sys
from pathlib import Path

# Add src to path so we can import pybag
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import sensor_msgs
from h264_compression import image_to_h264_compressed, create_test_image


def write_raw_images(output_path: str, num_images: int = 100, pattern: str = 'gradient'):
    """Write raw Image messages to MCAP file."""
    print(f"Writing {num_images} raw images with {pattern} pattern...")

    with McapFileWriter.open(output_path) as writer:
        for i in range(num_images):
            # Create test image
            image = create_test_image(
                width=640,
                height=480,
                encoding='rgb8',
                pattern=pattern
            )

            # Update timestamp
            image.header.stamp.sec = i
            image.header.stamp.nanosec = 0

            # Write message
            writer.write_message('/camera/image_raw', i * 100_000_000, image)

    file_size = os.path.getsize(output_path)
    print(f"  File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    return file_size


def write_compressed_images(
    output_path: str,
    num_images: int = 100,
    pattern: str = 'gradient',
    quality: int = 23,
    preset: str = 'medium'
):
    """Write CompressedImage messages with H.264 encoding to MCAP file."""
    print(f"Writing {num_images} H.264 compressed images (CRF={quality}, preset={preset})...")

    with McapFileWriter.open(output_path) as writer:
        for i in range(num_images):
            # Create test image
            image = create_test_image(
                width=640,
                height=480,
                encoding='rgb8',
                pattern=pattern
            )

            # Update timestamp
            image.header.stamp.sec = i
            image.header.stamp.nanosec = 0

            # Compress to H.264
            compressed = image_to_h264_compressed(image, quality=quality, preset=preset)

            # Write compressed message
            writer.write_message('/camera/image_compressed', i * 100_000_000, compressed)

    file_size = os.path.getsize(output_path)
    print(f"  File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    return file_size


def compare_compression(pattern: str = 'gradient', num_images: int = 100):
    """Compare raw and compressed image storage."""
    print(f"\n{'='*80}")
    print(f"Comparing Image vs CompressedImage (H.264) - Pattern: {pattern}")
    print(f"{'='*80}\n")

    # Create output directory
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)

    # File paths
    raw_path = output_dir / f'raw_images_{pattern}.mcap'
    compressed_path = output_dir / f'compressed_images_{pattern}.mcap'

    # Write raw images
    raw_size = write_raw_images(str(raw_path), num_images, pattern)

    # Write compressed images with different quality settings
    print()
    compressed_sizes = {}

    for quality in [18, 23, 28]:
        comp_path = output_dir / f'compressed_images_{pattern}_crf{quality}.mcap'
        compressed_sizes[quality] = write_compressed_images(
            str(comp_path), num_images, pattern, quality=quality
        )
        print()

    # Calculate and display results
    print(f"\n{'='*80}")
    print("COMPRESSION RESULTS")
    print(f"{'='*80}\n")
    print(f"Pattern: {pattern}")
    print(f"Number of images: {num_images}")
    print(f"Image size: 640x480 RGB8 = {640 * 480 * 3:,} bytes per image\n")

    print(f"Raw Image messages:")
    print(f"  Total size: {raw_size:,} bytes ({raw_size / 1024 / 1024:.2f} MB)")
    print(f"  Avg per image: {raw_size / num_images:,.0f} bytes\n")

    for quality, comp_size in compressed_sizes.items():
        compression_ratio = raw_size / comp_size
        space_saved = raw_size - comp_size
        percent_saved = (space_saved / raw_size) * 100

        print(f"H.264 CompressedImage (CRF={quality}):")
        print(f"  Total size: {comp_size:,} bytes ({comp_size / 1024 / 1024:.2f} MB)")
        print(f"  Avg per image: {comp_size / num_images:,.0f} bytes")
        print(f"  Compression ratio: {compression_ratio:.2f}x")
        print(f"  Space saved: {space_saved:,} bytes ({percent_saved:.1f}%)")
        print()

    # Test with MCAP chunk compression enabled
    print(f"{'='*80}")
    print("WITH MCAP CHUNK COMPRESSION (ZSTD)")
    print(f"{'='*80}\n")

    # Note: McapFileWriter in pybag supports chunk compression via the chunk_compression parameter
    # Let's test with chunk compression enabled

    raw_zstd_path = output_dir / f'raw_images_{pattern}_zstd.mcap'
    compressed_zstd_path = output_dir / f'compressed_images_{pattern}_zstd.mcap'

    print("Raw images with ZSTD chunk compression:")
    # Write with chunk compression
    with McapFileWriter.open(str(raw_zstd_path), chunk_compression='zstd') as writer:
        for i in range(num_images):
            image = create_test_image(width=640, height=480, encoding='rgb8', pattern=pattern)
            image.header.stamp.sec = i
            writer.write_message('/camera/image_raw', i * 100_000_000, image)
    raw_zstd_size = os.path.getsize(raw_zstd_path)
    print(f"  File size: {raw_zstd_size:,} bytes ({raw_zstd_size / 1024 / 1024:.2f} MB)")

    print("\nH.264 compressed images with ZSTD chunk compression:")
    with McapFileWriter.open(str(compressed_zstd_path), chunk_compression='zstd') as writer:
        for i in range(num_images):
            image = create_test_image(width=640, height=480, encoding='rgb8', pattern=pattern)
            image.header.stamp.sec = i
            compressed = image_to_h264_compressed(image, quality=23)
            writer.write_message('/camera/image_compressed', i * 100_000_000, compressed)
    compressed_zstd_size = os.path.getsize(compressed_zstd_path)
    print(f"  File size: {compressed_zstd_size:,} bytes ({compressed_zstd_size / 1024 / 1024:.2f} MB)")

    print(f"\nComparison:")
    zstd_compression = raw_zstd_size / compressed_zstd_size
    zstd_saved = raw_zstd_size - compressed_zstd_size
    zstd_percent = (zstd_saved / raw_zstd_size) * 100
    print(f"  Compression ratio: {zstd_compression:.2f}x")
    print(f"  Space saved: {zstd_saved:,} bytes ({zstd_percent:.1f}%)")

    print(f"\n{'='*80}\n")


def main():
    """Run comparisons for different image patterns."""
    patterns = ['gradient', 'checkerboard', 'noise', 'solid']

    for pattern in patterns:
        compare_compression(pattern=pattern, num_images=100)


if __name__ == '__main__':
    main()
