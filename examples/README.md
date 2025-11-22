# H.264 Image Compression for pybag

This directory contains utilities and examples for using H.264 compression with sensor_msgs/Image and sensor_msgs/CompressedImage in pybag.

## Overview

H.264 compression can dramatically reduce the storage space required for image data in MCAP files. The compression ratio depends on the image content:

- **Gradient patterns**: 189x - 299x compression (99.5% - 99.7% space savings)
- **Checkerboard patterns**: 363x - 403x compression (99.7% - 99.8% space savings)
- **Noise (random)**: 3x - 4.5x compression (66.6% - 77.9% space savings)
- **Solid colors**: 700x - 711x compression (99.9% space savings)

## Files

- `h264_compression.py` - Core utilities for H.264 compression/decompression
- `reencode_mcap.py` - Command-line tool to re-encode image topics with H.264
- `decode_mcap.py` - Command-line tool to decode H.264 images back to raw Image messages
- `decode_examples.py` - Examples demonstrating various decoding techniques
- `compare_image_compression.py` - Demonstration script comparing Image vs CompressedImage
- `test_reencode.py` - Test script for the reencode_mcap utility

## Installation

Install the required dependencies:

```bash
pip install av numpy
pip install -e .  # Install pybag
```

## Usage

### Re-encode Existing MCAP Files (Quick Start)

The easiest way to compress image topics in existing MCAP files is to use the command-line tool:

```bash
# Re-encode a single image topic
python reencode_mcap.py input.mcap output.mcap /camera/image_raw

# Re-encode with custom quality (lower = better quality)
python reencode_mcap.py input.mcap output.mcap /camera/image_raw --quality 18

# Re-encode to a different topic name
python reencode_mcap.py input.mcap output.mcap /camera/image_raw --output-topic /camera/h264

# Re-encode with MCAP chunk compression
python reencode_mcap.py input.mcap output.mcap /camera/image_raw --chunk-compression zstd
```

**Example:**
```bash
cd examples
python reencode_mcap.py my_bag.mcap my_bag_compressed.mcap /camera/image_raw
```

This will:
1. Read all messages from `my_bag.mcap`
2. Re-encode `/camera/image_raw` (Image) to H.264 CompressedImage
3. Copy all other topics unchanged
4. Write to `my_bag_compressed.mcap`

Typical results: **99.5% file size reduction** for camera images

### Programmatic Re-encoding

```python
from reencode_mcap import reencode_image_topic

stats = reencode_image_topic(
    input_mcap='input.mcap',
    output_mcap='output.mcap',
    image_topic='/camera/image_raw',
    quality=23,           # CRF: 0-51 (lower is better)
    preset='medium',      # Encoding speed
    output_topic=None,    # Use same topic name
    chunk_compression=None,  # Optional: 'lz4' or 'zstd'
    verbose=True
)

print(f"Compression ratio: {stats['compression_ratio']:.2f}x")
print(f"Space saved: {stats['percent_saved']:.1f}%")
```

### Basic Compression

```python
from pybag.ros2.humble import sensor_msgs
from h264_compression import image_to_h264_compressed, h264_compressed_to_image

# Compress an Image to CompressedImage with H.264
compressed = image_to_h264_compressed(
    image,
    quality=23,      # CRF value: 0 (best) to 51 (worst)
    preset='medium'  # Encoding speed: ultrafast, fast, medium, slow, veryslow
)

# Decompress back to raw Image
decompressed = h264_compressed_to_image(compressed, encoding='rgb8')
```

### Writing Compressed Images to MCAP

```python
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import sensor_msgs, std_msgs, builtin_interfaces
from h264_compression import image_to_h264_compressed

# Create test image
image = sensor_msgs.Image(
    header=std_msgs.Header(
        stamp=builtin_interfaces.Time(sec=0, nanosec=0),
        frame_id='camera'
    ),
    height=480,
    width=640,
    encoding='rgb8',
    is_bigendian=0,
    step=1920,
    data=[...]  # Your image data
)

# Compress and write
with McapFileWriter.open('output.mcap') as writer:
    compressed = image_to_h264_compressed(image, quality=23)
    writer.write_message('/camera/image_compressed', 0, compressed)
```

### Create Test Images

```python
from h264_compression import create_test_image

# Create various test patterns
gradient_img = create_test_image(pattern='gradient')
checkerboard_img = create_test_image(pattern='checkerboard')
noise_img = create_test_image(pattern='noise')
solid_img = create_test_image(pattern='solid')
```

## Decoding H.264 Compressed Images

### Decode Entire MCAP Files (Quick Start)

Use the command-line tool to decode H.264 compressed topics back to raw images:

```bash
# Decode a compressed topic back to raw images
python decode_mcap.py compressed.mcap decoded.mcap /camera/image_compressed

# Decode with specific output encoding
python decode_mcap.py compressed.mcap decoded.mcap /camera/h264 --encoding bgr8

# Decode to a different topic name
python decode_mcap.py compressed.mcap decoded.mcap /camera/h264 --output-topic /camera/image_raw
```

**Example workflow:**
```bash
# 1. Compress images
python reencode_mcap.py original.mcap compressed.mcap /camera/image_raw

# 2. Later, decode them back
python decode_mcap.py compressed.mcap decoded.mcap /camera/image_raw
```

### Programmatic Decoding

```python
from decode_mcap import decode_compressed_topic

stats = decode_compressed_topic(
    input_mcap='compressed.mcap',
    output_mcap='decoded.mcap',
    compressed_topic='/camera/image_compressed',
    output_encoding='rgb8',   # rgb8, bgr8, mono8, etc.
    verbose=True
)

print(f"Decoded {stats['decoded_messages']} images")
```

### Decode Individual Messages

```python
from pybag.mcap_reader import McapFileReader
from h264_compression import h264_compressed_to_image

# Read and decode H.264 compressed images
with McapFileReader.from_file('compressed.mcap') as reader:
    for msg in reader.messages('/camera/image_compressed'):
        compressed = msg.data  # CompressedImage message

        # Decode to raw Image
        decoded = h264_compressed_to_image(compressed, encoding='rgb8')

        print(f"Decoded: {decoded.width}x{decoded.height} {decoded.encoding}")
        # Now you can use decoded.data (raw pixel data)
```

### Decode Examples

Run the comprehensive examples demonstrating various decoding techniques:

```bash
cd examples
python decode_examples.py
```

This demonstrates:
1. **Single message decode** - Basic compression/decompression
2. **Reading from MCAP** - Decode entire files
3. **Multiple encodings** - Decode to rgb8, bgr8, mono8
4. **Streaming decode** - Process frames in real-time
5. **Selective decode** - Decode only certain frames (e.g., keyframes)

## Running the Comparison Demo

```bash
cd examples
python compare_image_compression.py
```

This will:
1. Generate 100 test images with different patterns
2. Write them as both raw Image and H.264 CompressedImage messages
3. Compare file sizes and compression ratios
4. Test with and without MCAP chunk compression (ZSTD)
5. Output results to the `output/` directory

## Quality Settings

### CRF (Constant Rate Factor)
- **Lower values** = better quality, larger file size
- **Higher values** = lower quality, smaller file size
- **Range**: 0-51
- **Recommended**: 18 (visually lossless), 23 (default), 28 (high compression)

### Preset (Encoding Speed)
- **Slower presets** = better compression, slower encoding
- **Faster presets** = larger files, faster encoding
- **Options**: ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow
- **Recommended**: medium (good balance)

## When to Use H.264 Compression

### Good Use Cases
- Camera image streams with structured content
- Large datasets with many similar frames
- Storage-constrained environments
- Long-term archival of camera data

### Poor Use Cases
- Random noise images (low compression ratio)
- When lossless compression is required
- Real-time processing with CPU constraints (encoding can be slow)

## Combining with MCAP Chunk Compression

pybag supports MCAP-level chunk compression (LZ4, ZSTD). For H.264 compressed images, chunk compression provides minimal additional benefit since the data is already compressed. However, for raw images:

- Raw images without chunk compression: 88 MB
- Raw images with ZSTD chunk compression: 88 MB (no improvement for uncompressible random test data)
- H.264 compressed images: 0.4 MB (220x compression)

The best strategy is to use H.264 compression for the image data itself rather than relying on chunk compression.

## Performance Notes

- H.264 encoding is CPU-intensive, especially for high-quality settings
- Decoding is generally faster than encoding
- The `preset` parameter affects encoding speed significantly:
  - `ultrafast`: ~10x faster encoding, ~20% larger files
  - `medium`: balanced (default)
  - `veryslow`: ~5x slower encoding, ~5-10% smaller files

## Supported Encodings

- `rgb8` - 8-bit RGB
- `bgr8` - 8-bit BGR
- `rgba8` - 8-bit RGBA
- `bgra8` - 8-bit BGRA
- `mono8` - 8-bit grayscale

## Example Results

### Gradient Pattern (100 images, 640x480 RGB8)

| Method | File Size | Compression Ratio | Space Saved |
|--------|-----------|-------------------|-------------|
| Raw Image | 87.90 MB | 1.0x | 0% |
| H.264 (CRF=18) | 0.46 MB | 189x | 99.5% |
| H.264 (CRF=23) | 0.40 MB | 220x | 99.5% |
| H.264 (CRF=28) | 0.29 MB | 299x | 99.7% |

### Solid Color Pattern (100 images, 640x480 RGB8)

| Method | File Size | Compression Ratio | Space Saved |
|--------|-----------|-------------------|-------------|
| Raw Image | 87.90 MB | 1.0x | 0% |
| H.264 (CRF=23) | 0.12 MB | 707x | 99.9% |

## License

This example code is provided under the same license as pybag-sdk.
