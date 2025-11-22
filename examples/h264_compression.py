"""
H.264 Image Compression Utility for pybag

This module provides utilities to compress sensor_msgs/Image messages
to sensor_msgs/CompressedImage using H.264 encoding, and decompress them back.
"""

import io
from typing import Optional

import av
import numpy as np

from pybag.ros2.humble import sensor_msgs, std_msgs, builtin_interfaces


def image_to_h264_compressed(
    image: sensor_msgs.Image,
    quality: int = 23,  # CRF value: 0 (best) to 51 (worst), 23 is default
    preset: str = 'medium'  # ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow
) -> sensor_msgs.CompressedImage:
    """
    Convert a sensor_msgs/Image to CompressedImage using H.264 encoding.

    Args:
        image: The raw image message to compress
        quality: CRF (Constant Rate Factor) value for H.264 encoding.
                 Lower values = better quality, larger file size.
                 Range: 0-51, default: 23
        preset: Encoding speed preset. Slower presets provide better compression.
                Options: ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow

    Returns:
        CompressedImage message with H.264 encoded data
    """
    # Convert image data to numpy array
    img_array = np.array(image.data, dtype=np.uint8)

    # Reshape based on image dimensions and encoding
    if image.encoding == 'rgb8':
        img_array = img_array.reshape((image.height, image.width, 3))
        pix_fmt = 'rgb24'
    elif image.encoding == 'bgr8':
        img_array = img_array.reshape((image.height, image.width, 3))
        pix_fmt = 'bgr24'
    elif image.encoding == 'rgba8':
        img_array = img_array.reshape((image.height, image.width, 4))
        pix_fmt = 'rgba'
    elif image.encoding == 'bgra8':
        img_array = img_array.reshape((image.height, image.width, 4))
        pix_fmt = 'bgra'
    elif image.encoding == 'mono8':
        img_array = img_array.reshape((image.height, image.width))
        pix_fmt = 'gray'
    else:
        raise ValueError(f"Unsupported encoding: {image.encoding}")

    # Create in-memory output buffer
    output = io.BytesIO()

    # Create output container
    container = av.open(output, mode='w', format='h264')

    # Add video stream with H.264 codec
    stream = container.add_stream('libx264', rate=1)
    stream.width = image.width
    stream.height = image.height
    stream.pix_fmt = 'yuv420p'  # H.264 uses YUV420P internally

    # Set encoding options
    stream.options = {
        'crf': str(quality),
        'preset': preset,
    }

    # Create frame from numpy array
    frame = av.VideoFrame.from_ndarray(img_array, format=pix_fmt)

    # Encode frame
    for packet in stream.encode(frame):
        container.mux(packet)

    # Flush encoder
    for packet in stream.encode():
        container.mux(packet)

    # Close container
    container.close()

    # Get compressed data
    compressed_data = output.getvalue()

    # Create CompressedImage message
    compressed_image = sensor_msgs.CompressedImage(
        header=image.header,
        format='h264',
        data=list(compressed_data)
    )

    return compressed_image


def h264_compressed_to_image(
    compressed: sensor_msgs.CompressedImage,
    encoding: str = 'rgb8'
) -> sensor_msgs.Image:
    """
    Decompress a CompressedImage with H.264 format back to raw Image.

    Args:
        compressed: The compressed image message
        encoding: Desired output encoding (rgb8, bgr8, etc.)

    Returns:
        Decompressed Image message
    """
    if compressed.format != 'h264':
        raise ValueError(f"Expected h264 format, got {compressed.format}")

    # Convert data list to bytes
    compressed_bytes = bytes(compressed.data)

    # Create in-memory input buffer
    input_buffer = io.BytesIO(compressed_bytes)

    # Open container for reading
    container = av.open(input_buffer, mode='r', format='h264')

    # Decode first frame
    frame = None
    for frame in container.decode(video=0):
        break  # Only need first frame

    if frame is None:
        raise ValueError("No frame found in H.264 data")

    # Convert frame to numpy array
    if encoding == 'rgb8':
        img_array = frame.to_ndarray(format='rgb24')
    elif encoding == 'bgr8':
        img_array = frame.to_ndarray(format='bgr24')
    elif encoding == 'rgba8':
        img_array = frame.to_ndarray(format='rgba')
    elif encoding == 'bgra8':
        img_array = frame.to_ndarray(format='bgra')
    elif encoding == 'mono8':
        img_array = frame.to_ndarray(format='gray')
    else:
        raise ValueError(f"Unsupported encoding: {encoding}")

    # Get image properties
    height, width = img_array.shape[:2]

    # Calculate step (bytes per row)
    if encoding in ['rgb8', 'bgr8']:
        step = width * 3
    elif encoding in ['rgba8', 'bgra8']:
        step = width * 4
    elif encoding == 'mono8':
        step = width
    else:
        step = width * 3  # default

    # Flatten array to list
    data = img_array.flatten().tolist()

    # Create Image message
    image = sensor_msgs.Image(
        header=compressed.header,
        height=height,
        width=width,
        encoding=encoding,
        is_bigendian=0,
        step=step,
        data=data
    )

    container.close()

    return image


def create_test_image(
    width: int = 640,
    height: int = 480,
    encoding: str = 'rgb8',
    pattern: str = 'gradient'
) -> sensor_msgs.Image:
    """
    Create a test image with various patterns.

    Args:
        width: Image width
        height: Image height
        encoding: Image encoding (rgb8, bgr8, mono8)
        pattern: Pattern type ('gradient', 'checkerboard', 'noise', 'solid')

    Returns:
        Test Image message
    """
    # Create pattern
    if pattern == 'gradient':
        # Horizontal gradient
        img = np.zeros((height, width, 3), dtype=np.uint8)
        for x in range(width):
            img[:, x, 0] = int(255 * x / width)  # Red gradient
            img[:, x, 1] = int(255 * (1 - x / width))  # Green inverse gradient
            img[:, x, 2] = 128  # Constant blue
    elif pattern == 'checkerboard':
        # 8x8 checkerboard
        img = np.zeros((height, width, 3), dtype=np.uint8)
        square_size = min(height, width) // 8
        for i in range(0, height, square_size):
            for j in range(0, width, square_size):
                if (i // square_size + j // square_size) % 2 == 0:
                    img[i:i+square_size, j:j+square_size] = 255
    elif pattern == 'noise':
        # Random noise
        img = np.random.randint(0, 256, (height, width, 3), dtype=np.uint8)
    elif pattern == 'solid':
        # Solid color (blue)
        img = np.full((height, width, 3), [0, 0, 255], dtype=np.uint8)
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    # Convert based on encoding
    if encoding == 'bgr8':
        img = img[:, :, ::-1]  # Swap RGB to BGR
    elif encoding == 'mono8':
        img = np.mean(img, axis=2).astype(np.uint8)
        step = width
        data = img.flatten().tolist()
        return sensor_msgs.Image(
            header=std_msgs.Header(
                stamp=builtin_interfaces.Time(sec=0, nanosec=0),
                frame_id='camera'
            ),
            height=height,
            width=width,
            encoding='mono8',
            is_bigendian=0,
            step=step,
            data=data
        )

    step = width * 3
    data = img.flatten().tolist()

    return sensor_msgs.Image(
        header=std_msgs.Header(
            stamp=builtin_interfaces.Time(sec=0, nanosec=0),
            frame_id='camera'
        ),
        height=height,
        width=width,
        encoding=encoding,
        is_bigendian=0,
        step=step,
        data=data
    )
