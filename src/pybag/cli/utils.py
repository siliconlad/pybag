"""Shared utilities for CLI commands."""

import logging
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

# Magic bytes for format detection
MCAP_MAGIC = b'\x89MCAP'
BAG_MAGIC = b'#ROSBAG V'


def get_file_format(input_path: Path) -> Literal["mcap", "bag"]:
    """Detect file format based on file extension.

    Args:
        input_path: Path to the input file.

    Returns:
        The detected format ('mcap' or 'bag').

    Raises:
        ValueError: If the file extension is not recognized.
    """
    suffix = input_path.suffix.lower()
    if suffix == ".mcap":
        return "mcap"
    elif suffix == ".bag":
        return "bag"
    else:
        raise ValueError(
            f"Unsupported file format: {suffix}. Expected .mcap or .bag"
        )


def get_file_format_from_magic(file_path: Path) -> Literal["mcap", "bag"]:
    """Detect file format from magic bytes with extension fallback.

    This function first tries to detect the format using magic bytes,
    which is more reliable for potentially corrupted files. If magic
    bytes detection fails, it falls back to extension-based detection.

    Args:
        file_path: Path to the file.

    Returns:
        The detected format ('mcap' or 'bag').

    Raises:
        ValueError: If the file format cannot be determined.
    """
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(9)  # Enough for both formats

        if magic.startswith(MCAP_MAGIC):
            return "mcap"
        elif magic.startswith(BAG_MAGIC):
            return "bag"
    except (OSError, IOError):
        pass  # Fall back to extension-based detection

    # Fallback to extension-based detection
    return get_file_format(file_path)


def validate_compression_for_bag(
    compression: Literal["lz4", "zstd", "none", "bz2"] | None
) -> Literal["none", "bz2"] | None:
    """Map MCAP compression options to bag-compatible compression.

    Bag files only support 'none' or 'bz2' compression. This function maps
    MCAP-specific compression options to their bag equivalents:
    - 'lz4' -> 'none' (not supported, with warning)
    - 'zstd' -> 'none' (not supported, with warning)
    - 'none' -> 'none'
    - 'bz2' -> 'bz2'
    - None -> None

    Args:
        compression: The requested compression option.

    Returns:
        A bag-compatible compression option.
    """
    if compression is None:
        return None
    if compression in ("lz4", "zstd"):
        raise ValueError(
            f"Compression '{compression}' not supported for bag files. "
            "Bag files support 'none' or 'bz2'."
        )
    return compression  # 'none' or 'bz2'


def validate_compression_for_mcap(
    compression: Literal["lz4", "zstd", "none", "bz2"] | None
) -> Literal["none", "lz4", "zstd"] | None:
    """Validate and map compression options for MCAP files.

    MCAP files only support 'lz4' or 'zstd' compression (or None for default).
    This function validates the compression option and raises an error for
    bag-only compression options.

    Args:
        compression: The requested compression option.

    Returns:
        An MCAP-compatible compression option, or None for default.

    Raises:
        ValueError: If compression is 'none' or 'bz2' (bag-only options).
    """
    if compression is None:
        return None
    if compression == "bz2":
        raise ValueError(
            f"Compression '{compression}' is not supported for MCAP files. "
            "MCAP files support 'none', 'lz4', or 'zstd' compression."
        )
    return compression  # 'none', 'lz4', or 'zstd'
