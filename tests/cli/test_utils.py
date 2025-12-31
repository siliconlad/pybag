from pathlib import Path

import pytest

from pybag.cli.utils import (
    get_file_format,
    validate_compression_for_bag,
    validate_compression_for_mcap
)

# =============================================================================
# Format detection tests
# =============================================================================

def test_get_file_format_mcap() -> None:
    """Test file format detection for MCAP files."""
    assert get_file_format(Path("test.mcap")) == "mcap"
    assert get_file_format(Path("test.MCAP")) == "mcap"
    assert get_file_format(Path("/path/to/file.mcap")) == "mcap"


def test_get_file_format_bag() -> None:
    """Test file format detection for bag files."""
    assert get_file_format(Path("test.bag")) == "bag"
    assert get_file_format(Path("test.BAG")) == "bag"
    assert get_file_format(Path("/path/to/file.bag")) == "bag"


def test_get_file_format_unsupported() -> None:
    """Test file format detection for unsupported formats."""
    with pytest.raises(ValueError, match="Unsupported file format"):
        get_file_format(Path("test.txt"))

    with pytest.raises(ValueError, match="Unsupported file format"):
        get_file_format(Path("test.rosbag"))


def test_map_compression_for_bag() -> None:
    """Test compression mapping for bag files."""
    # Supported compressions pass through
    assert validate_compression_for_bag(None) is None
    assert validate_compression_for_bag("none") == "none"
    assert validate_compression_for_bag("bz2") == "bz2"

    # Unsupported compressions fall back to 'none'
    with pytest.raises(ValueError, match="not supported"):
        assert validate_compression_for_bag("lz4")
    with pytest.raises(ValueError, match="not supported"):
        assert validate_compression_for_bag("zstd")


def test_map_compression_for_mcap() -> None:
    """Test compression mapping for bag files."""
    # Supported compressions pass through
    assert validate_compression_for_mcap(None) is None
    assert validate_compression_for_mcap("none") == "none"
    assert validate_compression_for_mcap("lz4") == "lz4"
    assert validate_compression_for_mcap("zstd") == "zstd"

    with pytest.raises(ValueError, match="not supported"):
        assert validate_compression_for_mcap("bz2") == "bz2"
