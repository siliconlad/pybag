"""
Benchmarks using real-world MCAP files from HuggingFace datasets.

This module downloads and benchmarks against real MCAP files from:
https://huggingface.co/datasets/DapengFeng/MCAP

These benchmarks test performance on diverse, real-world ROS2 bag files
with multiple topics and message types.
"""

import hashlib
import logging
import os
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any, Iterator

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader
from pybag.mcap.record_reader import McapRecordReaderFactory


logger = logging.getLogger(__name__)

# HuggingFace dataset base URL
HF_BASE_URL = "https://huggingface.co/datasets/DapengFeng/MCAP/resolve/main"

# Sample MCAP files from the dataset
# Format: (filename, expected_sha256, description)
SAMPLE_MCAP_FILES = [
    (
        "demo_lidar_2.mcap",
        None,  # SHA256 to be computed on first download
        "LiDAR point cloud data sample",
    ),
]


def get_cache_dir() -> Path:
    """Get or create the cache directory for downloaded files."""
    cache_dir = Path.home() / ".cache" / "pybag_benchmarks"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def download_file(url: str, dest_path: Path) -> None:
    """Download a file from URL to destination path."""
    logger.info(f"Downloading {url} to {dest_path}")
    urllib.request.urlretrieve(url, dest_path)


def get_mcap_file(filename: str) -> Path:
    """
    Get an MCAP file from the HuggingFace dataset.

    Downloads and caches the file if not already present.

    Args:
        filename: Name of the MCAP file to download

    Returns:
        Path to the local MCAP file
    """
    cache_dir = get_cache_dir()
    local_path = cache_dir / filename

    if not local_path.exists():
        url = f"{HF_BASE_URL}/{filename}"
        try:
            download_file(url, local_path)
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            raise

    return local_path


# ============================================================================
# Helper Functions for Reading
# ============================================================================


def read_decoded_with_pybag(mcap: Path) -> Iterator[Any]:
    """Read decoded messages using pybag."""
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                yield message.data


def read_decoded_with_rosbags(mcap: Path) -> Iterator[Any]:
    """Read decoded messages using rosbags."""
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([mcap.parent]) as reader:
        for conn, _, data in reader.messages():
            yield typestore.deserialize_cdr(data, conn.msgtype)


def read_decoded_with_official(mcap: Path) -> Iterator[Any]:
    """Read decoded messages using official mcap library."""
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, ros_msg in reader.iter_decoded_messages(log_time_order=False):
            yield ros_msg


def read_raw_with_pybag(mcap: Path) -> Iterator[bytes]:
    """Read raw messages using pybag."""
    with McapRecordReaderFactory.from_file(mcap) as reader:
        for message in reader.get_messages():
            yield message.data


def read_raw_with_rosbags(mcap: Path) -> Iterator[bytes]:
    """Read raw messages using rosbags."""
    with AnyReader([mcap.parent]) as reader:
        for _, _, data in reader.messages():
            yield data


def read_raw_with_official(mcap: Path) -> Iterator[bytes]:
    """Read raw messages using official mcap library."""
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _, _, message in reader.iter_messages(log_time_order=False):
            yield message.data


def count_messages_pybag(mcap: Path) -> int:
    """Count messages using pybag."""
    count = 0
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            count += reader.get_message_count(topic)
    return count


def count_messages_official(mcap: Path) -> int:
    """Count messages using official mcap library."""
    count = 0
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        if summary and summary.statistics:
            count = summary.statistics.message_count
    return count


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def lidar_mcap() -> Path | None:
    """Fixture to download and provide a LiDAR MCAP file."""
    try:
        return get_mcap_file("demo_lidar_2.mcap")
    except Exception as e:
        logger.warning(f"Could not download HuggingFace MCAP: {e}")
        pytest.skip("HuggingFace MCAP file not available")
        return None


# ============================================================================
# Benchmarks for HuggingFace MCAP Files - Decoded Reading
# ============================================================================


class TestHuggingFaceDecodedRead:
    """Benchmarks for reading decoded messages from HuggingFace MCAP files."""

    def test_pybag_read_decoded_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_decoded_with_pybag(lidar_mcap), maxlen=0))

    def test_rosbags_read_decoded_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_decoded_with_rosbags(lidar_mcap), maxlen=0))

    def test_official_read_decoded_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_decoded_with_official(lidar_mcap), maxlen=0))


# ============================================================================
# Benchmarks for HuggingFace MCAP Files - Raw Reading
# ============================================================================


class TestHuggingFaceRawRead:
    """Benchmarks for reading raw messages from HuggingFace MCAP files."""

    def test_pybag_read_raw_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_raw_with_pybag(lidar_mcap), maxlen=0))

    def test_rosbags_read_raw_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_raw_with_rosbags(lidar_mcap), maxlen=0))

    def test_official_read_raw_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: deque(read_raw_with_official(lidar_mcap), maxlen=0))


# ============================================================================
# Benchmarks for Message Counting
# ============================================================================


class TestHuggingFaceMetadata:
    """Benchmarks for reading metadata from HuggingFace MCAP files."""

    def test_pybag_count_messages_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: count_messages_pybag(lidar_mcap))

    def test_official_count_messages_hf(
        self, benchmark: BenchmarkFixture, lidar_mcap: Path | None
    ) -> None:
        if lidar_mcap is None:
            pytest.skip("MCAP file not available")
        benchmark(lambda: count_messages_official(lidar_mcap))


# ============================================================================
# Test MCAP File Info (not a benchmark, just informational)
# ============================================================================


def test_print_hf_mcap_info(lidar_mcap: Path | None) -> None:
    """Print information about the HuggingFace MCAP file."""
    if lidar_mcap is None:
        pytest.skip("MCAP file not available")

    logger.info(f"MCAP file: {lidar_mcap}")
    logger.info(f"File size: {lidar_mcap.stat().st_size / (1024*1024):.2f} MB")

    # Get info using pybag
    with McapFileReader.from_file(lidar_mcap) as reader:
        topics = reader.get_topics()
        logger.info(f"Number of topics: {len(topics)}")
        for topic in topics:
            count = reader.get_message_count(topic)
            logger.info(f"  {topic}: {count} messages")
