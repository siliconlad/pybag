"""
Benchmarks for raw MCAP reading comparing pybag, rosbags, and official mcap.

This module tests reading raw message bytes from MCAP files without deserialization,
across different file sizes and topic configurations.
"""

from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

import pytest
from mcap.reader import make_reader
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader

from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import (
    MULTI_TOPIC_CONFIG,
    MULTI_TOPIC_WITH_IMAGES_CONFIG,
    create_multi_topic_mcap,
    create_small_mcap,
    create_test_mcap,
)


def read_raw_with_pybag(mcap: Path) -> Iterator[bytes]:
    with McapRecordReaderFactory.from_file(mcap) as reader:
        for message in reader.get_messages():
            yield message.data


def read_raw_with_rosbags(mcap: Path) -> Iterator[bytes]:
    with AnyReader([mcap.parent]) as reader:
        for _connection, _timestamp, data in reader.messages():
            yield data


def read_raw_with_official(mcap: Path) -> Iterator[bytes]:
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _schema, _channel, message in reader.iter_messages(log_time_order=True):
            yield message.data


def read_raw_with_official_unordered(mcap: Path) -> Iterator[bytes]:
    """Read raw messages without log time ordering (faster)."""
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _schema, _channel, message in reader.iter_messages(log_time_order=False):
            yield message.data


# ============================================================================
# Standard Raw Read Benchmarks (1000 messages, single topic)
# ============================================================================


class TestReadRawStandard:
    """Raw read benchmarks with standard 1000 message MCAP file."""

    def test_pybag_read_raw(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_raw_with_pybag(mcap), maxlen=0))

    def test_rosbags_read_raw(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_raw_with_rosbags(mcap), maxlen=0))

    def test_official_read_raw_ordered(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_raw_with_official(mcap), maxlen=0))

    def test_official_read_raw_unordered(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_raw_with_official_unordered(mcap), maxlen=0))


# ============================================================================
# Small MCAP Raw Read Benchmarks (10 messages)
# ============================================================================


class TestReadRawSmall:
    """Raw read benchmarks with small 10 message MCAP file."""

    def test_pybag_read_raw_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_raw_with_pybag(mcap), maxlen=0))

    def test_rosbags_read_raw_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_raw_with_rosbags(mcap), maxlen=0))

    def test_official_read_raw_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_raw_with_official(mcap), maxlen=0))


# ============================================================================
# Multi-Topic Raw Read Benchmarks (6 topics)
# ============================================================================


class TestReadRawMultiTopic:
    """Raw read benchmarks with multi-topic MCAP file (1 second duration)."""

    def test_pybag_read_raw_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_raw_with_pybag(mcap), maxlen=0))

    def test_rosbags_read_raw_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_raw_with_rosbags(mcap), maxlen=0))

    def test_official_read_raw_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_raw_with_official(mcap), maxlen=0))


# ============================================================================
# Multi-Topic with Images Raw Read Benchmarks (7 topics with large data)
# ============================================================================


class TestReadRawMultiTopicWithImages:
    """Raw read benchmarks with multi-topic MCAP including images and point clouds."""

    @pytest.fixture
    def mcap_path(self, tmp_path: Path) -> Path:
        return create_multi_topic_mcap(
            tmp_path / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_WITH_IMAGES_CONFIG
        )

    def test_pybag_read_raw_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_pybag(mcap_path), maxlen=0))

    def test_rosbags_read_raw_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_rosbags(mcap_path), maxlen=0))

    def test_official_read_raw_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_official(mcap_path), maxlen=0))


# ============================================================================
# Extended Duration Multi-Topic Raw Read Benchmarks (10 seconds)
# ============================================================================


class TestReadRawMultiTopicExtended:
    """Raw read benchmarks with multi-topic MCAP file (10 second duration)."""

    @pytest.fixture
    def mcap_path(self, tmp_path: Path) -> Path:
        return create_multi_topic_mcap(
            tmp_path / "test", duration_sec=10.0, topic_config=MULTI_TOPIC_CONFIG
        )

    def test_pybag_read_raw_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_pybag(mcap_path), maxlen=0))

    def test_rosbags_read_raw_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_rosbags(mcap_path), maxlen=0))

    def test_official_read_raw_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_raw_with_official(mcap_path), maxlen=0))
