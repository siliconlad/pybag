"""
Benchmarks for decoded MCAP reading comparing pybag, rosbags, and official mcap-ros2.

This module tests reading messages from MCAP files with full deserialization,
across different file sizes and topic configurations.
"""

from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader

from .benchmark_utils import (
    MULTI_TOPIC_CONFIG,
    MULTI_TOPIC_WITH_IMAGES_CONFIG,
    create_multi_topic_mcap,
    create_small_mcap,
    create_test_mcap,
)


def read_with_pybag(mcap: Path) -> Iterator[Any]:
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                yield message.data


def read_with_rosbags(mcap: Path) -> Iterator[Any]:
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([mcap.parent]) as reader:
        for conn, _, data in reader.messages():
            yield typestore.deserialize_cdr(data, conn.msgtype)


def read_with_official(mcap: Path) -> Iterator[Any]:
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, ros_msg in reader.iter_decoded_messages(log_time_order=False):
            yield ros_msg


# ============================================================================
# Standard Read Benchmarks (1000 messages, single topic)
# ============================================================================


class TestReadStandard:
    """Read benchmarks with standard 1000 message MCAP file."""

    def test_pybag_read(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_with_pybag(mcap), maxlen=0))

    def test_rosbags_read(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_with_rosbags(mcap), maxlen=0))

    def test_official_read(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_test_mcap(Path(tmpdir) / "test")
            benchmark(lambda: deque(read_with_official(mcap), maxlen=0))


# ============================================================================
# Small MCAP Read Benchmarks (10 messages)
# ============================================================================


class TestReadSmall:
    """Read benchmarks with small 10 message MCAP file."""

    def test_pybag_read_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_with_pybag(mcap), maxlen=0))

    def test_rosbags_read_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_with_rosbags(mcap), maxlen=0))

    def test_official_read_small(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_small_mcap(Path(tmpdir) / "test", message_count=10)
            benchmark(lambda: deque(read_with_official(mcap), maxlen=0))


# ============================================================================
# Multi-Topic Read Benchmarks (6 topics, ~412 messages per second)
# ============================================================================


class TestReadMultiTopic:
    """Read benchmarks with multi-topic MCAP file (1 second duration)."""

    def test_pybag_read_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_with_pybag(mcap), maxlen=0))

    def test_rosbags_read_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_with_rosbags(mcap), maxlen=0))

    def test_official_read_multi_topic(self, benchmark: BenchmarkFixture) -> None:
        with TemporaryDirectory() as tmpdir:
            mcap = create_multi_topic_mcap(
                Path(tmpdir) / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_CONFIG
            )
            benchmark(lambda: deque(read_with_official(mcap), maxlen=0))


# ============================================================================
# Multi-Topic with Images Read Benchmarks (7 topics with large data)
# ============================================================================


class TestReadMultiTopicWithImages:
    """Read benchmarks with multi-topic MCAP including images and point clouds."""

    @pytest.fixture
    def mcap_path(self, tmp_path: Path) -> Path:
        return create_multi_topic_mcap(
            tmp_path / "test", duration_sec=1.0, topic_config=MULTI_TOPIC_WITH_IMAGES_CONFIG
        )

    def test_pybag_read_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_pybag(mcap_path), maxlen=0))

    def test_rosbags_read_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_rosbags(mcap_path), maxlen=0))

    def test_official_read_multi_topic_with_images(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_official(mcap_path), maxlen=0))


# ============================================================================
# Extended Duration Multi-Topic Read Benchmarks (10 seconds)
# ============================================================================


class TestReadMultiTopicExtended:
    """Read benchmarks with multi-topic MCAP file (10 second duration)."""

    @pytest.fixture
    def mcap_path(self, tmp_path: Path) -> Path:
        return create_multi_topic_mcap(
            tmp_path / "test", duration_sec=10.0, topic_config=MULTI_TOPIC_CONFIG
        )

    def test_pybag_read_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_pybag(mcap_path), maxlen=0))

    def test_rosbags_read_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_rosbags(mcap_path), maxlen=0))

    def test_official_read_multi_topic_10s(
        self, benchmark: BenchmarkFixture, mcap_path: Path
    ) -> None:
        benchmark(lambda: deque(read_with_official(mcap_path), maxlen=0))
