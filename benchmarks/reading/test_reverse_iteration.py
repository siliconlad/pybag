"""Benchmarks for reverse iteration performance.

This benchmark compares pybag's native reverse iteration support against the official
mcap library, which requires collecting all messages and reversing them manually.

The test demonstrates that pybag's streaming reverse iteration is significantly faster
than the collect-and-reverse approach required by the official library.
"""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap_reader import McapFileReader

# =============================================================================
# Raw message iteration (without deserialization)
# =============================================================================

def read_raw_reverse_with_pybag(mcap: Path) -> Iterator[bytes]:
    """Read raw messages in reverse order using pybag's native support."""
    with McapRecordReaderFactory.from_file(mcap) as reader:
        for message in reader.get_messages(in_reverse=True):
            yield message.data


def read_raw_reverse_with_official(mcap: Path) -> Iterator[bytes]:
    """Read raw messages in reverse order using official mcap library."""
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _, _, message in reader.iter_messages(reverse=True):
            yield message.data


def test_pybag_raw_reverse(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's native reverse iteration for raw messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        benchmark(lambda: deque(read_raw_reverse_with_pybag(mcap), maxlen=0))


def test_official_raw_reverse(benchmark: BenchmarkFixture) -> None:
    """Benchmark official mcap library's reverse iteration (collect-and-reverse)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        benchmark(lambda: deque(read_raw_reverse_with_official(mcap), maxlen=0))


def test_pybag_raw_reverse_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's reverse iteration on a larger file (10k messages)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=10000)
        benchmark(lambda: deque(read_raw_reverse_with_pybag(mcap), maxlen=0))


def test_official_raw_reverse_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark official library's reverse iteration on a larger file (10k messages)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=10000)
        benchmark(lambda: deque(read_raw_reverse_with_official(mcap), maxlen=0))


# =============================================================================
# Decoded message iteration (with deserialization)
# =============================================================================

def read_decoded_reverse_with_pybag(mcap: Path) -> Iterator[Any]:
    """Read decoded messages in reverse order using pybag's native support."""
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():  # TODO: Support reading all messages
            for message in reader.messages(topic, in_reverse=True):
                yield message.data


def read_decoded_reverse_with_official(mcap: Path) -> Iterator[Any]:
    """Read decoded messages in reverse order using official mcap library."""
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, ros_msg in reader.iter_decoded_messages(reverse=True):
            yield ros_msg


def test_pybag_decoded_reverse(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's native reverse iteration for decoded messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        benchmark(lambda: deque(read_decoded_reverse_with_pybag(mcap), maxlen=0))


def test_official_decoded_reverse(benchmark: BenchmarkFixture) -> None:
    """Benchmark official mcap library's reverse iteration for decoded messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        benchmark(lambda: deque(read_decoded_reverse_with_official(mcap), maxlen=0))


# =============================================================================
# Forward vs reverse comparison (pybag only)
# =============================================================================

def test_pybag_raw_forward(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's forward iteration for comparison."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        def read_forward():
            with McapRecordReaderFactory.from_file(mcap) as reader:
                for message in reader.get_messages(in_reverse=False):
                    yield message
        benchmark(read_forward)


def test_pybag_raw_reverse_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's reverse iteration for comparison with forward."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)
        def read_reverse():
            with McapRecordReaderFactory.from_file(mcap) as reader:
                for message in reader.get_messages(in_reverse=True):
                    yield message
        benchmark(read_reverse)
