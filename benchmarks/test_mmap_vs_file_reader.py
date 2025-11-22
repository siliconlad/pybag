"""Benchmark comparing FileReader vs MmapReader performance."""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import create_test_mcap, create_test_mcap_by_size


def read_raw_with_file_reader(mcap: Path) -> Iterator[bytes]:
    """Read MCAP file using traditional FileReader."""
    with McapRecordReaderFactory.from_file(mcap, use_mmap=False) as reader:
        for message in reader.get_messages():
            yield message.data


def read_raw_with_mmap_reader(mcap: Path) -> Iterator[bytes]:
    """Read MCAP file using MmapReader."""
    with McapRecordReaderFactory.from_file(mcap, use_mmap=True) as reader:
        for message in reader.get_messages():
            yield message.data


# Small file benchmarks (1000 messages)
def test_file_reader_small(benchmark: BenchmarkFixture) -> None:
    """Benchmark FileReader with small MCAP file."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_raw_with_file_reader(mcap), maxlen=0))


def test_mmap_reader_small(benchmark: BenchmarkFixture) -> None:
    """Benchmark MmapReader with small MCAP file."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_raw_with_mmap_reader(mcap), maxlen=0))


# Medium file benchmarks (100MB)
def test_file_reader_medium(benchmark: BenchmarkFixture) -> None:
    """Benchmark FileReader with medium MCAP file (100MB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * 1024 * 1024)
        benchmark(lambda: deque(read_raw_with_file_reader(mcap), maxlen=0))


def test_mmap_reader_medium(benchmark: BenchmarkFixture) -> None:
    """Benchmark MmapReader with medium MCAP file (100MB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * 1024 * 1024)
        benchmark(lambda: deque(read_raw_with_mmap_reader(mcap), maxlen=0))


# Large file benchmarks (1GB)
def test_file_reader_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark FileReader with large MCAP file (1GB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=1024 * 1024 * 1024)
        benchmark(lambda: deque(read_raw_with_file_reader(mcap), maxlen=0))


def test_mmap_reader_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark MmapReader with large MCAP file (1GB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=1024 * 1024 * 1024)
        benchmark(lambda: deque(read_raw_with_mmap_reader(mcap), maxlen=0))
