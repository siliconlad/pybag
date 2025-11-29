"""Benchmark comparison between Python pybag and Rust pybag_rs implementations."""

from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

from pytest_benchmark.fixture import BenchmarkFixture

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap_reader import McapFileReader
import pybag_rs


def read_with_pybag_python(mcap: Path) -> Iterator[Any]:
    """Read and decode messages using the pure Python pybag implementation."""
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                yield message.data


def read_with_pybag_rust(mcap: Path) -> Iterator[Any]:
    """Read and decode messages using the Rust pybag_rs implementation."""
    with pybag_rs.PyMcapFileReader.from_file(str(mcap)) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                yield message.data


def test_pybag_python_read(benchmark: BenchmarkFixture) -> None:
    """Benchmark the pure Python pybag implementation."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_pybag_python(mcap), maxlen=0))


def test_pybag_rust_read(benchmark: BenchmarkFixture) -> None:
    """Benchmark the Rust pybag_rs implementation."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_pybag_rust(mcap), maxlen=0))


# Additional benchmarks with different message counts

def test_pybag_python_read_10k(benchmark: BenchmarkFixture) -> None:
    """Benchmark Python implementation with 10,000 messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=10000)
        benchmark(lambda: deque(read_with_pybag_python(mcap), maxlen=0))


def test_pybag_rust_read_10k(benchmark: BenchmarkFixture) -> None:
    """Benchmark Rust implementation with 10,000 messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=10000)
        benchmark(lambda: deque(read_with_pybag_rust(mcap), maxlen=0))
