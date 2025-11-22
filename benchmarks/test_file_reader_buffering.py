"""Benchmark for FileReader buffering optimization.

This benchmark tests the impact of different buffer sizes on sequential file reading
performance, which is critical for MCAP file parsing.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from pybag.io.raw_reader import FileReader
from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import MEGABYTE, create_test_mcap_by_size


def sequential_read_with_buffer_size(mcap_path: Path, buffer_size: int) -> int:
    """Read entire file sequentially with specified buffer size.

    Args:
        mcap_path: Path to MCAP file to read
        buffer_size: Buffer size in bytes

    Returns:
        Total bytes read (for verification)
    """
    total_bytes = 0
    reader = FileReader(mcap_path, buffering=buffer_size)
    try:
        # Read in chunks of 64KB (typical read size during MCAP parsing)
        chunk_size = 64 * 1024
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            total_bytes += len(data)
    finally:
        reader.close()
    return total_bytes


def read_mcap_messages_with_buffer_size(mcap_path: Path, buffer_size: int) -> int:
    """Read all messages from MCAP file using specified buffer size.

    This simulates the actual use case of parsing MCAP files.

    Args:
        mcap_path: Path to MCAP file to read
        buffer_size: Buffer size in bytes for FileReader

    Returns:
        Number of messages read
    """
    # Patch FileReader to use specified buffer size
    original_init = FileReader.__init__

    def patched_init(self, file_path, mode='rb', buffering=buffer_size):
        original_init(self, file_path, mode, buffering=buffering)

    FileReader.__init__ = patched_init

    try:
        message_count = 0
        with McapRecordReaderFactory.from_file(mcap_path) as reader:
            for _ in reader.get_messages():
                message_count += 1
        return message_count
    finally:
        FileReader.__init__ = original_init


# Benchmark: System default buffer (Python default ~8KB)
def test_sequential_read_default_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading with system default buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)
        result = benchmark(sequential_read_with_buffer_size, mcap, -1)
        assert result > 0


# Benchmark: Small buffer (64KB)
def test_sequential_read_64kb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading with 64KB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)
        result = benchmark(sequential_read_with_buffer_size, mcap, 64 * 1024)
        assert result > 0


# Benchmark: Medium buffer (256KB)
def test_sequential_read_256kb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading with 256KB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)
        result = benchmark(sequential_read_with_buffer_size, mcap, 256 * 1024)
        assert result > 0


# Benchmark: Large buffer (1MB) - NEW DEFAULT
def test_sequential_read_1mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading with 1MB buffer (new default)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)
        result = benchmark(sequential_read_with_buffer_size, mcap, 1024 * 1024)
        assert result > 0


# Benchmark: Very large buffer (4MB)
def test_sequential_read_4mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading with 4MB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)
        result = benchmark(sequential_read_with_buffer_size, mcap, 4 * 1024 * 1024)
        assert result > 0


# Real-world use case benchmarks

@pytest.mark.slow
def test_mcap_parsing_default_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark MCAP message parsing with default system buffer (baseline)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=50 * MEGABYTE)
        result = benchmark(read_mcap_messages_with_buffer_size, mcap, -1)
        assert result > 0


@pytest.mark.slow
def test_mcap_parsing_1mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark MCAP message parsing with 1MB buffer (optimized)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=50 * MEGABYTE)
        result = benchmark(read_mcap_messages_with_buffer_size, mcap, 1024 * 1024)
        assert result > 0


@pytest.mark.slow
def test_mcap_parsing_4mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Benchmark MCAP message parsing with 4MB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=50 * MEGABYTE)
        result = benchmark(read_mcap_messages_with_buffer_size, mcap, 4 * 1024 * 1024)
        assert result > 0
