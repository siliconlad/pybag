"""Benchmark multithreaded file reading for concurrent file access."""
import concurrent.futures
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import create_test_mcap


def create_multiple_test_files(tmpdir: Path, num_files: int = 5, messages_per_file: int = 1000) -> list[Path]:
    """Create multiple test MCAP files for reading."""
    files = []
    for i in range(num_files):
        file_path = create_test_mcap(tmpdir / f"test_{i}", message_count=messages_per_file, seed=i)
        files.append(file_path)
    return files


def read_file_sequential(file_path: Path) -> int:
    """Read a single file and return message count."""
    count = 0
    with McapRecordReaderFactory.from_file(file_path) as reader:
        for _ in reader.get_messages():
            count += 1
    return count


def benchmark_sequential_reads(file_paths: list[Path]) -> int:
    """Read multiple files sequentially."""
    total_messages = 0
    for file_path in file_paths:
        total_messages += read_file_sequential(file_path)
    return total_messages


def benchmark_parallel_reads(file_paths: list[Path], max_workers: int = 4) -> int:
    """Read multiple files in parallel using ThreadPoolExecutor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all read tasks
        futures = [executor.submit(read_file_sequential, file_path) for file_path in file_paths]
        # Wait for all to complete and sum results
        total_messages = sum(future.result() for future in concurrent.futures.as_completed(futures))
    return total_messages


def benchmark_parallel_reads_process_pool(file_paths: list[Path], max_workers: int = 4) -> int:
    """Read multiple files in parallel using ProcessPoolExecutor."""
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all read tasks
        futures = [executor.submit(read_file_sequential, file_path) for file_path in file_paths]
        # Wait for all to complete and sum results
        total_messages = sum(future.result() for future in concurrent.futures.as_completed(futures))
    return total_messages


# Sequential reading benchmarks
def test_sequential_read_3_files(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading of 3 files."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=3, messages_per_file=1000)
        result = benchmark(lambda: benchmark_sequential_reads(files))
        assert result == 3000


def test_sequential_read_5_files(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading of 5 files."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=5, messages_per_file=1000)
        result = benchmark(lambda: benchmark_sequential_reads(files))
        assert result == 5000


def test_sequential_read_10_files(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential reading of 10 files."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=10, messages_per_file=1000)
        result = benchmark(lambda: benchmark_sequential_reads(files))
        assert result == 10000


# Parallel reading benchmarks with ThreadPoolExecutor
def test_parallel_read_3_files_2_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 3 files with 2 worker threads."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=3, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads(files, max_workers=2))
        assert result == 3000


def test_parallel_read_5_files_2_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 5 files with 2 worker threads."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=5, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads(files, max_workers=2))
        assert result == 5000


def test_parallel_read_5_files_4_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 5 files with 4 worker threads."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=5, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads(files, max_workers=4))
        assert result == 5000


def test_parallel_read_10_files_4_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 10 files with 4 worker threads."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=10, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads(files, max_workers=4))
        assert result == 10000


def test_parallel_read_10_files_8_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 10 files with 8 worker threads."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=10, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads(files, max_workers=8))
        assert result == 10000


# Parallel reading benchmarks with ProcessPoolExecutor
def test_parallel_process_read_3_files_2_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 3 files with 2 worker processes."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=3, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads_process_pool(files, max_workers=2))
        assert result == 3000


def test_parallel_process_read_5_files_4_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 5 files with 4 worker processes."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=5, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads_process_pool(files, max_workers=4))
        assert result == 5000


def test_parallel_process_read_10_files_4_workers(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel reading of 10 files with 4 worker processes."""
    with TemporaryDirectory() as tmpdir:
        files = create_multiple_test_files(Path(tmpdir), num_files=10, messages_per_file=1000)
        result = benchmark(lambda: benchmark_parallel_reads_process_pool(files, max_workers=4))
        assert result == 10000
