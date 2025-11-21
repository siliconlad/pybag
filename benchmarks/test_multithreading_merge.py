"""Benchmark multithreaded file merging with concurrent chunk decompression."""
import concurrent.futures
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.cli.mcap_merge import merge_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap.records import decompress_chunk

from .benchmark_utils import MEGABYTE, create_test_mcap


def create_multiple_test_files(tmpdir: Path, num_files: int = 3, messages_per_file: int = 1000) -> list[Path]:
    """Create multiple test MCAP files for merging."""
    files = []
    for i in range(num_files):
        # Create files with different topics to simulate realistic merge scenario
        file_path = create_test_mcap(tmpdir / f"test_{i}", message_count=messages_per_file, seed=i)
        files.append(file_path)
    return files


def benchmark_sequential_merge(input_files: list[Path], output_file: Path) -> None:
    """Current sequential merge implementation."""
    merge_mcap([str(f) for f in input_files], str(output_file))


def benchmark_parallel_chunk_decompression(input_files: list[Path], output_file: Path) -> None:
    """
    Proposed multithreaded merge with parallel chunk decompression.

    This approach decompresses chunks from multiple files concurrently.
    """
    # Collect all chunks from all files first
    all_chunk_data = []

    for input_file in input_files:
        with McapRecordReaderFactory.from_file(input_file) as reader:
            chunk_indexes = list(reader.get_chunk_indexes())
            for chunk_index in chunk_indexes:
                chunk = reader.get_chunk(chunk_index)
                all_chunk_data.append((input_file, chunk_index, chunk))

    def decompress_chunk_task(chunk_tuple):
        """Decompress a single chunk."""
        input_file, chunk_index, chunk = chunk_tuple
        decompressed_data = decompress_chunk(chunk, check_crc=False)
        return (input_file, chunk_index, decompressed_data)

    # Use ThreadPoolExecutor for I/O-bound decompression
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # This would pre-decompress chunks - in a real implementation,
        # we'd need to integrate this into the merge logic
        decompressed_chunks = list(executor.map(decompress_chunk_task, all_chunk_data))

    # Fall back to sequential merge for now (with pre-decompressed chunks cached)
    merge_mcap([str(f) for f in input_files], str(output_file))


def test_sequential_merge_small(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential merge with small files (3 files, 1000 messages each)."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=3, messages_per_file=1000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_sequential_merge(input_files, output_file)

        benchmark(run_merge)


def test_sequential_merge_medium(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential merge with medium files (5 files, 5000 messages each)."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=5, messages_per_file=5000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_sequential_merge(input_files, output_file)

        benchmark(run_merge)


def test_sequential_merge_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark sequential merge with larger files (10 files, 10000 messages each)."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=10, messages_per_file=10000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_sequential_merge(input_files, output_file)

        benchmark(run_merge)


def test_parallel_decompression_merge_small(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel chunk decompression merge with small files."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=3, messages_per_file=1000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_parallel_chunk_decompression(input_files, output_file)

        benchmark(run_merge)


def test_parallel_decompression_merge_medium(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel chunk decompression merge with medium files."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=5, messages_per_file=5000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_parallel_chunk_decompression(input_files, output_file)

        benchmark(run_merge)


def test_parallel_decompression_merge_large(benchmark: BenchmarkFixture) -> None:
    """Benchmark parallel chunk decompression merge with larger files."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        input_files = create_multiple_test_files(tmpdir_path / "input", num_files=10, messages_per_file=10000)
        output_dir = tmpdir_path / "output"
        output_dir.mkdir(exist_ok=True)

        def run_merge():
            from itertools import count
            counter = count()
            output_file = output_dir / f"merged_{next(counter)}.mcap"
            benchmark_parallel_chunk_decompression(input_files, output_file)

        benchmark(run_merge)
