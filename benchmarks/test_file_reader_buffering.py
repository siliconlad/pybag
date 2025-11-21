"""Benchmark to test FileReader buffering performance."""
import io
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.io.raw_reader import FileReader
from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import MEGABYTE, create_test_mcap_by_size


def test_file_reader_default_buffer(benchmark: BenchmarkFixture) -> None:
    """Test FileReader with Python's default buffer size (~8KB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_file():
            reader = FileReader(mcap, buffering=io.DEFAULT_BUFFER_SIZE)
            # Read entire file in chunks
            while chunk := reader.read(8192):
                pass
            reader.close()

        benchmark(read_file)


def test_file_reader_64kb_buffer(benchmark: BenchmarkFixture) -> None:
    """Test FileReader with 64KB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_file():
            reader = FileReader(mcap, buffering=64 * 1024)
            # Read entire file in chunks
            while chunk := reader.read(8192):
                pass
            reader.close()

        benchmark(read_file)


def test_file_reader_256kb_buffer(benchmark: BenchmarkFixture) -> None:
    """Test FileReader with 256KB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_file():
            reader = FileReader(mcap, buffering=256 * 1024)
            # Read entire file in chunks
            while chunk := reader.read(8192):
                pass
            reader.close()

        benchmark(read_file)


def test_file_reader_1mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Test FileReader with 1MB buffer (recommended size)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_file():
            reader = FileReader(mcap, buffering=1024 * 1024)
            # Read entire file in chunks
            while chunk := reader.read(8192):
                pass
            reader.close()

        benchmark(read_file)


def test_file_reader_4mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Test FileReader with 4MB buffer."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_file():
            reader = FileReader(mcap, buffering=4 * 1024 * 1024)
            # Read entire file in chunks
            while chunk := reader.read(8192):
                pass
            reader.close()

        benchmark(read_file)


def test_mcap_reader_default_buffer(benchmark: BenchmarkFixture) -> None:
    """Test MCAP reader with default buffer (old behavior)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_mcap():
            with McapRecordReaderFactory.from_file(mcap, buffering=io.DEFAULT_BUFFER_SIZE) as reader:
                deque(reader.get_messages(), maxlen=0)

        benchmark(read_mcap)


def test_mcap_reader_1mb_buffer(benchmark: BenchmarkFixture) -> None:
    """Test MCAP reader with 1MB buffer (new behavior)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap_by_size(Path(tmpdir) / "test", target_size_bytes=100 * MEGABYTE)

        def read_mcap():
            with McapRecordReaderFactory.from_file(mcap, buffering=1024 * 1024) as reader:
                deque(reader.get_messages(), maxlen=0)

        benchmark(read_mcap)
