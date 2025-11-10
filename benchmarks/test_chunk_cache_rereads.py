"""Benchmark showing chunk cache benefit with repeated reads."""
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap_reader import McapFileReader

from .benchmark_utils import create_test_mcap


def test_pybag_reread_with_cache(benchmark: BenchmarkFixture) -> None:
    """Test re-reading same messages multiple times WITH chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=200)

        def read_multiple_times():
            with McapFileReader.from_file(mcap, chunk_cache_size=8) as reader:
                # Read the same messages 5 times (cache will help after first read)
                for _ in range(5):
                    for topic in reader.get_topics():
                        msg_count = 0
                        for _ in reader.messages(topic):
                            msg_count += 1

        benchmark(read_multiple_times)


def test_pybag_reread_no_cache(benchmark: BenchmarkFixture) -> None:
    """Test re-reading same messages multiple times WITHOUT chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=200)

        def read_multiple_times():
            with McapFileReader.from_file(mcap, chunk_cache_size=0) as reader:
                # Read the same messages 5 times (will decompress every time)
                for _ in range(5):
                    for topic in reader.get_topics():
                        msg_count = 0
                        for _ in reader.messages(topic):
                            msg_count += 1

        benchmark(read_multiple_times)
