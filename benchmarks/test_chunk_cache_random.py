"""Benchmark showing chunk cache benefit with random access."""
import random
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap.record_reader import McapRecordReaderFactory

from .benchmark_utils import create_test_mcap


def test_pybag_random_access_with_cache(benchmark: BenchmarkFixture) -> None:
    """Test random message access WITH chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)

        def random_access():
            with McapRecordReaderFactory.from_file(mcap, chunk_cache_size=8) as reader:
                # Get all channels and their messages
                channels = list(reader.get_channels().keys())
                all_messages = list(reader.get_messages())

                # Sample 200 random message timestamps (with repetition)
                rng = random.Random(42)
                sampled = rng.choices(all_messages, k=200)

                # Access messages randomly - this will cause chunk re-reads
                for msg in sampled:
                    # Access the same message to force cache hits
                    retrieved = reader.get_message(msg.channel_id, msg.log_time)
                    if retrieved:
                        _ = retrieved.data

        benchmark(random_access)


def test_pybag_random_access_no_cache(benchmark: BenchmarkFixture) -> None:
    """Test random message access WITHOUT chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)

        def random_access():
            with McapRecordReaderFactory.from_file(mcap, chunk_cache_size=0) as reader:
                # Get all channels and their messages
                channels = list(reader.get_channels().keys())
                all_messages = list(reader.get_messages())

                # Sample 200 random message timestamps (with repetition)
                rng = random.Random(42)
                sampled = rng.choices(all_messages, k=200)

                # Access messages randomly - will decompress every time
                for msg in sampled:
                    retrieved = reader.get_message(msg.channel_id, msg.log_time)
                    if retrieved:
                        _ = retrieved.data

        benchmark(random_access)
