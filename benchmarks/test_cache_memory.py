"""Test memory usage with and without chunk cache."""
import gc
import resource
from pathlib import Path
from tempfile import TemporaryDirectory

from pybag.mcap_reader import McapFileReader

from .benchmark_utils import create_test_mcap


def get_memory_usage_kb() -> int:
    """Get current memory usage in KB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def test_memory_with_cache() -> None:
    """Test memory usage with chunk cache enabled."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)

        gc.collect()
        mem_before = get_memory_usage_kb()

        with McapFileReader.from_file(mcap, chunk_cache_size=8) as reader:
            # Read all messages
            for topic in reader.get_topics():
                for _ in reader.messages(topic):
                    pass

        mem_after = get_memory_usage_kb()
        print(f"Memory with cache (8): {mem_after - mem_before} KB delta, peak: {mem_after} KB")


def test_memory_no_cache() -> None:
    """Test memory usage without chunk cache."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)

        gc.collect()
        mem_before = get_memory_usage_kb()

        with McapFileReader.from_file(mcap, chunk_cache_size=0) as reader:
            # Read all messages
            for topic in reader.get_topics():
                for _ in reader.messages(topic):
                    pass

        mem_after = get_memory_usage_kb()
        print(f"Memory without cache: {mem_after - mem_before} KB delta, peak: {mem_after} KB")


if __name__ == "__main__":
    print("Testing memory usage...")
    test_memory_no_cache()
    test_memory_with_cache()
