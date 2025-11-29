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
    """Read raw messages in reverse order using official mcap library.

    The official library doesn't support native reverse iteration,
    so we must collect all messages and reverse them.
    """
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        # Must collect all messages first, then reverse
        messages = list(reader.iter_messages(log_time_order=True))
        for _schema, _channel, message in reversed(messages):
            yield message.data


def read_raw_reverse_with_official_generator(mcap: Path) -> Iterator[bytes]:
    """Read raw messages in reverse order using official mcap library.

    Alternative approach: use a deque with maxlen to keep last N messages,
    but this still requires full iteration for complete reverse.
    This version collects and reverses.
    """
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        # Collect all and reverse - the only way to get true reverse order
        all_messages = []
        for _schema, _channel, message in reader.iter_messages(log_time_order=True):
            all_messages.append(message.data)
        for data in reversed(all_messages):
            yield data


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


# =============================================================================
# Decoded message iteration (with deserialization)
# =============================================================================

def read_decoded_reverse_with_pybag(mcap: Path) -> Iterator[Any]:
    """Read decoded messages in reverse order using pybag's native support."""
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic, in_reverse=True):
                yield message.data


def read_decoded_reverse_with_official(mcap: Path) -> Iterator[Any]:
    """Read decoded messages in reverse order using official mcap library.

    Must collect all messages and reverse since there's no native reverse support.
    """
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        # Collect all messages first
        messages = list(reader.iter_decoded_messages(log_time_order=True))
        # Reverse and yield
        for _schema, _channel, _message, ros_msg in reversed(messages):
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
                    pass

        benchmark(read_forward)


def test_pybag_raw_reverse_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark pybag's reverse iteration for comparison with forward."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)

        def read_reverse():
            with McapRecordReaderFactory.from_file(mcap) as reader:
                for message in reader.get_messages(in_reverse=True):
                    pass

        benchmark(read_reverse)


# =============================================================================
# Partial reverse iteration (early exit scenario)
# =============================================================================

def test_pybag_raw_reverse_first_100(benchmark: BenchmarkFixture) -> None:
    """Benchmark getting first 100 messages in reverse order with pybag.

    This demonstrates pybag's streaming advantage - we only read what we need.
    """
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)

        def read_first_100_reverse():
            with McapRecordReaderFactory.from_file(mcap) as reader:
                count = 0
                for message in reader.get_messages(in_reverse=True):
                    count += 1
                    if count >= 100:
                        break

        benchmark(read_first_100_reverse)


def test_official_raw_reverse_first_100(benchmark: BenchmarkFixture) -> None:
    """Benchmark getting first 100 messages in reverse order with official library.

    The official library must read ALL messages before reversing, even if we only
    need the first 100 from the end.
    """
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=5000)

        def read_first_100_reverse():
            with open(mcap, "rb") as f:
                reader = make_reader(f)
                # Must read ALL messages first
                messages = list(reader.iter_messages(log_time_order=True))
                # Then get last 100 (first 100 in reverse order)
                count = 0
                for _schema, _channel, message in reversed(messages):
                    count += 1
                    if count >= 100:
                        break

        benchmark(read_first_100_reverse)


# =============================================================================
# Large file benchmarks
# =============================================================================

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
