from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from mcap.reader import make_reader
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory

# TODO: Also test for log_time_order=False

def read_raw_with_pybag(mcap: Path) -> Iterator[bytes]:
    with McapRecordReaderFactory.from_file(mcap) as reader:
        for message in reader.get_messages():
            yield message.data


def read_raw_with_rosbags(mcap: Path) -> Iterator[bytes]:
    with AnyReader([mcap.parent]) as reader:
        for _connection, _timestamp, data in reader.messages():
            yield data


def read_raw_with_official(mcap: Path) -> Iterator[bytes]:
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _schema, _channel, message in reader.iter_messages(log_time_order=True):
            yield message.data


def test_pybag_read_raw(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_raw_with_pybag(mcap), maxlen=0))


def test_rosbags_read_raw(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_raw_with_rosbags(mcap), maxlen=0))


def test_official_read_raw(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_raw_with_official(mcap), maxlen=0))
