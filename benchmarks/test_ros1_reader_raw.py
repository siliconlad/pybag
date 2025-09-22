from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from mcap.reader import make_reader
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader

from .benchmark_utils import create_test_ros1_bag, create_test_ros1_mcap


def read_raw_with_rosbags(bag: Path) -> Iterator[bytes]:
    with AnyReader([bag]) as reader:
        for _connection, _timestamp, data in reader.messages():
            yield data


def read_raw_with_official(mcap: Path) -> Iterator[bytes]:
    with open(mcap, "rb") as stream:
        reader = make_reader(stream)
        for _schema, _channel, message in reader.iter_messages(log_time_order=True):
            yield message.data


def test_rosbags_read_raw(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        bag = create_test_ros1_bag(Path(tmpdir) / "ros1")
        benchmark(lambda: deque(read_raw_with_rosbags(bag), maxlen=0))


def test_official_read_raw(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_ros1_mcap(Path(tmpdir) / "ros1")
        benchmark(lambda: deque(read_raw_with_official(mcap), maxlen=0))
