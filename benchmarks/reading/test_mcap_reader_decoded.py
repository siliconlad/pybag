from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap_reader import McapFileReader


def read_with_pybag(mcap: Path) -> Iterator[Any]:
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                yield message.data


def read_with_rosbags(mcap: Path) -> Iterator[Any]:
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([mcap.parent]) as reader:
        for conn, _, data in reader.messages():
            yield typestore.deserialize_cdr(data, conn.msgtype)


def read_with_official(mcap: Path) -> Iterator[Any]:
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, ros_msg in reader.iter_decoded_messages(log_time_order=False):
            yield ros_msg


def test_official_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_official(mcap), maxlen=0))


def test_rosbags_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_rosbags(mcap), maxlen=0))


def test_pybag_read(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")
        benchmark(lambda: deque(read_with_pybag(mcap), maxlen=0))
