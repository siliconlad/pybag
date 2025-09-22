from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator

from mcap.reader import make_reader
from mcap_ros1.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from .benchmark_utils import create_test_ros1_bag, create_test_ros1_mcap


def read_decoded_with_rosbags(bag: Path) -> Iterator[Any]:
    typestore = get_typestore(Stores.ROS1_NOETIC)
    with AnyReader([bag], default_typestore=typestore) as reader:
        for connection, _timestamp, data in reader.messages():
            yield reader.deserialize(data, connection.msgtype)


def read_decoded_with_official(mcap: Path) -> Iterator[Any]:
    with open(mcap, "rb") as stream:
        reader = make_reader(stream, decoder_factories=[DecoderFactory()])
        for _schema, _channel, _message, ros_msg in reader.iter_decoded_messages(
            log_time_order=True,
        ):
            yield ros_msg


def test_rosbags_read_decoded(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        bag = create_test_ros1_bag(Path(tmpdir) / "ros1")
        benchmark(lambda: deque(read_decoded_with_rosbags(bag), maxlen=0))


def test_official_read_decoded(benchmark: BenchmarkFixture) -> None:
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_ros1_mcap(Path(tmpdir) / "ros1")
        benchmark(lambda: deque(read_decoded_with_official(mcap), maxlen=0))
