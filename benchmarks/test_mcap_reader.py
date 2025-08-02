from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from mcap.reader import make_reader
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def create_test_mcap(path: Path, offset: int) -> Path:
    """Create a deterministic MCAP file with values offset by ``offset``."""
    typestore = get_typestore(Stores.LATEST)
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]
    Twist = typestore.types["geometry_msgs/msg/Twist"]

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        vec_conn = writer.add_connection("/vector3", Vector3.__msgtype__, typestore=typestore)
        twist_conn = writer.add_connection("/twist", Twist.__msgtype__, typestore=typestore)

        for i in range(100):
            timestamp = i * 1_000_000
            val = offset + i
            vec = Vector3(x=float(val), y=float(val * 2), z=float(val * 3))
            writer.write(vec_conn, timestamp, typestore.serialize_cdr(vec, Vector3.__msgtype__))

            twist = Twist(
                linear=Vector3(x=float(val), y=float(val), z=float(val)),
                angular=Vector3(x=float(val + 1), y=float(val + 2), z=float(val + 3)),
            )
            writer.write(twist_conn, timestamp + 1, typestore.serialize_cdr(twist, Twist.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def read_with_pybag(mcap: Path) -> float:
    start = perf_counter()
    reader = McapFileReader.from_file(mcap)
    for topic in reader.get_topics():
        for _ in reader.messages(topic):
            pass
    return perf_counter() - start


def read_with_rosbags(mcap: Path) -> float:
    typestore = get_typestore(Stores.LATEST)
    start = perf_counter()
    with AnyReader([mcap]) as reader:
        for conn in reader.connections:
            typestore.register(conn.msgtype, conn.msgdef)
        for conn, timestamp, data in reader.messages():
            typestore.deserialize_cdr(data, conn.msgtype)
    return perf_counter() - start


def read_with_official(mcap: Path) -> float:
    start = perf_counter()
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        for _ in reader.iter_messages():
            pass
    return perf_counter() - start


def test_mcap_readers(benchmark: BenchmarkFixture) -> None:
    """Benchmark MCAP readers and ensure pybag is fastest."""
    with TemporaryDirectory() as tmpdir:
        pybag_mcap = create_test_mcap(Path(tmpdir) / "pybag", offset=0)
        rosbags_mcap = create_test_mcap(Path(tmpdir) / "rosbags", offset=1000)
        official_mcap = create_test_mcap(Path(tmpdir) / "official", offset=2000)

        benchmark(read_with_pybag, pybag_mcap)
        pybag_mean = benchmark.stats.stats["mean"]

        benchmark(read_with_rosbags, rosbags_mcap)
        rosbags_mean = benchmark.stats.stats["mean"]

        benchmark(read_with_official, official_mcap)
        official_mean = benchmark.stats.stats["mean"]

    assert pybag_mean <= rosbags_mean
    assert pybag_mean <= official_mean
