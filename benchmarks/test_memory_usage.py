from __future__ import annotations

import multiprocessing as mp
from collections.abc import Callable
from functools import wraps
from pathlib import Path

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader

from .benchmark_utils import create_large_mcap


def memory_usage(func: Callable[[str], None]) -> Callable[[str], int]:
    @wraps(func)
    def wrapper(path_str: str) -> int:
        import gc
        import resource

        gc.collect()
        func(path_str)
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    return wrapper


@memory_usage
def _consume_with_pybag(path_str: str) -> None:
    path = Path(path_str)
    with McapFileReader.from_file(path) as reader:
        for topic in reader.get_topics():
            for _ in reader.messages(topic):
                pass


@memory_usage
def _consume_with_rosbags(path_str: str) -> None:
    path = Path(path_str)
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([path.parent]) as reader:
        for connection, _, data in reader.messages():
            typestore.deserialize_cdr(data, connection.msgtype)


@memory_usage
def _consume_with_official(path_str: str) -> None:
    with open(path_str, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _ in reader.iter_decoded_messages(log_time_order=True):
            pass


def _measure_peak_rss(func: Callable[[str], int], path: Path) -> int:
    ctx = mp.get_context()
    with ctx.Pool(1) as pool:
        rss_kb = pool.apply(func, (str(path),))
    if rss_kb <= 0:
        raise RuntimeError("Memory measurement process did not report RSS usage")
    return rss_kb


def _benchmark_memory_usage(
    benchmark: BenchmarkFixture,
    label: str,
    func: Callable[[str], int],
    mcap: Path,
) -> None:
    result: dict[str, int] = {}

    def run() -> None:
        result[label] = _measure_peak_rss(func, mcap)

    benchmark.pedantic(run, iterations=1, rounds=1)

    rss_kb = result[label]
    benchmark.extra_info[f"{label}_rss_kb"] = rss_kb
    assert rss_kb > 0


@pytest.fixture(scope="module")
def large_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    mcap = create_large_mcap(tmp_path_factory.mktemp("large") / "large")
    assert mcap.stat().st_size >= 1 << 30
    return mcap


def test_pybag_memory_usage(benchmark: BenchmarkFixture, large_mcap: Path) -> None:
    _benchmark_memory_usage(benchmark, "pybag", _consume_with_pybag, large_mcap)


def test_rosbags_memory_usage(benchmark: BenchmarkFixture, large_mcap: Path) -> None:
    _benchmark_memory_usage(benchmark, "rosbags", _consume_with_rosbags, large_mcap)


def test_official_mcap_memory_usage(
    benchmark: BenchmarkFixture, large_mcap: Path
) -> None:
    _benchmark_memory_usage(benchmark, "mcap", _consume_with_official, large_mcap)
