import logging
import multiprocessing as mp
from collections.abc import Callable
from functools import wraps
from pathlib import Path

import pytest
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore

from benchmarks.benchmark_utils import create_test_mcap_by_size
from pybag.mcap_reader import McapFileReader


@pytest.fixture(scope="module")
def large_mcap(tmp_path_factory: pytest.TempPathFactory) -> Path:
    mcap = create_test_mcap_by_size(tmp_path_factory.mktemp("large") / "large")
    assert mcap.stat().st_size >= 1 << 30
    return mcap


def _benchmark_memory_usage(label: str, mcap: Path, func: Callable[[str], int]) -> None:
    ctx = mp.get_context()
    with ctx.Pool(1) as pool:
        rss_kb = pool.apply(func, (str(mcap),))
    logging.info(f'{label}: {rss_kb}')


def return_memory_usage(func: Callable[[str], None]) -> Callable[[str], int]:
    @wraps(func)
    def wrapper(path_str: str) -> int:
        import gc
        import resource

        gc.collect()
        func(path_str)
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    return wrapper


@return_memory_usage
def _consume_with_pybag(path_str: str) -> None:
    path = Path(path_str)
    with McapFileReader.from_file(path) as reader:
        for topic in reader.get_topics():
            for _ in reader.messages(topic):
                pass


@return_memory_usage
def _consume_with_rosbags(path_str: str) -> None:
    path = Path(path_str)
    typestore = get_typestore(Stores.LATEST)
    with AnyReader([path.parent]) as reader:
        for connection, _, data in reader.messages():
            typestore.deserialize_cdr(data, connection.msgtype)


@return_memory_usage
def _consume_with_official(path_str: str) -> None:
    with open(path_str, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _ in reader.iter_decoded_messages(log_time_order=False):
            pass


def test_pybag_memory_usage(large_mcap: Path) -> None:
    _benchmark_memory_usage("pybag", large_mcap, _consume_with_pybag)


def test_rosbags_memory_usage(large_mcap: Path) -> None:
    _benchmark_memory_usage("rosbags", large_mcap, _consume_with_rosbags)


def test_official_mcap_memory_usage(large_mcap: Path) -> None:
    _benchmark_memory_usage("mcap", large_mcap, _consume_with_official)
