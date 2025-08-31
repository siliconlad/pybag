"""Test writing builtin_interfaces with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
from pybag.mcap_writer import McapFileWriter


def _write_pybag(temp_dir: str, msg, topic: str = "/pybag", *, timestamp: int = 0) -> Path:
    mcap_path = Path(temp_dir) / "data.mcap"
    with McapFileWriter.open(mcap_path) as writer:
        writer.write_message(topic, timestamp, msg)
    return mcap_path


def _roundtrip_write(msg) -> list:
    with TemporaryDirectory() as temp_dir:
        path = _write_pybag(temp_dir, msg)
        with open(path, 'rb') as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            return [ros_msg for _, _, _, ros_msg in reader.iter_decoded_messages()]

# Tests ---------------------------------------------------------------------

def test_duration_pybag() -> None:
    msg = builtin_interfaces.Duration(sec=1, nanosec=2)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Duration'
    assert decoded_msgs[0].sec == 1
    assert decoded_msgs[0].nanosec == 2


def test_time_pybag() -> None:
    msg = builtin_interfaces.Time(sec=1, nanosec=2)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Time'
    assert decoded_msgs[0].sec == 1
    assert decoded_msgs[0].nanosec == 2
