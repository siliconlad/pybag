from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_writer import McapFileWriter


def test_chunk_roundtrip() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "data.mcap"
        with McapFileWriter.open(path, chunk_size=1) as writer:
            writer.write_message("/pybag", 0, std_msgs.String(data="a"))
            writer.write_message("/pybag", 1, std_msgs.String(data="b"))
        with open(path, "rb") as f:
            reader = make_reader(f, decoder_factories=[DecoderFactory()])
            msgs = [m.data for _, _, _, m in reader.iter_decoded_messages()]
    assert msgs == ["a", "b"]
