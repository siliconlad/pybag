"""Tests for the MCAP reader."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob("*.mcap"))


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request):
    return get_typestore(request.param)


def test_messages_filter(typestore: Typestore):
    # Write a temporary mcap file
    Int32 = typestore.types["std_msgs/msg/Int32"]
    with TemporaryDirectory() as temp_dir:
        with Writer(
            Path(temp_dir) / "rosbags",
            version=9,
            storage_plugin=StoragePlugin.MCAP,
        ) as writer:
            conn = writer.add_connection("/rosbags", Int32.__msgtype__, typestore=typestore)
            writer.write(conn, 0, typestore.serialize_cdr(Int32(data=1), Int32.__msgtype__))
            writer.write(conn, 1, typestore.serialize_cdr(Int32(data=-1), Int32.__msgtype__))

        mcap_file = _find_mcap_file(temp_dir)
        with McapFileReader.from_file(mcap_file) as reader:
            all_messages = list(reader.messages("/rosbags"))
            assert len(all_messages) == 2
            assert all_messages[0].data.data == 1
            assert all_messages[1].data.data == -1

            positive = list(reader.messages("/rosbags", filter=lambda msg: msg.data.data > 0))
            assert len(positive) == 1
            assert positive[0].data.data == 1

            negative = list(reader.messages("/rosbags", filter=lambda msg: msg.data.data < 0))
            assert len(negative) == 1
            assert negative[0].data.data == -1


@pytest.mark.parametrize(
    "chunk_size",
    [pytest.param(None, id="without_chunks"), pytest.param(10_000, id="with_chunks")],
)
def test_messages_read_in_order_when_written_out_of_order(chunk_size):
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "unordered.mcap"
        with McapFileWriter.open(path, chunk_size=chunk_size, chunk_compression=None) as writer:
            writer.write_message("/unordered", 20, std_msgs.String(data="b"))
            writer.write_message("/unordered", 10, std_msgs.String(data="a"))
            writer.write_message("/unordered", 30, std_msgs.String(data="c"))

        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/unordered"))

    assert [message.log_time for message in messages] == [10, 20, 30]
    assert [message.data.data for message in messages] == ["a", "b", "c"]
