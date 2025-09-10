"""Test filtering of messages using a lambda."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader


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
        reader = McapFileReader.from_file(mcap_file)

        all_messages = list(reader.messages("/rosbags"))
        assert len(all_messages) == 2
        assert all_messages[0].data.data == 1
        assert all_messages[1].data.data == -1

        positive = list(reader.messages("/rosbags", filter=lambda m: m.data.data > 0))
        assert len(positive) == 1
        assert positive[0].data.data == 1

        negative = list(reader.messages("/rosbags", filter=lambda m: m.data.data < 0))
        assert len(negative) == 1
        assert negative[0].data.data == -1