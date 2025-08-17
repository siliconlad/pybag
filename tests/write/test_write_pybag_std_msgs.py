"""Test writing std_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from rosbags.interfaces import (
    Connection,
    ConnectionExtRosbag2,
    MessageDefinition,
    MessageDefinitionFormat
)
from rosbags.rosbag2.storage_mcap import McapReader
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_writer import McapFileWriter

DEF_TOPIC = "/pybag"


def _write_pybag(
    temp_dir: str,
    msg,
    typestore: Typestore,
    topic: str = DEF_TOPIC,
    *,
    timestamp: int = 0,
) -> tuple[Path, Connection]:
    mcap_path = Path(temp_dir) / "data.mcap"
    with McapFileWriter.open(mcap_path) as writer:
        writer.add_channel(topic, type(msg))
        writer.write_message(topic, timestamp, msg)
    msgdef, digest = typestore.generate_msgdef(msg.__msg_name__)
    connection = Connection(
        id=0,
        topic=topic,
        msgtype=msg.__msg_name__,
        msgdef=MessageDefinition(MessageDefinitionFormat.MSG, msgdef),
        digest=digest,
        msgcount=1,
        ext=ConnectionExtRosbag2("cdr", []),
        owner=None,
    )
    return mcap_path, connection


def _roundtrip(msg, typestore: Typestore):
    with TemporaryDirectory() as temp_dir:
        path, connection = _write_pybag(temp_dir, msg, typestore)
        reader = McapReader([path], [connection])
        reader.open()
        return next(reader.messages())


@pytest.fixture
def typestore() -> Typestore:
    return get_typestore(Stores.ROS2_HUMBLE)


# Tests ---------------------------------------------------------------------

def test_bool_pybag(typestore: Typestore) -> None:
    msg = std_msgs.Bool(data=True)
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.data is True


def test_string_pybag(typestore: Typestore) -> None:
    msg = std_msgs.String(data="hello")
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.data == "hello"


def test_int32_pybag(typestore: Typestore) -> None:
    msg = std_msgs.Int32(data=42)
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.data == 42


def test_float32_pybag(typestore: Typestore) -> None:
    msg = std_msgs.Float32(data=1.5)
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.data == pytest.approx(1.5)
