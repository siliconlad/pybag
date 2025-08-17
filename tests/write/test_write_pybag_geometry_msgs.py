"""Test writing geometry_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
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
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_writer import McapFileWriter

DEF_TOPIC = "/pybag"


def _make_header(frame_id: str = "frame", sec: int = 1, nanosec: int = 2) -> std_msgs.Header:
    return std_msgs.Header(
        stamp=builtin_interfaces.Time(sec=sec, nanosec=nanosec),
        frame_id=frame_id,
    )


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

def test_vector3_pybag(typestore: Typestore) -> None:
    msg = geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0)
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert connection.topic == DEF_TOPIC
    assert timestamp == 0
    assert decoded.x == 1.0
    assert decoded.y == 2.0
    assert decoded.z == 3.0


def test_pose_pybag(typestore: Typestore) -> None:
    msg = geometry_msgs.Pose(
        position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
        orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert connection.topic == DEF_TOPIC
    assert timestamp == 0
    assert decoded.position.x == 1.0
    assert decoded.position.y == 2.0
    assert decoded.position.z == 3.0
    assert decoded.orientation.x == 0.0
    assert decoded.orientation.y == 0.0
    assert decoded.orientation.z == 0.0
    assert decoded.orientation.w == 1.0


def test_vector3_stamped_pybag(typestore: Typestore) -> None:
    msg = geometry_msgs.Vector3Stamped(
        header=_make_header(),
        vector=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
    )
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.header.frame_id == "frame"
    assert decoded.header.stamp.sec == 1
    assert decoded.header.stamp.nanosec == 2
    assert decoded.vector.x == 1.0
    assert decoded.vector.y == 2.0
    assert decoded.vector.z == 3.0


def test_pose_with_covariance_pybag(typestore: Typestore) -> None:
    msg = geometry_msgs.PoseWithCovariance(
        pose=geometry_msgs.Pose(
            position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
        covariance=[float(i) for i in range(36)],
    )
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert connection.topic == DEF_TOPIC
    assert timestamp == 0
    assert decoded.pose.position.x == 1.0
    assert decoded.pose.position.y == 2.0
    assert decoded.pose.position.z == 3.0
    assert decoded.pose.orientation.w == 1.0
    assert list(decoded.covariance) == [float(i) for i in range(36)]
