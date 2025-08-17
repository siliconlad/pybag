"""Test writing nav_msgs with pybag and reading with rosbags."""
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
import pybag.ros2.humble.nav_msgs as nav_msgs
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

def test_map_metadata_pybag(typestore: Typestore) -> None:
    msg = nav_msgs.MapMetaData(
        map_load_time=builtin_interfaces.Time(sec=1, nanosec=2),
        resolution=0.5,
        width=10,
        height=20,
        origin=geometry_msgs.Pose(
            position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
    )
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.resolution == 0.5
    assert decoded.width == 10
    assert decoded.height == 20
    assert decoded.origin.position.x == 1.0


def test_occupancy_grid_pybag(typestore: Typestore) -> None:
    info = nav_msgs.MapMetaData(
        map_load_time=builtin_interfaces.Time(sec=1, nanosec=2),
        resolution=0.5,
        width=2,
        height=2,
        origin=geometry_msgs.Pose(
            position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
    )
    msg = nav_msgs.OccupancyGrid(header=_make_header(), info=info, data=[0, 1, 1, -1])
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.info.resolution == 0.5
    assert list(decoded.data) == [0, 1, 1, -1]


def test_odometry_pybag(typestore: Typestore) -> None:
    msg = nav_msgs.Odometry(
        header=_make_header(),
        child_frame_id="child",
        pose=geometry_msgs.PoseWithCovariance(
            pose=geometry_msgs.Pose(
                position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
                orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
            covariance=[float(i) for i in range(36)],
        ),
        twist=geometry_msgs.TwistWithCovariance(
            twist=geometry_msgs.Twist(
                linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
                angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0),
            ),
            covariance=[float(i) for i in range(36)],
        ),
    )
    connection, timestamp, raw = _roundtrip(msg, typestore)
    decoded = typestore.deserialize_cdr(raw, connection.msgtype)
    assert decoded.child_frame_id == "child"
    assert decoded.pose.pose.position.x == 1.0
    assert decoded.twist.twist.angular.z == 6.0
