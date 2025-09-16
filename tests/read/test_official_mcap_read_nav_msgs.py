"""Test reading nav_msgs messages written with the official MCAP writer."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from mcap.writer import Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader
from tests.read._sample_message_factory import create_message, to_plain


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request) -> Typestore:
    return get_typestore(request.param)


def _write_mcap(
    temp_dir: str,
    typestore: Typestore,
    msg,
    msgtype: str,
    schema_text: str,
) -> tuple[Path, int]:
    path = Path(temp_dir) / "test.mcap"
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start()
        schema_id = writer.register_schema(msgtype, "ros2msg", schema_text.encode())
        channel_id = writer.register_channel("/rosbags", "cdr", schema_id)
        writer.add_message(
            channel_id,
            log_time=0,
            data=typestore.serialize_cdr(msg, msgtype),
            publish_time=0,
        )
        writer.finish()
    return path, channel_id


def test_nav_msgs_gridcells(typestore: Typestore):
    msgtype = "nav_msgs/msg/GridCells"
    msg = create_message(typestore, msgtype, seed=1)

    schema = (
        "std_msgs/Header header\n"
        "float32 cell_width\n"
        "float32 cell_height\n"
        "geometry_msgs/Point[] cells\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_nav_msgs_mapmetadata(typestore: Typestore):
    msgtype = "nav_msgs/msg/MapMetaData"
    msg = create_message(typestore, msgtype, seed=2)

    schema = (
        "builtin_interfaces/Time map_load_time\n"
        "float32 resolution\n"
        "uint32 width\n"
        "uint32 height\n"
        "geometry_msgs/Pose origin\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Pose\n"
        "geometry_msgs/Point position\n"
        "geometry_msgs/Quaternion orientation\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_nav_msgs_occupancygrid(typestore: Typestore):
    msgtype = "nav_msgs/msg/OccupancyGrid"
    msg = create_message(typestore, msgtype, seed=3)

    schema = (
        "std_msgs/Header header\n"
        "nav_msgs/MapMetaData info\n"
        "int8[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: nav_msgs/MapMetaData\n"
        "builtin_interfaces/Time map_load_time\n"
        "float32 resolution\n"
        "uint32 width\n"
        "uint32 height\n"
        "geometry_msgs/Pose origin\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Pose\n"
        "geometry_msgs/Point position\n"
        "geometry_msgs/Quaternion orientation\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_nav_msgs_odometry(typestore: Typestore):
    msgtype = "nav_msgs/msg/Odometry"
    msg = create_message(typestore, msgtype, seed=4)

    schema = (
        "std_msgs/Header header\n"
        "string child_frame_id\n"
        "geometry_msgs/PoseWithCovariance pose\n"
        "geometry_msgs/TwistWithCovariance twist\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/PoseWithCovariance\n"
        "geometry_msgs/Pose pose\n"
        "float64[36] covariance\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Pose\n"
        "geometry_msgs/Point position\n"
        "geometry_msgs/Quaternion orientation\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: geometry_msgs/TwistWithCovariance\n"
        "geometry_msgs/Twist twist\n"
        "float64[36] covariance\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Twist\n"
        "geometry_msgs/Vector3 linear\n"
        "geometry_msgs/Vector3 angular\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Vector3\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_nav_msgs_path(typestore: Typestore):
    msgtype = "nav_msgs/msg/Path"
    msg = create_message(typestore, msgtype, seed=5)

    schema = (
        "std_msgs/Header header\n"
        "geometry_msgs/PoseStamped[] poses\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/PoseStamped\n"
        "std_msgs/Header header\n"
        "geometry_msgs/Pose pose\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Pose\n"
        "geometry_msgs/Point position\n"
        "geometry_msgs/Quaternion orientation\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected

