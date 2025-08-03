"""Test the reading of nav_msgs messages."""
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob("*.mcap"))


def _write_rosbags(
    temp_dir: str,
    msg,
    typestore,
    topic: str = "/rosbags",
    *,
    timestamp: int = 0,
) -> tuple[Path, int]:
    with Writer(Path(temp_dir) / "rosbags", version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        connection = writer.add_connection(topic, msg.__msgtype__, typestore=typestore)
        writer.write(connection, timestamp, typestore.serialize_cdr(msg, msg.__msgtype__))
    return _find_mcap_file(temp_dir), connection.id


def _make_header(typestore: Typestore, frame_id: str = "frame", sec: int = 1, nanosec: int = 2):
    Header = typestore.types["std_msgs/msg/Header"]
    Time = typestore.types["builtin_interfaces/msg/Time"]
    return Header(stamp=Time(sec=sec, nanosec=nanosec), frame_id=frame_id)


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request):
    return get_typestore(request.param)


# TODO: rosbags does not support Goals message
# def test_goals_rosbags(typestore: Typestore):
#     Goals = typestore.types["nav_msgs/msg/Goals"]
#     Point = typestore.types["geometry_msgs/msg/Point"]

#     msg = Goals(
#         header=_make_header(typestore),
#         goals=[
#             Point(x=1.0, y=2.0, z=3.0),
#             Point(x=4.0, y=5.0, z=6.0),
#             Point(x=7.0, y=8.0, z=9.0),
#         ],
#     )
#     with TemporaryDirectory() as temp_dir:
#         mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
#         reader = McapFileReader.from_file(mcap_file)
#         messages = list(reader.messages("/rosbags"))

#         assert len(messages) == 1
#         assert messages[0].log_time == 0
#         assert messages[0].publish_time == 0
#         assert messages[0].sequence == 0
#         assert messages[0].channel_id == channel_id
#         assert messages[0].data.header.frame_id == "frame"
#         assert messages[0].data.header.stamp.sec == 1
#         assert messages[0].data.header.stamp.nanosec == 2
#         assert len(messages[0].data.goals) == 3
#         assert messages[0].data.goals[0].x == 1.0
#         assert messages[0].data.goals[0].y == 2.0
#         assert messages[0].data.goals[0].z == 3.0
#         assert messages[0].data.goals[1].x == 4.0
#         assert messages[0].data.goals[1].y == 5.0
#         assert messages[0].data.goals[1].z == 6.0
#         assert messages[0].data.goals[2].x == 7.0
#         assert messages[0].data.goals[2].y == 8.0
#         assert messages[0].data.goals[2].z == 9.0


def test_grid_cells_rosbags(typestore: Typestore):
    GridCells = typestore.types["nav_msgs/msg/GridCells"]
    Point = typestore.types["geometry_msgs/msg/Point"]

    msg = GridCells(
        header=_make_header(typestore),
        cell_width=1.0,
        cell_height=2.0,
        cells=[
            Point(x=1.0, y=2.0, z=3.0),
            Point(x=4.0, y=5.0, z=6.0),
        ],
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages("/rosbags"))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == "frame"
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.cell_width == 1.0
        assert messages[0].data.cell_height == 2.0
        assert len(messages[0].data.cells) == 2
        assert messages[0].data.cells[0].x == 1.0
        assert messages[0].data.cells[0].y == 2.0
        assert messages[0].data.cells[0].z == 3.0
        assert messages[0].data.cells[1].x == 4.0
        assert messages[0].data.cells[1].y == 5.0
        assert messages[0].data.cells[1].z == 6.0


def test_map_metadata_rosbags(typestore: Typestore):
    MapMetaData = typestore.types["nav_msgs/msg/MapMetaData"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    Time = typestore.types["builtin_interfaces/msg/Time"]

    msg = MapMetaData(
        map_load_time=Time(sec=1, nanosec=2),
        resolution=0.5,
        width=10,
        height=20,
        origin=Pose(
            position=Point(x=1.0, y=2.0, z=3.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages("/rosbags"))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.map_load_time.sec == 1
        assert messages[0].data.map_load_time.nanosec == 2
        assert messages[0].data.resolution == 0.5
        assert messages[0].data.width == 10
        assert messages[0].data.height == 20
        assert messages[0].data.origin.position.x == 1.0
        assert messages[0].data.origin.position.y == 2.0
        assert messages[0].data.origin.position.z == 3.0
        assert messages[0].data.origin.orientation.x == 0.0
        assert messages[0].data.origin.orientation.y == 0.0
        assert messages[0].data.origin.orientation.z == 0.0
        assert messages[0].data.origin.orientation.w == 1.0


def test_occupancy_grid_rosbags(typestore: Typestore):
    OccupancyGrid = typestore.types["nav_msgs/msg/OccupancyGrid"]
    MapMetaData = typestore.types["nav_msgs/msg/MapMetaData"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    Time = typestore.types["builtin_interfaces/msg/Time"]

    msg = OccupancyGrid(
        header=_make_header(typestore),
        info=MapMetaData(
            map_load_time=Time(sec=1, nanosec=2),
            resolution=0.5,
            width=2,
            height=2,
            origin=Pose(
                position=Point(x=1.0, y=2.0, z=3.0),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
        ),
        data=np.array([0, 1, 1, -1], dtype=np.int8),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages("/rosbags"))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == "frame"
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.info.map_load_time.sec == 1
        assert messages[0].data.info.map_load_time.nanosec == 2
        assert messages[0].data.info.resolution == 0.5
        assert messages[0].data.info.width == 2
        assert messages[0].data.info.height == 2
        assert messages[0].data.info.origin.position.x == 1.0
        assert messages[0].data.info.origin.position.y == 2.0
        assert messages[0].data.info.origin.position.z == 3.0
        assert messages[0].data.info.origin.orientation.x == 0.0
        assert messages[0].data.info.origin.orientation.y == 0.0
        assert messages[0].data.info.origin.orientation.z == 0.0
        assert messages[0].data.info.origin.orientation.w == 1.0
        assert messages[0].data.data == [0, 1, 1, -1]


def test_odometry_rosbags(typestore: Typestore):
    Odometry = typestore.types["nav_msgs/msg/Odometry"]
    PoseWithCovariance = typestore.types["geometry_msgs/msg/PoseWithCovariance"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    TwistWithCovariance = typestore.types["geometry_msgs/msg/TwistWithCovariance"]
    Twist = typestore.types["geometry_msgs/msg/Twist"]
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]

    msg = Odometry(
        header=_make_header(typestore),
        child_frame_id="child",
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(x=1.0, y=2.0, z=3.0),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
            covariance=np.array([float(i) for i in range(36)]),
        ),
        twist=TwistWithCovariance(
            twist=Twist(
                linear=Vector3(x=1.0, y=2.0, z=3.0),
                angular=Vector3(x=4.0, y=5.0, z=6.0),
            ),
            covariance=np.array([float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages("/rosbags"))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == "frame"
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.child_frame_id == "child"
        assert messages[0].data.pose.pose.position.x == 1.0
        assert messages[0].data.pose.pose.position.y == 2.0
        assert messages[0].data.pose.pose.position.z == 3.0
        assert messages[0].data.pose.pose.orientation.x == 0.0
        assert messages[0].data.pose.pose.orientation.y == 0.0
        assert messages[0].data.pose.pose.orientation.z == 0.0
        assert messages[0].data.pose.pose.orientation.w == 1.0
        assert messages[0].data.pose.covariance == [float(i) for i in range(36)]
        assert messages[0].data.twist.twist.linear.x == 1.0
        assert messages[0].data.twist.twist.linear.y == 2.0
        assert messages[0].data.twist.twist.linear.z == 3.0
        assert messages[0].data.twist.twist.angular.x == 4.0
        assert messages[0].data.twist.twist.angular.y == 5.0
        assert messages[0].data.twist.twist.angular.z == 6.0
        assert messages[0].data.twist.covariance == [float(i) for i in range(36)]


def test_path_rosbags(typestore: Typestore):
    PathMsg = typestore.types["nav_msgs/msg/Path"]
    PoseStamped = typestore.types["geometry_msgs/msg/PoseStamped"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]

    msg = PathMsg(
        header=_make_header(typestore),
        poses=[
            PoseStamped(
                header=_make_header(typestore),
                pose=Pose(
                    position=Point(x=1.0, y=2.0, z=3.0),
                    orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
            PoseStamped(
                header=_make_header(typestore),
                pose=Pose(
                    position=Point(x=4.0, y=5.0, z=6.0),
                    orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
        ],
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages("/rosbags"))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == "frame"
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert len(messages[0].data.poses) == 2
        assert messages[0].data.poses[0].header.frame_id == "frame"
        assert messages[0].data.poses[0].header.stamp.sec == 1
        assert messages[0].data.poses[0].header.stamp.nanosec == 2
        assert messages[0].data.poses[0].pose.position.x == 1.0
        assert messages[0].data.poses[0].pose.position.y == 2.0
        assert messages[0].data.poses[0].pose.position.z == 3.0
        assert messages[0].data.poses[0].pose.orientation.x == 0.0
        assert messages[0].data.poses[0].pose.orientation.y == 0.0
        assert messages[0].data.poses[0].pose.orientation.z == 0.0
        assert messages[0].data.poses[0].pose.orientation.w == 1.0
        assert messages[0].data.poses[1].header.frame_id == "frame"
        assert messages[0].data.poses[1].header.stamp.sec == 1
        assert messages[0].data.poses[1].header.stamp.nanosec == 2
        assert messages[0].data.poses[1].pose.position.x == 4.0
        assert messages[0].data.poses[1].pose.position.y == 5.0
        assert messages[0].data.poses[1].pose.position.z == 6.0
        assert messages[0].data.poses[1].pose.orientation.x == 0.0
        assert messages[0].data.poses[1].pose.orientation.y == 0.0
        assert messages[0].data.poses[1].pose.orientation.z == 0.0
        assert messages[0].data.poses[1].pose.orientation.w == 1.0
