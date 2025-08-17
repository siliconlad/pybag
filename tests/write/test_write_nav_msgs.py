"""Test writing nav_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.nav_msgs as nav_msgs
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_writer import McapFileWriter


def _make_header(frame_id: str = 'frame', sec: int = 1, nanosec: int = 2) -> std_msgs.Header:
    return std_msgs.Header(
        stamp=builtin_interfaces.Time(sec=sec, nanosec=nanosec),
        frame_id=frame_id,
    )


def _write_pybag(temp_dir: str, msg, topic: str = '/pybag', *, timestamp: int = 0) -> Path:
    mcap_path = Path(temp_dir) / 'data.mcap'
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

def test_goals_pybag() -> None:
    msg = nav_msgs.Goals(
        header=_make_header(),
        goals=[
            geometry_msgs.PoseStamped(
                header=_make_header(),
                pose=geometry_msgs.Pose(
                    position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
                    orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
            geometry_msgs.PoseStamped(
                header=_make_header(),
                pose=geometry_msgs.Pose(
                    position=geometry_msgs.Point(x=4.0, y=5.0, z=6.0),
                    orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=1.0, w=1.0),
                ),
            ),
        ],
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Goals'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert len(decoded_msgs[0].goals) == 2
    assert decoded_msgs[0].goals[0].header.frame_id == 'frame'
    assert decoded_msgs[0].goals[0].header.stamp.sec == 1
    assert decoded_msgs[0].goals[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].goals[0].pose.position.x == 1.0
    assert decoded_msgs[0].goals[0].pose.position.y == 2.0
    assert decoded_msgs[0].goals[0].pose.position.z == 3.0
    assert decoded_msgs[0].goals[0].pose.orientation.x == 0.0
    assert decoded_msgs[0].goals[0].pose.orientation.y == 0.0
    assert decoded_msgs[0].goals[0].pose.orientation.z == 0.0
    assert decoded_msgs[0].goals[0].pose.orientation.w == 1.0
    assert decoded_msgs[0].goals[1].header.frame_id == 'frame'
    assert decoded_msgs[0].goals[1].header.stamp.sec == 1
    assert decoded_msgs[0].goals[1].header.stamp.nanosec == 2
    assert decoded_msgs[0].goals[1].pose.position.x == 4.0
    assert decoded_msgs[0].goals[1].pose.position.y == 5.0
    assert decoded_msgs[0].goals[1].pose.position.z == 6.0
    assert decoded_msgs[0].goals[1].pose.orientation.x == 0.0
    assert decoded_msgs[0].goals[1].pose.orientation.y == 0.0
    assert decoded_msgs[0].goals[1].pose.orientation.z == 1.0
    assert decoded_msgs[0].goals[1].pose.orientation.w == 1.0


def test_grid_cells_pybag() -> None:
    msg = nav_msgs.GridCells(
        header=_make_header(),
        cell_width=0.5,
        cell_height=0.5,
        cells=[
            geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            geometry_msgs.Point(x=4.0, y=5.0, z=6.0),
        ],
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'GridCells'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].cell_width == 0.5
    assert decoded_msgs[0].cell_height == 0.5
    assert len(decoded_msgs[0].cells) == 2
    assert decoded_msgs[0].cells[0].x == 1.0
    assert decoded_msgs[0].cells[0].y == 2.0
    assert decoded_msgs[0].cells[0].z == 3.0
    assert decoded_msgs[0].cells[1].x == 4.0
    assert decoded_msgs[0].cells[1].y == 5.0
    assert decoded_msgs[0].cells[1].z == 6.0


def test_map_metadata_pybag() -> None:
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
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MapMetaData'
    assert decoded_msgs[0].map_load_time.sec == 1
    assert decoded_msgs[0].map_load_time.nanosec == 2
    assert decoded_msgs[0].resolution == 0.5
    assert decoded_msgs[0].width == 10
    assert decoded_msgs[0].height == 20
    assert decoded_msgs[0].origin.position.x == 1.0
    assert decoded_msgs[0].origin.position.y == 2.0
    assert decoded_msgs[0].origin.position.z == 3.0
    assert decoded_msgs[0].origin.orientation.x == 0.0
    assert decoded_msgs[0].origin.orientation.y == 0.0
    assert decoded_msgs[0].origin.orientation.z == 0.0
    assert decoded_msgs[0].origin.orientation.w == 1.0


def test_occupancy_grid_pybag() -> None:
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
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'OccupancyGrid'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].info.map_load_time.sec == 1
    assert decoded_msgs[0].info.map_load_time.nanosec == 2
    assert decoded_msgs[0].info.resolution == 0.5
    assert decoded_msgs[0].info.width == 2
    assert decoded_msgs[0].info.height == 2
    assert decoded_msgs[0].info.origin.position.x == 1.0
    assert decoded_msgs[0].info.origin.position.y == 2.0
    assert decoded_msgs[0].info.origin.position.z == 3.0
    assert decoded_msgs[0].info.origin.orientation.x == 0.0
    assert decoded_msgs[0].info.origin.orientation.y == 0.0
    assert decoded_msgs[0].info.origin.orientation.z == 0.0
    assert decoded_msgs[0].info.origin.orientation.w == 1.0
    assert list(decoded_msgs[0].data) == [0, 1, 1, -1]


def test_odometry_pybag() -> None:
    msg = nav_msgs.Odometry(
        header=_make_header(),
        child_frame_id='child',
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
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Odometry'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].child_frame_id == 'child'
    assert decoded_msgs[0].pose.pose.position.x == 1.0
    assert decoded_msgs[0].pose.pose.position.y == 2.0
    assert decoded_msgs[0].pose.pose.position.z == 3.0
    assert decoded_msgs[0].pose.pose.orientation.x == 0.0
    assert decoded_msgs[0].pose.pose.orientation.y == 0.0
    assert decoded_msgs[0].pose.pose.orientation.z == 0.0
    assert decoded_msgs[0].pose.pose.orientation.w == 1.0
    assert decoded_msgs[0].pose.covariance == [float(i) for i in range(36)]
    assert decoded_msgs[0].twist.twist.linear.x == 1.0
    assert decoded_msgs[0].twist.twist.linear.y == 2.0
    assert decoded_msgs[0].twist.twist.linear.z == 3.0
    assert decoded_msgs[0].twist.twist.angular.x == 4.0
    assert decoded_msgs[0].twist.twist.angular.y == 5.0
    assert decoded_msgs[0].twist.twist.angular.z == 6.0
    assert decoded_msgs[0].twist.covariance == [float(i) for i in range(36)]


def test_path_pybag() -> None:
    msg = nav_msgs.Path(
        header=_make_header(),
        poses=[
            geometry_msgs.PoseStamped(
                header=_make_header(),
                pose=geometry_msgs.Pose(
                    position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
                    orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
            geometry_msgs.PoseStamped(
                header=_make_header(),
                pose=geometry_msgs.Pose(
                    position=geometry_msgs.Point(x=4.0, y=5.0, z=6.0),
                    orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
            ),
        ],
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Path'
    assert len(decoded_msgs[0].poses) == 2
    assert decoded_msgs[0].poses[0].header.frame_id == 'frame'
    assert decoded_msgs[0].poses[0].header.stamp.sec == 1
    assert decoded_msgs[0].poses[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].poses[0].pose.position.x == 1.0
    assert decoded_msgs[0].poses[0].pose.position.y == 2.0
    assert decoded_msgs[0].poses[0].pose.position.z == 3.0
    assert decoded_msgs[0].poses[0].pose.orientation.x == 0.0
    assert decoded_msgs[0].poses[0].pose.orientation.y == 0.0
    assert decoded_msgs[0].poses[0].pose.orientation.z == 0.0
    assert decoded_msgs[0].poses[0].pose.orientation.w == 1.0
    assert decoded_msgs[0].poses[1].header.frame_id == 'frame'
    assert decoded_msgs[0].poses[1].header.stamp.sec == 1
    assert decoded_msgs[0].poses[1].header.stamp.nanosec == 2
    assert decoded_msgs[0].poses[1].pose.position.x == 4.0
    assert decoded_msgs[0].poses[1].pose.position.y == 5.0
    assert decoded_msgs[0].poses[1].pose.position.z == 6.0
    assert decoded_msgs[0].poses[1].pose.orientation.x == 0.0
    assert decoded_msgs[0].poses[1].pose.orientation.y == 0.0
    assert decoded_msgs[0].poses[1].pose.orientation.z == 0.0
    assert decoded_msgs[0].poses[1].pose.orientation.w == 1.0
