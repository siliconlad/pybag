"""Test reading geometry_msgs messages written with the official MCAP writer."""

from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any

from mcap_ros2.writer import Writer as McapWriter

from pybag.mcap_reader import McapFileReader


def _write_mcap(temp_dir: str, msg: Any, msgtype: str, schema_text: str) -> Path:
    path = Path(temp_dir) / "test.mcap"
    with open(path, "wb") as f:
        writer = McapWriter(f)
        schema = writer.register_msgdef(msgtype, schema_text)
        writer.write_message(
            topic="/rosbags",
            schema=schema,
            message=msg,
            log_time=0,
            publish_time=0,
            sequence=0,
        )
        writer.finish()
    return path


def test_geometry_msgs_accel():
    msgtype = "geometry_msgs/Accel"
    schema = dedent("""
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular": {"x": 4.0, "y": 5.0, "z": 6.0},
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.linear.x == 1.0
    assert messages[0].data.linear.y == 2.0
    assert messages[0].data.linear.z == 3.0
    assert messages[0].data.angular.x == 4.0
    assert messages[0].data.angular.y == 5.0
    assert messages[0].data.angular.z == 6.0


def test_geometry_msgs_accelstamped():
    msgtype = "geometry_msgs/AccelStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Accel accel
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Accel
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "accel": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.accel.linear.x == 1.0
    assert messages[0].data.accel.linear.y == 2.0
    assert messages[0].data.accel.linear.z == 3.0
    assert messages[0].data.accel.angular.x == 4.0
    assert messages[0].data.accel.angular.y == 5.0
    assert messages[0].data.accel.angular.z == 6.0


def test_geometry_msgs_accelwithcovariance():
    msgtype = "geometry_msgs/AccelWithCovariance"
    schema = dedent("""
        geometry_msgs/Accel accel
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Accel
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "accel": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.accel.linear.x == 1.0
    assert messages[0].data.accel.linear.y == 2.0
    assert messages[0].data.accel.linear.z == 3.0
    assert messages[0].data.accel.angular.x == 4.0
    assert messages[0].data.accel.angular.y == 5.0
    assert messages[0].data.accel.angular.z == 6.0
    assert len(messages[0].data.covariance) == 36


def test_geometry_msgs_accelwithcovariancestamped():
    msgtype = "geometry_msgs/AccelWithCovarianceStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/AccelWithCovariance accel
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/AccelWithCovariance
        geometry_msgs/Accel accel
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Accel
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "accel": {
                "accel": {
                    "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
                },
                "covariance": [0.0] * 36
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.accel.accel.linear.x == 1.0
    assert messages[0].data.accel.accel.linear.y == 2.0
    assert messages[0].data.accel.accel.linear.z == 3.0
    assert messages[0].data.accel.accel.angular.x == 4.0
    assert messages[0].data.accel.accel.angular.y == 5.0
    assert messages[0].data.accel.accel.angular.z == 6.0
    assert len(messages[0].data.accel.covariance) == 36


def test_geometry_msgs_inertia():
    msgtype = "geometry_msgs/Inertia"
    schema = dedent("""
        float64 m
        geometry_msgs/Vector3 com
        float64 ixx
        float64 ixy
        float64 ixz
        float64 iyy
        float64 iyz
        float64 izz
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "m": 10.5,
            "com": {"x": 1.0, "y": 2.0, "z": 3.0},
            "ixx": 1.1,
            "ixy": 1.2,
            "ixz": 1.3,
            "iyy": 2.2,
            "iyz": 2.3,
            "izz": 3.3
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.m == 10.5
    assert messages[0].data.com.x == 1.0
    assert messages[0].data.com.y == 2.0
    assert messages[0].data.com.z == 3.0
    assert messages[0].data.ixx == 1.1
    assert messages[0].data.ixy == 1.2
    assert messages[0].data.ixz == 1.3
    assert messages[0].data.iyy == 2.2
    assert messages[0].data.iyz == 2.3
    assert messages[0].data.izz == 3.3


def test_geometry_msgs_inertiastamped():
    msgtype = "geometry_msgs/InertiaStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Inertia inertia
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Inertia
        float64 m
        geometry_msgs/Vector3 com
        float64 ixx
        float64 ixy
        float64 ixz
        float64 iyy
        float64 iyz
        float64 izz
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "inertia": {
                "m": 10.5,
                "com": {"x": 1.0, "y": 2.0, "z": 3.0},
                "ixx": 1.1,
                "ixy": 1.2,
                "ixz": 1.3,
                "iyy": 2.2,
                "iyz": 2.3,
                "izz": 3.3
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.inertia.m == 10.5
    assert messages[0].data.inertia.com.x == 1.0
    assert messages[0].data.inertia.com.y == 2.0
    assert messages[0].data.inertia.com.z == 3.0
    assert messages[0].data.inertia.ixx == 1.1
    assert messages[0].data.inertia.ixy == 1.2
    assert messages[0].data.inertia.ixz == 1.3
    assert messages[0].data.inertia.iyy == 2.2
    assert messages[0].data.inertia.iyz == 2.3
    assert messages[0].data.inertia.izz == 3.3


def test_geometry_msgs_point():
    msgtype = "geometry_msgs/Point"
    schema = dedent("""
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_point32():
    msgtype = "geometry_msgs/Point32"
    schema = dedent("""
        float32 x
        float32 y
        float32 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_pointstamped():
    msgtype = "geometry_msgs/PointStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Point point
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "point": {"x": 1.0, "y": 2.0, "z": 3.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.point.x == 1.0
    assert messages[0].data.point.y == 2.0
    assert messages[0].data.point.z == 3.0


def test_geometry_msgs_polygon():
    msgtype = "geometry_msgs/Polygon"
    schema = dedent("""
        geometry_msgs/Point32[] points
        ================================================================================
        MSG: geometry_msgs/Point32
        float32 x
        float32 y
        float32 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "points": [
                {"x": 1.0, "y": 2.0, "z": 3.0},
                {"x": 4.0, "y": 5.0, "z": 6.0}
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.points) == 2
    assert messages[0].data.points[0].x == 1.0
    assert messages[0].data.points[0].y == 2.0
    assert messages[0].data.points[0].z == 3.0
    assert messages[0].data.points[1].x == 4.0
    assert messages[0].data.points[1].y == 5.0
    assert messages[0].data.points[1].z == 6.0


def test_geometry_msgs_polygonstamped():
    msgtype = "geometry_msgs/PolygonStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Polygon polygon
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Polygon
        geometry_msgs/Point32[] points
        ================================================================================
        MSG: geometry_msgs/Point32
        float32 x
        float32 y
        float32 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "polygon": {
                "points": [
                    {"x": 1.0, "y": 2.0, "z": 3.0},
                    {"x": 4.0, "y": 5.0, "z": 6.0}
                ]
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert len(messages[0].data.polygon.points) == 2
    assert messages[0].data.polygon.points[0].x == 1.0
    assert messages[0].data.polygon.points[0].y == 2.0
    assert messages[0].data.polygon.points[0].z == 3.0
    assert messages[0].data.polygon.points[1].x == 4.0
    assert messages[0].data.polygon.points[1].y == 5.0
    assert messages[0].data.polygon.points[1].z == 6.0


def test_geometry_msgs_pose():
    msgtype = "geometry_msgs/Pose"
    schema = dedent("""
        geometry_msgs/Point position
        geometry_msgs/Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "position": {"x": 1.0, "y": 2.0, "z": 3.0},
            "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.position.x == 1.0
    assert messages[0].data.position.y == 2.0
    assert messages[0].data.position.z == 3.0
    assert messages[0].data.orientation.x == 0.0
    assert messages[0].data.orientation.y == 0.0
    assert messages[0].data.orientation.z == 0.0
    assert messages[0].data.orientation.w == 1.0


def test_geometry_msgs_pose2d():
    msgtype = "geometry_msgs/Pose2D"
    schema = dedent("""
        float64 x
        float64 y
        float64 theta
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "theta": 1.5708}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.theta == 1.5708


def test_geometry_msgs_posearray():
    msgtype = "geometry_msgs/PoseArray"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Pose[] poses
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Pose
        geometry_msgs/Point position
        geometry_msgs/Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "poses": [
                {
                    "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                }
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert len(messages[0].data.poses) == 1
    assert messages[0].data.poses[0].position.x == 1.0
    assert messages[0].data.poses[0].position.y == 2.0
    assert messages[0].data.poses[0].position.z == 3.0
    assert messages[0].data.poses[0].orientation.x == 0.0
    assert messages[0].data.poses[0].orientation.y == 0.0
    assert messages[0].data.poses[0].orientation.z == 0.0
    assert messages[0].data.poses[0].orientation.w == 1.0


def test_geometry_msgs_posestamped():
    msgtype = "geometry_msgs/PoseStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Pose pose
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Pose
        geometry_msgs/Point position
        geometry_msgs/Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "pose": {
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.pose.position.x == 1.0
    assert messages[0].data.pose.position.y == 2.0
    assert messages[0].data.pose.position.z == 3.0
    assert messages[0].data.pose.orientation.x == 0.0
    assert messages[0].data.pose.orientation.y == 0.0
    assert messages[0].data.pose.orientation.z == 0.0
    assert messages[0].data.pose.orientation.w == 1.0


def test_geometry_msgs_posewithcovariance():
    msgtype = "geometry_msgs/PoseWithCovariance"
    schema = dedent("""
        geometry_msgs/Pose pose
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Pose
        geometry_msgs/Point position
        geometry_msgs/Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "pose": {
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.pose.position.x == 1.0
    assert messages[0].data.pose.position.y == 2.0
    assert messages[0].data.pose.position.z == 3.0
    assert messages[0].data.pose.orientation.x == 0.0
    assert messages[0].data.pose.orientation.y == 0.0
    assert messages[0].data.pose.orientation.z == 0.0
    assert messages[0].data.pose.orientation.w == 1.0
    assert len(messages[0].data.covariance) == 36


def test_geometry_msgs_posewithcovariancestamped():
    msgtype = "geometry_msgs/PoseWithCovarianceStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/PoseWithCovariance pose
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseWithCovariance
        geometry_msgs/Pose pose
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Pose
        geometry_msgs/Point position
        geometry_msgs/Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "pose": {
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                },
                "covariance": [0.0] * 36
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.pose.pose.position.x == 1.0
    assert messages[0].data.pose.pose.position.y == 2.0
    assert messages[0].data.pose.pose.position.z == 3.0
    assert messages[0].data.pose.pose.orientation.x == 0.0
    assert messages[0].data.pose.pose.orientation.y == 0.0
    assert messages[0].data.pose.pose.orientation.z == 0.0
    assert messages[0].data.pose.pose.orientation.w == 1.0
    assert len(messages[0].data.pose.covariance) == 36


def test_geometry_msgs_quaternion():
    msgtype = "geometry_msgs/Quaternion"
    schema = dedent("""
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 0.0
    assert messages[0].data.y == 0.0
    assert messages[0].data.z == 0.0
    assert messages[0].data.w == 1.0


def test_geometry_msgs_quaternionstamped():
    msgtype = "geometry_msgs/QuaternionStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Quaternion quaternion
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "quaternion": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.quaternion.x == 0.0
    assert messages[0].data.quaternion.y == 0.0
    assert messages[0].data.quaternion.z == 0.0
    assert messages[0].data.quaternion.w == 1.0


def test_geometry_msgs_transform():
    msgtype = "geometry_msgs/Transform"
    schema = dedent("""
        geometry_msgs/Vector3 translation
        geometry_msgs/Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
            "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.translation.x == 1.0
    assert messages[0].data.translation.y == 2.0
    assert messages[0].data.translation.z == 3.0
    assert messages[0].data.rotation.x == 0.0
    assert messages[0].data.rotation.y == 0.0
    assert messages[0].data.rotation.z == 0.0
    assert messages[0].data.rotation.w == 1.0


def test_geometry_msgs_transformstamped():
    msgtype = "geometry_msgs/TransformStamped"
    schema = dedent("""
        std_msgs/Header header
        string child_frame_id
        geometry_msgs/Transform transform
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Transform
        geometry_msgs/Vector3 translation
        geometry_msgs/Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "child_frame_id": "child_frame",
            "transform": {
                "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
                "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.child_frame_id == "child_frame"
    assert messages[0].data.transform.translation.x == 1.0
    assert messages[0].data.transform.translation.y == 2.0
    assert messages[0].data.transform.translation.z == 3.0
    assert messages[0].data.transform.rotation.x == 0.0
    assert messages[0].data.transform.rotation.y == 0.0
    assert messages[0].data.transform.rotation.z == 0.0
    assert messages[0].data.transform.rotation.w == 1.0


def test_geometry_msgs_twist():
    msgtype = "geometry_msgs/Twist"
    schema = dedent("""
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.linear.x == 1.0
    assert messages[0].data.linear.y == 2.0
    assert messages[0].data.linear.z == 3.0
    assert messages[0].data.angular.x == 4.0
    assert messages[0].data.angular.y == 5.0
    assert messages[0].data.angular.z == 6.0


def test_geometry_msgs_twiststamped():
    msgtype = "geometry_msgs/TwistStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Twist twist
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Twist
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "twist": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.twist.linear.x == 1.0
    assert messages[0].data.twist.linear.y == 2.0
    assert messages[0].data.twist.linear.z == 3.0
    assert messages[0].data.twist.angular.x == 4.0
    assert messages[0].data.twist.angular.y == 5.0
    assert messages[0].data.twist.angular.z == 6.0


def test_geometry_msgs_twistwithcovariance():
    msgtype = "geometry_msgs/TwistWithCovariance"
    schema = dedent("""
        geometry_msgs/Twist twist
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Twist
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "twist": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.twist.linear.x == 1.0
    assert messages[0].data.twist.linear.y == 2.0
    assert messages[0].data.twist.linear.z == 3.0
    assert messages[0].data.twist.angular.x == 4.0
    assert messages[0].data.twist.angular.y == 5.0
    assert messages[0].data.twist.angular.z == 6.0
    assert len(messages[0].data.covariance) == 36


def test_geometry_msgs_twistwithcovariancestamped():
    msgtype = "geometry_msgs/TwistWithCovarianceStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/TwistWithCovariance twist
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/TwistWithCovariance
        geometry_msgs/Twist twist
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Twist
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "twist": {
                "twist": {
                    "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
                },
                "covariance": [0.0] * 36
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.twist.twist.linear.x == 1.0
    assert messages[0].data.twist.twist.linear.y == 2.0
    assert messages[0].data.twist.twist.linear.z == 3.0
    assert messages[0].data.twist.twist.angular.x == 4.0
    assert messages[0].data.twist.twist.angular.y == 5.0
    assert messages[0].data.twist.twist.angular.z == 6.0
    assert len(messages[0].data.twist.covariance) == 36


def test_geometry_msgs_vector3():
    msgtype = "geometry_msgs/Vector3"
    schema = dedent("""
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_vector3stamped():
    msgtype = "geometry_msgs/Vector3Stamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Vector3 vector
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "vector": {"x": 1.0, "y": 2.0, "z": 3.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.vector.x == 1.0
    assert messages[0].data.vector.y == 2.0
    assert messages[0].data.vector.z == 3.0


def test_geometry_msgs_wrench():
    msgtype = "geometry_msgs/Wrench"
    schema = dedent("""
        geometry_msgs/Vector3 force
        geometry_msgs/Vector3 torque
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "force": {"x": 1.0, "y": 2.0, "z": 3.0},
            "torque": {"x": 4.0, "y": 5.0, "z": 6.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.force.x == 1.0
    assert messages[0].data.force.y == 2.0
    assert messages[0].data.force.z == 3.0
    assert messages[0].data.torque.x == 4.0
    assert messages[0].data.torque.y == 5.0
    assert messages[0].data.torque.z == 6.0


def test_geometry_msgs_wrenchstamped():
    msgtype = "geometry_msgs/WrenchStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Wrench wrench
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Wrench
        geometry_msgs/Vector3 force
        geometry_msgs/Vector3 torque
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "wrench": {
                "force": {"x": 1.0, "y": 2.0, "z": 3.0},
                "torque": {"x": 4.0, "y": 5.0, "z": 6.0}
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.wrench.force.x == 1.0
    assert messages[0].data.wrench.force.y == 2.0
    assert messages[0].data.wrench.force.z == 3.0
    assert messages[0].data.wrench.torque.x == 4.0
    assert messages[0].data.wrench.torque.y == 5.0
    assert messages[0].data.wrench.torque.z == 6.0
