"""Test reading nav_msgs messages written with the official MCAP writer."""

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


def test_nav_msgs_goals():
    msgtype = "nav_msgs/Goals"
    schema = dedent("""
        # An array of poses that represents goals for a robot
        std_msgs/Header header
        geometry_msgs/PoseStamped[] goals
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseStamped
        # A Pose with reference coordinate frame and timestamp
        std_msgs/Header header
        Pose pose
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of position and orientation in free space
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "map"
            },
            "goals": [
                {
                    "header": {
                        "stamp": {"sec": 124, "nanosec": 100000},
                        "frame_id": "map"
                    },
                    "pose": {
                        "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                        "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                    }
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
    assert messages[0].data.header.frame_id == "map"
    assert len(messages[0].data.goals) == 1
    assert messages[0].data.goals[0].header.stamp.sec == 124
    assert messages[0].data.goals[0].header.stamp.nanosec == 100000
    assert messages[0].data.goals[0].header.frame_id == "map"
    assert messages[0].data.goals[0].pose.position.x == 1.0
    assert messages[0].data.goals[0].pose.position.y == 2.0
    assert messages[0].data.goals[0].pose.position.z == 3.0
    assert messages[0].data.goals[0].pose.orientation.x == 0.0
    assert messages[0].data.goals[0].pose.orientation.y == 0.0
    assert messages[0].data.goals[0].pose.orientation.z == 0.0
    assert messages[0].data.goals[0].pose.orientation.w == 1.0


def test_nav_msgs_grid_cells():
    msgtype = "nav_msgs/GridCells"
    schema = dedent("""
        # An array of cells in a 2D grid
        std_msgs/Header header
        float32 cell_width
        float32 cell_height
        geometry_msgs/Point[] cells
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "map"
            },
            "cell_width": 0.05,
            "cell_height": 0.05,
            "cells": [
                {"x": 1.0, "y": 2.0, "z": 0.0},
                {"x": 1.5, "y": 2.5, "z": 0.0}
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
    assert messages[0].data.header.frame_id == "map"
    assert abs(messages[0].data.cell_width - 0.05) < 0.001
    assert abs(messages[0].data.cell_height - 0.05) < 0.001
    assert len(messages[0].data.cells) == 2
    assert messages[0].data.cells[0].x == 1.0
    assert messages[0].data.cells[0].y == 2.0
    assert messages[0].data.cells[0].z == 0.0
    assert messages[0].data.cells[1].x == 1.5
    assert messages[0].data.cells[1].y == 2.5
    assert messages[0].data.cells[1].z == 0.0


def test_nav_msgs_map_meta_data():
    msgtype = "nav_msgs/MapMetaData"
    schema = dedent("""
        # This hold basic information about the characterists of the OccupancyGrid

        # The time at which the map was loaded
        builtin_interfaces/Time map_load_time
        # The map resolution [m/cell]
        float32 resolution
        # Map width [cells]
        uint32 width
        # Map height [cells]
        uint32 height
        # The origin of the map [m, m, rad]. This is the real-world pose of the
        # cell (0,0) in the map.
        geometry_msgs/Pose origin
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of position and orientation in free space
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "map_load_time": {"sec": 100, "nanosec": 500000},
            "resolution": 0.05,
            "width": 800,
            "height": 600,
            "origin": {
                "position": {"x": -20.0, "y": -15.0, "z": 0.0},
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
    assert messages[0].data.map_load_time.sec == 100
    assert messages[0].data.map_load_time.nanosec == 500000
    assert abs(messages[0].data.resolution - 0.05) < 0.001
    assert messages[0].data.width == 800
    assert messages[0].data.height == 600
    assert messages[0].data.origin.position.x == -20.0
    assert messages[0].data.origin.position.y == -15.0
    assert messages[0].data.origin.position.z == 0.0
    assert messages[0].data.origin.orientation.x == 0.0
    assert messages[0].data.origin.orientation.y == 0.0
    assert messages[0].data.origin.orientation.z == 0.0
    assert messages[0].data.origin.orientation.w == 1.0


def test_nav_msgs_occupancy_grid():
    msgtype = "nav_msgs/OccupancyGrid"
    schema = dedent("""
        # This represents a 2-D grid map, in which each cell represents the probability of
        # occupancy.

        std_msgs/Header header

        # MetaData for the map
        nav_msgs/MapMetaData info

        # The map data, in row-major order, starting with (0,0). Occupancy
        # probabilities are in the range [0,100]. Unknown is -1.
        int8[] data
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: nav_msgs/MapMetaData
        # This hold basic information about the characterists of the OccupancyGrid

        # The time at which the map was loaded
        builtin_interfaces/Time map_load_time
        # The map resolution [m/cell]
        float32 resolution
        # Map width [cells]
        uint32 width
        # Map height [cells]
        uint32 height
        # The origin of the map [m, m, rad]. This is the real-world pose of the
        # cell (0,0) in the map.
        geometry_msgs/Pose origin
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of position and orientation in free space
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "map"
            },
            "info": {
                "map_load_time": {"sec": 100, "nanosec": 500000},
                "resolution": 0.25,
                "width": 4,
                "height": 3,
                "origin": {
                    "position": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                }
            },
            "data": [0, -1, 100, 50, 25, 75, 0, -1, 100, 50, 25, 75]  # 4x3 grid
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
    assert messages[0].data.header.frame_id == "map"
    assert messages[0].data.info.map_load_time.sec == 100
    assert messages[0].data.info.map_load_time.nanosec == 500000
    assert messages[0].data.info.resolution == 0.25
    assert messages[0].data.info.width == 4
    assert messages[0].data.info.height == 3
    assert messages[0].data.info.origin.position.x == 0.0
    assert messages[0].data.info.origin.position.y == 0.0
    assert messages[0].data.info.origin.position.z == 0.0
    assert messages[0].data.info.origin.orientation.x == 0.0
    assert messages[0].data.info.origin.orientation.y == 0.0
    assert messages[0].data.info.origin.orientation.z == 0.0
    assert messages[0].data.info.origin.orientation.w == 1.0
    assert messages[0].data.data == [0, -1, 100, 50, 25, 75, 0, -1, 100, 50, 25, 75]


def test_nav_msgs_odometry():
    msgtype = "nav_msgs/Odometry"
    schema = dedent("""
        # This represents an estimate of a position and velocity in free space.
        # The pose in this message should be specified in the coordinate frame given by header.frame_id.
        # The twist in this message should be specified in the coordinate frame given by the child_frame_id
        std_msgs/Header header
        string child_frame_id
        geometry_msgs/PoseWithCovariance pose
        geometry_msgs/TwistWithCovariance twist
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseWithCovariance
        # This represents a pose in free space with uncertainty.

        Pose pose

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of position and orientation in free space
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
        ================================================================================
        MSG: geometry_msgs/TwistWithCovariance
        # This expresses velocity in free space with uncertainty.

        Twist twist

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.
        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.
        # It is only meant to represent a direction. Therefore, it does not
        # make sense to apply a translation to it (e.g., when applying a
        # generic rigid transformation to a Vector3, tf2 will only apply the
        # rotation). If you want your data to be translatable too, use the
        # geometry_msgs/Point message instead.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "odom"
            },
            "child_frame_id": "base_link",
            "pose": {
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                },
                "covariance": [0.0] * 36
            },
            "twist": {
                "twist": {
                    "linear": {"x": 0.5, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.1}
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
    assert messages[0].data.header.frame_id == "odom"
    assert messages[0].data.child_frame_id == "base_link"
    assert messages[0].data.pose.pose.position.x == 1.0
    assert messages[0].data.pose.pose.position.y == 2.0
    assert messages[0].data.pose.pose.position.z == 0.0
    assert messages[0].data.pose.pose.orientation.x == 0.0
    assert messages[0].data.pose.pose.orientation.y == 0.0
    assert messages[0].data.pose.pose.orientation.z == 0.0
    assert messages[0].data.pose.pose.orientation.w == 1.0
    assert messages[0].data.pose.covariance == [0.0] * 36
    assert messages[0].data.twist.twist.linear.x == 0.5
    assert messages[0].data.twist.twist.linear.y == 0.0
    assert messages[0].data.twist.twist.linear.z == 0.0
    assert messages[0].data.twist.twist.angular.x == 0.0
    assert messages[0].data.twist.twist.angular.y == 0.0
    assert messages[0].data.twist.twist.angular.z == 0.1
    assert messages[0].data.twist.covariance == [0.0] * 36


def test_nav_msgs_path():
    msgtype = "nav_msgs/Path"
    schema = dedent("""
        # An array of poses that represents a Path for a robot to follow
        std_msgs/Header header
        geometry_msgs/PoseStamped[] poses
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseStamped
        # A Pose with reference coordinate frame and timestamp
        std_msgs/Header header
        Pose pose
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of position and orientation in free space
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "map"
            },
            "poses": [
                {
                    "header": {
                        "stamp": {"sec": 124, "nanosec": 100000},
                        "frame_id": "map"
                    },
                    "pose": {
                        "position": {"x": 0.0, "y": 0.0, "z": 0.0},
                        "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                    }
                },
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
    assert messages[0].data.header.frame_id == "map"
    assert len(messages[0].data.poses) == 1
    assert messages[0].data.poses[0].header.stamp.sec == 124
    assert messages[0].data.poses[0].header.stamp.nanosec == 100000
    assert messages[0].data.poses[0].header.frame_id == "map"
    assert messages[0].data.poses[0].pose.position.x == 0.0
    assert messages[0].data.poses[0].pose.position.y == 0.0
    assert messages[0].data.poses[0].pose.position.z == 0.0
    assert messages[0].data.poses[0].pose.orientation.x == 0.0
    assert messages[0].data.poses[0].pose.orientation.y == 0.0
    assert messages[0].data.poses[0].pose.orientation.z == 0.0
    assert messages[0].data.poses[0].pose.orientation.w == 1.0
