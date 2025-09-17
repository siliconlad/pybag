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


def test_nav_msgs_gridcells():
    msgtype = "nav_msgs/GridCells"
    schema = dedent("""
        std_msgs/Header header
        float32 cell_width
        float32 cell_height
        geometry_msgs/Point[] cells
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


def test_nav_msgs_mapmetadata():
    msgtype = "nav_msgs/MapMetaData"
    schema = dedent("""
        builtin_interfaces/Time map_load_time
        float32 resolution
        uint32 width
        uint32 height
        geometry_msgs/Pose origin
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


def test_nav_msgs_occupancygrid():
    msgtype = "nav_msgs/OccupancyGrid"
    schema = dedent("""
        std_msgs/Header header
        nav_msgs/MapMetaData info
        int8[] data
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: nav_msgs/MapMetaData
        builtin_interfaces/Time map_load_time
        float32 resolution
        uint32 width
        uint32 height
        geometry_msgs/Pose origin
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
                "frame_id": "map"
            },
            "info": {
                "map_load_time": {"sec": 100, "nanosec": 500000},
                "resolution": 0.05,
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
    assert messages[0].data.info.width == 4
    assert messages[0].data.info.height == 3
    assert abs(messages[0].data.info.resolution - 0.05) < 0.001
    assert len(messages[0].data.data) == 12
    assert list(messages[0].data.data) == [0, -1, 100, 50, 25, 75, 0, -1, 100, 50, 25, 75]


def test_nav_msgs_odometry():
    msgtype = "nav_msgs/Odometry"
    schema = dedent("""
        std_msgs/Header header
        string child_frame_id
        geometry_msgs/PoseWithCovariance pose
        geometry_msgs/TwistWithCovariance twist
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
    assert messages[0].data.twist.twist.linear.x == 0.5
    assert messages[0].data.twist.twist.angular.z == 0.1
    assert len(messages[0].data.pose.covariance) == 36
    assert len(messages[0].data.twist.covariance) == 36


def test_nav_msgs_path():
    msgtype = "nav_msgs/Path"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/PoseStamped[] poses
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseStamped
        std_msgs/Header header
        geometry_msgs/Pose pose
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
                {
                    "header": {
                        "stamp": {"sec": 125, "nanosec": 200000},
                        "frame_id": "map"
                    },
                    "pose": {
                        "position": {"x": 1.0, "y": 0.0, "z": 0.0},
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
    assert len(messages[0].data.poses) == 2
    assert messages[0].data.poses[0].header.stamp.sec == 124
    assert messages[0].data.poses[0].pose.position.x == 0.0
    assert messages[0].data.poses[0].pose.position.y == 0.0
    assert messages[0].data.poses[1].header.stamp.sec == 125
    assert messages[0].data.poses[1].pose.position.x == 1.0
    assert messages[0].data.poses[1].pose.position.y == 0.0
