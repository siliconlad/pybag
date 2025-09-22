import random
from pathlib import Path

import numpy as np
from mcap.writer import Writer as McapWriter
from rosbags.rosbag1 import Writer as Rosbag1Writer
from rosbags.rosbag2 import StoragePlugin
from rosbags.rosbag2 import Writer as Rosbag2Writer
from rosbags.typesys import Stores, get_typestore


def create_test_mcap(path: Path, message_count: int = 1000, seed: int = 0) -> Path:
    """Create an MCAP file with a single `/odom` topic of Odometry messages."""
    rng = random.Random(seed)

    typestore = get_typestore(Stores.LATEST)
    Odometry = typestore.types["nav_msgs/msg/Odometry"]
    Header = typestore.types["std_msgs/msg/Header"]
    Time = typestore.types["builtin_interfaces/msg/Time"]
    PoseWithCovariance = typestore.types["geometry_msgs/msg/PoseWithCovariance"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    TwistWithCovariance = typestore.types["geometry_msgs/msg/TwistWithCovariance"]
    Twist = typestore.types["geometry_msgs/msg/Twist"]
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]

    with Rosbag2Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=typestore)
        for i in range(message_count):
            timestamp = int(i * 1_500_000_000)
            odom = Odometry(
                header=Header(
                    stamp=Time(
                        sec=timestamp // 1_000_000_000,
                        nanosec=timestamp % 1_000_000_000,
                    ),
                    frame_id="map"
                ),
                child_frame_id="base_link",
                pose=PoseWithCovariance(
                    pose=Pose(
                        position=Point(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random()
                        ),
                        orientation=Quaternion(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                            w=rng.random(),
                        ),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)]),
                ),
                twist=TwistWithCovariance(
                    twist=Twist(
                        linear=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                        angular=Vector3(
                            x=rng.random(),
                            y=rng.random(),
                            z=rng.random(),
                        ),
                    ),
                    covariance=np.array([rng.random() for _ in range(36)]),
                ),
            )
            writer.write(odom_conn, timestamp, typestore.serialize_cdr(odom, Odometry.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def generate_ros1_odometries(message_count: int = 1000, seed: int = 0) -> list:
    """Generate ROS 1 Odometry messages."""

    rng = random.Random(seed)

    typestore = get_typestore(Stores.ROS1_NOETIC)
    Odometry = typestore.types["nav_msgs/msg/Odometry"]
    Header = typestore.types["std_msgs/msg/Header"]
    Time = typestore.types["builtin_interfaces/msg/Time"]
    PoseWithCovariance = typestore.types["geometry_msgs/msg/PoseWithCovariance"]
    Pose = typestore.types["geometry_msgs/msg/Pose"]
    Point = typestore.types["geometry_msgs/msg/Point"]
    Quaternion = typestore.types["geometry_msgs/msg/Quaternion"]
    TwistWithCovariance = typestore.types["geometry_msgs/msg/TwistWithCovariance"]
    Twist = typestore.types["geometry_msgs/msg/Twist"]
    Vector3 = typestore.types["geometry_msgs/msg/Vector3"]

    messages: list = []
    for i in range(message_count):
        timestamp = int(i * 1_500_000_000)
        odom = Odometry(
            header=Header(
                seq=i,
                stamp=Time(
                    sec=timestamp // 1_000_000_000,
                    nanosec=timestamp % 1_000_000_000,
                ),
                frame_id="map",
            ),
            child_frame_id="base_link",
            pose=PoseWithCovariance(
                pose=Pose(
                    position=Point(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    orientation=Quaternion(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                        w=rng.random(),
                    ),
                ),
                covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
            ),
            twist=TwistWithCovariance(
                twist=Twist(
                    linear=Vector3(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    angular=Vector3(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                ),
                covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
            ),
        )
        messages.append(odom)

    return messages


def create_test_ros1_bag(path: Path, message_count: int = 1000, seed: int = 0) -> Path:
    """Create a ROS 1 rosbag with Odometry messages."""

    typestore = get_typestore(Stores.ROS1_NOETIC)
    msgtype = "nav_msgs/msg/Odometry"
    bag_path = path.with_suffix(".bag")

    with Rosbag1Writer(bag_path) as writer:
        connection = writer.add_connection("/odom", msgtype, typestore=typestore)
        for i, ros_msg in enumerate(generate_ros1_odometries(message_count, seed)):
            timestamp = int(i * 1_500_000_000)
            serialized_msg = typestore.serialize_ros1(ros_msg, msgtype)
            writer.write(connection, timestamp, serialized_msg)

    return bag_path


def create_test_ros1_mcap(path: Path, message_count: int = 1000, seed: int = 0) -> Path:
    """Create an MCAP file with ROS 1 Odometry messages."""

    typestore = get_typestore(Stores.ROS1_NOETIC)
    msgtype = "nav_msgs/msg/Odometry"
    ros1_msgtype = msgtype.replace("/msg/", "/")
    msgdef, _ = typestore.generate_msgdef(msgtype)
    mcap_path = path.with_suffix(".mcap")

    with open(mcap_path, "wb") as stream:
        writer = McapWriter(stream)
        writer.start(profile="ros1", library="pybag-benchmarks")
        schema_id = writer.register_schema(
            name=ros1_msgtype,
            data=msgdef.encode(),
            encoding="ros1msg",
        )
        channel_id = writer.register_channel(
            topic="/odom",
            schema_id=schema_id,
            message_encoding="ros1",
        )
        for i, ros_msg in enumerate(generate_ros1_odometries(message_count, seed)):
            timestamp = int(i * 1_500_000_000)
            serialized_msg = typestore.serialize_ros1(ros_msg, msgtype)
            writer.add_message(
                channel_id=channel_id,
                log_time=timestamp,
                publish_time=timestamp,
                data=serialized_msg,
            )
        writer.finish()

    return mcap_path
