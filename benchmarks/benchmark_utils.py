import math
import random
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import StoragePlugin, Writer
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

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
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


def create_large_mcap(
    path: Path,
    target_size_bytes: int = 1 << 30,
    string_payload_bytes: int = 2 * 1024 * 1024,
    seed: int = 0,
) -> Path:
    """Create a large MCAP file that is at least ``target_size_bytes`` in size."""

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

    large_frame_id = "map_" + ("x" * string_payload_bytes)
    large_child_frame_id = "base_" + ("y" * string_payload_bytes)

    odom = Odometry(
        header=Header(
            stamp=Time(sec=0, nanosec=0),
            frame_id=large_frame_id,
        ),
        child_frame_id=large_child_frame_id,
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

    serialized = typestore.serialize_cdr(odom, Odometry.__msgtype__)
    message_count = max(1, math.ceil(target_size_bytes / len(serialized)))

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=typestore)
        for i in range(message_count):
            timestamp = int(i * 1_500_000_000)
            writer.write(odom_conn, timestamp, serialized)

    mcap_path = next(Path(path).rglob("*.mcap"))
    if mcap_path.stat().st_size < target_size_bytes:
        raise RuntimeError(
            f"Generated MCAP size {mcap_path.stat().st_size} bytes is smaller than expected"
        )
    return mcap_path
