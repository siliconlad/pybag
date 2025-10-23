import logging
import math
import random
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

# Size in bytes
KILOBYTE = 1 << 10
MEGABYTE = 1 << 20
GIGABYTE = 1 << 30

# Rosbags type
TYPESTORE = get_typestore(Stores.LATEST)
Odometry = TYPESTORE.types["nav_msgs/msg/Odometry"]
Header = TYPESTORE.types["std_msgs/msg/Header"]
Time = TYPESTORE.types["builtin_interfaces/msg/Time"]
PoseWithCovariance = TYPESTORE.types["geometry_msgs/msg/PoseWithCovariance"]
Pose = TYPESTORE.types["geometry_msgs/msg/Pose"]
Point = TYPESTORE.types["geometry_msgs/msg/Point"]
Quaternion = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
TwistWithCovariance = TYPESTORE.types["geometry_msgs/msg/TwistWithCovariance"]
Twist = TYPESTORE.types["geometry_msgs/msg/Twist"]
Vector3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]


def create_test_mcap(path: Path, message_count: int = 1000, seed: int = 0) -> Path:
    """Create an MCAP file with a single `/odom` topic of Odometry messages."""
    rng = random.Random(seed)

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=TYPESTORE)
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
            writer.write(odom_conn, timestamp, TYPESTORE.serialize_cdr(odom, Odometry.__msgtype__))

    return next(Path(path).rglob("*.mcap"))


def create_test_mcap_by_size(
    path: Path,
    target_size_bytes: int = GIGABYTE,
    seed: int = 0,
) -> Path:
    """Create a large MCAP file that is at least ``target_size_bytes`` in size."""
    rng = random.Random(seed)

    odom = Odometry(
        header=Header(
            stamp=Time(sec=0, nanosec=0),
            frame_id='frame_id',
        ),
        child_frame_id='child_frame_id',
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

    serialized = TYPESTORE.serialize_cdr(odom, Odometry.__msgtype__)
    message_count = max(1, math.ceil(target_size_bytes / len(serialized)))

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=TYPESTORE)
        for i in range(message_count):
            timestamp = int(i * 1_500_000_000)
            writer.write(odom_conn, timestamp, serialized)

    mcap_path = next(Path(path).rglob("*.mcap"))
    if mcap_path.stat().st_size < target_size_bytes:
        raise RuntimeError(
            f"Generated MCAP size {mcap_path.stat().st_size} bytes is smaller than expected"
        )
    return mcap_path
