import logging
import math
import random
from pathlib import Path
from typing import Any, Callable

import numpy as np
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

# Size in bytes
KILOBYTE = 1 << 10
MEGABYTE = 1 << 20
GIGABYTE = 1 << 30

# Rosbags types
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

# Additional rosbags types for multi-topic benchmarks
Imu = TYPESTORE.types["sensor_msgs/msg/Imu"]
LaserScan = TYPESTORE.types["sensor_msgs/msg/LaserScan"]
PointCloud2 = TYPESTORE.types["sensor_msgs/msg/PointCloud2"]
PointField = TYPESTORE.types["sensor_msgs/msg/PointField"]
Image = TYPESTORE.types["sensor_msgs/msg/Image"]
CameraInfo = TYPESTORE.types["sensor_msgs/msg/CameraInfo"]
RegionOfInterest = TYPESTORE.types["sensor_msgs/msg/RegionOfInterest"]
TransformStamped = TYPESTORE.types["geometry_msgs/msg/TransformStamped"]
Transform = TYPESTORE.types["geometry_msgs/msg/Transform"]
String = TYPESTORE.types["std_msgs/msg/String"]
Float64 = TYPESTORE.types["std_msgs/msg/Float64"]


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


def generate_rosbags_imu(rng: random.Random, timestamp: int) -> Any:
    """Generate an IMU message using rosbags types."""
    return Imu(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="imu_link",
        ),
        orientation=Quaternion(
            x=rng.random(),
            y=rng.random(),
            z=rng.random(),
            w=rng.random(),
        ),
        orientation_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
        angular_velocity=Vector3(
            x=rng.random() * 0.1,
            y=rng.random() * 0.1,
            z=rng.random() * 0.1,
        ),
        angular_velocity_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
        linear_acceleration=Vector3(
            x=rng.random() * 9.8,
            y=rng.random() * 9.8,
            z=9.8 + rng.random(),
        ),
        linear_acceleration_covariance=np.array([rng.random() for _ in range(9)], dtype=np.float64),
    )


def generate_rosbags_laser_scan(rng: random.Random, timestamp: int, num_ranges: int = 360) -> Any:
    """Generate a LaserScan message using rosbags types."""
    return LaserScan(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="laser_link",
        ),
        angle_min=-3.14159,
        angle_max=3.14159,
        angle_increment=6.28318 / num_ranges,
        time_increment=0.0001,
        scan_time=0.1,
        range_min=0.1,
        range_max=30.0,
        ranges=np.array([rng.random() * 10.0 for _ in range(num_ranges)], dtype=np.float32),
        intensities=np.array([rng.random() * 100.0 for _ in range(num_ranges)], dtype=np.float32),
    )


def generate_rosbags_point_cloud2(
    rng: random.Random, timestamp: int, num_points: int = 1000
) -> Any:
    """Generate a PointCloud2 message using rosbags types."""
    # Generate XYZ + intensity point cloud
    point_step = 16  # 4 floats * 4 bytes
    row_step = point_step * num_points
    data = np.zeros(num_points * 4, dtype=np.float32)
    for i in range(num_points):
        data[i * 4] = rng.random() * 10.0 - 5.0  # x
        data[i * 4 + 1] = rng.random() * 10.0 - 5.0  # y
        data[i * 4 + 2] = rng.random() * 3.0  # z
        data[i * 4 + 3] = rng.random() * 100.0  # intensity

    return PointCloud2(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="lidar_link",
        ),
        height=1,
        width=num_points,
        fields=[
            PointField(name="x", offset=0, datatype=7, count=1),  # FLOAT32
            PointField(name="y", offset=4, datatype=7, count=1),
            PointField(name="z", offset=8, datatype=7, count=1),
            PointField(name="intensity", offset=12, datatype=7, count=1),
        ],
        is_bigendian=False,
        point_step=point_step,
        row_step=row_step,
        data=data.view(np.uint8),
        is_dense=True,
    )


def generate_rosbags_image(
    rng: random.Random, timestamp: int, width: int = 640, height: int = 480
) -> Any:
    """Generate an Image message using rosbags types."""
    return Image(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="camera_link",
        ),
        height=height,
        width=width,
        encoding="rgb8",
        is_bigendian=0,
        step=width * 3,
        data=np.array([rng.randint(0, 255) for _ in range(width * height * 3)], dtype=np.uint8),
    )


def generate_rosbags_camera_info(
    rng: random.Random, timestamp: int, width: int = 640, height: int = 480
) -> Any:
    """Generate a CameraInfo message using rosbags types."""
    return CameraInfo(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="camera_optical_frame",
        ),
        height=height,
        width=width,
        distortion_model="plumb_bob",
        d=np.array([0.0, 0.0, 0.0, 0.0, 0.0], dtype=np.float64),
        k=np.array([500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0], dtype=np.float64),
        r=np.eye(3, dtype=np.float64).flatten(),
        p=np.array(
            [500.0, 0.0, 320.0, 0.0, 0.0, 500.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0], dtype=np.float64
        ),
        binning_x=0,
        binning_y=0,
        roi=RegionOfInterest(
            x_offset=0,
            y_offset=0,
            height=0,
            width=0,
            do_rectify=False,
        ),
    )


def generate_rosbags_transform_stamped(rng: random.Random, timestamp: int) -> Any:
    """Generate a TransformStamped message using rosbags types."""
    return TransformStamped(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="map",
        ),
        child_frame_id="base_link",
        transform=Transform(
            translation=Vector3(
                x=rng.random() * 10.0,
                y=rng.random() * 10.0,
                z=rng.random(),
            ),
            rotation=Quaternion(
                x=rng.random(),
                y=rng.random(),
                z=rng.random(),
                w=rng.random(),
            ),
        ),
    )


def generate_rosbags_odometry(rng: random.Random, timestamp: int) -> Any:
    """Generate an Odometry message using rosbags types."""
    return Odometry(
        header=Header(
            stamp=Time(
                sec=timestamp // 1_000_000_000,
                nanosec=timestamp % 1_000_000_000,
            ),
            frame_id="odom",
        ),
        child_frame_id="base_link",
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(
                    x=rng.random() * 10.0,
                    y=rng.random() * 10.0,
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


def generate_rosbags_string(rng: random.Random, timestamp: int) -> Any:
    """Generate a String message using rosbags types."""
    return String(data=f"message_{timestamp}_{rng.randint(0, 1000000)}")


def generate_rosbags_float64(rng: random.Random, timestamp: int) -> Any:
    """Generate a Float64 message using rosbags types."""
    return Float64(data=rng.random() * 1000.0)


# Topic configurations for multi-topic benchmarks
MULTI_TOPIC_CONFIG = [
    ("/odom", Odometry, generate_rosbags_odometry, 100),  # 100 Hz
    ("/imu", Imu, generate_rosbags_imu, 200),  # 200 Hz
    ("/scan", LaserScan, generate_rosbags_laser_scan, 10),  # 10 Hz
    ("/tf", TransformStamped, generate_rosbags_transform_stamped, 100),  # 100 Hz
    ("/status", String, generate_rosbags_string, 1),  # 1 Hz
    ("/battery_voltage", Float64, generate_rosbags_float64, 1),  # 1 Hz
]

MULTI_TOPIC_WITH_IMAGES_CONFIG = [
    ("/odom", Odometry, generate_rosbags_odometry, 100),  # 100 Hz
    ("/imu", Imu, generate_rosbags_imu, 200),  # 200 Hz
    ("/scan", LaserScan, generate_rosbags_laser_scan, 10),  # 10 Hz
    ("/camera/image_raw", Image, generate_rosbags_image, 30),  # 30 Hz
    ("/camera/camera_info", CameraInfo, generate_rosbags_camera_info, 30),  # 30 Hz
    ("/points", PointCloud2, generate_rosbags_point_cloud2, 10),  # 10 Hz
    ("/tf", TransformStamped, generate_rosbags_transform_stamped, 100),  # 100 Hz
]


def create_multi_topic_mcap(
    path: Path,
    duration_sec: float = 1.0,
    topic_config: list | None = None,
    seed: int = 0,
) -> Path:
    """
    Create an MCAP file with multiple topics at different frequencies.

    Args:
        path: Base path for the MCAP file
        duration_sec: Duration of recording in seconds
        topic_config: List of (topic_name, msg_type, generator_func, frequency_hz) tuples
        seed: Random seed for reproducibility

    Returns:
        Path to the created MCAP file
    """
    if topic_config is None:
        topic_config = MULTI_TOPIC_CONFIG

    rng = random.Random(seed)

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        # Register all connections
        connections = {}
        for topic_name, msg_type, _, _ in topic_config:
            connections[topic_name] = writer.add_connection(
                topic_name, msg_type.__msgtype__, typestore=TYPESTORE
            )

        # Generate messages for all topics based on their frequencies
        # Use a unified timeline approach
        duration_ns = int(duration_sec * 1_000_000_000)
        messages: list[tuple[int, str, Any]] = []

        for topic_name, msg_type, generator, freq_hz in topic_config:
            period_ns = int(1_000_000_000 / freq_hz)
            timestamp = 0
            while timestamp < duration_ns:
                msg = generator(rng, timestamp)
                messages.append((timestamp, topic_name, msg))
                timestamp += period_ns

        # Sort by timestamp and write
        messages.sort(key=lambda x: x[0])
        for timestamp, topic_name, msg in messages:
            conn = connections[topic_name]
            serialized = TYPESTORE.serialize_cdr(msg, conn.msgtype)
            writer.write(conn, timestamp, serialized)

    return next(Path(path).rglob("*.mcap"))


def create_small_mcap(path: Path, message_count: int = 10, seed: int = 0) -> Path:
    """Create a small MCAP file with a few messages for quick benchmarks."""
    return create_test_mcap(path, message_count=message_count, seed=seed)
