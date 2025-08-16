import random
from dataclasses import dataclass
from itertools import count
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

import numpy as np
from mcap.reader import make_reader
from mcap.writer import Writer as McapWriter
from pytest_benchmark.fixture import BenchmarkFixture
from rosbags.rosbag2 import StoragePlugin
from rosbags.rosbag2 import Writer as RosbagsWriter
from rosbags.typesys import Stores, get_typestore

import pybag.types as t
from pybag.mcap_writer import McapFileWriter

_typestore = get_typestore(Stores.LATEST)
OdometryRos = _typestore.types["nav_msgs/msg/Odometry"]
HeaderRos = _typestore.types["std_msgs/msg/Header"]
TimeRos = _typestore.types["builtin_interfaces/msg/Time"]
PoseWithCovarianceRos = _typestore.types["geometry_msgs/msg/PoseWithCovariance"]
PoseRos = _typestore.types["geometry_msgs/msg/Pose"]
PointRos = _typestore.types["geometry_msgs/msg/Point"]
QuaternionRos = _typestore.types["geometry_msgs/msg/Quaternion"]
TwistWithCovarianceRos = _typestore.types["geometry_msgs/msg/TwistWithCovariance"]
TwistRos = _typestore.types["geometry_msgs/msg/Twist"]
Vector3Ros = _typestore.types["geometry_msgs/msg/Vector3"]


@dataclass
class Time:
    sec: t.int32
    nanosec: t.uint32


@dataclass
class Header:
    stamp: t.Complex(Time)
    frame_id: t.string


@dataclass
class Point:
    x: t.float64
    y: t.float64
    z: t.float64


@dataclass
class Quaternion:
    x: t.float64
    y: t.float64
    z: t.float64
    w: t.float64


@dataclass
class Pose:
    position: t.Complex(Point)
    orientation: t.Complex(Quaternion)


@dataclass
class PoseWithCovariance:
    pose: t.Complex(Pose)
    covariance: t.Array(t.float64, length=36)


@dataclass
class Vector3:
    x: t.float64
    y: t.float64
    z: t.float64


@dataclass
class Twist:
    linear: t.Complex(Vector3)
    angular: t.Complex(Vector3)


@dataclass
class TwistWithCovariance:
    twist: t.Complex(Twist)
    covariance: t.Array(t.float64, length=36)


@dataclass
class Odometry:
    header: t.Complex(Header)
    child_frame_id: t.string
    pose: t.Complex(PoseWithCovariance)
    twist: t.Complex(TwistWithCovariance)

# Default instances for schema generation
Header.stamp = Time(0, 0)
Header.frame_id = ""
Pose.position = Point(0.0, 0.0, 0.0)
Pose.orientation = Quaternion(0.0, 0.0, 0.0, 0.0)
PoseWithCovariance.pose = Pose(Point(0.0, 0.0, 0.0), Quaternion(0.0, 0.0, 0.0, 0.0))
PoseWithCovariance.covariance = [0.0] * 36
Vector3.x = 0.0
Vector3.y = 0.0
Vector3.z = 0.0
Twist.linear = Vector3(0.0, 0.0, 0.0)
Twist.angular = Vector3(0.0, 0.0, 0.0)
TwistWithCovariance.twist = Twist(Vector3(0.0, 0.0, 0.0), Vector3(0.0, 0.0, 0.0))
TwistWithCovariance.covariance = [0.0] * 36
Odometry.header = Header(Time(0, 0), "")
Odometry.child_frame_id = ""
Odometry.pose = PoseWithCovariance(
    Pose(Point(0.0, 0.0, 0.0), Quaternion(0.0, 0.0, 0.0, 0.0)),
    [0.0] * 36,
)
Odometry.twist = TwistWithCovariance(
    Twist(Vector3(0.0, 0.0, 0.0), Vector3(0.0, 0.0, 0.0)),
    [0.0] * 36,
)


def _ros_to_pybag(odom: OdometryRos) -> Odometry:
    return Odometry(
        header=Header(
            stamp=Time(
                sec=int(odom.header.stamp.sec),
                nanosec=int(odom.header.stamp.nanosec),
            ),
            frame_id=odom.header.frame_id,
        ),
        child_frame_id=odom.child_frame_id,
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(
                    x=float(odom.pose.pose.position.x),
                    y=float(odom.pose.pose.position.y),
                    z=float(odom.pose.pose.position.z),
                ),
                orientation=Quaternion(
                    x=float(odom.pose.pose.orientation.x),
                    y=float(odom.pose.pose.orientation.y),
                    z=float(odom.pose.pose.orientation.z),
                    w=float(odom.pose.pose.orientation.w),
                ),
            ),
            covariance=[float(v) for v in odom.pose.covariance],
        ),
        twist=TwistWithCovariance(
            twist=Twist(
                linear=Vector3(
                    x=float(odom.twist.twist.linear.x),
                    y=float(odom.twist.twist.linear.y),
                    z=float(odom.twist.twist.linear.z),
                ),
                angular=Vector3(
                    x=float(odom.twist.twist.angular.x),
                    y=float(odom.twist.twist.angular.y),
                    z=float(odom.twist.twist.angular.z),
                ),
            ),
            covariance=[float(v) for v in odom.twist.covariance],
        ),
    )


def generate_odometries(count: int = 1000, seed: int = 0) -> list[OdometryRos]:
    rng = random.Random(seed)
    messages: list[OdometryRos] = []
    for i in range(count):
        timestamp = int(i * 1_500_000_000)
        msg = OdometryRos(
            header=HeaderRos(
                stamp=TimeRos(
                    sec=timestamp // 1_000_000_000,
                    nanosec=timestamp % 1_000_000_000,
                ),
                frame_id="map",
            ),
            child_frame_id="base_link",
            pose=PoseWithCovarianceRos(
                pose=PoseRos(
                    position=PointRos(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    orientation=QuaternionRos(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                        w=rng.random(),
                    ),
                ),
                covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
            ),
            twist=TwistWithCovarianceRos(
                twist=TwistRos(
                    linear=Vector3Ros(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    angular=Vector3Ros(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                ),
                covariance=np.array([rng.random() for _ in range(36)], dtype=np.float64),
            ),
        )
        messages.append(msg)
    return messages


def write_with_pybag(path: Path, messages: Iterable[OdometryRos]) -> Path:
    with McapFileWriter.open(path) as writer:
        for i, ros_msg in enumerate(messages):
            timestamp = int(i * 1_500_000_000)
            writer.write_message("/odom", timestamp, _ros_to_pybag(ros_msg))
    return path


def write_with_rosbags(path: Path, messages: Iterable[OdometryRos]) -> Path:
    with RosbagsWriter(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        conn = writer.add_connection("/odom", OdometryRos.__msgtype__, typestore=_typestore)
        for i, ros_msg in enumerate(messages):
            timestamp = int(i * 1_500_000_000)
            writer.write(conn, timestamp, _typestore.serialize_cdr(ros_msg, OdometryRos.__msgtype__))
    return next(Path(path).rglob("*.mcap"))


def write_with_official(path: Path, messages: Iterable[OdometryRos]) -> Path:
    schema_data = _typestore.generate_msgdef(OdometryRos.__msgtype__)[0].encode()
    writer = McapWriter(str(path))
    writer.start(profile="ros2")
    schema_id = writer.register_schema(OdometryRos.__msgtype__, "ros2msg", schema_data)
    channel_id = writer.register_channel("/odom", "cdr", schema_id)
    for i, ros_msg in enumerate(messages):
        timestamp = int(i * 1_500_000_000)
        data = _typestore.serialize_cdr(ros_msg, OdometryRos.__msgtype__)
        writer.add_message(channel_id, timestamp, data, timestamp, sequence=i)
    writer.finish()
    return path


def read_odometries(mcap: Path) -> list[OdometryRos]:
    with open(mcap, "rb") as f:
        reader = make_reader(f)
        msgs: list[OdometryRos] = []
        for schema, channel, message in reader.iter_messages():
            if channel.topic == "/odom":
                msgs.append(_typestore.deserialize_cdr(message.data, OdometryRos.__msgtype__))
        return msgs


def assert_odometry_equal(odom1: OdometryRos, odom2: OdometryRos) -> None:
    assert odom1.header.frame_id == odom2.header.frame_id
    assert odom1.header.stamp.sec == odom2.header.stamp.sec
    assert odom1.header.stamp.nanosec == odom2.header.stamp.nanosec
    assert odom1.child_frame_id == odom2.child_frame_id
    assert np.isclose(odom1.pose.pose.position.x, odom2.pose.pose.position.x)
    assert np.isclose(odom1.pose.pose.position.y, odom2.pose.pose.position.y)
    assert np.isclose(odom1.pose.pose.position.z, odom2.pose.pose.position.z)
    assert np.isclose(odom1.pose.pose.orientation.x, odom2.pose.pose.orientation.x)
    assert np.isclose(odom1.pose.pose.orientation.y, odom2.pose.pose.orientation.y)
    assert np.isclose(odom1.pose.pose.orientation.z, odom2.pose.pose.orientation.z)
    assert np.isclose(odom1.pose.pose.orientation.w, odom2.pose.pose.orientation.w)
    assert np.allclose(odom1.pose.covariance, odom2.pose.covariance)
    assert np.isclose(odom1.twist.twist.linear.x, odom2.twist.twist.linear.x)
    assert np.isclose(odom1.twist.twist.linear.y, odom2.twist.twist.linear.y)
    assert np.isclose(odom1.twist.twist.linear.z, odom2.twist.twist.linear.z)
    assert np.isclose(odom1.twist.twist.angular.x, odom2.twist.twist.angular.x)
    assert np.isclose(odom1.twist.twist.angular.y, odom2.twist.twist.angular.y)
    assert np.isclose(odom1.twist.twist.angular.z, odom2.twist.twist.angular.z)
    assert np.allclose(odom1.twist.covariance, odom2.twist.covariance)


def test_writers_produce_same_messages() -> None:
    with TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        messages = generate_odometries(10)
        pybag_mcap = write_with_pybag(tmp / "pybag.mcap", messages)
        official_mcap = write_with_official(tmp / "official.mcap", messages)
        rosbags_mcap = write_with_rosbags(tmp / "rosbags", messages)

        pybag_msgs = read_odometries(pybag_mcap)
        official_msgs = read_odometries(official_mcap)
        rosbags_msgs = read_odometries(rosbags_mcap)

        for pybag_msg, rosbags_msg in zip(pybag_msgs, rosbags_msgs, strict=True):
            assert_odometry_equal(pybag_msg, rosbags_msg)

        for pybag_msg, official_msg in zip(pybag_msgs, official_msgs, strict=True):
            assert_odometry_equal(pybag_msg, official_msg)


def test_pybag(benchmark: BenchmarkFixture) -> None:
    messages = generate_odometries()
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "pybag.mcap"
        benchmark(lambda: write_with_pybag(path, messages))


def test_official(benchmark: BenchmarkFixture) -> None:
    messages = generate_odometries()
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "official.mcap"
        benchmark(lambda: write_with_official(path, messages))


def test_rosbags(benchmark: BenchmarkFixture) -> None:
    messages = generate_odometries()
    with TemporaryDirectory() as tmpdir:
        counter = count()
        benchmark(lambda: write_with_rosbags(Path(tmpdir) / str(next(counter)), messages))

