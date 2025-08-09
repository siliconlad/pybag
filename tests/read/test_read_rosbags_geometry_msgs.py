"""Test the reading of geometry_msgs messages."""
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob('*.mcap'))


def _write_rosbags(
    temp_dir: str,
    msg,
    typestore,
    topic: str = '/rosbags',
    *,
    timestamp: int = 0,
) -> tuple[Path, int]:
    with Writer(Path(temp_dir) / 'rosbags', version=9, storage_plugin=StoragePlugin.MCAP) as writer:
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


def test_vector3_rosbags(typestore: Typestore):
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Vector3(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.z == 3.0
        # TODO: Test the type somehow?


def test_pose_with_covariance_rosbags(typestore: Typestore):
    PoseWithCovariance = typestore.types['geometry_msgs/msg/PoseWithCovariance']
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = PoseWithCovariance(
        pose=Pose(
            position=Point(x=1.0, y=2.0, z=3.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
        covariance=np.array([float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.pose.position.x == 1.0
        assert messages[0].data.pose.position.y == 2.0
        assert messages[0].data.pose.position.z == 3.0
        assert messages[0].data.pose.orientation.x == 0.0
        assert messages[0].data.pose.orientation.y == 0.0
        assert messages[0].data.pose.orientation.z == 0.0
        assert messages[0].data.pose.orientation.w == 1.0
        assert messages[0].data.covariance == [float(i) for i in range(36)]


def test_vector3_stamped_rosbags(typestore: Typestore):
    Vector3Stamped = typestore.types['geometry_msgs/msg/Vector3Stamped']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Vector3Stamped(
        header=_make_header(typestore),
        vector=Vector3(x=1.0, y=2.0, z=3.0)
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.vector.x == 1.0
        assert messages[0].data.vector.y == 2.0
        assert messages[0].data.vector.z == 3.0


def test_pose_rosbags(typestore: Typestore):
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = Pose(
        position=Point(x=1.0, y=2.0, z=3.0),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.position.x == 1.0
        assert messages[0].data.position.y == 2.0
        assert messages[0].data.position.z == 3.0
        assert messages[0].data.orientation.x == 0.0
        assert messages[0].data.orientation.y == 0.0
        assert messages[0].data.orientation.z == 0.0
        assert messages[0].data.orientation.w == 1.0


def test_inertia_stamped_rosbags(typestore: Typestore):
    InertiaStamped = typestore.types['geometry_msgs/msg/InertiaStamped']
    Inertia = typestore.types['geometry_msgs/msg/Inertia']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = InertiaStamped(
        header=_make_header(typestore),
        inertia=Inertia(
            m=1.0,
            com=Vector3(x=1.0, y=2.0, z=3.0),
            ixx=0.1,
            ixy=0.2,
            ixz=0.3,
            iyy=0.4,
            iyz=0.5,
            izz=0.6,
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.inertia.m == 1.0
        assert messages[0].data.inertia.com.x == 1.0
        assert messages[0].data.inertia.com.y == 2.0
        assert messages[0].data.inertia.com.z == 3.0
        assert messages[0].data.inertia.ixx == 0.1
        assert messages[0].data.inertia.ixy == 0.2
        assert messages[0].data.inertia.ixz == 0.3
        assert messages[0].data.inertia.iyy == 0.4
        assert messages[0].data.inertia.iyz == 0.5
        assert messages[0].data.inertia.izz == 0.6


def test_transform_stamped_rosbags(typestore: Typestore):
    TransformStamped = typestore.types['geometry_msgs/msg/TransformStamped']
    Transform = typestore.types['geometry_msgs/msg/Transform']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = TransformStamped(
        header=_make_header(typestore),
        child_frame_id='child',
        transform=Transform(
            translation=Vector3(x=1.0, y=2.0, z=3.0),
            rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.child_frame_id == 'child'
        assert messages[0].data.transform.translation.x == 1.0
        assert messages[0].data.transform.translation.y == 2.0
        assert messages[0].data.transform.translation.z == 3.0
        assert messages[0].data.transform.rotation.x == 0.0
        assert messages[0].data.transform.rotation.y == 0.0
        assert messages[0].data.transform.rotation.z == 0.0
        assert messages[0].data.transform.rotation.w == 1.0


def test_twist_rosbags(typestore: Typestore):
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Twist(
        linear=Vector3(x=1.0, y=2.0, z=3.0),
        angular=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.linear.x == 1.0
        assert messages[0].data.linear.y == 2.0
        assert messages[0].data.linear.z == 3.0
        assert messages[0].data.angular.x == 4.0
        assert messages[0].data.angular.y == 5.0
        assert messages[0].data.angular.z == 6.0


def test_accel_with_covariance_rosbags(typestore: Typestore):
    AccelWithCovariance = typestore.types['geometry_msgs/msg/AccelWithCovariance']
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = AccelWithCovariance(
        accel=Accel(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
        covariance=np.array([float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.accel.linear.x == 1.0
        assert messages[0].data.accel.linear.y == 2.0
        assert messages[0].data.accel.linear.z == 3.0
        assert messages[0].data.accel.angular.x == 4.0
        assert messages[0].data.accel.angular.y == 5.0
        assert messages[0].data.accel.angular.z == 6.0
        assert messages[0].data.covariance == [float(i) for i in range(36)]


def test_quaternion_stamped_rosbags(typestore: Typestore):
    QuaternionStamped = typestore.types['geometry_msgs/msg/QuaternionStamped']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = QuaternionStamped(
        header=_make_header(typestore),
        quaternion=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.quaternion.x == 0.0
        assert messages[0].data.quaternion.y == 0.0
        assert messages[0].data.quaternion.z == 0.0
        assert messages[0].data.quaternion.w == 1.0


def test_transform_rosbags(typestore: Typestore):
    Transform = typestore.types['geometry_msgs/msg/Transform']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = Transform(
        translation=Vector3(x=1.0, y=2.0, z=3.0),
        rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.translation.x == 1.0
        assert messages[0].data.translation.y == 2.0
        assert messages[0].data.translation.z == 3.0
        assert messages[0].data.rotation.x == 0.0
        assert messages[0].data.rotation.y == 0.0
        assert messages[0].data.rotation.z == 0.0
        assert messages[0].data.rotation.w == 1.0


def test_wrench_stamped_rosbags(typestore: Typestore):
    WrenchStamped = typestore.types['geometry_msgs/msg/WrenchStamped']
    Wrench = typestore.types['geometry_msgs/msg/Wrench']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = WrenchStamped(
        header=_make_header(typestore),
        wrench=Wrench(
            force=Vector3(x=1.0, y=2.0, z=3.0),
            torque=Vector3(x=4.0, y=5.0, z=6.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.wrench.force.x == 1.0
        assert messages[0].data.wrench.force.y == 2.0
        assert messages[0].data.wrench.force.z == 3.0
        assert messages[0].data.wrench.torque.x == 4.0
        assert messages[0].data.wrench.torque.y == 5.0
        assert messages[0].data.wrench.torque.z == 6.0


def test_accel_rosbags(typestore: Typestore):
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Accel(
        linear=Vector3(x=1.0, y=2.0, z=3.0),
        angular=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.linear.x == 1.0
        assert messages[0].data.linear.y == 2.0
        assert messages[0].data.linear.z == 3.0
        assert messages[0].data.angular.x == 4.0
        assert messages[0].data.angular.y == 5.0
        assert messages[0].data.angular.z == 6.0


def test_polygon_rosbags(typestore: Typestore):
    Polygon = typestore.types['geometry_msgs/msg/Polygon']
    Point32 = typestore.types['geometry_msgs/msg/Point32']

    msg = Polygon(
        points=[
            Point32(x=1.0, y=2.0, z=3.0),
            Point32(x=4.0, y=5.0, z=6.0),
        ]
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert len(messages[0].data.points) == 2
        assert messages[0].data.points[0].x == 1.0
        assert messages[0].data.points[0].y == 2.0
        assert messages[0].data.points[0].z == 3.0
        assert messages[0].data.points[1].x == 4.0
        assert messages[0].data.points[1].y == 5.0
        assert messages[0].data.points[1].z == 6.0


def test_pose2d_rosbags(typestore: Typestore):
    Pose2D = typestore.types['geometry_msgs/msg/Pose2D']

    msg = Pose2D(x=1.0, y=2.0, theta=3.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.theta == 3.0


def test_pose_stamped_rosbags(typestore: Typestore):
    PoseStamped = typestore.types['geometry_msgs/msg/PoseStamped']
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = PoseStamped(
        header=_make_header(typestore),
        pose=Pose(
            position=Point(x=1.0, y=2.0, z=3.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.pose.position.x == 1.0
        assert messages[0].data.pose.position.y == 2.0
        assert messages[0].data.pose.position.z == 3.0
        assert messages[0].data.pose.orientation.x == 0.0
        assert messages[0].data.pose.orientation.y == 0.0
        assert messages[0].data.pose.orientation.z == 0.0
        assert messages[0].data.pose.orientation.w == 1.0


def test_accel_stamped_rosbags(typestore: Typestore):
    AccelStamped = typestore.types['geometry_msgs/msg/AccelStamped']
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = AccelStamped(
        header=_make_header(typestore),
        accel=Accel(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.accel.linear.x == 1.0
        assert messages[0].data.accel.linear.y == 2.0
        assert messages[0].data.accel.linear.z == 3.0
        assert messages[0].data.accel.angular.x == 4.0
        assert messages[0].data.accel.angular.y == 5.0
        assert messages[0].data.accel.angular.z == 6.0


def test_accel_with_covariance_stamped_rosbags(typestore: Typestore):
    AccelWithCovarianceStamped = typestore.types['geometry_msgs/msg/AccelWithCovarianceStamped']
    AccelWithCovariance = typestore.types['geometry_msgs/msg/AccelWithCovariance']
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = AccelWithCovarianceStamped(
        header=_make_header(typestore),
        accel=AccelWithCovariance(
            accel=Accel(
                linear=Vector3(x=1.0, y=2.0, z=3.0),
                angular=Vector3(x=4.0, y=5.0, z=6.0),
            ),
            covariance=np.array([float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.accel.accel.linear.x == 1.0
        assert messages[0].data.accel.accel.linear.y == 2.0
        assert messages[0].data.accel.accel.linear.z == 3.0
        assert messages[0].data.accel.accel.angular.x == 4.0
        assert messages[0].data.accel.accel.angular.y == 5.0
        assert messages[0].data.accel.accel.angular.z == 6.0
        assert messages[0].data.accel.covariance == [float(i) for i in range(36)]


def test_quaternion_rosbags(typestore: Typestore):
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x == 0.0
        assert messages[0].data.y == 0.0
        assert messages[0].data.z == 0.0
        assert messages[0].data.w == 1.0


def test_point_rosbags(typestore: Typestore):
    Point = typestore.types['geometry_msgs/msg/Point']

    msg = Point(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.z == 3.0


def test_inertia_rosbags(typestore: Typestore):
    Inertia = typestore.types['geometry_msgs/msg/Inertia']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Inertia(
        m=1.0,
        com=Vector3(x=1.0, y=2.0, z=3.0),
        ixx=0.1,
        ixy=0.2,
        ixz=0.3,
        iyy=0.4,
        iyz=0.5,
        izz=0.6,
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.m == 1.0
        assert messages[0].data.com.x == 1.0
        assert messages[0].data.com.y == 2.0
        assert messages[0].data.com.z == 3.0
        assert messages[0].data.ixx == 0.1
        assert messages[0].data.ixy == 0.2
        assert messages[0].data.ixz == 0.3
        assert messages[0].data.iyy == 0.4
        assert messages[0].data.iyz == 0.5
        assert messages[0].data.izz == 0.6


def test_polygon_stamped_rosbags(typestore: Typestore):
    PolygonStamped = typestore.types['geometry_msgs/msg/PolygonStamped']
    Polygon = typestore.types['geometry_msgs/msg/Polygon']
    Point32 = typestore.types['geometry_msgs/msg/Point32']

    msg = PolygonStamped(
        header=_make_header(typestore),
        polygon=Polygon(
            points=[
                Point32(x=1.0, y=2.0, z=3.0),
                Point32(x=4.0, y=5.0, z=6.0),
            ]
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert len(messages[0].data.polygon.points) == 2
        assert messages[0].data.polygon.points[0].x == 1.0
        assert messages[0].data.polygon.points[0].y == 2.0
        assert messages[0].data.polygon.points[0].z == 3.0
        assert messages[0].data.polygon.points[1].x == 4.0
        assert messages[0].data.polygon.points[1].y == 5.0
        assert messages[0].data.polygon.points[1].z == 6.0


def test_pose_array_rosbags(typestore: Typestore):
    PoseArray = typestore.types['geometry_msgs/msg/PoseArray']
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = PoseArray(
        header=_make_header(typestore),
        poses=[
            Pose(
                position=Point(x=1.0, y=2.0, z=3.0),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
            Pose(
                position=Point(x=4.0, y=5.0, z=6.0),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
        ],
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert len(messages[0].data.poses) == 2
        assert messages[0].data.poses[0].position.x == 1.0
        assert messages[0].data.poses[0].position.y == 2.0
        assert messages[0].data.poses[0].position.z == 3.0
        assert messages[0].data.poses[0].orientation.x == 0.0
        assert messages[0].data.poses[0].orientation.y == 0.0
        assert messages[0].data.poses[0].orientation.z == 0.0
        assert messages[0].data.poses[0].orientation.w == 1.0
        assert messages[0].data.poses[1].position.x == 4.0
        assert messages[0].data.poses[1].position.y == 5.0
        assert messages[0].data.poses[1].position.z == 6.0
        assert messages[0].data.poses[1].orientation.x == 0.0
        assert messages[0].data.poses[1].orientation.y == 0.0
        assert messages[0].data.poses[1].orientation.z == 0.0
        assert messages[0].data.poses[1].orientation.w == 1.0


def test_point_stamped_rosbags(typestore: Typestore):
    PointStamped = typestore.types['geometry_msgs/msg/PointStamped']
    Point = typestore.types['geometry_msgs/msg/Point']

    msg = PointStamped(
        header=_make_header(typestore),
        point=Point(x=1.0, y=2.0, z=3.0)
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.point.x == 1.0
        assert messages[0].data.point.y == 2.0
        assert messages[0].data.point.z == 3.0


def test_point32_rosbags(typestore: Typestore):
    Point32 = typestore.types['geometry_msgs/msg/Point32']

    msg = Point32(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.z == 3.0


def test_wrench_rosbags(typestore: Typestore):
    Wrench = typestore.types['geometry_msgs/msg/Wrench']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Wrench(
        force=Vector3(x=1.0, y=2.0, z=3.0),
        torque=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.force.x == 1.0
        assert messages[0].data.force.y == 2.0
        assert messages[0].data.force.z == 3.0
        assert messages[0].data.torque.x == 4.0
        assert messages[0].data.torque.y == 5.0
        assert messages[0].data.torque.z == 6.0


def test_twist_stamped_rosbags(typestore: Typestore):
    TwistStamped = typestore.types['geometry_msgs/msg/TwistStamped']
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = TwistStamped(
        header=_make_header(typestore),
        twist=Twist(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.twist.linear.x == 1.0
        assert messages[0].data.twist.linear.y == 2.0
        assert messages[0].data.twist.linear.z == 3.0
        assert messages[0].data.twist.angular.x == 4.0
        assert messages[0].data.twist.angular.y == 5.0
        assert messages[0].data.twist.angular.z == 6.0


def test_twist_with_covariance_stamped_rosbags(typestore: Typestore):
    TwistWithCovarianceStamped = typestore.types['geometry_msgs/msg/TwistWithCovarianceStamped']
    TwistWithCovariance = typestore.types['geometry_msgs/msg/TwistWithCovariance']
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = TwistWithCovarianceStamped(
        header=_make_header(typestore),
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
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.twist.twist.linear.x == 1.0
        assert messages[0].data.twist.twist.linear.y == 2.0
        assert messages[0].data.twist.twist.linear.z == 3.0
        assert messages[0].data.twist.twist.angular.x == 4.0
        assert messages[0].data.twist.twist.angular.y == 5.0
        assert messages[0].data.twist.twist.angular.z == 6.0
        assert messages[0].data.twist.covariance == [float(i) for i in range(36)]


def test_twist_with_covariance_rosbags(typestore: Typestore):
    TwistWithCovariance = typestore.types['geometry_msgs/msg/TwistWithCovariance']
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = TwistWithCovariance(
        twist=Twist(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
        covariance=np.array([float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.twist.linear.x == 1.0
        assert messages[0].data.twist.linear.y == 2.0
        assert messages[0].data.twist.linear.z == 3.0
        assert messages[0].data.twist.angular.x == 4.0
        assert messages[0].data.twist.angular.y == 5.0
        assert messages[0].data.twist.angular.z == 6.0
        assert messages[0].data.covariance == [float(i) for i in range(36)]


def test_pose_with_covariance_stamped_rosbags(typestore: Typestore):
    PoseWithCovarianceStamped = typestore.types['geometry_msgs/msg/PoseWithCovarianceStamped']
    PoseWithCovariance = typestore.types['geometry_msgs/msg/PoseWithCovariance']
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']

    msg = PoseWithCovarianceStamped(
        header=_make_header(typestore),
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(x=1.0, y=2.0, z=3.0),
                orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            ),
            covariance=np.array([float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.pose.pose.position.x == 1.0
        assert messages[0].data.pose.pose.position.y == 2.0
        assert messages[0].data.pose.pose.position.z == 3.0
        assert messages[0].data.pose.pose.orientation.x == 0.0
        assert messages[0].data.pose.pose.orientation.y == 0.0
        assert messages[0].data.pose.pose.orientation.z == 0.0
        assert messages[0].data.pose.pose.orientation.w == 1.0
        assert messages[0].data.pose.covariance == [float(i) for i in range(36)]
