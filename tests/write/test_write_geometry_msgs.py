"""Test writing geometry_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
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

def test_point_pybag() -> None:
    msg = geometry_msgs.Point(x=1.0, y=2.0, z=3.0)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Point'
    assert decoded_msgs[0].x == 1.0
    assert decoded_msgs[0].y == 2.0
    assert decoded_msgs[0].z == 3.0


def test_point32_pybag() -> None:
    msg = geometry_msgs.Point32(x=1.0, y=2.0, z=3.0)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Point32'
    assert decoded_msgs[0].x == 1.0
    assert decoded_msgs[0].y == 2.0
    assert decoded_msgs[0].z == 3.0


def test_point_stamped_pybag() -> None:
    msg = geometry_msgs.PointStamped(
        header=_make_header(),
        point=geometry_msgs.Point(x=1.0, y=2.0, z=3.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PointStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].point.x == 1.0
    assert decoded_msgs[0].point.y == 2.0
    assert decoded_msgs[0].point.z == 3.0


def test_polygon_pybag() -> None:
    msg = geometry_msgs.Polygon(
        points=[
            geometry_msgs.Point32(x=1.0, y=2.0, z=3.0),
            geometry_msgs.Point32(x=4.0, y=5.0, z=6.0)
        ]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Polygon'
    assert len(decoded_msgs[0].points) == 2
    assert decoded_msgs[0].points[0].x == 1.0
    assert decoded_msgs[0].points[0].y == 2.0
    assert decoded_msgs[0].points[0].z == 3.0
    assert decoded_msgs[0].points[1].x == 4.0
    assert decoded_msgs[0].points[1].y == 5.0
    assert decoded_msgs[0].points[1].z == 6.0


def test_polygon_instance_pybag() -> None:
    msg = geometry_msgs.PolygonInstance(
        polygon=geometry_msgs.Polygon(
            points=[geometry_msgs.Point32(x=1.0, y=2.0, z=3.0)]
        ),
        id=42
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PolygonInstance'
    assert decoded_msgs[0].id == 42
    assert len(decoded_msgs[0].polygon.points) == 1
    assert decoded_msgs[0].polygon.points[0].x == 1.0
    assert decoded_msgs[0].polygon.points[0].y == 2.0
    assert decoded_msgs[0].polygon.points[0].z == 3.0


def test_polygon_instance_stamped_pybag() -> None:
    msg = geometry_msgs.PolygonInstanceStamped(
        header=_make_header(),
        polygon=geometry_msgs.PolygonInstance(
            polygon=geometry_msgs.Polygon(
                points=[geometry_msgs.Point32(x=1.0, y=2.0, z=3.0)]
            ),
            id=42
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PolygonInstanceStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].polygon.id == 42
    assert len(decoded_msgs[0].polygon.polygon.points) == 1
    assert decoded_msgs[0].polygon.polygon.points[0].x == 1.0
    assert decoded_msgs[0].polygon.polygon.points[0].y == 2.0
    assert decoded_msgs[0].polygon.polygon.points[0].z == 3.0


def test_polygon_stamped_pybag() -> None:
    msg = geometry_msgs.PolygonStamped(
        header=_make_header(),
        polygon=geometry_msgs.Polygon(
            points=[geometry_msgs.Point32(x=1.0, y=2.0, z=3.0)]
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PolygonStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert len(decoded_msgs[0].polygon.points) == 1
    assert decoded_msgs[0].polygon.points[0].x == 1.0
    assert decoded_msgs[0].polygon.points[0].y == 2.0
    assert decoded_msgs[0].polygon.points[0].z == 3.0


def test_quaternion_pybag() -> None:
    msg = geometry_msgs.Quaternion(x=0.1, y=0.2, z=0.3, w=0.4)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Quaternion'
    assert decoded_msgs[0].x == 0.1
    assert decoded_msgs[0].y == 0.2
    assert decoded_msgs[0].z == 0.3
    assert decoded_msgs[0].w == 0.4


def test_quaternion_stamped_pybag() -> None:
    msg = geometry_msgs.QuaternionStamped(
        header=_make_header(),
        quaternion=geometry_msgs.Quaternion(x=0.1, y=0.2, z=0.3, w=0.4)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'QuaternionStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].quaternion.x == 0.1
    assert decoded_msgs[0].quaternion.y == 0.2
    assert decoded_msgs[0].quaternion.z == 0.3
    assert decoded_msgs[0].quaternion.w == 0.4


def test_pose_pybag() -> None:
    msg = geometry_msgs.Pose(
        position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
        orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Pose'
    assert decoded_msgs[0].position.x == 1.0
    assert decoded_msgs[0].position.y == 2.0
    assert decoded_msgs[0].position.z == 3.0
    assert decoded_msgs[0].orientation.x == 0.0
    assert decoded_msgs[0].orientation.y == 0.0
    assert decoded_msgs[0].orientation.z == 0.0
    assert decoded_msgs[0].orientation.w == 1.0


def test_pose2d_pybag() -> None:
    msg = geometry_msgs.Pose2D(x=1.0, y=2.0, theta=3.0)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Pose2D'
    assert decoded_msgs[0].x == 1.0
    assert decoded_msgs[0].y == 2.0
    assert decoded_msgs[0].theta == 3.0


def test_pose_array_pybag() -> None:
    msg = geometry_msgs.PoseArray(
        header=_make_header(),
        poses=[
            geometry_msgs.Pose(
                position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
                orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
            ),
            geometry_msgs.Pose(
                position=geometry_msgs.Point(x=4.0, y=5.0, z=6.0),
                orientation=geometry_msgs.Quaternion(x=0.1, y=0.2, z=0.3, w=0.4)
            )
        ]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PoseArray'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert len(decoded_msgs[0].poses) == 2
    # First pose
    assert decoded_msgs[0].poses[0].position.x == 1.0
    assert decoded_msgs[0].poses[0].position.y == 2.0
    assert decoded_msgs[0].poses[0].position.z == 3.0
    assert decoded_msgs[0].poses[0].orientation.x == 0.0
    assert decoded_msgs[0].poses[0].orientation.y == 0.0
    assert decoded_msgs[0].poses[0].orientation.z == 0.0
    assert decoded_msgs[0].poses[0].orientation.w == 1.0
    # Second pose
    assert decoded_msgs[0].poses[1].position.x == 4.0
    assert decoded_msgs[0].poses[1].position.y == 5.0
    assert decoded_msgs[0].poses[1].position.z == 6.0
    assert decoded_msgs[0].poses[1].orientation.x == 0.1
    assert decoded_msgs[0].poses[1].orientation.y == 0.2
    assert decoded_msgs[0].poses[1].orientation.z == 0.3
    assert decoded_msgs[0].poses[1].orientation.w == 0.4


def test_pose_stamped_pybag() -> None:
    msg = geometry_msgs.PoseStamped(
        header=_make_header(),
        pose=geometry_msgs.Pose(
            position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PoseStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].pose.position.x == 1.0
    assert decoded_msgs[0].pose.position.y == 2.0
    assert decoded_msgs[0].pose.position.z == 3.0
    assert decoded_msgs[0].pose.orientation.x == 0.0
    assert decoded_msgs[0].pose.orientation.y == 0.0
    assert decoded_msgs[0].pose.orientation.z == 0.0
    assert decoded_msgs[0].pose.orientation.w == 1.0


def test_pose_with_covariance_pybag() -> None:
    msg = geometry_msgs.PoseWithCovariance(
        pose=geometry_msgs.Pose(
            position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
            orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        ),
        covariance=[float(i) for i in range(36)]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PoseWithCovariance'
    assert decoded_msgs[0].pose.position.x == 1.0
    assert decoded_msgs[0].pose.position.y == 2.0
    assert decoded_msgs[0].pose.position.z == 3.0
    assert decoded_msgs[0].pose.orientation.x == 0.0
    assert decoded_msgs[0].pose.orientation.y == 0.0
    assert decoded_msgs[0].pose.orientation.z == 0.0
    assert decoded_msgs[0].pose.orientation.w == 1.0
    assert list(decoded_msgs[0].covariance) == [float(i) for i in range(36)]


def test_pose_with_covariance_stamped_pybag() -> None:
    msg = geometry_msgs.PoseWithCovarianceStamped(
        header=_make_header(),
        pose=geometry_msgs.PoseWithCovariance(
            pose=geometry_msgs.Pose(
                position=geometry_msgs.Point(x=1.0, y=2.0, z=3.0),
                orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
            ),
            covariance=[float(i) for i in range(36)]
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PoseWithCovarianceStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].pose.pose.position.x == 1.0
    assert decoded_msgs[0].pose.pose.position.y == 2.0
    assert decoded_msgs[0].pose.pose.position.z == 3.0
    assert decoded_msgs[0].pose.pose.orientation.x == 0.0
    assert decoded_msgs[0].pose.pose.orientation.y == 0.0
    assert decoded_msgs[0].pose.pose.orientation.z == 0.0
    assert decoded_msgs[0].pose.pose.orientation.w == 1.0
    assert list(decoded_msgs[0].pose.covariance) == [float(i) for i in range(36)]


def test_vector3_pybag() -> None:
    msg = geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Vector3'
    assert decoded_msgs[0].x == 1.0
    assert decoded_msgs[0].y == 2.0
    assert decoded_msgs[0].z == 3.0


def test_vector3_stamped_pybag() -> None:
    msg = geometry_msgs.Vector3Stamped(
        header=_make_header(),
        vector=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Vector3Stamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].vector.x == 1.0
    assert decoded_msgs[0].vector.y == 2.0
    assert decoded_msgs[0].vector.z == 3.0


def test_accel_pybag() -> None:
    msg = geometry_msgs.Accel(
        linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Accel'
    assert decoded_msgs[0].linear.x == 1.0
    assert decoded_msgs[0].linear.y == 2.0
    assert decoded_msgs[0].linear.z == 3.0
    assert decoded_msgs[0].angular.x == 4.0
    assert decoded_msgs[0].angular.y == 5.0
    assert decoded_msgs[0].angular.z == 6.0


def test_accel_stamped_pybag() -> None:
    msg = geometry_msgs.AccelStamped(
        header=_make_header(),
        accel=geometry_msgs.Accel(
            linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'AccelStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].accel.linear.x == 1.0
    assert decoded_msgs[0].accel.linear.y == 2.0
    assert decoded_msgs[0].accel.linear.z == 3.0
    assert decoded_msgs[0].accel.angular.x == 4.0
    assert decoded_msgs[0].accel.angular.y == 5.0
    assert decoded_msgs[0].accel.angular.z == 6.0


def test_accel_with_covariance_pybag() -> None:
    msg = geometry_msgs.AccelWithCovariance(
        accel=geometry_msgs.Accel(
            linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        ),
        covariance=[float(i) for i in range(36)]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'AccelWithCovariance'
    assert decoded_msgs[0].accel.linear.x == 1.0
    assert decoded_msgs[0].accel.linear.y == 2.0
    assert decoded_msgs[0].accel.linear.z == 3.0
    assert decoded_msgs[0].accel.angular.x == 4.0
    assert decoded_msgs[0].accel.angular.y == 5.0
    assert decoded_msgs[0].accel.angular.z == 6.0
    assert list(decoded_msgs[0].covariance) == [float(i) for i in range(36)]


def test_accel_with_covariance_stamped_pybag() -> None:
    msg = geometry_msgs.AccelWithCovarianceStamped(
        header=_make_header(),
        accel=geometry_msgs.AccelWithCovariance(
            accel=geometry_msgs.Accel(
                linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
                angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
            ),
            covariance=[float(i) for i in range(36)]
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'AccelWithCovarianceStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].accel.accel.linear.x == 1.0
    assert decoded_msgs[0].accel.accel.linear.y == 2.0
    assert decoded_msgs[0].accel.accel.linear.z == 3.0
    assert decoded_msgs[0].accel.accel.angular.x == 4.0
    assert decoded_msgs[0].accel.accel.angular.y == 5.0
    assert decoded_msgs[0].accel.accel.angular.z == 6.0
    assert list(decoded_msgs[0].accel.covariance) == [float(i) for i in range(36)]


def test_inertia_pybag() -> None:
    msg = geometry_msgs.Inertia(
        m=1.0,
        com=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        ixx=1.1, ixy=1.2, ixz=1.3,
        iyy=2.1, iyz=2.2, izz=3.1
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Inertia'
    assert decoded_msgs[0].m == 1.0
    assert decoded_msgs[0].com.x == 1.0
    assert decoded_msgs[0].com.y == 2.0
    assert decoded_msgs[0].com.z == 3.0
    assert decoded_msgs[0].ixx == 1.1
    assert decoded_msgs[0].ixy == 1.2
    assert decoded_msgs[0].ixz == 1.3
    assert decoded_msgs[0].iyy == 2.1
    assert decoded_msgs[0].iyz == 2.2
    assert decoded_msgs[0].izz == 3.1


def test_inertia_stamped_pybag() -> None:
    msg = geometry_msgs.InertiaStamped(
        header=_make_header(),
        inertia=geometry_msgs.Inertia(
            m=1.0,
            com=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            ixx=1.1, ixy=1.2, ixz=1.3,
            iyy=2.1, iyz=2.2, izz=3.1
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'InertiaStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].inertia.m == 1.0
    assert decoded_msgs[0].inertia.com.x == 1.0
    assert decoded_msgs[0].inertia.com.y == 2.0
    assert decoded_msgs[0].inertia.com.z == 3.0
    assert decoded_msgs[0].inertia.ixx == 1.1
    assert decoded_msgs[0].inertia.ixy == 1.2
    assert decoded_msgs[0].inertia.ixz == 1.3
    assert decoded_msgs[0].inertia.iyy == 2.1
    assert decoded_msgs[0].inertia.iyz == 2.2
    assert decoded_msgs[0].inertia.izz == 3.1


def test_transform_pybag() -> None:
    msg = geometry_msgs.Transform(
        translation=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        rotation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Transform'
    assert decoded_msgs[0].translation.x == 1.0
    assert decoded_msgs[0].translation.y == 2.0
    assert decoded_msgs[0].translation.z == 3.0
    assert decoded_msgs[0].rotation.x == 0.0
    assert decoded_msgs[0].rotation.y == 0.0
    assert decoded_msgs[0].rotation.z == 0.0
    assert decoded_msgs[0].rotation.w == 1.0


def test_transform_stamped_pybag() -> None:
    msg = geometry_msgs.TransformStamped(
        header=_make_header(),
        child_frame_id='child_frame',
        transform=geometry_msgs.Transform(
            translation=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            rotation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'TransformStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].child_frame_id == 'child_frame'
    assert decoded_msgs[0].transform.translation.x == 1.0
    assert decoded_msgs[0].transform.translation.y == 2.0
    assert decoded_msgs[0].transform.translation.z == 3.0
    assert decoded_msgs[0].transform.rotation.x == 0.0
    assert decoded_msgs[0].transform.rotation.y == 0.0
    assert decoded_msgs[0].transform.rotation.z == 0.0
    assert decoded_msgs[0].transform.rotation.w == 1.0


def test_twist_pybag() -> None:
    msg = geometry_msgs.Twist(
        linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Twist'
    assert decoded_msgs[0].linear.x == 1.0
    assert decoded_msgs[0].linear.y == 2.0
    assert decoded_msgs[0].linear.z == 3.0
    assert decoded_msgs[0].angular.x == 4.0
    assert decoded_msgs[0].angular.y == 5.0
    assert decoded_msgs[0].angular.z == 6.0


def test_twist_stamped_pybag() -> None:
    msg = geometry_msgs.TwistStamped(
        header=_make_header(),
        twist=geometry_msgs.Twist(
            linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'TwistStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].twist.linear.x == 1.0
    assert decoded_msgs[0].twist.linear.y == 2.0
    assert decoded_msgs[0].twist.linear.z == 3.0
    assert decoded_msgs[0].twist.angular.x == 4.0
    assert decoded_msgs[0].twist.angular.y == 5.0
    assert decoded_msgs[0].twist.angular.z == 6.0


def test_twist_with_covariance_pybag() -> None:
    msg = geometry_msgs.TwistWithCovariance(
        twist=geometry_msgs.Twist(
            linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        ),
        covariance=[float(i) for i in range(36)]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'TwistWithCovariance'
    assert decoded_msgs[0].twist.linear.x == 1.0
    assert decoded_msgs[0].twist.linear.y == 2.0
    assert decoded_msgs[0].twist.linear.z == 3.0
    assert decoded_msgs[0].twist.angular.x == 4.0
    assert decoded_msgs[0].twist.angular.y == 5.0
    assert decoded_msgs[0].twist.angular.z == 6.0
    assert list(decoded_msgs[0].covariance) == [float(i) for i in range(36)]


def test_twist_with_covariance_stamped_pybag() -> None:
    msg = geometry_msgs.TwistWithCovarianceStamped(
        header=_make_header(),
        twist=geometry_msgs.TwistWithCovariance(
            twist=geometry_msgs.Twist(
                linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
                angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
            ),
            covariance=[float(i) for i in range(36)]
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'TwistWithCovarianceStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].twist.twist.linear.x == 1.0
    assert decoded_msgs[0].twist.twist.linear.y == 2.0
    assert decoded_msgs[0].twist.twist.linear.z == 3.0
    assert decoded_msgs[0].twist.twist.angular.x == 4.0
    assert decoded_msgs[0].twist.twist.angular.y == 5.0
    assert decoded_msgs[0].twist.twist.angular.z == 6.0
    assert list(decoded_msgs[0].twist.covariance) == [float(i) for i in range(36)]


def test_velocity_stamped_pybag() -> None:
    msg = geometry_msgs.VelocityStamped(
        header=_make_header(),
        body_frame_id='body_frame',
        reference_frame_id='reference_frame',
        velocity=geometry_msgs.Twist(
            linear=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            angular=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'VelocityStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].body_frame_id == 'body_frame'
    assert decoded_msgs[0].reference_frame_id == 'reference_frame'
    assert decoded_msgs[0].velocity.linear.x == 1.0
    assert decoded_msgs[0].velocity.linear.y == 2.0
    assert decoded_msgs[0].velocity.linear.z == 3.0
    assert decoded_msgs[0].velocity.angular.x == 4.0
    assert decoded_msgs[0].velocity.angular.y == 5.0
    assert decoded_msgs[0].velocity.angular.z == 6.0


def test_wrench_pybag() -> None:
    msg = geometry_msgs.Wrench(
        force=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        torque=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Wrench'
    assert decoded_msgs[0].force.x == 1.0
    assert decoded_msgs[0].force.y == 2.0
    assert decoded_msgs[0].force.z == 3.0
    assert decoded_msgs[0].torque.x == 4.0
    assert decoded_msgs[0].torque.y == 5.0
    assert decoded_msgs[0].torque.z == 6.0


def test_wrench_stamped_pybag() -> None:
    msg = geometry_msgs.WrenchStamped(
        header=_make_header(),
        wrench=geometry_msgs.Wrench(
            force=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
            torque=geometry_msgs.Vector3(x=4.0, y=5.0, z=6.0)
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'WrenchStamped'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].wrench.force.x == 1.0
    assert decoded_msgs[0].wrench.force.y == 2.0
    assert decoded_msgs[0].wrench.force.z == 3.0
    assert decoded_msgs[0].wrench.torque.x == 4.0
    assert decoded_msgs[0].wrench.torque.y == 5.0
    assert decoded_msgs[0].wrench.torque.z == 6.0
