"""Test the reading of geometry_msgs messages."""
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob('*.mcap'))


def _write_rosbags(temp_dir: str, msg, typestore, *, timestamp: int = 0) -> int:
    with Writer(
        Path(temp_dir) / 'rosbags',
        version=9,
        storage_plugin=StoragePlugin.MCAP,
    ) as writer:
        connection = writer.add_connection(
            '/rosbags', msg.__msgtype__, typestore=typestore
        )
        serialized_msg = typestore.serialize_cdr(msg, msg.__msgtype__)
        writer.write(connection, timestamp, serialized_msg)
        return connection.id


def _make_header(typestore):
    Header = typestore.types['std_msgs/msg/Header']
    Time = typestore.types['builtin_interfaces/msg/Time']
    return Header(stamp=Time(sec=1, nanosec=2), frame_id='frame')


def test_vector3_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = Vector3(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        timestamp = 123
        connection_id = _write_rosbags(
            temp_dir, msg, typestore, timestamp=timestamp
        )
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == timestamp
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.z == 3.0
        # TODO: Test the type somehow?


def test_pose_with_covariance_rosbags():
    typestore = get_typestore(Stores.LATEST)
    PoseWithCovariance = typestore.types[
        'geometry_msgs/msg/PoseWithCovariance'
    ]
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    msg = PoseWithCovariance(
        pose=Pose(
            position=Point(x=1.0, y=2.0, z=3.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        ),
        covariance=array('d', [float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.pose.position.x == 1.0
        assert data.pose.position.y == 2.0
        assert data.pose.position.z == 3.0
        assert data.pose.orientation.w == 1.0
        assert data.covariance == array('d', [float(i) for i in range(36)])


def test_vector3_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Vector3Stamped = typestore.types['geometry_msgs/msg/Vector3Stamped']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = Vector3Stamped(
        header=_make_header(typestore), vector=Vector3(x=1.0, y=2.0, z=3.0)
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.vector.x == 1.0
        assert data.vector.y == 2.0
        assert data.vector.z == 3.0


def test_pose_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Pose = typestore.types['geometry_msgs/msg/Pose']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    msg = Pose(
        position=Point(x=1.0, y=2.0, z=3.0),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.position.x == 1.0
        assert data.position.y == 2.0
        assert data.position.z == 3.0
        assert data.orientation.w == 1.0


def test_inertia_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.inertia.m == 1.0
        assert data.inertia.com.x == 1.0
        assert data.inertia.izz == 0.6


def test_transform_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.child_frame_id == 'child'
        assert data.transform.translation.x == 1.0
        assert data.transform.rotation.w == 1.0


def test_twist_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = Twist(
        linear=Vector3(x=1.0, y=2.0, z=3.0),
        angular=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.linear.x == 1.0
        assert data.angular.z == 6.0


def test_accel_with_covariance_rosbags():
    typestore = get_typestore(Stores.LATEST)
    AccelWithCovariance = typestore.types[
        'geometry_msgs/msg/AccelWithCovariance'
    ]
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = AccelWithCovariance(
        accel=Accel(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
        covariance=array('d', [float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.accel.linear.x == 1.0
        assert data.accel.angular.z == 6.0
        assert data.covariance == array('d', [float(i) for i in range(36)])


def test_quaternion_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    QuaternionStamped = typestore.types[
        'geometry_msgs/msg/QuaternionStamped'
    ]
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    msg = QuaternionStamped(
        header=_make_header(typestore),
        quaternion=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.quaternion.w == 1.0


def test_transform_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Transform = typestore.types['geometry_msgs/msg/Transform']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    msg = Transform(
        translation=Vector3(x=1.0, y=2.0, z=3.0),
        rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.translation.y == 2.0
        assert data.rotation.w == 1.0


def test_wrench_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.wrench.force.x == 1.0
        assert data.wrench.torque.z == 6.0


def test_accel_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = Accel(
        linear=Vector3(x=1.0, y=2.0, z=3.0),
        angular=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.linear.y == 2.0
        assert data.angular.x == 4.0


def test_polygon_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Polygon = typestore.types['geometry_msgs/msg/Polygon']
    Point32 = typestore.types['geometry_msgs/msg/Point32']
    msg = Polygon(
        points=[
            Point32(x=1.0, y=2.0, z=3.0),
            Point32(x=4.0, y=5.0, z=6.0),
        ]
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert len(data.points) == 2
        assert data.points[1].y == 5.0


def test_pose2d_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Pose2D = typestore.types['geometry_msgs/msg/Pose2D']
    msg = Pose2D(x=1.0, y=2.0, theta=3.0)
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.x == 1.0
        assert data.theta == 3.0


def test_pose_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.pose.position.x == 1.0
        assert data.pose.orientation.w == 1.0


def test_accel_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.accel.angular.y == 5.0


def test_accel_with_covariance_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    AccelWithCovarianceStamped = typestore.types[
        'geometry_msgs/msg/AccelWithCovarianceStamped'
    ]
    AccelWithCovariance = typestore.types[
        'geometry_msgs/msg/AccelWithCovariance'
    ]
    Accel = typestore.types['geometry_msgs/msg/Accel']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = AccelWithCovarianceStamped(
        header=_make_header(typestore),
        accel=AccelWithCovariance(
            accel=Accel(
                linear=Vector3(x=1.0, y=2.0, z=3.0),
                angular=Vector3(x=4.0, y=5.0, z=6.0),
            ),
            covariance=array('d', [float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.accel.accel.linear.x == 1.0
        assert data.accel.covariance == array('d', [float(i) for i in range(36)])


def test_quaternion_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    msg = Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.w == 1.0


def test_point_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Point = typestore.types['geometry_msgs/msg/Point']
    msg = Point(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.y == 2.0


def test_inertia_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.m == 1.0
        assert data.com.y == 2.0
        assert data.izz == 0.6


def test_polygon_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.polygon.points[0].x == 1.0
        assert data.polygon.points[1].z == 6.0


def test_pose_array_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert len(data.poses) == 2
        assert data.poses[1].position.y == 5.0


def test_point_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    PointStamped = typestore.types['geometry_msgs/msg/PointStamped']
    Point = typestore.types['geometry_msgs/msg/Point']
    msg = PointStamped(
        header=_make_header(typestore), point=Point(x=1.0, y=2.0, z=3.0)
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.point.z == 3.0


def test_point32_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Point32 = typestore.types['geometry_msgs/msg/Point32']
    msg = Point32(x=1.0, y=2.0, z=3.0)
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.x == 1.0
        assert data.z == 3.0


def test_wrench_rosbags():
    typestore = get_typestore(Stores.LATEST)
    Wrench = typestore.types['geometry_msgs/msg/Wrench']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = Wrench(
        force=Vector3(x=1.0, y=2.0, z=3.0),
        torque=Vector3(x=4.0, y=5.0, z=6.0),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.force.y == 2.0
        assert data.torque.z == 6.0


def test_twist_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
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
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.header.frame_id == 'frame'
        assert data.twist.angular.y == 5.0


def test_twist_with_covariance_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    TwistWithCovarianceStamped = typestore.types[
        'geometry_msgs/msg/TwistWithCovarianceStamped'
    ]
    TwistWithCovariance = typestore.types[
        'geometry_msgs/msg/TwistWithCovariance'
    ]
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = TwistWithCovarianceStamped(
        header=_make_header(typestore),
        twist=TwistWithCovariance(
            twist=Twist(
                linear=Vector3(x=1.0, y=2.0, z=3.0),
                angular=Vector3(x=4.0, y=5.0, z=6.0),
            ),
            covariance=array('d', [float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.twist.twist.linear.x == 1.0
        assert data.twist.covariance == array('d', [float(i) for i in range(36)])


def test_twist_with_covariance_rosbags():
    typestore = get_typestore(Stores.LATEST)
    TwistWithCovariance = typestore.types[
        'geometry_msgs/msg/TwistWithCovariance'
    ]
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    msg = TwistWithCovariance(
        twist=Twist(
            linear=Vector3(x=1.0, y=2.0, z=3.0),
            angular=Vector3(x=4.0, y=5.0, z=6.0),
        ),
        covariance=array('d', [float(i) for i in range(36)]),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.twist.angular.z == 6.0
        assert data.covariance == array('d', [float(i) for i in range(36)])


def test_pose_with_covariance_stamped_rosbags():
    typestore = get_typestore(Stores.LATEST)
    PoseWithCovarianceStamped = typestore.types[
        'geometry_msgs/msg/PoseWithCovarianceStamped'
    ]
    PoseWithCovariance = typestore.types[
        'geometry_msgs/msg/PoseWithCovariance'
    ]
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
            covariance=array('d', [float(i) for i in range(36)]),
        ),
    )
    with TemporaryDirectory() as temp_dir:
        connection_id = _write_rosbags(temp_dir, msg, typestore)
        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection_id
        data = messages[0].data
        assert data.pose.pose.position.y == 2.0
        assert data.pose.covariance == array('d', [float(i) for i in range(36)])
