"""Test the reading of sensor_msgs messages."""
import math
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader
from pybag.schema.ros2msg import Ros2MsgError


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


def test_battery_state_rosbags(typestore: Typestore):
    BatteryState = typestore.types['sensor_msgs/msg/BatteryState']

    msg = BatteryState(
        header=_make_header(typestore),
        voltage=12.0,
        temperature=25.0,
        current=math.nan,
        charge=2.0,
        capacity=3.0,
        design_capacity=4.0,
        percentage=0.5,
        power_supply_status=BatteryState.POWER_SUPPLY_STATUS_DISCHARGING,
        power_supply_health=BatteryState.POWER_SUPPLY_HEALTH_GOOD,
        power_supply_technology=BatteryState.POWER_SUPPLY_TECHNOLOGY_LION,
        present=True,
        cell_voltage=np.array([3.5, 3.5, math.nan], dtype=np.float32),
        cell_temperature=np.array([30.0, 30.0, math.nan], dtype=np.float32),
        location='loc',
        serial_number='123',
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.voltage == 12.0
        assert messages[0].data.temperature == 25.0
        assert math.isnan(messages[0].data.current)
        assert messages[0].data.charge == 2.0
        assert messages[0].data.capacity == 3.0
        assert messages[0].data.design_capacity == 4.0
        assert messages[0].data.percentage == 0.5
        assert messages[0].data.power_supply_status == 2  # POWER_SUPPLY_STATUS_DISCHARGING
        assert messages[0].data.power_supply_health == 1  # POWER_SUPPLY_HEALTH_GOOD
        assert messages[0].data.power_supply_technology == 2  # POWER_SUPPLY_TECHNOLOGY_LION
        assert messages[0].data.present is True
        assert len(messages[0].data.cell_voltage) == 3
        assert messages[0].data.cell_voltage[0] == 3.5
        assert messages[0].data.cell_voltage[1] == 3.5
        assert math.isnan(messages[0].data.cell_voltage[2])
        assert len(messages[0].data.cell_temperature) == 3
        assert messages[0].data.cell_temperature[0] == 30.0
        assert messages[0].data.cell_temperature[1] == 30.0
        assert math.isnan(messages[0].data.cell_temperature[2])
        assert messages[0].data.location == 'loc'
        assert messages[0].data.serial_number == '123'


def test_camera_info_rosbags(typestore: Typestore):
    CameraInfo = typestore.types['sensor_msgs/msg/CameraInfo']
    RegionOfInterest = typestore.types['sensor_msgs/msg/RegionOfInterest']

    msg = CameraInfo(
        header=_make_header(typestore),
        height=480,
        width=640,
        distortion_model='plumb_bob',
        d=np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float64),
        k=np.array([float(i) for i in range(9)], dtype=np.float64),
        r=np.array([float(i) for i in range(9)], dtype=np.float64),
        p=np.array([float(i) for i in range(12)], dtype=np.float64),
        binning_x=1,
        binning_y=1,
        roi=RegionOfInterest(x_offset=0, y_offset=0, height=0, width=0, do_rectify=False),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.width == 640
        assert messages[0].data.height == 480
        assert messages[0].data.distortion_model == 'plumb_bob'
        assert messages[0].data.d == [0.1, 0.2, 0.3, 0.4, 0.5]
        assert messages[0].data.k == [float(i) for i in range(9)]
        assert messages[0].data.r == [float(i) for i in range(9)]
        assert messages[0].data.p == [float(i) for i in range(12)]
        assert messages[0].data.binning_x == 1
        assert messages[0].data.binning_y == 1
        assert messages[0].data.roi.x_offset == 0
        assert messages[0].data.roi.y_offset == 0
        assert messages[0].data.roi.height == 0
        assert messages[0].data.roi.width == 0
        assert messages[0].data.roi.do_rectify is False


def test_channel_float32_rosbags(typestore: Typestore):
    ChannelFloat32 = typestore.types['sensor_msgs/msg/ChannelFloat32']

    msg = ChannelFloat32(name='chan', values=np.array([1.0, 2.0], dtype=np.float32))
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.name == 'chan'
        assert messages[0].data.values == [1.0, 2.0]


def test_compressed_image_rosbags(typestore: Typestore):
    CompressedImage = typestore.types['sensor_msgs/msg/CompressedImage']

    msg = CompressedImage(
        header=_make_header(typestore),
        format='jpeg',
        data=np.array([1, 2, 3], dtype=np.uint8),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.format == 'jpeg'
        assert messages[0].data.data == b'\x01\x02\x03'


def test_fluid_pressure_rosbags(typestore: Typestore):
    FluidPressure = typestore.types['sensor_msgs/msg/FluidPressure']

    msg = FluidPressure(
        header=_make_header(typestore),
        fluid_pressure=1013.25,
        variance=0.1,
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.fluid_pressure == 1013.25
        assert messages[0].data.variance == 0.1


def test_illuminance_rosbags(typestore: Typestore):
    Illuminance = typestore.types['sensor_msgs/msg/Illuminance']

    msg = Illuminance(
        header=_make_header(typestore),
        illuminance=100.0,
        variance=0.1,
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.illuminance == 100.0
        assert messages[0].data.variance == 0.1


def test_image_rosbags(typestore: Typestore):
    Image = typestore.types['sensor_msgs/msg/Image']

    msg = Image(
        header=_make_header(typestore),
        height=480,
        width=640,
        encoding='rgb8',
        is_bigendian=0,
        step=1920,
        data=np.array([0, 1, 2, 3], dtype=np.uint8),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.width == 640
        assert messages[0].data.height == 480
        assert messages[0].data.encoding == 'rgb8'
        assert messages[0].data.is_bigendian == 0
        assert messages[0].data.step == 1920
        assert messages[0].data.data == b'\x00\x01\x02\x03'


def test_imu_rosbags(typestore: Typestore):
    Imu = typestore.types['sensor_msgs/msg/Imu']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = Imu(
        header=_make_header(typestore),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        orientation_covariance=np.array([float(i) for i in range(9)]),
        angular_velocity=Vector3(x=1.0, y=2.0, z=3.0),
        angular_velocity_covariance=np.array([float(i) for i in range(9)]),
        linear_acceleration=Vector3(x=4.0, y=5.0, z=6.0),
        linear_acceleration_covariance=np.array([float(i) for i in range(9)]),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.orientation.x == 0.0
        assert messages[0].data.orientation.y == 0.0
        assert messages[0].data.orientation.z == 0.0
        assert messages[0].data.orientation.w == 1.0
        assert messages[0].data.orientation_covariance == [float(i) for i in range(9)]
        assert messages[0].data.angular_velocity.x == 1.0
        assert messages[0].data.angular_velocity.y == 2.0
        assert messages[0].data.angular_velocity.z == 3.0
        assert messages[0].data.angular_velocity_covariance == [float(i) for i in range(9)]
        assert messages[0].data.linear_acceleration.x == 4.0
        assert messages[0].data.linear_acceleration.y == 5.0
        assert messages[0].data.linear_acceleration.z == 6.0
        assert messages[0].data.linear_acceleration_covariance == [float(i) for i in range(9)]


def test_joint_state_rosbags(typestore: Typestore):
    JointState = typestore.types['sensor_msgs/msg/JointState']

    msg = JointState(
        header=_make_header(typestore),
        name=['joint1', 'joint2'],
        position=np.array([1.0, 2.0], dtype=np.float64),
        velocity=np.array([3.0, 4.0], dtype=np.float64),
        effort=np.array([5.0, 6.0], dtype=np.float64),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.name == ['joint1', 'joint2']
        assert messages[0].data.position == [1.0, 2.0]
        assert messages[0].data.velocity == [3.0, 4.0]
        assert messages[0].data.effort == [5.0, 6.0]


def test_joy_rosbags(typestore: Typestore):
    Joy = typestore.types['sensor_msgs/msg/Joy']

    msg = Joy(
        header=_make_header(typestore),
        axes=np.array([1.0, 2.0], dtype=np.float32),
        buttons=np.array([1, 0], dtype=np.int32),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.axes == [1.0, 2.0]
        assert messages[0].data.buttons == [1, 0]


def test_joy_feedback_rosbags(typestore: Typestore):
    JoyFeedback = typestore.types['sensor_msgs/msg/JoyFeedback']

    msg = JoyFeedback(type=JoyFeedback.TYPE_LED, id=1, intensity=0.5)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].channel_id == channel_id
        assert messages[0].data.type == JoyFeedback.TYPE_LED
        assert messages[0].data.id == 1
        assert messages[0].data.intensity == 0.5


def test_joy_feedback_array_rosbags(typestore: Typestore):
    JoyFeedbackArray = typestore.types['sensor_msgs/msg/JoyFeedbackArray']
    JoyFeedback = typestore.types['sensor_msgs/msg/JoyFeedback']

    msg = JoyFeedbackArray(
        array=[JoyFeedback(type=JoyFeedback.TYPE_LED, id=1, intensity=0.5)]
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
        assert len(messages[0].data.array) == 1
        assert messages[0].data.array[0].type == 0  # TYPE_LED
        assert messages[0].data.array[0].id == 1
        assert messages[0].data.array[0].intensity == 0.5


def test_laser_echo_rosbags(typestore: Typestore):
    LaserEcho = typestore.types['sensor_msgs/msg/LaserEcho']

    msg = LaserEcho(echoes=np.array([1.0, 2.0], dtype=np.float32))
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.echoes == [1.0, 2.0]


def test_laser_scan_rosbags(typestore: Typestore):
    LaserScan = typestore.types['sensor_msgs/msg/LaserScan']

    msg = LaserScan(
        header=_make_header(typestore),
        angle_min=0.5,
        angle_max=0.5,
        angle_increment=0.5,
        time_increment=0.5,
        scan_time=0.5,
        range_min=0.5,
        range_max=10.0,
        ranges=np.array([1.0, 2.0], dtype=np.float32),
        intensities=np.array([3.0, 4.0], dtype=np.float32),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.angle_min == 0.5
        assert messages[0].data.angle_max == 0.5
        assert messages[0].data.angle_increment == 0.5
        assert messages[0].data.time_increment == 0.5
        assert messages[0].data.scan_time == 0.5
        assert messages[0].data.range_min == 0.5
        assert messages[0].data.range_max == 10.0
        assert messages[0].data.ranges == [1.0, 2.0]
        assert messages[0].data.intensities == [3.0, 4.0]


def test_magnetic_field_rosbags(typestore: Typestore):
    MagneticField = typestore.types['sensor_msgs/msg/MagneticField']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    msg = MagneticField(
        header=_make_header(typestore),
        magnetic_field=Vector3(x=1.0, y=2.0, z=3.0),
        magnetic_field_covariance=np.array([float(i) for i in range(9)]),
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.magnetic_field.x == 1.0
        assert messages[0].data.magnetic_field.y == 2.0
        assert messages[0].data.magnetic_field.z == 3.0
        assert messages[0].data.magnetic_field_covariance == [float(i) for i in range(9)]


def test_multi_dof_joint_state_rosbags(typestore: Typestore):
    MultiDOFJointState = typestore.types['sensor_msgs/msg/MultiDOFJointState']
    Transform = typestore.types['geometry_msgs/msg/Transform']
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']
    Quaternion = typestore.types['geometry_msgs/msg/Quaternion']
    Twist = typestore.types['geometry_msgs/msg/Twist']
    Wrench = typestore.types['geometry_msgs/msg/Wrench']

    msg = MultiDOFJointState(
        header=_make_header(typestore),
        joint_names=['joint1'],
        transforms=[
            Transform(
                translation=Vector3(x=1.0, y=2.0, z=3.0),
                rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
            )
        ],
        twist=[
            Twist(
                linear=Vector3(x=1.0, y=2.0, z=3.0),
                angular=Vector3(x=4.0, y=5.0, z=6.0),
            )
        ],
        wrench=[
            Wrench(
                force=Vector3(x=1.0, y=2.0, z=3.0),
                torque=Vector3(x=4.0, y=5.0, z=6.0),
            )
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.joint_names == ['joint1']
        assert messages[0].data.transforms[0].translation.x == 1.0
        assert messages[0].data.transforms[0].translation.y == 2.0
        assert messages[0].data.transforms[0].translation.z == 3.0
        assert messages[0].data.transforms[0].rotation.x == 0.0
        assert messages[0].data.transforms[0].rotation.y == 0.0
        assert messages[0].data.transforms[0].rotation.z == 0.0
        assert messages[0].data.transforms[0].rotation.w == 1.0
        assert messages[0].data.twist[0].linear.x == 1.0
        assert messages[0].data.twist[0].linear.y == 2.0
        assert messages[0].data.twist[0].linear.z == 3.0
        assert messages[0].data.twist[0].angular.x == 4.0
        assert messages[0].data.twist[0].angular.y == 5.0
        assert messages[0].data.twist[0].angular.z == 6.0
        assert messages[0].data.wrench[0].force.x == 1.0
        assert messages[0].data.wrench[0].force.y == 2.0
        assert messages[0].data.wrench[0].force.z == 3.0
        assert messages[0].data.wrench[0].torque.x == 4.0
        assert messages[0].data.wrench[0].torque.y == 5.0
        assert messages[0].data.wrench[0].torque.z == 6.0


def test_multi_echo_laser_scan_rosbags(typestore: Typestore):
    MultiEchoLaserScan = typestore.types['sensor_msgs/msg/MultiEchoLaserScan']
    LaserEcho = typestore.types['sensor_msgs/msg/LaserEcho']

    msg = MultiEchoLaserScan(
        header=_make_header(typestore),
        angle_min=0.5,
        angle_max=0.5,
        angle_increment=0.5,
        time_increment=0.5,
        scan_time=0.5,
        range_min=0.5,
        range_max=10.0,
        ranges=[LaserEcho(echoes=np.array([1.0, 2.0], dtype=np.float32))],
        intensities=[LaserEcho(echoes=np.array([3.0, 4.0], dtype=np.float32))],
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.angle_min == 0.5
        assert messages[0].data.angle_max == 0.5
        assert messages[0].data.angle_increment == 0.5
        assert messages[0].data.time_increment == 0.5
        assert messages[0].data.scan_time == 0.5
        assert messages[0].data.range_min == 0.5
        assert messages[0].data.range_max == 10.0
        assert messages[0].data.ranges[0].echoes == [1.0, 2.0]
        assert messages[0].data.intensities[0].echoes == [3.0, 4.0]


def test_nav_sat_fix_rosbags(typestore: Typestore):
    NavSatFix = typestore.types['sensor_msgs/msg/NavSatFix']
    NavSatStatus = typestore.types['sensor_msgs/msg/NavSatStatus']

    msg = NavSatFix(
        header=_make_header(typestore),
        status=NavSatStatus(status=NavSatStatus.STATUS_FIX, service=NavSatStatus.SERVICE_GPS),
        latitude=1.0,
        longitude=2.0,
        altitude=3.0,
        position_covariance=np.array([float(i) for i in range(9)]),
        position_covariance_type=NavSatFix.COVARIANCE_TYPE_DIAGONAL_KNOWN,
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.status.status == 0  # STATUS_FIX
        assert messages[0].data.latitude == 1.0
        assert messages[0].data.longitude == 2.0
        assert messages[0].data.altitude == 3.0
        assert messages[0].data.position_covariance == [float(i) for i in range(9)]
        assert messages[0].data.position_covariance_type == 2  # COVARIANCE_TYPE_DIAGONAL_KNOWN


def test_nav_sat_status_rosbags(typestore: Typestore):
    NavSatStatus = typestore.types['sensor_msgs/msg/NavSatStatus']

    msg = NavSatStatus(status=NavSatStatus.STATUS_FIX, service=NavSatStatus.SERVICE_GPS)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.status == 0  # STATUS_FIX
        assert messages[0].data.service == 1  # SERVICE_GPS


def test_point_cloud2_rosbags(typestore: Typestore):
    PointCloud2 = typestore.types['sensor_msgs/msg/PointCloud2']
    PointField = typestore.types['sensor_msgs/msg/PointField']

    msg = PointCloud2(
        header=_make_header(typestore),
        height=1,
        width=2,
        fields=[PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1)],
        is_bigendian=False,
        point_step=4,
        row_step=8,
        data=np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.uint8),
        is_dense=True,
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.height == 1
        assert messages[0].data.width == 2
        assert messages[0].data.fields[0].name == 'x'
        assert messages[0].data.fields[0].offset == 0
        assert messages[0].data.fields[0].datatype == 7  # FLOAT32
        assert messages[0].data.fields[0].count == 1
        assert messages[0].data.is_bigendian == False
        assert messages[0].data.point_step == 4
        assert messages[0].data.row_step == 8
        assert messages[0].data.data == b'\x01\x02\x03\x04\x05\x06\x07\x08'
        assert messages[0].data.is_dense == True


def test_point_field_rosbags(typestore: Typestore):
    PointField = typestore.types['sensor_msgs/msg/PointField']

    msg = PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.name == 'x'
        assert messages[0].data.offset == 0
        assert messages[0].data.datatype == 7  # FLOAT32
        assert messages[0].data.count == 1


def test_range_rosbags(typestore: Typestore):
    Range = typestore.types['sensor_msgs/msg/Range']

    kwargs = dict(
        header=_make_header(typestore),
        radiation_type=Range.ULTRASOUND,
        field_of_view=0.5,
        min_range=0.2,
        max_range=10.0,
        range=5.0,
    )
    # variance was added after ROS 2 Humble
    if hasattr(Range, '__dataclass_fields__') and 'variance' in Range.__dataclass_fields__:
        kwargs['variance'] = 0.5
    msg = Range(**kwargs)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.radiation_type == 0  # ULTRASOUND
        assert messages[0].data.range == 5.0
        if hasattr(Range, '__dataclass_fields__') and 'variance' in Range.__dataclass_fields__:
            assert messages[0].data.variance == 0.5


def test_region_of_interest_rosbags(typestore: Typestore):
    RegionOfInterest = typestore.types['sensor_msgs/msg/RegionOfInterest']

    msg = RegionOfInterest(x_offset=1, y_offset=2, height=3, width=4, do_rectify=True)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.x_offset == 1
        assert messages[0].data.y_offset == 2
        assert messages[0].data.height == 3
        assert messages[0].data.width == 4
        assert messages[0].data.do_rectify is True


def test_relative_humidity_rosbags(typestore: Typestore):
    RelativeHumidity = typestore.types['sensor_msgs/msg/RelativeHumidity']

    msg = RelativeHumidity(
        header=_make_header(typestore),
        relative_humidity=0.5,
        variance=0.1,
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.relative_humidity == 0.5
        assert messages[0].data.variance == 0.1


def test_temperature_rosbags(typestore: Typestore):
    Temperature = typestore.types['sensor_msgs/msg/Temperature']

    msg = Temperature(
        header=_make_header(typestore),
        temperature=36.5,
        variance=0.1,
    )
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].channel_id == channel_id
        assert messages[0].data.temperature == 36.5
        assert messages[0].data.variance == 0.1


def test_time_reference_rosbags(typestore: Typestore):
    TimeReference = typestore.types['sensor_msgs/msg/TimeReference']
    Time = typestore.types['builtin_interfaces/msg/Time']

    msg = TimeReference(
        header=_make_header(typestore),
        time_ref=Time(sec=3, nanosec=4),
        source='source',
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
        assert messages[0].data.header.stamp.sec == 1
        assert messages[0].data.header.stamp.nanosec == 2
        assert messages[0].data.header.frame_id == 'frame'
        assert messages[0].data.time_ref.sec == 3
        assert messages[0].data.time_ref.nanosec == 4
        assert messages[0].data.source == 'source'
