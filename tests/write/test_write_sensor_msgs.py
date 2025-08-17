"""Test writing sensor_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.sensor_msgs as sensor_msgs
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

def test_battery_state_pybag() -> None:
    msg = sensor_msgs.BatteryState(
        header=_make_header(),
        voltage=12.5,
        temperature=25.0,
        current=1.5,
        charge=100.0,
        capacity=200.0,
        design_capacity=200.0,
        percentage=50.0,
        power_supply_status=sensor_msgs.BatteryState.POWER_SUPPLY_STATUS_CHARGING,
        power_supply_health=sensor_msgs.BatteryState.POWER_SUPPLY_HEALTH_GOOD,
        power_supply_technology=sensor_msgs.BatteryState.POWER_SUPPLY_TECHNOLOGY_LION,
        present=True,
        cell_voltage=[3.0, 3.0, 3.0],
        cell_temperature=[25.0, 25.0, 25.0],
        location='main_battery',
        serial_number='BAT001'
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'BatteryState'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].voltage == 12.5
    assert decoded_msgs[0].temperature == 25.0
    assert decoded_msgs[0].current == 1.5
    assert decoded_msgs[0].charge == 100.0
    assert decoded_msgs[0].capacity == 200.0
    assert decoded_msgs[0].design_capacity == 200.0
    assert decoded_msgs[0].percentage == 50.0
    assert decoded_msgs[0].power_supply_status == 1      # POWER_SUPPLY_STATUS_CHARGING
    assert decoded_msgs[0].power_supply_health == 1      # POWER_SUPPLY_HEALTH_GOOD
    assert decoded_msgs[0].power_supply_technology == 2  # POWER_SUPPLY_TECHNOLOGY_LION
    assert decoded_msgs[0].present is True
    assert decoded_msgs[0].cell_voltage == [3.0, 3.0, 3.0]
    assert decoded_msgs[0].cell_temperature == [25.0, 25.0, 25.0]
    assert decoded_msgs[0].location == 'main_battery'
    assert decoded_msgs[0].serial_number == 'BAT001'


def test_region_of_interest_pybag() -> None:
    msg = sensor_msgs.RegionOfInterest(
        x_offset=10,
        y_offset=20,
        height=100,
        width=200,
        do_rectify=True
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'RegionOfInterest'
    assert decoded_msgs[0].x_offset == 10
    assert decoded_msgs[0].y_offset == 20
    assert decoded_msgs[0].height == 100
    assert decoded_msgs[0].width == 200
    assert decoded_msgs[0].do_rectify is True


def test_camera_info_pybag() -> None:
    msg = sensor_msgs.CameraInfo(
        header=_make_header(),
        height=480,
        width=640,
        distortion_model='plumb_bob',
        d=[0.1, 0.2, 0.0, 0.0, 0.0],
        k=[1000.0, 0.0, 320.0, 0.0, 1000.0, 240.0, 0.0, 0.0, 1.0],
        r=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
        p=[1000.0, 0.0, 320.0, 0.0, 0.0, 1000.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0],
        binning_x=1,
        binning_y=1,
        roi=sensor_msgs.RegionOfInterest(
            x_offset=0,
            y_offset=0,
            height=480,
            width=640,
            do_rectify=False
        )
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'CameraInfo'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].height == 480
    assert decoded_msgs[0].width == 640
    assert decoded_msgs[0].distortion_model == 'plumb_bob'
    assert decoded_msgs[0].d == [0.1, 0.2, 0.0, 0.0, 0.0]
    assert decoded_msgs[0].k == [1000.0, 0.0, 320.0, 0.0, 1000.0, 240.0, 0.0, 0.0, 1.0]
    assert decoded_msgs[0].r == [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]
    assert decoded_msgs[0].p == [1000.0, 0.0, 320.0, 0.0, 0.0, 1000.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0]
    assert decoded_msgs[0].binning_x == 1
    assert decoded_msgs[0].binning_y == 1
    assert decoded_msgs[0].roi.x_offset == 0
    assert decoded_msgs[0].roi.y_offset == 0
    assert decoded_msgs[0].roi.height == 480
    assert decoded_msgs[0].roi.width == 640
    assert decoded_msgs[0].roi.do_rectify is False


def test_channel_float32_pybag() -> None:
    msg = sensor_msgs.ChannelFloat32(
        name='intensity',
        values=[1.0, 2.0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'ChannelFloat32'
    assert decoded_msgs[0].name == 'intensity'
    assert decoded_msgs[0].values == [1.0, 2.0]


def test_compressed_image_pybag() -> None:
    msg = sensor_msgs.CompressedImage(
        header=_make_header(),
        format='jpeg',
        data=[255, 216, 255, 224, 0, 16, 74, 70, 73, 70, 0, 1]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'CompressedImage'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].format == 'jpeg'
    assert decoded_msgs[0].data == bytes([255, 216, 255, 224, 0, 16, 74, 70, 73, 70, 0, 1])


def test_fluid_pressure_pybag() -> None:
    msg = sensor_msgs.FluidPressure(
        header=_make_header(),
        fluid_pressure=101325.0,
        variance=1.0
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'FluidPressure'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].fluid_pressure == 101325.0
    assert decoded_msgs[0].variance == 1.0


def test_illuminance_pybag() -> None:
    msg = sensor_msgs.Illuminance(
        header=_make_header(),
        illuminance=500.0,
        variance=0.1
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Illuminance'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].illuminance == 500.0
    assert decoded_msgs[0].variance == 0.1


def test_image_pybag() -> None:
    msg = sensor_msgs.Image(
        header=_make_header(),
        height=480,
        width=640,
        encoding='rgb8',
        is_bigendian=0,
        step=1920,
        data=[255, 0, 0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Image'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].height == 480
    assert decoded_msgs[0].width == 640
    assert decoded_msgs[0].encoding == 'rgb8'
    assert decoded_msgs[0].is_bigendian == 0
    assert decoded_msgs[0].step == 1920
    assert decoded_msgs[0].data == bytes([255, 0, 0])


def test_imu_pybag() -> None:
    orientation = geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    angular_velocity = geometry_msgs.Vector3(x=0.1, y=0.2, z=0.3)
    linear_acceleration = geometry_msgs.Vector3(x=9.8, y=0.0, z=0.0)
    msg = sensor_msgs.Imu(
        header=_make_header(),
        orientation=orientation,
        orientation_covariance=[1.0] * 9,
        angular_velocity=angular_velocity,
        angular_velocity_covariance=[0.1] * 9,
        linear_acceleration=linear_acceleration,
        linear_acceleration_covariance=[0.5] * 9
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Imu'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].orientation.x == 0.0
    assert decoded_msgs[0].orientation.y == 0.0
    assert decoded_msgs[0].orientation.z == 0.0
    assert decoded_msgs[0].orientation.w == 1.0
    assert decoded_msgs[0].orientation_covariance == [1.0] * 9
    assert decoded_msgs[0].angular_velocity.x == 0.1
    assert decoded_msgs[0].angular_velocity.y == 0.2
    assert decoded_msgs[0].angular_velocity.z == 0.3
    assert decoded_msgs[0].angular_velocity_covariance == [0.1] * 9
    assert decoded_msgs[0].linear_acceleration.x == 9.8
    assert decoded_msgs[0].linear_acceleration.y == 0.0
    assert decoded_msgs[0].linear_acceleration.z == 0.0
    assert decoded_msgs[0].linear_acceleration_covariance == [0.5] * 9


def test_joint_state_pybag() -> None:
    msg = sensor_msgs.JointState(
        header=_make_header(),
        name=['joint1', 'joint2', 'joint3'],
        position=[0.5, 1.0, -0.3],
        velocity=[0.1, 0.2, 0.0],
        effort=[10.0, 5.0, 2.0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'JointState'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].name == ['joint1', 'joint2', 'joint3']
    assert decoded_msgs[0].position == [0.5, 1.0, -0.3]
    assert decoded_msgs[0].velocity == [0.1, 0.2, 0.0]
    assert decoded_msgs[0].effort == [10.0, 5.0, 2.0]


def test_joy_pybag() -> None:
    msg = sensor_msgs.Joy(
        header=_make_header(),
        axes=[0.5, -0.5, 0.0, 1.0, 0.0, 0.0],
        buttons=[0, 1, 0, 0, 1, 0, 0, 0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Joy'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].axes == [0.5, -0.5, 0.0, 1.0, 0.0, 0.0]
    assert decoded_msgs[0].buttons == [0, 1, 0, 0, 1, 0, 0, 0]


def test_joy_feedback_pybag() -> None:
    msg = sensor_msgs.JoyFeedback(
        type=sensor_msgs.JoyFeedback.TYPE_LED,
        id=1,
        intensity=0.5
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'JoyFeedback'
    assert decoded_msgs[0].type == 0  # TYPE_LED
    assert decoded_msgs[0].id == 1
    assert decoded_msgs[0].intensity == 0.5


def test_joy_feedback_array_pybag() -> None:
    feedback1 = sensor_msgs.JoyFeedback(
        type=sensor_msgs.JoyFeedback.TYPE_LED,
        id=1,
        intensity=0.5
    )
    feedback2 = sensor_msgs.JoyFeedback(
        type=sensor_msgs.JoyFeedback.TYPE_RUMBLE,
        id=2,
        intensity=0.5
    )
    msg = sensor_msgs.JoyFeedbackArray(
        array=[feedback1, feedback2]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'JoyFeedbackArray'
    assert len(decoded_msgs[0].array) == 2
    assert decoded_msgs[0].array[0].type == 0  # TYPE_LED
    assert decoded_msgs[0].array[0].id == 1
    assert decoded_msgs[0].array[0].intensity == 0.5
    assert decoded_msgs[0].array[1].type == 1  # TYPE_RUMBLE
    assert decoded_msgs[0].array[1].id == 2
    assert decoded_msgs[0].array[1].intensity == 0.5


def test_laser_echo_pybag() -> None:
    msg = sensor_msgs.LaserEcho(
        echoes=[1.5, 2.0, 3.5, 4.0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'LaserEcho'
    assert decoded_msgs[0].echoes == [1.5, 2.0, 3.5, 4.0]


def test_laser_scan_pybag() -> None:
    msg = sensor_msgs.LaserScan(
        header=_make_header(),
        angle_min=-1.5,
        angle_max=1.5,
        angle_increment=0.5,
        time_increment=0.25,
        scan_time=0.5,
        range_min=0.5,
        range_max=10.0,
        ranges=[1.0, 1.0, 1.0, 1.0, 1.0],
        intensities=[100.0, 110.0, 120.0, 130.0, 140.0]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'LaserScan'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].angle_min == -1.5
    assert decoded_msgs[0].angle_max == 1.5
    assert decoded_msgs[0].angle_increment == 0.5
    assert decoded_msgs[0].time_increment == 0.25
    assert decoded_msgs[0].scan_time == 0.5
    assert decoded_msgs[0].range_min == 0.5
    assert decoded_msgs[0].range_max == 10.0
    assert decoded_msgs[0].ranges == [1.0, 1.0, 1.0, 1.0, 1.0]
    assert decoded_msgs[0].intensities == [100.0, 110.0, 120.0, 130.0, 140.0]


def test_magnetic_field_pybag() -> None:
    magnetic_field = geometry_msgs.Vector3(x=0.1, y=0.2, z=0.3)
    msg = sensor_msgs.MagneticField(
        header=_make_header(),
        magnetic_field=magnetic_field,
        magnetic_field_covariance=[0.01] * 9
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MagneticField'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].magnetic_field.x == 0.1
    assert decoded_msgs[0].magnetic_field.y == 0.2
    assert decoded_msgs[0].magnetic_field.z == 0.3
    assert decoded_msgs[0].magnetic_field_covariance == [0.01] * 9


def test_multi_dof_joint_state_pybag() -> None:
    transform = geometry_msgs.Transform(
        translation=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0),
        rotation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    )
    twist = geometry_msgs.Twist(
        linear=geometry_msgs.Vector3(x=0.1, y=0.2, z=0.3),
        angular=geometry_msgs.Vector3(x=0.01, y=0.02, z=0.03)
    )
    wrench = geometry_msgs.Wrench(
        force=geometry_msgs.Vector3(x=10.0, y=20.0, z=30.0),
        torque=geometry_msgs.Vector3(x=1.0, y=2.0, z=3.0)
    )
    msg = sensor_msgs.MultiDOFJointState(
        header=_make_header(),
        joint_names=['joint1', 'joint2'],
        transforms=[transform, transform],
        twist=[twist, twist],
        wrench=[wrench, wrench]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MultiDOFJointState'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].joint_names == ['joint1', 'joint2']
    assert len(decoded_msgs[0].transforms) == 2
    assert decoded_msgs[0].transforms[0].translation.x == 1.0
    assert decoded_msgs[0].transforms[0].translation.y == 2.0
    assert decoded_msgs[0].transforms[0].translation.z == 3.0
    assert decoded_msgs[0].transforms[0].rotation.x == 0.0
    assert decoded_msgs[0].transforms[0].rotation.y == 0.0
    assert decoded_msgs[0].transforms[0].rotation.z == 0.0
    assert decoded_msgs[0].transforms[0].rotation.w == 1.0
    assert decoded_msgs[0].transforms[1].translation.x == 1.0
    assert decoded_msgs[0].transforms[1].translation.y == 2.0
    assert decoded_msgs[0].transforms[1].translation.z == 3.0
    assert decoded_msgs[0].transforms[1].rotation.x == 0.0
    assert decoded_msgs[0].transforms[1].rotation.y == 0.0
    assert decoded_msgs[0].transforms[1].rotation.z == 0.0
    assert decoded_msgs[0].transforms[1].rotation.w == 1.0
    assert decoded_msgs[0].twist[0].angular.x == 0.01
    assert decoded_msgs[0].twist[0].angular.y == 0.02
    assert decoded_msgs[0].twist[0].angular.z == 0.03
    assert decoded_msgs[0].twist[1].angular.x == 0.01
    assert decoded_msgs[0].twist[1].angular.y == 0.02
    assert decoded_msgs[0].twist[1].angular.z == 0.03
    assert decoded_msgs[0].wrench[0].force.x == 10.0
    assert decoded_msgs[0].wrench[0].force.y == 20.0
    assert decoded_msgs[0].wrench[0].force.z == 30.0
    assert decoded_msgs[0].wrench[0].torque.x == 1.0
    assert decoded_msgs[0].wrench[0].torque.y == 2.0
    assert decoded_msgs[0].wrench[0].torque.z == 3.0
    assert decoded_msgs[0].wrench[1].force.x == 10.0
    assert decoded_msgs[0].wrench[1].force.y == 20.0
    assert decoded_msgs[0].wrench[1].force.z == 30.0
    assert decoded_msgs[0].wrench[1].torque.x == 1.0
    assert decoded_msgs[0].wrench[1].torque.y == 2.0
    assert decoded_msgs[0].wrench[1].torque.z == 3.0


def test_multi_echo_laser_scan_pybag() -> None:
    echo1 = sensor_msgs.LaserEcho(echoes=[1.0, 1.5])
    echo2 = sensor_msgs.LaserEcho(echoes=[2.0, 2.5])
    msg = sensor_msgs.MultiEchoLaserScan(
        header=_make_header(),
        angle_min=-1.50,
        angle_max=1.50,
        angle_increment=0.5,
        time_increment=0.25,
        scan_time=0.5,
        range_min=0.5,
        range_max=10.0,
        ranges=[echo1, echo2],
        intensities=[echo1, echo2]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MultiEchoLaserScan'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].angle_min == -1.5
    assert decoded_msgs[0].angle_max == 1.5
    assert decoded_msgs[0].angle_increment == 0.5
    assert decoded_msgs[0].time_increment == 0.25
    assert decoded_msgs[0].scan_time == 0.5
    assert decoded_msgs[0].range_min == 0.5
    assert decoded_msgs[0].range_max == 10.0
    assert len(decoded_msgs[0].ranges) == 2
    assert decoded_msgs[0].ranges[0].echoes == [1.0, 1.5]
    assert decoded_msgs[0].ranges[1].echoes == [2.0, 2.5]


def test_nav_sat_status_pybag() -> None:
    msg = sensor_msgs.NavSatStatus(
        status=sensor_msgs.NavSatStatus.STATUS_FIX,
        service=sensor_msgs.NavSatStatus.SERVICE_GPS
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'NavSatStatus'
    assert decoded_msgs[0].status == 0   # STATUS_FIX
    assert decoded_msgs[0].service == 1  # SERVICE_GPS


def test_nav_sat_fix_pybag() -> None:
    status = sensor_msgs.NavSatStatus(
        status=sensor_msgs.NavSatStatus.STATUS_FIX,
        service=sensor_msgs.NavSatStatus.SERVICE_GPS
    )
    msg = sensor_msgs.NavSatFix(
        header=_make_header(),
        status=status,
        latitude=37.7749,
        longitude=-122.4194,
        altitude=100.0,
        position_covariance=[1.0] * 9,
        position_covariance_type=sensor_msgs.NavSatFix.COVARIANCE_TYPE_DIAGONAL_KNOWN
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'NavSatFix'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].status.status == 0  # STATUS_FIX
    assert decoded_msgs[0].status.service == 1  # SERVICE_GPS
    assert decoded_msgs[0].latitude == 37.7749
    assert decoded_msgs[0].longitude == -122.4194
    assert decoded_msgs[0].altitude == 100.0
    assert decoded_msgs[0].position_covariance == [1.0] * 9
    assert decoded_msgs[0].position_covariance_type == 2  # COVARIANCE_TYPE_DIAGONAL_KNOWN


def test_point_field_pybag() -> None:
    msg = sensor_msgs.PointField(
        name='x',
        offset=0,
        datatype=sensor_msgs.PointField.FLOAT32,
        count=1
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PointField'
    assert decoded_msgs[0].name == 'x'
    assert decoded_msgs[0].offset == 0
    assert decoded_msgs[0].datatype == 7  # FLOAT32
    assert decoded_msgs[0].count == 1


def test_point_cloud_pybag() -> None:
    point1 = geometry_msgs.Point32(x=1.0, y=2.0, z=3.0)
    point2 = geometry_msgs.Point32(x=4.0, y=5.0, z=6.0)
    channel = sensor_msgs.ChannelFloat32(name='intensity', values=[100.0, 200.0])
    msg = sensor_msgs.PointCloud(
        header=_make_header(),
        points=[point1, point2],
        channels=[channel]
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PointCloud'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert len(decoded_msgs[0].points) == 2
    assert decoded_msgs[0].points[0].x == 1.0
    assert decoded_msgs[0].points[0].y == 2.0
    assert decoded_msgs[0].points[0].z == 3.0
    assert decoded_msgs[0].points[1].x == 4.0
    assert decoded_msgs[0].points[1].y == 5.0
    assert decoded_msgs[0].points[1].z == 6.0
    assert len(decoded_msgs[0].channels) == 1
    assert decoded_msgs[0].channels[0].name == 'intensity'
    assert decoded_msgs[0].channels[0].values == [100.0, 200.0]


def test_point_cloud2_pybag() -> None:
    field1 = sensor_msgs.PointField(
        name='x',
        offset=0,
        datatype=sensor_msgs.PointField.FLOAT32,
        count=1
    )
    field2 = sensor_msgs.PointField(
        name='y',
        offset=4,
        datatype=sensor_msgs.PointField.FLOAT32,
        count=1
    )
    msg = sensor_msgs.PointCloud2(
        header=_make_header(),
        height=1,
        width=2,
        fields=[field1, field2],
        is_bigendian=False,
        point_step=8,
        row_step=16,
        data=[0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64],  # Sample point data
        is_dense=True
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'PointCloud2'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].height == 1
    assert decoded_msgs[0].width == 2
    assert len(decoded_msgs[0].fields) == 2
    assert decoded_msgs[0].fields[0].name == 'x'
    assert decoded_msgs[0].fields[0].offset == 0
    assert decoded_msgs[0].fields[0].datatype == 7  # FLOAT32
    assert decoded_msgs[0].fields[0].count == 1
    assert decoded_msgs[0].fields[1].name == 'y'
    assert decoded_msgs[0].fields[1].offset == 4
    assert decoded_msgs[0].fields[1].datatype == 7  # FLOAT32
    assert decoded_msgs[0].fields[1].count == 1
    assert decoded_msgs[0].is_bigendian == False
    assert decoded_msgs[0].point_step == 8
    assert decoded_msgs[0].row_step == 16
    assert decoded_msgs[0].data == bytes([0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64])
    assert decoded_msgs[0].is_dense == True


def test_range_pybag() -> None:
    msg = sensor_msgs.Range(
        header=_make_header(),
        radiation_type=sensor_msgs.Range.ULTRASOUND,
        field_of_view=0.5,
        min_range=0.25,
        max_range=10.0,
        range=2.5
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Range'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].radiation_type == 0  # ULTRASOUND
    assert decoded_msgs[0].field_of_view == 0.5
    assert decoded_msgs[0].min_range == 0.25
    assert decoded_msgs[0].max_range == 10.0
    assert decoded_msgs[0].range == 2.5


def test_relative_humidity_pybag() -> None:
    msg = sensor_msgs.RelativeHumidity(
        header=_make_header(),
        relative_humidity=65.5,
        variance=0.1
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'RelativeHumidity'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].relative_humidity == 65.5
    assert decoded_msgs[0].variance == 0.1


def test_temperature_pybag() -> None:
    msg = sensor_msgs.Temperature(
        header=_make_header(),
        temperature=25.0,
        variance=0.1
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Temperature'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].temperature == 25.0
    assert decoded_msgs[0].variance == 0.1


def test_time_reference_pybag() -> None:
    time_ref = builtin_interfaces.Time(sec=1000, nanosec=500000)
    msg = sensor_msgs.TimeReference(
        header=_make_header(),
        time_ref=time_ref,
        source='gps'
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'TimeReference'
    assert decoded_msgs[0].header.frame_id == 'frame'
    assert decoded_msgs[0].header.stamp.sec == 1
    assert decoded_msgs[0].header.stamp.nanosec == 2
    assert decoded_msgs[0].time_ref.sec == 1000
    assert decoded_msgs[0].time_ref.nanosec == 500000
    assert decoded_msgs[0].source == 'gps'
