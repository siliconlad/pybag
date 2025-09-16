"""Test reading sensor_msgs messages written with the official MCAP writer."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from mcap.writer import Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader
from tests.read._sample_message_factory import create_message, to_plain


@pytest.fixture(params=[Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE])
def typestore(request) -> Typestore:
    return get_typestore(request.param)


def _write_mcap(
    temp_dir: str,
    typestore: Typestore,
    msg,
    msgtype: str,
    schema_text: str,
) -> tuple[Path, int]:
    path = Path(temp_dir) / "test.mcap"
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start()
        schema_id = writer.register_schema(msgtype, "ros2msg", schema_text.encode())
        channel_id = writer.register_channel("/rosbags", "cdr", schema_id)
        writer.add_message(
            channel_id,
            log_time=0,
            data=typestore.serialize_cdr(msg, msgtype),
            publish_time=0,
        )
        writer.finish()
    return path, channel_id


def test_sensor_msgs_batterystate(typestore: Typestore):
    msgtype = "sensor_msgs/msg/BatteryState"
    msg = create_message(typestore, msgtype, seed=1)

    schema = (
        "uint8 POWER_SUPPLY_STATUS_UNKNOWN=0\n"
        "uint8 POWER_SUPPLY_STATUS_CHARGING=1\n"
        "uint8 POWER_SUPPLY_STATUS_DISCHARGING=2\n"
        "uint8 POWER_SUPPLY_STATUS_NOT_CHARGING=3\n"
        "uint8 POWER_SUPPLY_STATUS_FULL=4\n"
        "uint8 POWER_SUPPLY_HEALTH_UNKNOWN=0\n"
        "uint8 POWER_SUPPLY_HEALTH_GOOD=1\n"
        "uint8 POWER_SUPPLY_HEALTH_OVERHEAT=2\n"
        "uint8 POWER_SUPPLY_HEALTH_DEAD=3\n"
        "uint8 POWER_SUPPLY_HEALTH_OVERVOLTAGE=4\n"
        "uint8 POWER_SUPPLY_HEALTH_UNSPEC_FAILURE=5\n"
        "uint8 POWER_SUPPLY_HEALTH_COLD=6\n"
        "uint8 POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE=7\n"
        "uint8 POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE=8\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_UNKNOWN=0\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_NIMH=1\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_LION=2\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_LIPO=3\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_LIFE=4\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_NICD=5\n"
        "uint8 POWER_SUPPLY_TECHNOLOGY_LIMN=6\n"
        "std_msgs/Header header\n"
        "float32 voltage\n"
        "float32 temperature\n"
        "float32 current\n"
        "float32 charge\n"
        "float32 capacity\n"
        "float32 design_capacity\n"
        "float32 percentage\n"
        "uint8 power_supply_status\n"
        "uint8 power_supply_health\n"
        "uint8 power_supply_technology\n"
        "bool present\n"
        "float32[] cell_voltage\n"
        "float32[] cell_temperature\n"
        "string location\n"
        "string serial_number\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_camerainfo(typestore: Typestore):
    msgtype = "sensor_msgs/msg/CameraInfo"
    msg = create_message(typestore, msgtype, seed=2)

    schema = (
        "std_msgs/Header header\n"
        "uint32 height\n"
        "uint32 width\n"
        "string distortion_model\n"
        "float64[] d\n"
        "float64[9] k\n"
        "float64[9] r\n"
        "float64[12] p\n"
        "uint32 binning_x\n"
        "uint32 binning_y\n"
        "sensor_msgs/RegionOfInterest roi\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: sensor_msgs/RegionOfInterest\n"
        "uint32 x_offset\n"
        "uint32 y_offset\n"
        "uint32 height\n"
        "uint32 width\n"
        "bool do_rectify\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_channelfloat32(typestore: Typestore):
    msgtype = "sensor_msgs/msg/ChannelFloat32"
    msg = create_message(typestore, msgtype, seed=3)

    schema = (
        "string name\n"
        "float32[] values\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_compressedimage(typestore: Typestore):
    msgtype = "sensor_msgs/msg/CompressedImage"
    msg = create_message(typestore, msgtype, seed=4)

    schema = (
        "std_msgs/Header header\n"
        "string format\n"
        "uint8[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_fluidpressure(typestore: Typestore):
    msgtype = "sensor_msgs/msg/FluidPressure"
    msg = create_message(typestore, msgtype, seed=5)

    schema = (
        "std_msgs/Header header\n"
        "float64 fluid_pressure\n"
        "float64 variance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_illuminance(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Illuminance"
    msg = create_message(typestore, msgtype, seed=6)

    schema = (
        "std_msgs/Header header\n"
        "float64 illuminance\n"
        "float64 variance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_image(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Image"
    msg = create_message(typestore, msgtype, seed=7)

    schema = (
        "std_msgs/Header header\n"
        "uint32 height\n"
        "uint32 width\n"
        "string encoding\n"
        "uint8 is_bigendian\n"
        "uint32 step\n"
        "uint8[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_imu(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Imu"
    msg = create_message(typestore, msgtype, seed=8)

    schema = (
        "std_msgs/Header header\n"
        "geometry_msgs/Quaternion orientation\n"
        "float64[9] orientation_covariance\n"
        "geometry_msgs/Vector3 angular_velocity\n"
        "float64[9] angular_velocity_covariance\n"
        "geometry_msgs/Vector3 linear_acceleration\n"
        "float64[9] linear_acceleration_covariance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Vector3\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_jointstate(typestore: Typestore):
    msgtype = "sensor_msgs/msg/JointState"
    msg = create_message(typestore, msgtype, seed=9)

    schema = (
        "std_msgs/Header header\n"
        "string[] name\n"
        "float64[] position\n"
        "float64[] velocity\n"
        "float64[] effort\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_joy(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Joy"
    msg = create_message(typestore, msgtype, seed=10)

    schema = (
        "std_msgs/Header header\n"
        "float32[] axes\n"
        "int32[] buttons\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_joyfeedback(typestore: Typestore):
    msgtype = "sensor_msgs/msg/JoyFeedback"
    msg = create_message(typestore, msgtype, seed=11)

    schema = (
        "uint8 TYPE_LED=0\n"
        "uint8 TYPE_RUMBLE=1\n"
        "uint8 TYPE_BUZZER=2\n"
        "uint8 type\n"
        "uint8 id\n"
        "float32 intensity\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_joyfeedbackarray(typestore: Typestore):
    msgtype = "sensor_msgs/msg/JoyFeedbackArray"
    msg = create_message(typestore, msgtype, seed=12)

    schema = (
        "sensor_msgs/JoyFeedback[] array\n"
        "================================================================================\n"
        "MSG: sensor_msgs/JoyFeedback\n"
        "uint8 TYPE_LED=0\n"
        "uint8 TYPE_RUMBLE=1\n"
        "uint8 TYPE_BUZZER=2\n"
        "uint8 type\n"
        "uint8 id\n"
        "float32 intensity\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_laserecho(typestore: Typestore):
    msgtype = "sensor_msgs/msg/LaserEcho"
    msg = create_message(typestore, msgtype, seed=13)

    schema = (
        "float32[] echoes\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_laserscan(typestore: Typestore):
    msgtype = "sensor_msgs/msg/LaserScan"
    msg = create_message(typestore, msgtype, seed=14)

    schema = (
        "std_msgs/Header header\n"
        "float32 angle_min\n"
        "float32 angle_max\n"
        "float32 angle_increment\n"
        "float32 time_increment\n"
        "float32 scan_time\n"
        "float32 range_min\n"
        "float32 range_max\n"
        "float32[] ranges\n"
        "float32[] intensities\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_magneticfield(typestore: Typestore):
    msgtype = "sensor_msgs/msg/MagneticField"
    msg = create_message(typestore, msgtype, seed=15)

    schema = (
        "std_msgs/Header header\n"
        "geometry_msgs/Vector3 magnetic_field\n"
        "float64[9] magnetic_field_covariance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Vector3\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_multidofjointstate(typestore: Typestore):
    msgtype = "sensor_msgs/msg/MultiDOFJointState"
    msg = create_message(typestore, msgtype, seed=16)

    schema = (
        "std_msgs/Header header\n"
        "string[] joint_names\n"
        "geometry_msgs/Transform[] transforms\n"
        "geometry_msgs/Twist[] twist\n"
        "geometry_msgs/Wrench[] wrench\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Transform\n"
        "geometry_msgs/Vector3 translation\n"
        "geometry_msgs/Quaternion rotation\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Vector3\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Quaternion\n"
        "float64 x\n"
        "float64 y\n"
        "float64 z\n"
        "float64 w\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Twist\n"
        "geometry_msgs/Vector3 linear\n"
        "geometry_msgs/Vector3 angular\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Wrench\n"
        "geometry_msgs/Vector3 force\n"
        "geometry_msgs/Vector3 torque\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_multiecholaserscan(typestore: Typestore):
    msgtype = "sensor_msgs/msg/MultiEchoLaserScan"
    msg = create_message(typestore, msgtype, seed=17)

    schema = (
        "std_msgs/Header header\n"
        "float32 angle_min\n"
        "float32 angle_max\n"
        "float32 angle_increment\n"
        "float32 time_increment\n"
        "float32 scan_time\n"
        "float32 range_min\n"
        "float32 range_max\n"
        "sensor_msgs/LaserEcho[] ranges\n"
        "sensor_msgs/LaserEcho[] intensities\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: sensor_msgs/LaserEcho\n"
        "float32[] echoes\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_navsatfix(typestore: Typestore):
    msgtype = "sensor_msgs/msg/NavSatFix"
    msg = create_message(typestore, msgtype, seed=18)

    schema = (
        "uint8 COVARIANCE_TYPE_UNKNOWN=0\n"
        "uint8 COVARIANCE_TYPE_APPROXIMATED=1\n"
        "uint8 COVARIANCE_TYPE_DIAGONAL_KNOWN=2\n"
        "uint8 COVARIANCE_TYPE_KNOWN=3\n"
        "std_msgs/Header header\n"
        "sensor_msgs/NavSatStatus status\n"
        "float64 latitude\n"
        "float64 longitude\n"
        "float64 altitude\n"
        "float64[9] position_covariance\n"
        "uint8 position_covariance_type\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: sensor_msgs/NavSatStatus\n"
        "int8 STATUS_NO_FIX=-1\n"
        "int8 STATUS_FIX=0\n"
        "int8 STATUS_SBAS_FIX=1\n"
        "int8 STATUS_GBAS_FIX=2\n"
        "uint16 SERVICE_GPS=1\n"
        "uint16 SERVICE_GLONASS=2\n"
        "uint16 SERVICE_COMPASS=4\n"
        "uint16 SERVICE_GALILEO=8\n"
        "int8 status\n"
        "uint16 service\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_navsatstatus(typestore: Typestore):
    msgtype = "sensor_msgs/msg/NavSatStatus"
    msg = create_message(typestore, msgtype, seed=19)

    schema = (
        "int8 STATUS_NO_FIX=-1\n"
        "int8 STATUS_FIX=0\n"
        "int8 STATUS_SBAS_FIX=1\n"
        "int8 STATUS_GBAS_FIX=2\n"
        "uint16 SERVICE_GPS=1\n"
        "uint16 SERVICE_GLONASS=2\n"
        "uint16 SERVICE_COMPASS=4\n"
        "uint16 SERVICE_GALILEO=8\n"
        "int8 status\n"
        "uint16 service\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_pointcloud(typestore: Typestore):
    msgtype = "sensor_msgs/msg/PointCloud"
    msg = create_message(typestore, msgtype, seed=20)

    schema = (
        "std_msgs/Header header\n"
        "geometry_msgs/Point32[] points\n"
        "sensor_msgs/ChannelFloat32[] channels\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: geometry_msgs/Point32\n"
        "float32 x\n"
        "float32 y\n"
        "float32 z\n"
        "================================================================================\n"
        "MSG: sensor_msgs/ChannelFloat32\n"
        "string name\n"
        "float32[] values\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_pointcloud2(typestore: Typestore):
    msgtype = "sensor_msgs/msg/PointCloud2"
    msg = create_message(typestore, msgtype, seed=21)

    schema = (
        "std_msgs/Header header\n"
        "uint32 height\n"
        "uint32 width\n"
        "sensor_msgs/PointField[] fields\n"
        "bool is_bigendian\n"
        "uint32 point_step\n"
        "uint32 row_step\n"
        "uint8[] data\n"
        "bool is_dense\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: sensor_msgs/PointField\n"
        "uint8 INT8=1\n"
        "uint8 UINT8=2\n"
        "uint8 INT16=3\n"
        "uint8 UINT16=4\n"
        "uint8 INT32=5\n"
        "uint8 UINT32=6\n"
        "uint8 FLOAT32=7\n"
        "uint8 FLOAT64=8\n"
        "string name\n"
        "uint32 offset\n"
        "uint8 datatype\n"
        "uint32 count\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_pointfield(typestore: Typestore):
    msgtype = "sensor_msgs/msg/PointField"
    msg = create_message(typestore, msgtype, seed=22)

    schema = (
        "uint8 INT8=1\n"
        "uint8 UINT8=2\n"
        "uint8 INT16=3\n"
        "uint8 UINT16=4\n"
        "uint8 INT32=5\n"
        "uint8 UINT32=6\n"
        "uint8 FLOAT32=7\n"
        "uint8 FLOAT64=8\n"
        "string name\n"
        "uint32 offset\n"
        "uint8 datatype\n"
        "uint32 count\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_range(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Range"
    msg = create_message(typestore, msgtype, seed=23)

    schema = (
        "uint8 ULTRASOUND=0\n"
        "uint8 INFRARED=1\n"
        "std_msgs/Header header\n"
        "uint8 radiation_type\n"
        "float32 field_of_view\n"
        "float32 min_range\n"
        "float32 max_range\n"
        "float32 range\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_regionofinterest(typestore: Typestore):
    msgtype = "sensor_msgs/msg/RegionOfInterest"
    msg = create_message(typestore, msgtype, seed=24)

    schema = (
        "uint32 x_offset\n"
        "uint32 y_offset\n"
        "uint32 height\n"
        "uint32 width\n"
        "bool do_rectify\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_relativehumidity(typestore: Typestore):
    msgtype = "sensor_msgs/msg/RelativeHumidity"
    msg = create_message(typestore, msgtype, seed=25)

    schema = (
        "std_msgs/Header header\n"
        "float64 relative_humidity\n"
        "float64 variance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_temperature(typestore: Typestore):
    msgtype = "sensor_msgs/msg/Temperature"
    msg = create_message(typestore, msgtype, seed=26)

    schema = (
        "std_msgs/Header header\n"
        "float64 temperature\n"
        "float64 variance\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected


def test_sensor_msgs_timereference(typestore: Typestore):
    msgtype = "sensor_msgs/msg/TimeReference"
    msg = create_message(typestore, msgtype, seed=27)

    schema = (
        "std_msgs/Header header\n"
        "builtin_interfaces/Time time_ref\n"
        "string source\n"
        "================================================================================\n"
        "MSG: std_msgs/Header\n"
        "builtin_interfaces/Time stamp\n"
        "string frame_id\n"
        "================================================================================\n"
        "MSG: builtin_interfaces/Time\n"
        "int32 sec\n"
        "uint32 nanosec\n"
    )

    with TemporaryDirectory() as temp_dir:
        path, channel_id = _write_mcap(temp_dir, typestore, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    message = messages[0]
    assert message.channel_id == channel_id
    actual = to_plain(typestore, message.data, msgtype)
    expected = to_plain(typestore, msg, msgtype, actual.keys())
    assert actual == expected

