"""Test reading sensor_msgs messages written with the official MCAP writer."""

from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any

from mcap_ros2.writer import Writer as McapWriter

from pybag.mcap_reader import McapFileReader


def _write_mcap(temp_dir: str, msg: Any, msgtype: str, schema_text: str) -> Path:
    path = Path(temp_dir) / "test.mcap"
    with open(path, "wb") as f:
        writer = McapWriter(f)
        schema = writer.register_msgdef(msgtype, schema_text)
        writer.write_message(
            topic="/rosbags",
            schema=schema,
            message=msg,
            log_time=0,
            publish_time=0,
            sequence=0,
        )
        writer.finish()
    return path


def test_sensor_msgs_batterystate():
    msgtype = "sensor_msgs/BatteryState"
    schema = dedent("""
        uint8 POWER_SUPPLY_STATUS_UNKNOWN=0
        uint8 POWER_SUPPLY_STATUS_CHARGING=1
        uint8 POWER_SUPPLY_STATUS_DISCHARGING=2
        uint8 POWER_SUPPLY_STATUS_NOT_CHARGING=3
        uint8 POWER_SUPPLY_STATUS_FULL=4
        uint8 POWER_SUPPLY_HEALTH_UNKNOWN=0
        uint8 POWER_SUPPLY_HEALTH_GOOD=1
        uint8 POWER_SUPPLY_HEALTH_OVERHEAT=2
        uint8 POWER_SUPPLY_HEALTH_DEAD=3
        uint8 POWER_SUPPLY_HEALTH_OVERVOLTAGE=4
        uint8 POWER_SUPPLY_HEALTH_UNSPEC_FAILURE=5
        uint8 POWER_SUPPLY_HEALTH_COLD=6
        uint8 POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE=7
        uint8 POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE=8
        uint8 POWER_SUPPLY_TECHNOLOGY_UNKNOWN=0
        uint8 POWER_SUPPLY_TECHNOLOGY_NIMH=1
        uint8 POWER_SUPPLY_TECHNOLOGY_LION=2
        uint8 POWER_SUPPLY_TECHNOLOGY_LIPO=3
        uint8 POWER_SUPPLY_TECHNOLOGY_LIFE=4
        uint8 POWER_SUPPLY_TECHNOLOGY_NICD=5
        uint8 POWER_SUPPLY_TECHNOLOGY_LIMN=6
        std_msgs/Header header
        float32 voltage
        float32 temperature
        float32 current
        float32 charge
        float32 capacity
        float32 design_capacity
        float32 percentage
        uint8 power_supply_status
        uint8 power_supply_health
        uint8 power_supply_technology
        bool present
        float32[] cell_voltage
        float32[] cell_temperature
        string location
        string serial_number
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "battery"
            },
            "voltage": 12.6,
            "temperature": 25.0,
            "current": -2.5,  # negative = discharging
            "charge": 45.0,
            "capacity": 50.0,
            "design_capacity": 60.0,
            "percentage": 90.0,
            "power_supply_status": 2,  # DISCHARGING
            "power_supply_health": 1,  # GOOD
            "power_supply_technology": 2,  # LION
            "present": True,
            "cell_voltage": [4.2, 4.1, 4.3],
            "cell_temperature": [24.5, 25.0, 25.5],
            "location": "main_battery",
            "serial_number": "BAT123456"
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "battery"
    assert abs(messages[0].data.voltage - 12.6) < 0.001
    assert abs(messages[0].data.temperature - 25.0) < 0.001
    assert abs(messages[0].data.current - (-2.5)) < 0.001
    assert abs(messages[0].data.percentage - 90.0) < 0.001
    assert messages[0].data.power_supply_status == 2
    assert messages[0].data.power_supply_health == 1
    assert messages[0].data.power_supply_technology == 2
    assert messages[0].data.present is True
    assert len(messages[0].data.cell_voltage) == 3
    assert len(messages[0].data.cell_temperature) == 3
    assert messages[0].data.location == "main_battery"
    assert messages[0].data.serial_number == "BAT123456"


def test_sensor_msgs_camerainfo():
    msgtype = "sensor_msgs/CameraInfo"
    schema = dedent("""
        std_msgs/Header header
        uint32 height
        uint32 width
        string distortion_model
        float64[] d
        float64[9] k
        float64[9] r
        float64[12] p
        uint32 binning_x
        uint32 binning_y
        sensor_msgs/RegionOfInterest roi
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/RegionOfInterest
        uint32 x_offset
        uint32 y_offset
        uint32 height
        uint32 width
        bool do_rectify
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "camera_frame"
            },
            "height": 480,
            "width": 640,
            "distortion_model": "plumb_bob",
            "d": [-0.1, 0.05, 0.0, 0.0, 0.0],
            "k": [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0],
            "r": [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
            "p": [500.0, 0.0, 320.0, 0.0, 0.0, 500.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            "binning_x": 1,
            "binning_y": 1,
            "roi": {
                "x_offset": 0,
                "y_offset": 0,
                "height": 480,
                "width": 640,
                "do_rectify": False
            }
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "camera_frame"
    assert messages[0].data.height == 480
    assert messages[0].data.width == 640
    assert messages[0].data.distortion_model == "plumb_bob"
    assert len(messages[0].data.d) == 5
    assert abs(messages[0].data.d[0] + 0.1) < 0.001
    assert len(messages[0].data.k) == 9
    assert messages[0].data.k[0] == 500.0
    assert len(messages[0].data.r) == 9
    assert messages[0].data.r[0] == 1.0
    assert len(messages[0].data.p) == 12
    assert messages[0].data.p[0] == 500.0
    assert messages[0].data.binning_x == 1
    assert messages[0].data.binning_y == 1
    assert messages[0].data.roi.x_offset == 0
    assert messages[0].data.roi.height == 480
    assert messages[0].data.roi.do_rectify == False


def test_sensor_msgs_channelfloat32():
    # values -> valuess because of mcap_ros2 bug
    msgtype = "sensor_msgs/ChannelFloat32"
    schema = dedent("""
        string name
        float32[] valuess
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "name": "intensity",
            "valuess": [1.0, 2.5, 3.7]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.name == "intensity"
    assert len(messages[0].data.valuess) == 3
    assert abs(messages[0].data.valuess[0] - 1.0) < 0.001
    assert abs(messages[0].data.valuess[1] - 2.5) < 0.001
    assert abs(messages[0].data.valuess[2] - 3.7) < 0.001


def test_sensor_msgs_compressedimage():
    msgtype = "sensor_msgs/CompressedImage"
    schema = dedent("""
        std_msgs/Header header
        string format
        uint8[] data
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "camera"
            },
            "format": "jpeg",
            "data": [255, 216, 255, 224, 0, 16, 74, 70, 73, 70]  # Sample JPEG header bytes
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "camera"
    assert messages[0].data.format == "jpeg"
    assert len(messages[0].data.data) == 10
    assert list(messages[0].data.data[:4]) == [255, 216, 255, 224]


def test_sensor_msgs_fluidpressure():
    msgtype = "sensor_msgs/FluidPressure"
    schema = dedent("""
        std_msgs/Header header
        float64 fluid_pressure
        float64 variance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "pressure_sensor"
            },
            "fluid_pressure": 101325.0,  # Standard atmospheric pressure
            "variance": 0.1
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "pressure_sensor"
    assert messages[0].data.fluid_pressure == 101325.0
    assert messages[0].data.variance == 0.1


def test_sensor_msgs_illuminance():
    msgtype = "sensor_msgs/Illuminance"
    schema = dedent("""
        std_msgs/Header header
        float64 illuminance
        float64 variance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "light_sensor"
            },
            "illuminance": 450.5,
            "variance": 2.1
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "light_sensor"
    assert abs(messages[0].data.illuminance - 450.5) < 0.001
    assert abs(messages[0].data.variance - 2.1) < 0.001


def test_sensor_msgs_image():
    msgtype = "sensor_msgs/Image"
    schema = dedent("""
        std_msgs/Header header
        uint32 height
        uint32 width
        string encoding
        uint8 is_bigendian
        uint32 step
        uint8[] data
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "camera"
            },
            "height": 480,
            "width": 640,
            "encoding": "rgb8",
            "is_bigendian": 0,
            "step": 1920,  # width * 3 bytes per pixel for rgb8
            "data": [255, 0, 0] * 10  # Sample red pixels
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "camera"
    assert messages[0].data.height == 480
    assert messages[0].data.width == 640
    assert messages[0].data.encoding == "rgb8"
    assert messages[0].data.is_bigendian == 0
    assert messages[0].data.step == 1920
    assert len(messages[0].data.data) == 30
    assert list(messages[0].data.data[:3]) == [255, 0, 0]


def test_sensor_msgs_imu():
    msgtype = "sensor_msgs/Imu"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Quaternion orientation
        float64[9] orientation_covariance
        geometry_msgs/Vector3 angular_velocity
        float64[9] angular_velocity_covariance
        geometry_msgs/Vector3 linear_acceleration
        float64[9] linear_acceleration_covariance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "imu_link"
            },
            "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
            "orientation_covariance": [0.01] + [0.0] * 8,
            "angular_velocity": {"x": 0.1, "y": 0.05, "z": 0.02},
            "angular_velocity_covariance": [0.001] + [0.0] * 8,
            "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 9.81},
            "linear_acceleration_covariance": [0.1] + [0.0] * 8
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "imu_link"
    assert messages[0].data.orientation.x == 0.0
    assert messages[0].data.orientation.w == 1.0
    assert len(messages[0].data.orientation_covariance) == 9
    assert messages[0].data.orientation_covariance[0] == 0.01
    assert abs(messages[0].data.angular_velocity.x - 0.1) < 0.001
    assert abs(messages[0].data.linear_acceleration.z - 9.81) < 0.001


def test_sensor_msgs_jointstate():
    msgtype = "sensor_msgs/JointState"
    schema = dedent("""
        std_msgs/Header header
        string[] name
        float64[] position
        float64[] velocity
        float64[] effort
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "robot"
            },
            "name": ["joint_1", "joint_2", "joint_3"],
            "position": [1.57, 0.0, -1.57],
            "velocity": [0.1, 0.0, -0.1],
            "effort": [10.5, 0.0, -5.2]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "robot"
    assert len(messages[0].data.name) == 3
    assert messages[0].data.name[0] == "joint_1"
    assert len(messages[0].data.position) == 3
    assert abs(messages[0].data.position[0] - 1.57) < 0.001
    assert len(messages[0].data.velocity) == 3
    assert abs(messages[0].data.velocity[0] - 0.1) < 0.001
    assert len(messages[0].data.effort) == 3
    assert abs(messages[0].data.effort[0] - 10.5) < 0.001


def test_sensor_msgs_joy():
    msgtype = "sensor_msgs/Joy"
    schema = dedent("""
        std_msgs/Header header
        float32[] axes
        int32[] buttons
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "joy"
            },
            "axes": [0.5, -0.8, 0.0, 1.0],
            "buttons": [1, 0, 0, 1, 0, 0, 0, 0]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "joy"
    assert len(messages[0].data.axes) == 4
    assert abs(messages[0].data.axes[0] - 0.5) < 0.001
    assert len(messages[0].data.buttons) == 8
    assert messages[0].data.buttons[0] == 1
    assert messages[0].data.buttons[1] == 0


def test_sensor_msgs_joyfeedback():
    msgtype = "sensor_msgs/JoyFeedback"
    schema = dedent("""
        uint8 TYPE_LED=0
        uint8 TYPE_RUMBLE=1
        uint8 TYPE_BUZZER=2
        uint8 type
        uint8 id
        float32 intensity
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "type": 1,  # TYPE_RUMBLE
            "id": 0,
            "intensity": 0.7
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.type == 1
    assert messages[0].data.id == 0
    assert abs(messages[0].data.intensity - 0.7) < 0.001


def test_sensor_msgs_joyfeedbackarray():
    msgtype = "sensor_msgs/JoyFeedbackArray"
    schema = dedent("""
        sensor_msgs/JoyFeedback[] array
        ================================================================================
        MSG: sensor_msgs/JoyFeedback
        uint8 TYPE_LED=0
        uint8 TYPE_RUMBLE=1
        uint8 TYPE_BUZZER=2
        uint8 type
        uint8 id
        float32 intensity
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "array": [
                {"type": 0, "id": 0, "intensity": 1.0},  # LED
                {"type": 1, "id": 1, "intensity": 0.5}   # RUMBLE
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.array) == 2
    assert messages[0].data.array[0].type == 0
    assert messages[0].data.array[0].id == 0
    assert abs(messages[0].data.array[0].intensity - 1.0) < 0.001
    assert messages[0].data.array[1].type == 1
    assert abs(messages[0].data.array[1].intensity - 0.5) < 0.001


def test_sensor_msgs_laserecho():
    msgtype = "sensor_msgs/LaserEcho"
    schema = dedent("""
        float32[] echoes
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"echoes": [1.2, 2.4, 3.6]}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.echoes) == 3
    assert abs(messages[0].data.echoes[0] - 1.2) < 0.001
    assert abs(messages[0].data.echoes[1] - 2.4) < 0.001
    assert abs(messages[0].data.echoes[2] - 3.6) < 0.001


def test_sensor_msgs_laserscan():
    msgtype = "sensor_msgs/LaserScan"
    schema = dedent("""
        std_msgs/Header header
        float32 angle_min
        float32 angle_max
        float32 angle_increment
        float32 time_increment
        float32 scan_time
        float32 range_min
        float32 range_max
        float32[] ranges
        float32[] intensities
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "laser"
            },
            "angle_min": -1.57,
            "angle_max": 1.57,
            "angle_increment": 0.017,
            "time_increment": 0.0001,
            "scan_time": 0.1,
            "range_min": 0.1,
            "range_max": 10.0,
            "ranges": [1.5, 2.0, 2.5, 3.0],
            "intensities": [100.0, 120.0, 110.0, 90.0]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "laser"
    assert abs(messages[0].data.angle_min + 1.57) < 0.001
    assert abs(messages[0].data.angle_max - 1.57) < 0.001
    assert abs(messages[0].data.range_min - 0.1) < 0.001
    assert len(messages[0].data.ranges) == 4
    assert abs(messages[0].data.ranges[0] - 1.5) < 0.001
    assert len(messages[0].data.intensities) == 4
    assert abs(messages[0].data.intensities[0] - 100.0) < 0.001


def test_sensor_msgs_magneticfield():
    msgtype = "sensor_msgs/MagneticField"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Vector3 magnetic_field
        float64[9] magnetic_field_covariance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "magnetometer"
            },
            "magnetic_field": {"x": 2.1e-5, "y": 0.5e-5, "z": -4.2e-5},
            "magnetic_field_covariance": [1e-12] + [0.0] * 8
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "magnetometer"
    assert abs(messages[0].data.magnetic_field.x - 2.1e-5) < 1e-7
    assert abs(messages[0].data.magnetic_field.y - 0.5e-5) < 1e-7
    assert abs(messages[0].data.magnetic_field.z + 4.2e-5) < 1e-7
    assert len(messages[0].data.magnetic_field_covariance) == 9
    assert abs(messages[0].data.magnetic_field_covariance[0] - 1e-12) < 1e-15


def test_sensor_msgs_multidofjointstate():
    msgtype = "sensor_msgs/MultiDOFJointState"
    schema = dedent("""
        std_msgs/Header header
        string[] joint_names
        geometry_msgs/Transform[] transforms
        geometry_msgs/Twist[] twist
        geometry_msgs/Wrench[] wrench
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Transform
        geometry_msgs/Vector3 translation
        geometry_msgs/Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x
        float64 y
        float64 z
        float64 w
        ================================================================================
        MSG: geometry_msgs/Twist
        geometry_msgs/Vector3 linear
        geometry_msgs/Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Wrench
        geometry_msgs/Vector3 force
        geometry_msgs/Vector3 torque
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "robot"
            },
            "joint_names": ["base_joint", "arm_joint"],
            "transforms": [
                {
                    "translation": {"x": 1.0, "y": 0.0, "z": 0.0},
                    "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                },
                {
                    "translation": {"x": 0.5, "y": 0.5, "z": 0.0},
                    "rotation": {"x": 0.0, "y": 0.0, "z": 0.707, "w": 0.707}
                }
            ],
            "twist": [
                {
                    "linear": {"x": 0.1, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.1}
                },
                {
                    "linear": {"x": 0.0, "y": 0.1, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.1, "z": 0.0}
                }
            ],
            "wrench": [
                {
                    "force": {"x": 10.0, "y": 0.0, "z": 0.0},
                    "torque": {"x": 0.0, "y": 0.0, "z": 1.0}
                },
                {
                    "force": {"x": 0.0, "y": 5.0, "z": 0.0},
                    "torque": {"x": 0.0, "y": 0.5, "z": 0.0}
                }
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "robot"
    assert len(messages[0].data.joint_names) == 2
    assert messages[0].data.joint_names[0] == "base_joint"
    assert len(messages[0].data.transforms) == 2
    assert messages[0].data.transforms[0].translation.x == 1.0
    assert abs(messages[0].data.transforms[1].rotation.z - 0.707) < 0.001
    assert len(messages[0].data.twist) == 2
    assert abs(messages[0].data.twist[0].linear.x - 0.1) < 0.001
    assert len(messages[0].data.wrench) == 2
    assert abs(messages[0].data.wrench[0].force.x - 10.0) < 0.001


def test_sensor_msgs_multiecholaserscan():
    msgtype = "sensor_msgs/MultiEchoLaserScan"
    schema = dedent("""
        std_msgs/Header header
        float32 angle_min
        float32 angle_max
        float32 angle_increment
        float32 time_increment
        float32 scan_time
        float32 range_min
        float32 range_max
        sensor_msgs/LaserEcho[] ranges
        sensor_msgs/LaserEcho[] intensities
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/LaserEcho
        float32[] echoes
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "laser"
            },
            "angle_min": -1.57,
            "angle_max": 1.57,
            "angle_increment": 0.017,
            "time_increment": 0.0001,
            "scan_time": 0.1,
            "range_min": 0.1,
            "range_max": 10.0,
            "ranges": [
                {"echoes": [1.5, 1.6]},
                {"echoes": [2.0, 2.1, 2.2]}
            ],
            "intensities": [
                {"echoes": [100.0, 105.0]},
                {"echoes": [120.0, 125.0, 130.0]}
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "laser"
    assert abs(messages[0].data.angle_min + 1.57) < 0.001
    assert len(messages[0].data.ranges) == 2
    assert len(messages[0].data.ranges[0].echoes) == 2
    assert abs(messages[0].data.ranges[0].echoes[0] - 1.5) < 0.001
    assert len(messages[0].data.intensities) == 2
    assert abs(messages[0].data.intensities[0].echoes[0] - 100.0) < 0.001


def test_sensor_msgs_navsatfix():
    msgtype = "sensor_msgs/NavSatFix"
    schema = dedent("""
        uint8 COVARIANCE_TYPE_UNKNOWN=0
        uint8 COVARIANCE_TYPE_APPROXIMATED=1
        uint8 COVARIANCE_TYPE_DIAGONAL_KNOWN=2
        uint8 COVARIANCE_TYPE_KNOWN=3
        std_msgs/Header header
        sensor_msgs/NavSatStatus status
        float64 latitude
        float64 longitude
        float64 altitude
        float64[9] position_covariance
        uint8 position_covariance_type
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/NavSatStatus
        int8 STATUS_NO_FIX=-1
        int8 STATUS_FIX=0
        int8 STATUS_SBAS_FIX=1
        int8 STATUS_GBAS_FIX=2
        uint16 SERVICE_GPS=1
        uint16 SERVICE_GLONASS=2
        uint16 SERVICE_COMPASS=4
        uint16 SERVICE_GALILEO=8
        int8 status
        uint16 service
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "gps"
            },
            "status": {"status": 0, "service": 1},  # STATUS_FIX, SERVICE_GPS
            "latitude": 37.7749,
            "longitude": -122.4194,
            "altitude": 10.5,
            "position_covariance": [1.0] + [0.0] * 8,
            "position_covariance_type": 3  # COVARIANCE_TYPE_KNOWN
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "gps"
    assert messages[0].data.status.status == 0
    assert messages[0].data.status.service == 1
    assert abs(messages[0].data.latitude - 37.7749) < 0.0001
    assert abs(messages[0].data.longitude + 122.4194) < 0.0001
    assert abs(messages[0].data.altitude - 10.5) < 0.001
    assert len(messages[0].data.position_covariance) == 9
    assert messages[0].data.position_covariance_type == 3


def test_sensor_msgs_navsatstatus():
    msgtype = "sensor_msgs/NavSatStatus"
    schema = dedent("""
        int8 STATUS_NO_FIX=-1
        int8 STATUS_FIX=0
        int8 STATUS_SBAS_FIX=1
        int8 STATUS_GBAS_FIX=2
        uint16 SERVICE_GPS=1
        uint16 SERVICE_GLONASS=2
        uint16 SERVICE_COMPASS=4
        uint16 SERVICE_GALILEO=8
        int8 status
        uint16 service
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"status": 0, "service": 1}  # STATUS_FIX, SERVICE_GPS
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.status == 0
    assert messages[0].data.service == 1


def test_sensor_msgs_pointcloud():
    # values -> valuess because of mcap_ros2 bug
    msgtype = "sensor_msgs/PointCloud"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/Point32[] points
        sensor_msgs/ChannelFloat32[] channels
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Point32
        float32 x
        float32 y
        float32 z
        ================================================================================
        MSG: sensor_msgs/ChannelFloat32
        string name
        float32[] valuess
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "lidar"
            },
            "points": [
                {"x": 1.0, "y": 2.0, "z": 3.0},
                {"x": 4.0, "y": 5.0, "z": 6.0}
            ],
            "channels": [
                {"name": "intensity", "values": [100.0, 200.0]}
            ]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "lidar"
    assert len(messages[0].data.points) == 2
    assert messages[0].data.points[0].x == 1.0
    assert len(messages[0].data.channels) == 1
    assert messages[0].data.channels[0].name == "intensity"


def test_sensor_msgs_pointcloud2():
    msgtype = "sensor_msgs/PointCloud2"
    schema = dedent("""
        std_msgs/Header header
        uint32 height
        uint32 width
        sensor_msgs/PointField[] fields
        bool is_bigendian
        uint32 point_step
        uint32 row_step
        uint8[] data
        bool is_dense
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/PointField
        uint8 INT8=1
        uint8 UINT8=2
        uint8 INT16=3
        uint8 UINT16=4
        uint8 INT32=5
        uint8 UINT32=6
        uint8 FLOAT32=7
        uint8 FLOAT64=8
        string name
        uint32 offset
        uint8 datatype
        uint32 count
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "lidar"
            },
            "height": 1,
            "width": 2,
            "fields": [
                {"name": "x", "offset": 0, "datatype": 7, "count": 1},  # FLOAT32
                {"name": "y", "offset": 4, "datatype": 7, "count": 1},  # FLOAT32
                {"name": "z", "offset": 8, "datatype": 7, "count": 1}   # FLOAT32
            ],
            "is_bigendian": False,
            "point_step": 12,  # 3 floats * 4 bytes each
            "row_step": 24,    # 2 points * 12 bytes each
            "data": [0] * 24,  # 24 bytes of point data
            "is_dense": True
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "lidar"
    assert messages[0].data.height == 1
    assert messages[0].data.width == 2
    assert len(messages[0].data.fields) == 3
    assert messages[0].data.fields[0].name == "x"
    assert messages[0].data.point_step == 12
    assert messages[0].data.row_step == 24
    assert messages[0].data.is_dense == True


def test_sensor_msgs_pointfield():
    msgtype = "sensor_msgs/PointField"
    schema = dedent("""
        uint8 INT8=1
        uint8 UINT8=2
        uint8 INT16=3
        uint8 UINT16=4
        uint8 INT32=5
        uint8 UINT32=6
        uint8 FLOAT32=7
        uint8 FLOAT64=8
        string name
        uint32 offset
        uint8 datatype
        uint32 count
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "name": "x",
            "offset": 0,
            "datatype": 7,  # FLOAT32
            "count": 1
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.name == "x"
    assert messages[0].data.offset == 0
    assert messages[0].data.datatype == 7
    assert messages[0].data.count == 1


def test_sensor_msgs_range():
    msgtype = "sensor_msgs/Range"
    schema = dedent("""
        uint8 ULTRASOUND=0
        uint8 INFRARED=1
        std_msgs/Header header
        uint8 radiation_type
        float32 field_of_view
        float32 min_range
        float32 max_range
        float32 range
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "ultrasonic_sensor"
            },
            "radiation_type": 0,  # ULTRASOUND
            "field_of_view": 0.7854,  # 45 degrees in radians
            "min_range": 0.02,
            "max_range": 4.0,
            "range": 1.5
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "ultrasonic_sensor"
    assert messages[0].data.radiation_type == 0
    assert abs(messages[0].data.field_of_view - 0.7854) < 0.001
    assert abs(messages[0].data.min_range - 0.02) < 0.001
    assert abs(messages[0].data.max_range - 4.0) < 0.001
    assert abs(messages[0].data.range - 1.5) < 0.001


def test_sensor_msgs_regionofinterest():
    msgtype = "sensor_msgs/RegionOfInterest"
    schema = dedent("""
        uint32 x_offset
        uint32 y_offset
        uint32 height
        uint32 width
        bool do_rectify
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "x_offset": 10,
            "y_offset": 20,
            "height": 480,
            "width": 640,
            "do_rectify": True
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x_offset == 10
    assert messages[0].data.y_offset == 20
    assert messages[0].data.height == 480
    assert messages[0].data.width == 640
    assert messages[0].data.do_rectify is True


def test_sensor_msgs_relativehumidity():
    msgtype = "sensor_msgs/RelativeHumidity"
    schema = dedent("""
        std_msgs/Header header
        float64 relative_humidity
        float64 variance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "humidity_sensor"
            },
            "relative_humidity": 45.5,  # 45.5% humidity
            "variance": 0.1
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "humidity_sensor"
    assert abs(messages[0].data.relative_humidity - 45.5) < 0.001
    assert abs(messages[0].data.variance - 0.1) < 0.001


def test_sensor_msgs_temperature():
    msgtype = "sensor_msgs/Temperature"
    schema = dedent("""
        std_msgs/Header header
        float64 temperature
        float64 variance
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "temperature_sensor"
            },
            "temperature": 298.15,  # 25°C in Kelvin
            "variance": 0.01
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "temperature_sensor"
    assert messages[0].data.temperature == 298.15
    assert messages[0].data.variance == 0.01


def test_sensor_msgs_timereference():
    msgtype = "sensor_msgs/TimeReference"
    schema = dedent("""
        std_msgs/Header header
        builtin_interfaces/Time time_ref
        string source
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: builtin_interfaces/Time
        int32 sec
        uint32 nanosec
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "time_source"
            },
            "time_ref": {"sec": 1234567890, "nanosec": 123456789},
            "source": "GPS"
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.header.stamp.sec == 123
    assert messages[0].data.header.frame_id == "time_source"
    assert messages[0].data.time_ref.sec == 1234567890
    assert messages[0].data.time_ref.nanosec == 123456789
    assert messages[0].data.source == "GPS"
