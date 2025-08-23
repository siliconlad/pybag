from dataclasses import dataclass
from typing import Literal

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.std_msgs as std_msgs
import pybag.types as t


@dataclass(kw_only=True)
class BatteryState:
    __msg_name__ = 'sensor_msgs/msg/BatteryState'

    POWER_SUPPLY_STATUS_UNKNOWN: t.Constant[t.uint8] = 0
    POWER_SUPPLY_STATUS_CHARGING: t.Constant[t.uint8] = 1
    POWER_SUPPLY_STATUS_DISCHARGING: t.Constant[t.uint8] = 2
    POWER_SUPPLY_STATUS_NOT_CHARGING: t.Constant[t.uint8] = 3
    POWER_SUPPLY_STATUS_FULL: t.Constant[t.uint8] = 4

    POWER_SUPPLY_HEALTH_UNKNOWN: t.Constant[t.uint8] = 0
    POWER_SUPPLY_HEALTH_GOOD: t.Constant[t.uint8] = 1
    POWER_SUPPLY_HEALTH_OVERHEAT: t.Constant[t.uint8] = 2
    POWER_SUPPLY_HEALTH_DEAD: t.Constant[t.uint8] = 3
    POWER_SUPPLY_HEALTH_OVERVOLTAGE: t.Constant[t.uint8] = 4
    POWER_SUPPLY_HEALTH_UNSPEC_FAILURE: t.Constant[t.uint8] = 5
    POWER_SUPPLY_HEALTH_COLD: t.Constant[t.uint8] = 6
    POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE: t.Constant[t.uint8] = 7
    POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE: t.Constant[t.uint8] = 8

    POWER_SUPPLY_TECHNOLOGY_UNKNOWN: t.Constant[t.uint8] = 0
    POWER_SUPPLY_TECHNOLOGY_NIMH: t.Constant[t.uint8] = 1
    POWER_SUPPLY_TECHNOLOGY_LION: t.Constant[t.uint8] = 2
    POWER_SUPPLY_TECHNOLOGY_LIPO: t.Constant[t.uint8] = 3
    POWER_SUPPLY_TECHNOLOGY_LIFE: t.Constant[t.uint8] = 4
    POWER_SUPPLY_TECHNOLOGY_NICD: t.Constant[t.uint8] = 5
    POWER_SUPPLY_TECHNOLOGY_LIMN: t.Constant[t.uint8] = 6

    header: t.Complex[std_msgs.Header]
    voltage: t.float32
    temperature: t.float32
    current: t.float32
    charge: t.float32
    capacity: t.float32
    design_capacity: t.float32
    percentage: t.float32
    power_supply_status: t.uint8
    power_supply_health: t.uint8
    power_supply_technology: t.uint8
    present: t.bool
    cell_voltage: t.Array[t.float32]
    cell_temperature: t.Array[t.float32]
    location: t.string
    serial_number: t.string


@dataclass(kw_only=True)
class RegionOfInterest:
    __msg_name__ = 'sensor_msgs/msg/RegionOfInterest'

    x_offset: t.uint32
    y_offset: t.uint32
    height: t.uint32
    width: t.uint32
    do_rectify: t.bool


@dataclass(kw_only=True)
class CameraInfo:
    __msg_name__ = 'sensor_msgs/msg/CameraInfo'

    header: t.Complex[std_msgs.Header]
    height: t.uint32
    width: t.uint32
    distortion_model: t.string
    d: t.Array[t.float64]
    k: t.Array[t.float64, Literal[9]]
    r: t.Array[t.float64, Literal[9]]
    p: t.Array[t.float64, Literal[12]]
    binning_x: t.uint32
    binning_y: t.uint32
    roi: t.Complex[RegionOfInterest]


@dataclass(kw_only=True)
class ChannelFloat32:
    __msg_name__ = 'sensor_msgs/msg/ChannelFloat32'

    name: t.string
    values: t.Array[t.float32]


@dataclass(kw_only=True)
class CompressedImage:
    __msg_name__ = 'sensor_msgs/msg/CompressedImage'

    header: t.Complex[std_msgs.Header]
    format: t.string
    data: t.Array[t.uint8]


@dataclass(kw_only=True)
class FluidPressure:
    __msg_name__ = 'sensor_msgs/msg/FluidPressure'

    header: t.Complex[std_msgs.Header]
    fluid_pressure: t.float64
    variance: t.float64


@dataclass(kw_only=True)
class Illuminance:
    __msg_name__ = 'sensor_msgs/msg/Illuminance'

    header: t.Complex[std_msgs.Header]
    illuminance: t.float64
    variance: t.float64


@dataclass(kw_only=True)
class Image:
    __msg_name__ = 'sensor_msgs/msg/Image'

    header: t.Complex[std_msgs.Header]
    height: t.uint32
    width: t.uint32
    encoding: t.string
    is_bigendian: t.uint8
    step: t.uint32
    data: t.Array[t.uint8]


@dataclass(kw_only=True)
class Imu:
    __msg_name__ = 'sensor_msgs/msg/Imu'

    header: t.Complex[std_msgs.Header]
    orientation: t.Complex[geometry_msgs.Quaternion]
    orientation_covariance: t.Array[t.float64, Literal[9]]
    angular_velocity: t.Complex[geometry_msgs.Vector3]
    angular_velocity_covariance: t.Array[t.float64, Literal[9]]
    linear_acceleration: t.Complex[geometry_msgs.Vector3]
    linear_acceleration_covariance: t.Array[t.float64, Literal[9]]


@dataclass(kw_only=True)
class JointState:
    __msg_name__ = 'sensor_msgs/msg/JointState'

    header: t.Complex[std_msgs.Header]
    name: t.Array[t.string]
    position: t.Array[t.float64]
    velocity: t.Array[t.float64]
    effort: t.Array[t.float64]


@dataclass(kw_only=True)
class Joy:
    __msg_name__ = 'sensor_msgs/msg/Joy'

    header: t.Complex[std_msgs.Header]
    axes: t.Array[t.float32]
    buttons: t.Array[t.int32]


@dataclass(kw_only=True)
class JoyFeedback:
    __msg_name__ = 'sensor_msgs/msg/JoyFeedback'

    TYPE_LED: t.Constant[t.uint8] = 0
    TYPE_RUMBLE: t.Constant[t.uint8] = 1
    TYPE_BUZZER: t.Constant[t.uint8] = 2

    type: t.uint8
    id: t.uint8
    intensity: t.float32


@dataclass(kw_only=True)
class JoyFeedbackArray:
    __msg_name__ = 'sensor_msgs/msg/JoyFeedbackArray'

    array: t.Array[t.Complex[JoyFeedback]]


@dataclass(kw_only=True)
class LaserEcho:
    __msg_name__ = 'sensor_msgs/msg/LaserEcho'

    echoes: t.Array[t.float32]


@dataclass(kw_only=True)
class LaserScan:
    __msg_name__ = 'sensor_msgs/msg/LaserScan'

    header: t.Complex[std_msgs.Header]
    angle_min: t.float32
    angle_max: t.float32
    angle_increment: t.float32
    time_increment: t.float32
    scan_time: t.float32
    range_min: t.float32
    range_max: t.float32
    ranges: t.Array[t.float32]
    intensities: t.Array[t.float32]


@dataclass(kw_only=True)
class MagneticField:
    __msg_name__ = 'sensor_msgs/msg/MagneticField'

    header: t.Complex[std_msgs.Header]
    magnetic_field: t.Complex[geometry_msgs.Vector3]
    magnetic_field_covariance: t.Array[t.float64, Literal[9]]


@dataclass(kw_only=True)
class MultiDOFJointState:
    __msg_name__ = 'sensor_msgs/msg/MultiDOFJointState'

    header: t.Complex[std_msgs.Header]
    joint_names: t.Array[t.string]
    transforms: t.Array[t.Complex[geometry_msgs.Transform]]
    twist: t.Array[t.Complex[geometry_msgs.Twist]]
    wrench: t.Array[t.Complex[geometry_msgs.Wrench]]


@dataclass(kw_only=True)
class MultiEchoLaserScan:
    __msg_name__ = 'sensor_msgs/msg/MultiEchoLaserScan'

    header: t.Complex[std_msgs.Header]
    angle_min: t.float32
    angle_max: t.float32
    angle_increment: t.float32
    time_increment: t.float32
    scan_time: t.float32
    range_min: t.float32
    range_max: t.float32
    ranges: t.Array[t.Complex[LaserEcho]]
    intensities: t.Array[t.Complex[LaserEcho]]


@dataclass(kw_only=True)
class NavSatStatus:
    __msg_name__ = 'sensor_msgs/msg/NavSatStatus'

    STATUS_NO_FIX: t.Constant[t.int8] = -1
    STATUS_FIX: t.Constant[t.int8] = 0
    STATUS_SBAS_FIX: t.Constant[t.int8] = 1
    STATUS_GBAS_FIX: t.Constant[t.int8] = 2

    SERVICE_GPS: t.Constant[t.uint16] = 1
    SERVICE_GLONASS: t.Constant[t.uint16] = 2
    SERVICE_COMPASS: t.Constant[t.uint16] = 4
    SERVICE_GALILEO: t.Constant[t.uint16] = 8

    status: t.int8
    service: t.uint16


@dataclass(kw_only=True)
class NavSatFix:
    __msg_name__ = 'sensor_msgs/msg/NavSatFix'

    COVARIANCE_TYPE_UNKNOWN: t.Constant[t.uint8] = 0
    COVARIANCE_TYPE_APPROXIMATED: t.Constant[t.uint8] = 1
    COVARIANCE_TYPE_DIAGONAL_KNOWN: t.Constant[t.uint8] = 2
    COVARIANCE_TYPE_KNOWN: t.Constant[t.uint8] = 3

    header: t.Complex[std_msgs.Header]
    status: t.Complex[NavSatStatus]
    latitude: t.float64
    longitude: t.float64
    altitude: t.float64
    position_covariance: t.Array[t.float64, Literal[9]]
    position_covariance_type: t.uint8


@dataclass(kw_only=True)
class PointField:
    __msg_name__ = 'sensor_msgs/msg/PointField'

    INT8: t.Constant[t.uint8] = 1
    UINT8: t.Constant[t.uint8] = 2
    INT16: t.Constant[t.uint8] = 3
    UINT16: t.Constant[t.uint8] = 4
    INT32: t.Constant[t.uint8] = 5
    UINT32: t.Constant[t.uint8] = 6
    FLOAT32: t.Constant[t.uint8] = 7
    FLOAT64: t.Constant[t.uint8] = 8

    name: t.string
    offset: t.uint32
    datatype: t.uint8
    count: t.uint32


@dataclass(kw_only=True)
class PointCloud:
    __msg_name__ = 'sensor_msgs/msg/PointCloud'

    header: t.Complex[std_msgs.Header]
    points: t.Array[t.Complex[geometry_msgs.Point32]]
    channels: t.Array[t.Complex[ChannelFloat32]]


@dataclass(kw_only=True)
class PointCloud2:
    __msg_name__ = 'sensor_msgs/msg/PointCloud2'

    header: t.Complex[std_msgs.Header]
    height: t.uint32
    width: t.uint32
    fields: t.Array[t.Complex[PointField]]
    is_bigendian: t.bool
    point_step: t.uint32
    row_step: t.uint32
    data: t.Array[t.uint8]
    is_dense: t.bool


@dataclass(kw_only=True)
class Range:
    __msg_name__ = 'sensor_msgs/msg/Range'

    ULTRASOUND: t.Constant[t.uint8] = 0
    INFRARED: t.Constant[t.uint8] = 1

    header: t.Complex[std_msgs.Header]
    radiation_type: t.uint8
    field_of_view: t.float32
    min_range: t.float32
    max_range: t.float32
    range: t.float32


@dataclass(kw_only=True)
class RelativeHumidity:
    __msg_name__ = 'sensor_msgs/msg/RelativeHumidity'

    header: t.Complex[std_msgs.Header]
    relative_humidity: t.float64
    variance: t.float64


@dataclass(kw_only=True)
class Temperature:
    __msg_name__ = 'sensor_msgs/msg/Temperature'

    header: t.Complex[std_msgs.Header]
    temperature: t.float64
    variance: t.float64


@dataclass(kw_only=True)
class TimeReference:
    __msg_name__ = 'sensor_msgs/msg/TimeReference'

    header: t.Complex[std_msgs.Header]
    time_ref: t.Complex[builtin_interfaces.Time]
    source: t.string
