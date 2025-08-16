from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from . import types as t
from .builtin_interfaces import *
from .std_msgs import *
from .geometry_msgs import *

@dataclass
class BatteryState:
    """Class for sensor_msgs/msg/BatteryState."""

    header: t.Complex(Header)
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
    cell_voltage: t.Array(t.float32)
    cell_temperature: t.Array(t.float32)
    location: t.string
    serial_number: t.string
    POWER_SUPPLY_STATUS_UNKNOWN: ClassVar[int] = 0
    POWER_SUPPLY_STATUS_CHARGING: ClassVar[int] = 1
    POWER_SUPPLY_STATUS_DISCHARGING: ClassVar[int] = 2
    POWER_SUPPLY_STATUS_NOT_CHARGING: ClassVar[int] = 3
    POWER_SUPPLY_STATUS_FULL: ClassVar[int] = 4
    POWER_SUPPLY_HEALTH_UNKNOWN: ClassVar[int] = 0
    POWER_SUPPLY_HEALTH_GOOD: ClassVar[int] = 1
    POWER_SUPPLY_HEALTH_OVERHEAT: ClassVar[int] = 2
    POWER_SUPPLY_HEALTH_DEAD: ClassVar[int] = 3
    POWER_SUPPLY_HEALTH_OVERVOLTAGE: ClassVar[int] = 4
    POWER_SUPPLY_HEALTH_UNSPEC_FAILURE: ClassVar[int] = 5
    POWER_SUPPLY_HEALTH_COLD: ClassVar[int] = 6
    POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE: ClassVar[int] = 7
    POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE: ClassVar[int] = 8
    POWER_SUPPLY_TECHNOLOGY_UNKNOWN: ClassVar[int] = 0
    POWER_SUPPLY_TECHNOLOGY_NIMH: ClassVar[int] = 1
    POWER_SUPPLY_TECHNOLOGY_LION: ClassVar[int] = 2
    POWER_SUPPLY_TECHNOLOGY_LIPO: ClassVar[int] = 3
    POWER_SUPPLY_TECHNOLOGY_LIFE: ClassVar[int] = 4
    POWER_SUPPLY_TECHNOLOGY_NICD: ClassVar[int] = 5
    POWER_SUPPLY_TECHNOLOGY_LIMN: ClassVar[int] = 6
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/BatteryState'

@dataclass
class ChannelFloat32:
    """Class for sensor_msgs/msg/ChannelFloat32."""

    name: t.string
    values: t.Array(t.float32)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/ChannelFloat32'

@dataclass
class CompressedImage:
    """Class for sensor_msgs/msg/CompressedImage."""

    header: t.Complex(Header)
    format: t.string
    data: t.Array(t.uint8)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/CompressedImage'

@dataclass
class FluidPressure:
    """Class for sensor_msgs/msg/FluidPressure."""

    header: t.Complex(Header)
    fluid_pressure: t.float64
    variance: t.float64
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/FluidPressure'

@dataclass
class Illuminance:
    """Class for sensor_msgs/msg/Illuminance."""

    header: t.Complex(Header)
    illuminance: t.float64
    variance: t.float64
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Illuminance'

@dataclass
class Image:
    """Class for sensor_msgs/msg/Image."""

    header: t.Complex(Header)
    height: t.uint32
    width: t.uint32
    encoding: t.string
    is_bigendian: t.uint8
    step: t.uint32
    data: t.Array(t.uint8)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Image'

@dataclass
class Imu:
    """Class for sensor_msgs/msg/Imu."""

    header: t.Complex(Header)
    orientation: t.Complex(Quaternion)
    orientation_covariance: t.Array(t.float64, 9)
    angular_velocity: t.Complex(Vector3)
    angular_velocity_covariance: t.Array(t.float64, 9)
    linear_acceleration: t.Complex(Vector3)
    linear_acceleration_covariance: t.Array(t.float64, 9)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Imu'

@dataclass
class JointState:
    """Class for sensor_msgs/msg/JointState."""

    header: t.Complex(Header)
    name: t.Array(t.string)
    position: t.Array(t.float64)
    velocity: t.Array(t.float64)
    effort: t.Array(t.float64)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/JointState'

@dataclass
class Joy:
    """Class for sensor_msgs/msg/Joy."""

    header: t.Complex(Header)
    axes: t.Array(t.float32)
    buttons: t.Array(t.int32)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Joy'

@dataclass
class JoyFeedback:
    """Class for sensor_msgs/msg/JoyFeedback."""

    type: t.uint8
    id: t.uint8
    intensity: t.float32
    TYPE_LED: ClassVar[int] = 0
    TYPE_RUMBLE: ClassVar[int] = 1
    TYPE_BUZZER: ClassVar[int] = 2
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/JoyFeedback'

@dataclass
class LaserEcho:
    """Class for sensor_msgs/msg/LaserEcho."""

    echoes: t.Array(t.float32)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/LaserEcho'

@dataclass
class LaserScan:
    """Class for sensor_msgs/msg/LaserScan."""

    header: t.Complex(Header)
    angle_min: t.float32
    angle_max: t.float32
    angle_increment: t.float32
    time_increment: t.float32
    scan_time: t.float32
    range_min: t.float32
    range_max: t.float32
    ranges: t.Array(t.float32)
    intensities: t.Array(t.float32)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/LaserScan'

@dataclass
class MagneticField:
    """Class for sensor_msgs/msg/MagneticField."""

    header: t.Complex(Header)
    magnetic_field: t.Complex(Vector3)
    magnetic_field_covariance: t.Array(t.float64, 9)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/MagneticField'

@dataclass
class MultiDOFJointState:
    """Class for sensor_msgs/msg/MultiDOFJointState."""

    header: t.Complex(Header)
    joint_names: t.Array(t.string)
    transforms: t.Array(t.Complex(Transform))
    twist: t.Array(t.Complex(Twist))
    wrench: t.Array(t.Complex(Wrench))
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/MultiDOFJointState'

@dataclass
class NavSatStatus:
    """Class for sensor_msgs/msg/NavSatStatus."""

    status: t.int8
    service: t.uint16
    STATUS_NO_FIX: ClassVar[int] = -1
    STATUS_FIX: ClassVar[int] = 0
    STATUS_SBAS_FIX: ClassVar[int] = 1
    STATUS_GBAS_FIX: ClassVar[int] = 2
    SERVICE_GPS: ClassVar[int] = 1
    SERVICE_GLONASS: ClassVar[int] = 2
    SERVICE_COMPASS: ClassVar[int] = 4
    SERVICE_GALILEO: ClassVar[int] = 8
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/NavSatStatus'

@dataclass
class PointField:
    """Class for sensor_msgs/msg/PointField."""

    name: t.string
    offset: t.uint32
    datatype: t.uint8
    count: t.uint32
    INT8: ClassVar[int] = 1
    UINT8: ClassVar[int] = 2
    INT16: ClassVar[int] = 3
    UINT16: ClassVar[int] = 4
    INT32: ClassVar[int] = 5
    UINT32: ClassVar[int] = 6
    FLOAT32: ClassVar[int] = 7
    FLOAT64: ClassVar[int] = 8
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/PointField'

@dataclass
class Range:
    """Class for sensor_msgs/msg/Range."""

    header: t.Complex(Header)
    radiation_type: t.uint8
    field_of_view: t.float32
    min_range: t.float32
    max_range: t.float32
    range: t.float32
    ULTRASOUND: ClassVar[int] = 0
    INFRARED: ClassVar[int] = 1
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Range'

@dataclass
class RegionOfInterest:
    """Class for sensor_msgs/msg/RegionOfInterest."""

    x_offset: t.uint32
    y_offset: t.uint32
    height: t.uint32
    width: t.uint32
    do_rectify: t.bool
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/RegionOfInterest'

@dataclass
class RelativeHumidity:
    """Class for sensor_msgs/msg/RelativeHumidity."""

    header: t.Complex(Header)
    relative_humidity: t.float64
    variance: t.float64
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/RelativeHumidity'

@dataclass
class Temperature:
    """Class for sensor_msgs/msg/Temperature."""

    header: t.Complex(Header)
    temperature: t.float64
    variance: t.float64
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/Temperature'

@dataclass
class TimeReference:
    """Class for sensor_msgs/msg/TimeReference."""

    header: t.Complex(Header)
    time_ref: t.Complex(Time)
    source: t.string
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/TimeReference'

@dataclass
class PointCloud:
    """Class for sensor_msgs/msg/PointCloud."""

    header: t.Complex(Header)
    points: t.Array(t.Complex(Point32))
    channels: t.Array(t.Complex(ChannelFloat32))
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/PointCloud'

@dataclass
class JoyFeedbackArray:
    """Class for sensor_msgs/msg/JoyFeedbackArray."""

    array: t.Array(t.Complex(JoyFeedback))
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/JoyFeedbackArray'

@dataclass
class MultiEchoLaserScan:
    """Class for sensor_msgs/msg/MultiEchoLaserScan."""

    header: t.Complex(Header)
    angle_min: t.float32
    angle_max: t.float32
    angle_increment: t.float32
    time_increment: t.float32
    scan_time: t.float32
    range_min: t.float32
    range_max: t.float32
    ranges: t.Array(t.Complex(LaserEcho))
    intensities: t.Array(t.Complex(LaserEcho))
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/MultiEchoLaserScan'

@dataclass
class NavSatFix:
    """Class for sensor_msgs/msg/NavSatFix."""

    header: t.Complex(Header)
    status: t.Complex(NavSatStatus)
    latitude: t.float64
    longitude: t.float64
    altitude: t.float64
    position_covariance: t.Array(t.float64, 9)
    position_covariance_type: t.uint8
    COVARIANCE_TYPE_UNKNOWN: ClassVar[int] = 0
    COVARIANCE_TYPE_APPROXIMATED: ClassVar[int] = 1
    COVARIANCE_TYPE_DIAGONAL_KNOWN: ClassVar[int] = 2
    COVARIANCE_TYPE_KNOWN: ClassVar[int] = 3
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/NavSatFix'

@dataclass
class PointCloud2:
    """Class for sensor_msgs/msg/PointCloud2."""

    header: t.Complex(Header)
    height: t.uint32
    width: t.uint32
    fields: t.Array(t.Complex(PointField))
    is_bigendian: t.bool
    point_step: t.uint32
    row_step: t.uint32
    data: t.Array(t.uint8)
    is_dense: t.bool
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/PointCloud2'

@dataclass
class CameraInfo:
    """Class for sensor_msgs/msg/CameraInfo."""

    header: t.Complex(Header)
    height: t.uint32
    width: t.uint32
    distortion_model: t.string
    d: t.Array(t.float64)
    k: t.Array(t.float64, 9)
    r: t.Array(t.float64, 9)
    p: t.Array(t.float64, 12)
    binning_x: t.uint32
    binning_y: t.uint32
    roi: t.Complex(RegionOfInterest)
    __msgtype__: ClassVar[str] = 'sensor_msgs/msg/CameraInfo'
