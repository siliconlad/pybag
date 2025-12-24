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


def test_sensor_msgs_battery_state():
    msgtype = "sensor_msgs/BatteryState"
    schema = dedent("""
        # Constants are chosen to match the enums in the linux kernel
        # defined in include/linux/power_supply.h as of version 3.7
        # The one difference is for style reasons the constants are
        # all uppercase not mixed case.

        # Power supply status constants
        uint8 POWER_SUPPLY_STATUS_UNKNOWN = 0
        uint8 POWER_SUPPLY_STATUS_CHARGING = 1
        uint8 POWER_SUPPLY_STATUS_DISCHARGING = 2
        uint8 POWER_SUPPLY_STATUS_NOT_CHARGING = 3
        uint8 POWER_SUPPLY_STATUS_FULL = 4

        # Power supply health constants
        uint8 POWER_SUPPLY_HEALTH_UNKNOWN = 0
        uint8 POWER_SUPPLY_HEALTH_GOOD = 1
        uint8 POWER_SUPPLY_HEALTH_OVERHEAT = 2
        uint8 POWER_SUPPLY_HEALTH_DEAD = 3
        uint8 POWER_SUPPLY_HEALTH_OVERVOLTAGE = 4
        uint8 POWER_SUPPLY_HEALTH_UNSPEC_FAILURE = 5
        uint8 POWER_SUPPLY_HEALTH_COLD = 6
        uint8 POWER_SUPPLY_HEALTH_WATCHDOG_TIMER_EXPIRE = 7
        uint8 POWER_SUPPLY_HEALTH_SAFETY_TIMER_EXPIRE = 8

        # Power supply technology (chemistry) constants
        uint8 POWER_SUPPLY_TECHNOLOGY_UNKNOWN = 0
        uint8 POWER_SUPPLY_TECHNOLOGY_NIMH = 1
        uint8 POWER_SUPPLY_TECHNOLOGY_LION = 2
        uint8 POWER_SUPPLY_TECHNOLOGY_LIPO = 3
        uint8 POWER_SUPPLY_TECHNOLOGY_LIFE = 4
        uint8 POWER_SUPPLY_TECHNOLOGY_NICD = 5
        uint8 POWER_SUPPLY_TECHNOLOGY_LIMN = 6

        std_msgs/Header header
        float32 voltage          # Voltage in Volts (Mandatory)
        float32 temperature      # Temperature in Degrees Celsius (If unmeasured NaN)
        float32 current          # Negative when discharging (A)  (If unmeasured NaN)
        float32 charge           # Current charge in Ah  (If unmeasured NaN)
        float32 capacity         # Capacity in Ah (last full capacity)  (If unmeasured NaN)
        float32 design_capacity  # Capacity in Ah (design capacity)  (If unmeasured NaN)
        float32 percentage       # Charge percentage on 0 to 1 range  (If unmeasured NaN)
        uint8 power_supply_status     # The charging status as reported. Values defined above
        uint8 power_supply_health     # The battery health metric. Values defined above
        uint8 power_supply_technology # The battery chemistry. Values defined above
        bool present                  # True if the battery is present

        float32[] cell_voltage        # An array of individual cell voltages for each cell in the pack
                                      # If individual voltages unknown but number of cells known set each to NaN
        float32[] cell_temperature    # An array of individual cell temperatures for each cell in the pack
                                      # If individual temperatures unknown but number of cells known set each to NaN

        string location               # The location into which the battery is inserted. (slot number or plug)
        string serial_number          # The best approximation of the battery serial number
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "battery"
            },
            "voltage": 12.5,
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
            "cell_voltage": [4.25, 4.125, 4.5],
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
    assert messages[0].data.voltage == 12.5
    assert messages[0].data.temperature == 25.0
    assert messages[0].data.current == -2.5
    assert messages[0].data.percentage == 90.0
    assert messages[0].data.power_supply_status == 2
    assert messages[0].data.power_supply_health == 1
    assert messages[0].data.power_supply_technology == 2
    assert messages[0].data.present is True
    assert len(messages[0].data.cell_voltage) == 3
    assert messages[0].data.cell_voltage[0] == 4.25
    assert messages[0].data.cell_voltage[1] == 4.125
    assert messages[0].data.cell_voltage[2] == 4.5
    assert len(messages[0].data.cell_temperature) == 3
    assert messages[0].data.cell_temperature[0] == 24.5
    assert messages[0].data.cell_temperature[1] == 25.0
    assert messages[0].data.location == "main_battery"
    assert messages[0].data.serial_number == "BAT123456"


def test_sensor_msgs_camera_info():
    msgtype = "sensor_msgs/CameraInfo"
    schema = dedent("""
        # This message defines meta information for a camera. It should be in a
        # camera namespace on topic "camera_info" and accompanied by up to five
        # image topics named:
        #
        #   image_raw - raw data from the camera driver, possibly Bayer encoded
        #   image            - monochrome, distorted
        #   image_color      - color, distorted
        #   image_rect       - monochrome, rectified
        #   image_rect_color - color, rectified
        #
        # The image_pipeline contains packages (image_proc, stereo_image_proc)
        # for producing the four processed image topics from image_raw and
        # camera_info. The meaning of the camera parameters are described in
        # detail at http://www.ros.org/wiki/image_pipeline/CameraInfo.
        #
        # The image_geometry package provides a user-friendly interface to
        # common operations using this meta information. If you want to, e.g.,
        # project a 3d point into image coordinates, we strongly recommend
        # using image_geometry.
        #
        # If the camera is uncalibrated, the matrices D, K, R, P should be left
        # zeroed out. In particular, clients may assume that K[0] == 0.0
        # indicates an uncalibrated camera.

        std_msgs/Header header    # Header timestamp should be acquisition time of image
                                  # Header frame_id should be optical frame of camera
                                  # origin of frame should be optical center of camera
                                  # +x should point to the right in the image
                                  # +y should point down in the image
                                  # +z should point into to plane of the image

        uint32 height             # image height, that is, number of rows
        uint32 width              # image width, that is, number of columns

        # The distortion model used. Supported models are listed in
        # sensor_msgs/distortion_models.hpp. For most cameras, "plumb_bob" - a
        # simple model of radial and tangential distortion - is sufficient.
        string distortion_model   # distortion model used

        # The distortion parameters, size depending on the distortion model.
        # For "plumb_bob", the 5 parameters are: (k1, k2, t1, t2, k3).
        float64[] d               # distortion parameters

        # Intrinsic camera matrix for the raw (distorted) images.
        #     [fx  0 cx]
        # K = [ 0 fy cy]
        #     [ 0  0  1]
        # Projects 3D points in the camera coordinate frame to 2D pixel
        # coordinates using the focal lengths (fx, fy) and principal point
        # (cx, cy).
        float64[9] k              # 3x3 row-major matrix

        # Rectification matrix (stereo cameras only)
        # A rotation matrix aligning the camera coordinate system to the ideal
        # stereo image plane so that epipolar lines in both stereo images are
        # parallel.
        float64[9] r              # 3x3 row-major matrix

        # Projection/camera matrix
        #     [fx'  0  cx' Tx]
        # P = [ 0  fy' cy' Ty]
        #     [ 0   0   1   0]
        # By convention, this matrix specifies the intrinsic (camera) matrix
        #  of the processed (rectified) image. That is, the left 3x3 portion
        #  is the normal camera intrinsic matrix for the rectified image.
        # It projects 3D points in the camera coordinate frame to 2D pixel
        #  coordinates using the focal lengths (fx', fy') and principal point
        #  (cx', cy') - these may differ from the values in K.
        # For monocular cameras, Tx = Ty = 0. Normally, monocular cameras will
        #  also have R = the identity and P[1:3,1:3] = K.
        # For a stereo pair, the fourth column [Tx Ty 0]' is related to the
        #  position of the optical center of the second camera in the first
        #  camera's frame. We assume Tz = 0 so both cameras are in the same
        #  stereo image plane. The first camera always has Tx = Ty = 0. For
        #  the second camera, Tx = -fx' * B, where B is the baseline between
        #  the cameras.
        # Given a 3D point [X Y Z]', the projection (x, y) of the point onto
        #  the rectified image is given by:
        #  [u v w]' = P * [X Y Z 1]'
        #         x = u / w
        #         y = v / w
        #  This holds for both images of a stereo pair.
        float64[12] p             # 3x4 row-major matrix

        uint32 binning_x          # Binning refers to any camera setting which combines rectangular
                                  #  neighborhoods of pixels into larger "super-pixels." It reduces the
                                  #  resolution of the output image to
                                  #  (width / binning_x) x (height / binning_y).
                                  # The default values binning_x = binning_y = 0 is interpreted as no
                                  #  binning.
        uint32 binning_y
        RegionOfInterest roi      # Defines the ROI that was used to create this image
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
        ================================================================================
        MSG: sensor_msgs/RegionOfInterest
        # This message is used to specify a region of interest within an image.
        #
        # When used to specify the ROI setting of the camera when the image was
        # taken, the height and width fields should either both be zero if the
        # full resolution was captured, or both be non-zero if a subwindow was
        # captured. A height or width of zero (but not both) is not allowed.
        #
        # In general, setting x_offset = y_offset = 0 and height = width = 0
        # is interpreted as "full resolution."

        uint32 x_offset  # Leftmost pixel of the ROI
                         # (0 if the ROI includes the left edge of the image)
        uint32 y_offset  # Topmost pixel of the ROI
                         # (0 if the ROI includes the top edge of the image)
        uint32 height    # Height of ROI
        uint32 width     # Width of ROI

        # True if a distinct rectified ROI should be calculated from the "raw"
        # ROI in this message. Typically this should be False if the full image
        # is captured (ROI not used), and True if a subwindow is captured (ROI
        # used).
        bool do_rectify
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
    assert messages[0].data.d == [-0.1, 0.05, 0.0, 0.0, 0.0]
    assert messages[0].data.k == [500.0, 0.0, 320.0, 0.0, 500.0, 240.0, 0.0, 0.0, 1.0]
    assert messages[0].data.r == [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]
    assert messages[0].data.p == [500.0, 0.0, 320.0, 0.0, 0.0, 500.0, 240.0, 0.0, 0.0, 0.0, 1.0, 0.0]
    assert messages[0].data.binning_x == 1
    assert messages[0].data.binning_y == 1
    assert messages[0].data.roi.x_offset == 0
    assert messages[0].data.roi.y_offset == 0
    assert messages[0].data.roi.height == 480
    assert messages[0].data.roi.do_rectify == False


def test_sensor_msgs_channel_float32():
    # values -> valuess because of mcap_ros2 bug
    msgtype = "sensor_msgs/ChannelFloat32"
    schema = dedent("""
        # Common PointField names are x, y, z, intensity, rgb, rgba
        string name
        float32[] valuess
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "name": "intensity",
            "valuess": [1.0, 2.5, 3.5]
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
    assert messages[0].data.valuess == [1.0, 2.5, 3.5]


def test_sensor_msgs_compressed_image():
    msgtype = "sensor_msgs/CompressedImage"
    schema = dedent("""
        # This message contains a compressed image.

        std_msgs/Header header # Header timestamp should be acquisition time of image
                               # Header frame_id should be optical frame of camera
                               # origin of frame should be optical center of cameara
                               # +x should point to the right in the image
                               # +y should point down in the image
                               # +z should point into to plane of the image

        string format                # Specifies the format of the data
                                     #   Acceptable values:
                                     #     jpeg, png, tiff

        uint8[] data                 # Compressed image buffer
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "camera"
            },
            "format": "jpeg",
            "data": [255, 216, 255, 224]  # Sample JPEG header bytes
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
    assert messages[0].data.data == b'\xff\xd8\xff\xe0'


def test_sensor_msgs_fluid_pressure():
    msgtype = "sensor_msgs/FluidPressure"
    schema = dedent("""
        # Single pressure reading.  This message is appropriate for measuring the
        # pressure inside of a fluid (air, water, etc).  This also includes
        # atmospheric or barometric pressure.
        #
        # This message is not appropriate for force/pressure contact sensors.

        std_msgs/Header header # timestamp of the measurement
                               # frame_id is the location of the pressure sensor

        float64 fluid_pressure       # Absolute pressure reading in Pascals.

        float64 variance             # 0 is interpreted as variance unknown
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
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
        # Single photometric illuminance measurement.  Light should be assumed to be
        # measured along the sensor's x-axis (the area of detection is the y-z plane).
        # The illuminance should have a 0 or positive value and be received with
        # the sensor's +X axis pointing toward the light source.
        #
        # Photometric illuminance is the measure of the human eye's sensitivity of the
        # intensity of light encountering or passing through a surface.
        #
        # All other Photometric and Radiometric measurements should not use this message.
        # This message cannot represent:
        #  - Luminous intensity (candela/light source output)
        #  - Luminance (nits/light output per area)
        #  - Irradiance (watt/area), etc.

        std_msgs/Header header # timestamp is the time the illuminance was measured
                               # frame_id is the location and direction of the reading

        float64 illuminance          # Measurement of the Photometric Illuminance in Lux.

        float64 variance             # 0 is interpreted as variance unknown
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
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
    assert messages[0].data.illuminance == 450.5
    assert messages[0].data.variance == 2.1


def test_sensor_msgs_image():
    msgtype = "sensor_msgs/Image"
    schema = dedent("""
        # This message contains an uncompressed image
        # (0, 0) is at top-left corner of image

        std_msgs/Header header # Header timestamp should be acquisition time of image
                               # Header frame_id should be optical frame of camera
                               # origin of frame should be optical center of cameara
                               # +x should point to the right in the image
                               # +y should point down in the image
                               # +z should point into to plane of the image
                               # If the frame_id here and the frame_id of the CameraInfo
                               # message associated with the image conflict
                               # the behavior is undefined

        uint32 height                # image height, that is, number of rows
        uint32 width                 # image width, that is, number of columns

        # The legal values for encoding are in file src/image_encodings.cpp
        # If you want to standardize a new string format, join
        # ros-users@lists.ros.org and send an email proposing a new encoding.

        string encoding       # Encoding of pixels -- channel meaning, ordering, size
                              # taken from the list of strings in include/sensor_msgs/image_encodings.hpp

        uint8 is_bigendian    # is this data bigendian?
        uint32 step           # Full row length in bytes
        uint8[] data          # actual matrix data, size is (step * rows)
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
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
            "data": [255, 0, 0]  # Sample red pixel
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
    assert messages[0].data.data == b'\xff\x00\x00'


def test_sensor_msgs_imu():
    msgtype = "sensor_msgs/Imu"
    schema = dedent("""
        # This is a message to hold data from an IMU (Inertial Measurement Unit)
        #
        # Accelerations should be in m/s^2 (not in g's), and rotational velocity should be in rad/sec
        #
        # If the covariance of the measurement is known, it should be filled in (if all you know is the
        # variance of each measurement, e.g. from the datasheet, just put those along the diagonal)
        # A covariance matrix of all zeros will be interpreted as "covariance unknown", and to use the
        # data a covariance will have to be assumed or gotten from some other source
        #
        # If you have no estimate for one of the data elements (e.g. your IMU doesn't produce an
        # orientation estimate), please set element 0 of the associated covariance matrix to -1
        # If you are interpreting this message, please check for a value of -1 in the first element of each
        # covariance matrix, and disregard the associated estimate.

        std_msgs/Header header

        geometry_msgs/Quaternion orientation
        float64[9] orientation_covariance # Row major about x, y, z axes

        geometry_msgs/Vector3 angular_velocity
        float64[9] angular_velocity_covariance # Row major about x, y, z axes

        geometry_msgs/Vector3 linear_acceleration
        float64[9] linear_acceleration_covariance # Row major x, y z
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        #
        # Please note that whether the quaternion is normalized is neither assumed nor enforced.
        # See https://www.euclideanspace.com/maths/geometry/rotations/conversions/eulerToQuaternion/index.htm
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.
        # It is only meant to represent a direction. Therefore, it does not
        # make sense to apply a translation to it (e.g., when applying a
        # generic rigid transformation to a Vector3, tf2 will only apply the
        # rotation). If you want your data to be translatable too, use the
        # geometry_msgs/Point message instead.

        float64 x
        float64 y
        float64 z
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
    assert messages[0].data.orientation.y == 0.0
    assert messages[0].data.orientation.z == 0.0
    assert messages[0].data.orientation.w == 1.0
    assert messages[0].data.orientation_covariance == [0.01] + [0.0] * 8
    assert messages[0].data.angular_velocity.x == 0.1
    assert messages[0].data.angular_velocity.y == 0.05
    assert messages[0].data.angular_velocity.z == 0.02
    assert messages[0].data.angular_velocity_covariance == [0.001] + [0.0] * 8
    assert messages[0].data.linear_acceleration.x == 0.0
    assert messages[0].data.linear_acceleration.y == 0.0
    assert messages[0].data.linear_acceleration.z == 9.81
    assert messages[0].data.linear_acceleration_covariance == [0.1] + [0.0] * 8


def test_sensor_msgs_joint_state():
    msgtype = "sensor_msgs/JointState"
    schema = dedent("""
        # This is a message that holds data to describe the state of a set of torque controlled joints.
        #
        # The state of each joint (revolute or prismatic) is defined by:
        #  * the position of the joint (rad or m),
        #  * the velocity of the joint (rad/s or m/s) and
        #  * the effort that is applied in the joint (Nm or N).
        #
        # Each joint is uniquely identified by its name
        # The header specifies the time at which the joint states were recorded. All the joint states
        # in one message have to be recorded at the same time.
        #
        # This message consists of a multiple arrays, one for each part of the joint state.
        # The goal is to make each of the fields optional. When e.g. your joints have no
        # effort associated with them, you can leave the effort array empty.
        #
        # All arrays in this message should have the same size, or be empty.
        # This is the only way to uniquely associate the joint name with the correct
        # states.

        std_msgs/Header header

        string[] name
        float64[] position
        float64[] velocity
        float64[] effort
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
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
    assert messages[0].data.name == ["joint_1", "joint_2", "joint_3"]
    assert messages[0].data.position == [1.57, 0.0, -1.57]
    assert messages[0].data.velocity == [0.1, 0.0, -0.1]
    assert messages[0].data.effort == [10.5, 0.0, -5.2]


def test_sensor_msgs_joy():
    msgtype = "sensor_msgs/Joy"
    schema = dedent("""
        # Reports the state of a joystick's axes and buttons.

        # The timestamp is the time at which data is received from the joystick.
        std_msgs/Header header

        # The axes measurements from a joystick.
        float32[] axes

        # The buttons measurements from a joystick.
        int32[] buttons
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "joy"
            },
            "axes": [0.5, -0.75, 0.0, 1.0],
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
    assert messages[0].data.axes == [0.5, -0.75, 0.0, 1.0]
    assert messages[0].data.buttons == [1, 0, 0, 1, 0, 0, 0, 0]


def test_sensor_msgs_joy_feedback():
    msgtype = "sensor_msgs/JoyFeedback"
    schema = dedent("""
        # Represents one piece of feedback to send to a joystick/gamepad
        uint8 TYPE_LED    = 0
        uint8 TYPE_RUMBLE = 1
        uint8 TYPE_BUZZER = 2

        uint8 type      # The type of feedback to send

        uint8 id        # Device-specific feedback identification

        float32 intensity    # Feedback strength for LED/rumble/buzzer (range 0.0-1.0)
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "type": 1,  # TYPE_RUMBLE
            "id": 0,
            "intensity": 0.75
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
    assert messages[0].data.intensity == 0.75


def test_sensor_msgs_joy_feedback_array():
    msgtype = "sensor_msgs/JoyFeedbackArray"
    schema = dedent("""
        # Array of feedback commands to send to a joystick/gamepad
        JoyFeedback[] array
        ================================================================================
        MSG: sensor_msgs/JoyFeedback
        # Represents one piece of feedback to send to a joystick/gamepad
        uint8 TYPE_LED    = 0
        uint8 TYPE_RUMBLE = 1
        uint8 TYPE_BUZZER = 2

        uint8 type      # The type of feedback to send

        uint8 id        # Device-specific feedback identification

        float32 intensity    # Feedback strength for LED/rumble/buzzer (range 0.0-1.0)
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
    assert messages[0].data.array[0].intensity == 1.0
    assert messages[0].data.array[1].type == 1
    assert messages[0].data.array[1].id == 1
    assert messages[0].data.array[1].intensity == 0.5


def test_sensor_msgs_laser_echo():
    msgtype = "sensor_msgs/LaserEcho"
    schema = dedent("""
        # This message is a submessage of MultiEchoLaserScan
        # and holds multiple return ranges for a single direction

        float32[] echoes  # Multiple return values for a single beam direction [m]
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"echoes": [1.25, 2.5, 3.5]}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.echoes == [1.25, 2.5, 3.5]


def test_sensor_msgs_laserscan():
    msgtype = "sensor_msgs/LaserScan"
    schema = dedent("""
        # Single scan from a planar laser range-finder
        #
        # If you have another ranging device with different behavior (e.g. a sonar
        # array), please find or create a different message, since applications
        # will make fairly laser-specific assumptions about this data

        std_msgs/Header header # timestamp in the header is the acquisition time of
                               # the first ray in the scan.
                               #
                               # in frame frame_id, angles are measured around
                               # the positive Z axis (counterclockwise, if Z is up)
                               # with zero angle being forward along the x axis

        float32 angle_min            # start angle of the scan [rad]
        float32 angle_max            # end angle of the scan [rad]
        float32 angle_increment      # angular distance between measurements [rad]

        float32 time_increment       # time between measurements [seconds] - if your scanner
                                     # is moving, this will be used in interpolating position
                                     # of 3d points
        float32 scan_time            # time between scans [seconds]

        float32 range_min            # minimum range value [m]
        float32 range_max            # maximum range value [m]

        float32[] ranges             # range data [m]
                                     # (Note: values < range_min or > range_max should be discarded)
        float32[] intensities        # intensity data [device-specific units].  If your
                                     # device does not provide intensities, please leave
                                     # the array empty.
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "laser"
            },
            "angle_min": -1.625,
            "angle_max": 1.625,
            "angle_increment": 0.125,
            "time_increment": 0.125,
            "scan_time": 0.5,
            "range_min": 0.5,
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "laser"
    assert messages[0].data.angle_min == -1.625
    assert messages[0].data.angle_max == 1.625
    assert messages[0].data.angle_increment == 0.125
    assert messages[0].data.time_increment == 0.125
    assert messages[0].data.scan_time == 0.5
    assert messages[0].data.range_min == 0.5
    assert messages[0].data.range_max == 10.0
    assert messages[0].data.ranges == [1.5, 2.0, 2.5, 3.0]
    assert messages[0].data.intensities == [100.0, 120.0, 110.0, 90.0]


def test_sensor_msgs_magnetic_field():
    msgtype = "sensor_msgs/MagneticField"
    schema = dedent("""
        # Measurement of the Magnetic Field vector at a specific location.
        #
        # If the covariance of the measurement is known, it should be filled in.
        # If all you know is the variance of each measurement, e.g. from the datasheet,
        # just put those along the diagonal.
        # A covariance matrix of all zeros will be interpreted as "covariance unknown",
        # and to use the data a covariance will have to be assumed or gotten from some
        # other source.

        std_msgs/Header header               # timestamp is the time the
                                             # field was measured
                                             # frame_id is the location and orientation
                                             # of the field measurement

        geometry_msgs/Vector3 magnetic_field # x, y, and z components of the
                                             # field vector in Tesla
                                             # If your sensor does not output 3 axes,
                                             # put NaNs in the components not reported.

        float64[9] magnetic_field_covariance       # Row major about x, y, z axes
                                                   # 0 is interpreted as variance unknown
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.
        # It is only meant to represent a direction. Therefore, it does not
        # make sense to apply a translation to it (e.g., when applying a
        # generic rigid transformation to a Vector3, tf2 will only apply the
        # rotation). If you want your data to be translatable too, use the
        # geometry_msgs/Point message instead.

        float64 x
        float64 y
        float64 z
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "magnetometer"
    assert messages[0].data.magnetic_field.x == 2.1e-5
    assert messages[0].data.magnetic_field.y == 0.5e-5
    assert messages[0].data.magnetic_field.z == -4.2e-5
    assert messages[0].data.magnetic_field_covariance == [1e-12] + [0.0] * 8


def test_sensor_msgs_multi_dof_joint_state():
    msgtype = "sensor_msgs/MultiDOFJointState"
    schema = dedent("""
        # Representation of state for joints with multiple degrees of freedom,
        # following the structure of JointState.
        #
        # It is assumed that a joint in a system corresponds to a transform that gets applied
        # along the kinematic chain. For example, a planar joint (as in URDF) is 3DOF (x, y, yaw)
        # and those 3DOF can be expressed as a transformation matrix, and that transformation
        # matrix can be converted back to (x, y, yaw)
        #
        # Each joint is uniquely identified by its name
        # The header specifies the time at which the joint states were recorded. All the joint states
        # in one message have to be recorded at the same time.
        #
        # This message consists of a multiple arrays, one for each part of the joint state.
        # The goal is to make each of the fields optional. When e.g. your joints have no
        # velocity associated with them, you can leave the velocity array empty.
        #
        # All arrays in this message should have the same size, or be empty.
        # This is the only way to uniquely associate the joint name with the correct
        # states.

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
        # This represents the transform between two coordinate frames in free space.

        Vector3 translation
        Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.
        Vector3  linear
        Vector3  angular
        ================================================================================
        MSG: geometry_msgs/Wrench
        # This represents force in free space, separated into
        # its linear and angular parts.
        Vector3  force
        Vector3  torque
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
                }
            ],
            "twist": [
                {
                    "linear": {"x": 0.1, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.1}
                }
            ],
            "wrench": [
                {
                    "force": {"x": 10.0, "y": 0.0, "z": 0.0},
                    "torque": {"x": 0.0, "y": 0.0, "z": 1.0}
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "robot"
    assert messages[0].data.joint_names == ["base_joint", "arm_joint"]
    assert messages[0].data.transforms[0].translation.x == 1.0
    assert messages[0].data.transforms[0].translation.y == 0.0
    assert messages[0].data.transforms[0].translation.z == 0.0
    assert messages[0].data.transforms[0].rotation.x == 0.0
    assert messages[0].data.transforms[0].rotation.y == 0.0
    assert messages[0].data.transforms[0].rotation.z == 0.0
    assert messages[0].data.transforms[0].rotation.w == 1.0
    assert messages[0].data.twist[0].linear.x == 0.1
    assert messages[0].data.twist[0].linear.y == 0.0
    assert messages[0].data.twist[0].linear.z == 0.0
    assert messages[0].data.twist[0].angular.x == 0.0
    assert messages[0].data.twist[0].angular.y == 0.0
    assert messages[0].data.twist[0].angular.z == 0.1
    assert messages[0].data.wrench[0].force.x == 10.0
    assert messages[0].data.wrench[0].force.y == 0.0
    assert messages[0].data.wrench[0].force.z == 0.0
    assert messages[0].data.wrench[0].torque.x == 0.0
    assert messages[0].data.wrench[0].torque.y == 0.0
    assert messages[0].data.wrench[0].torque.z == 1.0


def test_sensor_msgs_multi_echo_laser_scan():
    msgtype = "sensor_msgs/MultiEchoLaserScan"
    schema = dedent("""
        # Single scan from a multi-echo planar laser range-finder
        #
        # If you have another ranging device with different behavior (e.g. a sonar
        # array), please find or create a different message, since applications
        # will make fairly laser-specific assumptions about this data

        std_msgs/Header header # timestamp in the header is the acquisition time of
                               # the first ray in the scan.
                               #
                               # in frame frame_id, angles are measured around
                               # the positive Z axis (counterclockwise, if Z is up)
                               # with zero angle being forward along the x axis

        float32 angle_min            # start angle of the scan [rad]
        float32 angle_max            # end angle of the scan [rad]
        float32 angle_increment      # angular distance between measurements [rad]

        float32 time_increment       # time between measurements [seconds] - if your scanner
                                     # is moving, this will be used in interpolating position
                                     # of 3d points
        float32 scan_time            # time between scans [seconds]

        float32 range_min            # minimum range value [m]
        float32 range_max            # maximum range value [m]

        LaserEcho[] ranges             # range data [m]
                                     # (Note: values < range_min or > range_max should be discarded)
        LaserEcho[] intensities        # intensity data [device-specific units].  If your
                                     # device does not provide intensities, please leave
                                     # the array empty.
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/LaserEcho
        # This message is a submessage of MultiEchoLaserScan and is not intended
        # to be used separately.

        float32[] echoes   # Multiple values of ranges or intensities.
                           # Each array represents data from the same angle increment.
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "laser"
            },
            "angle_min": -1.625,
            "angle_max": 1.625,
            "angle_increment": 0.125,
            "time_increment": 0.125,
            "scan_time": 0.5,
            "range_min": 0.5,
            "range_max": 10.0,
            "ranges": [
                {"echoes": [1.5, 1.0]},
            ],
            "intensities": [
                {"echoes": [100.0, 105.0]},
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "laser"
    assert messages[0].data.angle_min == -1.625
    assert messages[0].data.angle_max == 1.625
    assert messages[0].data.angle_increment == 0.125
    assert messages[0].data.time_increment == 0.125
    assert messages[0].data.scan_time == 0.5
    assert messages[0].data.range_min == 0.5
    assert messages[0].data.range_max == 10.0
    assert len(messages[0].data.ranges) == 1
    assert messages[0].data.ranges[0].echoes == [1.5, 1.0]
    assert len(messages[0].data.intensities) == 1
    assert messages[0].data.intensities[0].echoes == [100.0, 105.0]


def test_sensor_msgs_navsatfix():
    msgtype = "sensor_msgs/NavSatFix"
    schema = dedent("""
        # Navigation Satellite fix for any Global Navigation Satellite System
        # Specified using the WGS 84 reference ellipsoid

        # header.stamp specifies the ROS time for this measurement
        # header.frame_id is the frame of reference reported by the satellite receiver

        std_msgs/Header header

        # Satellite fix status information
        sensor_msgs/NavSatStatus status

        # Latitude [degrees]. Positive is north of equator; negative is south
        float64 latitude

        # Longitude [degrees]. Positive is east of prime meridian; negative is west
        float64 longitude

        # Altitude [m]. Positive is above the WGS 84 ellipsoid
        float64 altitude

        # Position covariance [m^2] defined relative to a tangential plane through the reported position
        # Components are East, North, and Up (ENU), in row-major order
        float64[9] position_covariance

        # Covariance type constants
        uint8 COVARIANCE_TYPE_UNKNOWN=0
        uint8 COVARIANCE_TYPE_APPROXIMATED=1
        uint8 COVARIANCE_TYPE_DIAGONAL_KNOWN=2
        uint8 COVARIANCE_TYPE_KNOWN=3

        # Covariance type specification
        uint8 position_covariance_type
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: sensor_msgs/NavSatStatus
        # Navigation Satellite fix status for any Global Navigation Satellite System.
        #
        # Whether to output an augmented fix is determined by both the fix
        # type and the last time differential corrections were received. A
        # fix is valid when status >= STATUS_FIX.

        int8 STATUS_NO_FIX = -1        # unable to fix position
        int8 STATUS_FIX = 0            # unaugmented fix
        int8 STATUS_SBAS_FIX = 1       # with satellite-based augmentation
        int8 STATUS_GBAS_FIX = 2       # with ground-based augmentation

        uint16 SERVICE_GPS = 1
        uint16 SERVICE_GLONASS = 2
        uint16 SERVICE_COMPASS = 4     # includes BeiDou.
        uint16 SERVICE_GALILEO = 8

        int8 status

        uint16 service
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "gps"
    assert messages[0].data.status.status == 0
    assert messages[0].data.status.service == 1
    assert messages[0].data.latitude == 37.7749
    assert messages[0].data.longitude == -122.4194
    assert messages[0].data.altitude == 10.5
    assert messages[0].data.position_covariance == [1.0] + [0.0] * 8
    assert messages[0].data.position_covariance_type == 3  # COVARIANCE_TYPE_KNOWN


def test_sensor_msgs_navsatstatus():
    msgtype = "sensor_msgs/NavSatStatus"
    schema = dedent("""
        # Navigation Satellite fix status for any Global Navigation Satellite System.
        #
        # Whether to output an augmented fix is determined by both the fix
        # type and the last time differential corrections were received. A
        # fix is valid when status >= STATUS_FIX.

        int8 STATUS_NO_FIX = -1        # unable to fix position
        int8 STATUS_FIX = 0            # unaugmented fix
        int8 STATUS_SBAS_FIX = 1       # with satellite-based augmentation
        int8 STATUS_GBAS_FIX = 2       # with ground-based augmentation

        int8 status

        # Bits defining which Global Navigation Satellite System signals were
        # used by the receiver.

        uint16 SERVICE_GPS = 1
        uint16 SERVICE_GLONASS = 2
        uint16 SERVICE_COMPASS = 4     # includes BeiDou.
        uint16 SERVICE_GALILEO = 8

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
    assert messages[0].data.status == 0   # STATUS_FIX
    assert messages[0].data.service == 1  # SERVICE_GPS


def test_sensor_msgs_point_cloud():
    # values -> valuess because of mcap_ros2 bug
    msgtype = "sensor_msgs/PointCloud"
    schema = dedent("""
        # THIS MESSAGE IS DEPRECATED AS OF FOXY
        # Please use sensor_msgs/PointCloud2

        # This message holds a collection of 3d points, plus optional additional
        # information about each point.

        # Time of sensor data acquisition, coordinate frame ID.
        std_msgs/Header header

        # Array of 3d points. Each Point32 should be interpreted as a 3d point
        # in the frame given in the header.
        geometry_msgs/Point32[] points

        # Each channel should have the same number of elements as points array,
        # and the data in each channel should correspond 1:1 with each point.
        # Channel names in common practice are listed in ChannelFloat32.msg.
        ChannelFloat32[] channels
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Point32
        # This contains the position of a point in free space(with 32 bits of precision).
        # It is recommended to use Point wherever possible instead of Point32.
        #
        # This recommendation is to promote interoperability.
        #
        # This message is designed to take up less space when sending
        # lots of points at once, as in the case of a PointCloud.

        float32 x
        float32 y
        float32 z
        ================================================================================
        MSG: sensor_msgs/ChannelFloat32
        # This message is used by the PointCloud message to hold optional data
        # associated with each point in the cloud. The length of the values
        # array should be the same as the length of the points array in the
        # PointCloud, and each value should be associated with the corresponding
        # point.

        # Channel names in existing practice include:
        #   "u", "v" - row and column (respectively) in the left stereo image.
        #              This is opposite to usual conventions but remains for
        #              historical reasons. The newer PointCloud2 message has no
        #              such problem.
        #   "rgb" - For point clouds produced by color stereo cameras. uint8
        #           (R,G,B) values packed into the least significant 24 bits,
        #           in order.
        #   "intensity" - laser or pixel intensity.
        #   "distance"

        string name      # The channel name should give semantics of the channel
                         # (e.g. "intensity" instead of "value").
        float32[] valuess    # The values array should be 1-1 with the elements of the associated
                         # PointCloud.
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
                {"name": "intensity", "valuess": [100.0, 200.0]}
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "lidar"
    assert len(messages[0].data.points) == 2
    assert messages[0].data.points[0].x == 1.0
    assert messages[0].data.points[0].y == 2.0
    assert messages[0].data.points[0].z == 3.0
    assert messages[0].data.points[1].x == 4.0
    assert messages[0].data.points[1].y == 5.0
    assert messages[0].data.points[1].z == 6.0
    assert len(messages[0].data.channels) == 1
    assert messages[0].data.channels[0].name == "intensity"
    assert messages[0].data.channels[0].valuess == [100.0, 200.0]


def test_sensor_msgs_point_cloud2():
    msgtype = "sensor_msgs/PointCloud2"
    schema = dedent("""
        # This message holds a collection of N-dimensional points, which may
        # contain additional information such as normals, intensity, etc. The
        # point data is stored as a binary blob, its layout described by the
        # contents of the "fields" array.

        # The point cloud data may be organized 2d (image-like) or 1d (unordered).
        # Point clouds organized as 2d images may be produced by camera depth sensors
        # such as stereo or time-of-flight.

        # Time of sensor data acquisition, and the coordinate frame ID (for 3d points).
        std_msgs/Header header

        # 2D structure of the point cloud. If the cloud is unordered, height is
        # 1 and width is the length of the point cloud.
        uint32 height
        uint32 width

        # Describes the channels and their layout in the binary data blob.
        PointField[] fields

        bool    is_bigendian     # Is this data bigendian?
        uint32  point_step       # Length of a point in bytes
        uint32  row_step         # Length of a row in bytes
        uint8[] data             # Actual point data, size is (row_step*height)

        bool is_dense            # True if there are no invalid points
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "lidar"
    assert messages[0].data.height == 1
    assert messages[0].data.width == 2
    assert len(messages[0].data.fields) == 3
    assert messages[0].data.fields[0].name == "x"
    assert messages[0].data.fields[0].offset == 0
    assert messages[0].data.fields[0].datatype == 7  # FLOAT32
    assert messages[0].data.fields[0].count == 1
    assert messages[0].data.fields[1].name == "y"
    assert messages[0].data.fields[1].offset == 4
    assert messages[0].data.fields[1].datatype == 7  # FLOAT32
    assert messages[0].data.fields[1].count == 1
    assert messages[0].data.fields[2].name == "z"
    assert messages[0].data.fields[2].offset == 8
    assert messages[0].data.fields[2].datatype == 7  # FLOAT32
    assert messages[0].data.fields[2].count == 1
    assert messages[0].data.is_bigendian == False
    assert messages[0].data.point_step == 12
    assert messages[0].data.row_step == 24
    assert messages[0].data.data == b'\x00' * 24
    assert messages[0].data.is_dense == True


def test_sensor_msgs_pointfield():
    msgtype = "sensor_msgs/PointField"
    schema = dedent("""
        # This message holds the description of one point entry in the
        # PointCloud2 message format.
        uint8 INT8 = 1
        uint8 UINT8 = 2
        uint8 INT16 = 3
        uint8 UINT16 = 4
        uint8 INT32 = 5
        uint8 UINT32 = 6
        uint8 FLOAT32 = 7
        uint8 FLOAT64 = 8

        # Common PointField names are x, y, z, intensity, rgb, rgba
        string name      # Name of field
        uint32 offset    # Offset from start of point struct
        uint8 datatype   # Datatype enumeration, see above
        uint32 count     # How many elements in the field
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
    assert messages[0].data.datatype == 7  # FLOAT32
    assert messages[0].data.count == 1


def test_sensor_msgs_range():
    msgtype = "sensor_msgs/Range"
    schema = dedent("""
        # Single range reading from an active ranger that emits energy and reports
        # one range reading that is valid along an arc at the distance measured.
        # This message is not appropriate for laser scanners. See the LaserScan
        # message if you are working with a laser scanner.
        #
        # This message also can represent a fixed-distance (binary) ranger. This
        # sensor will have min_range===max_range===distance of detection.
        # These sensors follow REP 117 and will output -Inf if the object is detected
        # and +Inf if the object is outside of the detection range.

        std_msgs/Header header # timestamp in the header is the time the ranger returned the distance reading

        # Radiation type enums
        # If you want a value added to this list, send an email to the ros-users list
        uint8 ULTRASOUND=0
        uint8 INFRARED=1

        uint8 radiation_type # the type of radiation used by the sensor (sound, IR, etc) [enum]

        float32 field_of_view # the size of the arc that the distance reading is valid for [rad]
         # the object causing the range reading may have been anywhere within -field_of_view/2 and
         # field_of_view/2 at the measured range.
         # 0 angle corresponds to the x-axis of the sensor.

        float32 min_range # minimum range value [m]
        float32 max_range # maximum range value [m]
         # Fixed distance rangers require min_range==max_range

        float32 range # range data [m]
         # (Note: values < range_min or > range_max should be discarded)
         # Fixed distance rangers only output -Inf or +Inf.
         # -Inf represents a detection within fixed distance.
         # (Detection too close to the sensor to quantify)
         # +Inf represents no detection within the fixed distance.
         # (Object out of range)
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "ultrasonic_sensor"
            },
            "radiation_type": 0,  # ULTRASOUND
            "field_of_view": 0.75,
            "min_range": 0.25,
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "ultrasonic_sensor"
    assert messages[0].data.radiation_type == 0  # ULTRASOUND
    assert messages[0].data.field_of_view == 0.75
    assert messages[0].data.min_range == 0.25
    assert messages[0].data.max_range == 4.0
    assert messages[0].data.range == 1.5


def test_sensor_msgs_regionofinterest():
    msgtype = "sensor_msgs/RegionOfInterest"
    schema = dedent("""
        # This message is used to specify a region of interest within an image.
        #
        # When used to specify the ROI setting of the camera when the image was
        # taken, the height and width fields should either match the height and
        # width fields for the associated image; or height = width = 0
        # indicates that the full resolution image was captured.

        uint32 x_offset # Leftmost pixel of the ROI
         # (0 if the ROI includes the left edge of the image)
        uint32 y_offset # Topmost pixel of the ROI
         # (0 if the ROI includes the top edge of the image)
        uint32 height # Height of ROI
        uint32 width # Width of ROI

        # True if a distinct rectified ROI should be calculated from the "raw"
        # ROI in this message. Typically this should be False if the full image
        # is captured (ROI not used), and True if a subwindow is captured (ROI
        # used).
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


def test_sensor_msgs_relative_humidity():
    msgtype = "sensor_msgs/RelativeHumidity"
    schema = dedent("""
        # Single reading from a relative humidity sensor.
        # Defines the ratio of partial pressure of water vapor to the saturated vapor
        # pressure at a temperature.

        std_msgs/Header header # timestamp of the measurement
                               # frame_id is the location of the humidity sensor

        float64 relative_humidity    # Expression of the relative humidity
                                     # from 0.0 to 1.0.
                                     # 0.0 is no partial pressure of water vapor
                                     # 1.0 represents partial pressure of saturation

        float64 variance             # 0 is interpreted as variance unknown
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "humidity_sensor"
            },
            "relative_humidity": 45.5,
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
    assert messages[0].data.header.frame_id == "humidity_sensor"
    assert messages[0].data.relative_humidity == 45.5
    assert messages[0].data.variance == 0.1


def test_sensor_msgs_temperature():
    msgtype = "sensor_msgs/Temperature"
    schema = dedent("""
        # Single temperature reading.

        std_msgs/Header header # timestamp is the time the temperature was measured
                               # frame_id is the location of the temperature reading

        float64 temperature          # Measurement of the Temperature in Degrees Celsius.

        float64 variance             # 0 is interpreted as variance unknown.
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.
        #
        # sequence ID: consecutively increasing ID
        builtin_interfaces/Time stamp
            # Two-integer timestamp that is expressed as:
            # * stamp.sec: seconds (stamp_secs) since epoch (in Python the variable is called 'secs')
            # * stamp.nanosec: nanoseconds since stamp_secs (in Python the variable is called 'nsecs')
            # time-handling sugar is provided by the client library
        string frame_id # Frame this data is associated with
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
        # Measurement from an external time source not actively synchronized with the system clock.

        std_msgs/Header header # stamp is system time for which measurement was valid
         # frame_id is not used

        builtin_interfaces/Time time_ref # corresponding time from this external source
        string source # (optional) name of time source
        ================================================================================
        MSG: std_msgs/Header
        builtin_interfaces/Time stamp
        string frame_id
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
    assert messages[0].data.header.stamp.nanosec == 456789
    assert messages[0].data.header.frame_id == "time_source"
    assert messages[0].data.time_ref.sec == 1234567890
    assert messages[0].data.time_ref.nanosec == 123456789
    assert messages[0].data.source == "GPS"
