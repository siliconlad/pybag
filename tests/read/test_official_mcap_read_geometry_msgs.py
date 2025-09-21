"""Test reading geometry_msgs messages written with the official MCAP writer."""

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


def test_geometry_msgs_accel():
    msgtype = "geometry_msgs/Accel"
    schema = dedent("""
        # This expresses acceleration in free space broken into its linear and angular parts.
        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular": {"x": 4.0, "y": 5.0, "z": 6.0},
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.linear.x == 1.0
    assert messages[0].data.linear.y == 2.0
    assert messages[0].data.linear.z == 3.0
    assert messages[0].data.angular.x == 4.0
    assert messages[0].data.angular.y == 5.0
    assert messages[0].data.angular.z == 6.0


def test_geometry_msgs_accel_stamped():
    msgtype = "geometry_msgs/AccelStamped"
    schema = dedent("""
        # An accel with reference coordinate frame and timestamp
        std_msgs/Header header
        Accel accel
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Accel
        # This expresses acceleration in free space broken into its linear and angular parts.
        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "accel": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.accel.linear.x == 1.0
    assert messages[0].data.accel.linear.y == 2.0
    assert messages[0].data.accel.linear.z == 3.0
    assert messages[0].data.accel.angular.x == 4.0
    assert messages[0].data.accel.angular.y == 5.0
    assert messages[0].data.accel.angular.z == 6.0


def test_geometry_msgs_accel_with_covariance():
    msgtype = "geometry_msgs/AccelWithCovariance"
    schema = dedent("""
        # This expresses acceleration in free space with uncertainty.

        Accel accel

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Accel
        # This expresses acceleration in free space broken into its linear and angular parts.
        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "accel": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.accel.linear.x == 1.0
    assert messages[0].data.accel.linear.y == 2.0
    assert messages[0].data.accel.linear.z == 3.0
    assert messages[0].data.accel.angular.x == 4.0
    assert messages[0].data.accel.angular.y == 5.0
    assert messages[0].data.accel.angular.z == 6.0
    assert messages[0].data.covariance == [0.0] * 36


def test_geometry_msgs_accel_with_covariance_stamped():
    msgtype = "geometry_msgs/AccelWithCovarianceStamped"
    schema = dedent("""
        # An accel with covariance with reference coordinate frame and timestamp
        std_msgs/Header header
        AccelWithCovariance accel
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/AccelWithCovariance
        # This expresses acceleration in free space with uncertainty.

        Accel accel

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Accel
        # This expresses acceleration in free space broken into its linear and angular parts.
        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "accel": {
                "accel": {
                    "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
                },
                "covariance": [0.0] * 36
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.accel.accel.linear.x == 1.0
    assert messages[0].data.accel.accel.linear.y == 2.0
    assert messages[0].data.accel.accel.linear.z == 3.0
    assert messages[0].data.accel.accel.angular.x == 4.0
    assert messages[0].data.accel.accel.angular.y == 5.0
    assert messages[0].data.accel.accel.angular.z == 6.0
    assert messages[0].data.accel.covariance == [0.0] * 36


def test_geometry_msgs_inertia():
    msgtype = "geometry_msgs/Inertia"
    schema = dedent("""
        # Mass [kg]
        float64 m

        # Center of mass [m]
        geometry_msgs/Vector3 com

        # Inertia Tensor [kg-m^2]
        #     | ixx ixy ixz |
        # I = | ixy iyy iyz |
        #     | ixz iyz izz |
        float64 ixx
        float64 ixy
        float64 ixz
        float64 iyy
        float64 iyz
        float64 izz
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "m": 10.5,
            "com": {"x": 1.0, "y": 2.0, "z": 3.0},
            "ixx": 1.1,
            "ixy": 1.2,
            "ixz": 1.3,
            "iyy": 2.2,
            "iyz": 2.3,
            "izz": 3.3
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.m == 10.5
    assert messages[0].data.com.x == 1.0
    assert messages[0].data.com.y == 2.0
    assert messages[0].data.com.z == 3.0
    assert messages[0].data.ixx == 1.1
    assert messages[0].data.ixy == 1.2
    assert messages[0].data.ixz == 1.3
    assert messages[0].data.iyy == 2.2
    assert messages[0].data.iyz == 2.3
    assert messages[0].data.izz == 3.3


def test_geometry_msgs_inertia_stamped():
    msgtype = "geometry_msgs/InertiaStamped"
    schema = dedent("""
        # An inertia with reference coordinate frame and timestamp
        std_msgs/Header header
        Inertia inertia
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Inertia
        # Mass [kg]
        float64 m

        # Center of mass [m]
        geometry_msgs/Vector3 com

        # Inertia Tensor [kg-m^2]
        #     | ixx ixy ixz |
        # I = | ixy iyy iyz |
        #     | ixz iyz izz |
        float64 ixx
        float64 ixy
        float64 ixz
        float64 iyy
        float64 iyz
        float64 izz
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "inertia": {
                "m": 10.5,
                "com": {"x": 1.0, "y": 2.0, "z": 3.0},
                "ixx": 1.1,
                "ixy": 1.2,
                "ixz": 1.3,
                "iyy": 2.2,
                "iyz": 2.3,
                "izz": 3.3
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.inertia.m == 10.5
    assert messages[0].data.inertia.com.x == 1.0
    assert messages[0].data.inertia.com.y == 2.0
    assert messages[0].data.inertia.com.z == 3.0
    assert messages[0].data.inertia.ixx == 1.1
    assert messages[0].data.inertia.ixy == 1.2
    assert messages[0].data.inertia.ixz == 1.3
    assert messages[0].data.inertia.iyy == 2.2
    assert messages[0].data.inertia.iyz == 2.3
    assert messages[0].data.inertia.izz == 3.3


def test_geometry_msgs_point():
    msgtype = "geometry_msgs/Point"
    schema = dedent("""
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_point32():
    msgtype = "geometry_msgs/Point32"
    schema = dedent("""
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
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_point_stamped():
    msgtype = "geometry_msgs/PointStamped"
    schema = dedent("""
        # A Point with reference coordinate frame and timestamp
        std_msgs/Header header
        Point point
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "point": {"x": 1.0, "y": 2.0, "z": 3.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.point.x == 1.0
    assert messages[0].data.point.y == 2.0
    assert messages[0].data.point.z == 3.0


def test_geometry_msgs_polygon():
    msgtype = "geometry_msgs/Polygon"
    schema = dedent("""
        # A specification of a polygon where the first and last points are assumed to be connected
        Point32[] points
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
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "points": [
                {"x": 1.0, "y": 2.0, "z": 3.0},
                {"x": 4.0, "y": 5.0, "z": 6.0}
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
    assert len(messages[0].data.points) == 2
    assert messages[0].data.points[0].x == 1.0
    assert messages[0].data.points[0].y == 2.0
    assert messages[0].data.points[0].z == 3.0
    assert messages[0].data.points[1].x == 4.0
    assert messages[0].data.points[1].y == 5.0
    assert messages[0].data.points[1].z == 6.0


def test_geometry_msgs_polygon_instance():
    msgtype = "geometry_msgs/PolygonInstance"
    schema = dedent("""
        # A Polygon with an ID for discrimination between multiple polygons
        geometry_msgs/Polygon polygon
        int64 id
        ================================================================================
        MSG: geometry_msgs/Polygon
        # A specification of a polygon where the first and last points are assumed to be connected
        Point32[] points
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
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "polygon": {"points": [{"x": 1.0, "y": 2.0, "z": 3.0}]},
            "id": 42
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.id == 42
    assert len(messages[0].data.polygon.points) == 1
    assert messages[0].data.polygon.points[0].x == 1.0
    assert messages[0].data.polygon.points[0].y == 2.0
    assert messages[0].data.polygon.points[0].z == 3.0


def test_geometry_msgs_polygon_instance_stamped():
    msgtype = "geometry_msgs/PolygonInstanceStamped"
    schema = dedent("""
        std_msgs/Header header
        geometry_msgs/PolygonInstance polygon
        ================================================================================
        MSG: geometry_msgs/PolygonInstance
        geometry_msgs/Polygon polygon
        int64 id
        ================================================================================
        MSG: geometry_msgs/Polygon
        Point32[] points
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
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
    """)
    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "polygon": {
                "polygon": {"points": [{"x": 1.0, "y": 2.0, "z": 3.0}]},
                "id": 42
            },
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.polygon.id == 42
    assert len(messages[0].data.polygon.polygon.points) == 1
    assert messages[0].data.polygon.polygon.points[0].x == 1.0
    assert messages[0].data.polygon.polygon.points[0].y == 2.0
    assert messages[0].data.polygon.polygon.points[0].z == 3.0


def test_geometry_msgs_polygon_stamped():
    msgtype = "geometry_msgs/PolygonStamped"
    schema = dedent("""
        std_msgs/Header header
        Polygon polygon
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Polygon
        Point32[] points
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
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "polygon": {
                "points": [
                    {"x": 1.0, "y": 2.0, "z": 3.0},
                    {"x": 4.0, "y": 5.0, "z": 6.0}
                ]
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert len(messages[0].data.polygon.points) == 2
    assert messages[0].data.polygon.points[0].x == 1.0
    assert messages[0].data.polygon.points[0].y == 2.0
    assert messages[0].data.polygon.points[0].z == 3.0
    assert messages[0].data.polygon.points[1].x == 4.0
    assert messages[0].data.polygon.points[1].y == 5.0
    assert messages[0].data.polygon.points[1].z == 6.0


def test_geometry_msgs_pose():
    msgtype = "geometry_msgs/Pose"
    schema = dedent("""
        # A representation of pose in free space, composed of position and orientation.

        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.

        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "position": {"x": 1.0, "y": 2.0, "z": 3.0},
            "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.position.x == 1.0
    assert messages[0].data.position.y == 2.0
    assert messages[0].data.position.z == 3.0
    assert messages[0].data.orientation.x == 0.0
    assert messages[0].data.orientation.y == 0.0
    assert messages[0].data.orientation.z == 0.0
    assert messages[0].data.orientation.w == 1.0


def test_geometry_msgs_pose2d():
    msgtype = "geometry_msgs/Pose2D"
    schema = dedent("""
        # Deprecated as of Foxy and will potentially be removed in any following release.
        # Please use the full 3D pose.

        # In general our recommendation is to use a full 3D representation of everything and for 2D specific applications make the appropriate projections into the plane for their calculations but optimally will preserve the 3D information during processing.

        # If we have parallel copies of 2D datatypes every UI and other pipeline will end up needing to have dual interfaces to plot everything. And you will end up with not being able to use 3D tools for 2D use cases even if they're completely valid, as you'd have to reimplement it with different inputs and outputs. It's not particularly hard to plot the 2D pose or compute the yaw error for the Pose message and there are already tools and libraries that can do this for you.

        # This expresses a position and orientation on a 2D manifold.
        float64 x
        float64 y
        float64 theta
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "theta": 1.5708}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.theta == 1.5708


def test_geometry_msgs_pose_array():
    msgtype = "geometry_msgs/PoseArray"
    schema = dedent("""
        std_msgs/Header header
        Pose[] poses
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of pose in free space, composed of position and orientation.

        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.

        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "poses": [
                {
                    "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert len(messages[0].data.poses) == 1
    assert messages[0].data.poses[0].position.x == 1.0
    assert messages[0].data.poses[0].position.y == 2.0
    assert messages[0].data.poses[0].position.z == 3.0
    assert messages[0].data.poses[0].orientation.x == 0.0
    assert messages[0].data.poses[0].orientation.y == 0.0
    assert messages[0].data.poses[0].orientation.z == 0.0
    assert messages[0].data.poses[0].orientation.w == 1.0


def test_geometry_msgs_pose_stamped():
    msgtype = "geometry_msgs/PoseStamped"
    schema = dedent("""
        # A Pose with reference coordinate frame and timestamp
        std_msgs/Header header
        Pose pose
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of pose in free space, composed of position and orientation.
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "pose": {
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.pose.position.x == 1.0
    assert messages[0].data.pose.position.y == 2.0
    assert messages[0].data.pose.position.z == 3.0
    assert messages[0].data.pose.orientation.x == 0.0
    assert messages[0].data.pose.orientation.y == 0.0
    assert messages[0].data.pose.orientation.z == 0.0
    assert messages[0].data.pose.orientation.w == 1.0


def test_geometry_msgs_pose_with_covariance():
    msgtype = "geometry_msgs/PoseWithCovariance"
    schema = dedent("""
        # This represents a pose in free space with uncertainty.
        Pose pose

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of pose in free space, composed of position and orientation.
        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "pose": {
                "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.pose.position.x == 1.0
    assert messages[0].data.pose.position.y == 2.0
    assert messages[0].data.pose.position.z == 3.0
    assert messages[0].data.pose.orientation.x == 0.0
    assert messages[0].data.pose.orientation.y == 0.0
    assert messages[0].data.pose.orientation.z == 0.0
    assert messages[0].data.pose.orientation.w == 1.0
    assert messages[0].data.covariance == [0.0] * 36


def test_geometry_msgs_pose_with_covariance_stamped():
    msgtype = "geometry_msgs/PoseWithCovarianceStamped"
    schema = dedent("""
        std_msgs/Header header
        PoseWithCovariance pose
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/PoseWithCovariance
        Pose pose
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Pose
        # A representation of pose in free space, composed of position and orientation.

        Point position
        Quaternion orientation
        ================================================================================
        MSG: geometry_msgs/Point
        # This contains the position of a point in free space
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.

        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "pose": {
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
                },
                "covariance": [0.0] * 36
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.pose.pose.position.x == 1.0
    assert messages[0].data.pose.pose.position.y == 2.0
    assert messages[0].data.pose.pose.position.z == 3.0
    assert messages[0].data.pose.pose.orientation.x == 0.0
    assert messages[0].data.pose.pose.orientation.y == 0.0
    assert messages[0].data.pose.pose.orientation.z == 0.0
    assert messages[0].data.pose.pose.orientation.w == 1.0
    assert messages[0].data.pose.covariance == [0.0] * 36


def test_geometry_msgs_quaternion():
    msgtype = "geometry_msgs/Quaternion"
    schema = dedent("""
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 0.0
    assert messages[0].data.y == 0.0
    assert messages[0].data.z == 0.0
    assert messages[0].data.w == 1.0


def test_geometry_msgs_quaternion_stamped():
    msgtype = "geometry_msgs/QuaternionStamped"
    schema = dedent("""
        std_msgs/Header header
        Quaternion quaternion
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.

        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "quaternion": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.quaternion.x == 0.0
    assert messages[0].data.quaternion.y == 0.0
    assert messages[0].data.quaternion.z == 0.0
    assert messages[0].data.quaternion.w == 1.0


def test_geometry_msgs_transform():
    msgtype = "geometry_msgs/Transform"
    schema = dedent("""
        # This represents the transform between two coordinate frames in free space.
        Vector3 translation
        Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.
        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.
        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.
        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
            "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.translation.x == 1.0
    assert messages[0].data.translation.y == 2.0
    assert messages[0].data.translation.z == 3.0
    assert messages[0].data.rotation.x == 0.0
    assert messages[0].data.rotation.y == 0.0
    assert messages[0].data.rotation.z == 0.0
    assert messages[0].data.rotation.w == 1.0


def test_geometry_msgs_transform_stamped():
    msgtype = "geometry_msgs/TransformStamped"
    schema = dedent("""
        std_msgs/Header header
        string child_frame_id
        Transform transform
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Transform
        Vector3 translation
        Quaternion rotation
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
        ================================================================================
        MSG: geometry_msgs/Quaternion
        # This represents an orientation in free space in quaternion form.

        float64 x 0
        float64 y 0
        float64 z 0
        float64 w 1
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "child_frame_id": "child_frame",
            "transform": {
                "translation": {"x": 1.0, "y": 2.0, "z": 3.0},
                "rotation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.child_frame_id == "child_frame"
    assert messages[0].data.transform.translation.x == 1.0
    assert messages[0].data.transform.translation.y == 2.0
    assert messages[0].data.transform.translation.z == 3.0
    assert messages[0].data.transform.rotation.x == 0.0
    assert messages[0].data.transform.rotation.y == 0.0
    assert messages[0].data.transform.rotation.z == 0.0
    assert messages[0].data.transform.rotation.w == 1.0


def test_geometry_msgs_twist():
    msgtype = "geometry_msgs/Twist"
    schema = dedent("""
        # This expresses velocity in free space broken into its linear and angular parts.

        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.linear.x == 1.0
    assert messages[0].data.linear.y == 2.0
    assert messages[0].data.linear.z == 3.0
    assert messages[0].data.angular.x == 4.0
    assert messages[0].data.angular.y == 5.0
    assert messages[0].data.angular.z == 6.0


def test_geometry_msgs_twist_stamped():
    msgtype = "geometry_msgs/TwistStamped"
    schema = dedent("""
        std_msgs/Header header
        Twist twist
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.

        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "twist": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.twist.linear.x == 1.0
    assert messages[0].data.twist.linear.y == 2.0
    assert messages[0].data.twist.linear.z == 3.0
    assert messages[0].data.twist.angular.x == 4.0
    assert messages[0].data.twist.angular.y == 5.0
    assert messages[0].data.twist.angular.z == 6.0


def test_geometry_msgs_twist_with_covariance():
    msgtype = "geometry_msgs/TwistWithCovariance"
    schema = dedent("""
        # This expresses velocity in free space with uncertainty.

        Twist twist

        # Row-major representation of the 6x6 covariance matrix
        # The orientation parameters use a fixed-axis representation.
        # In order, the parameters are:
        # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.

        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "twist": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
            },
            "covariance": [0.0] * 36
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.twist.linear.x == 1.0
    assert messages[0].data.twist.linear.y == 2.0
    assert messages[0].data.twist.linear.z == 3.0
    assert messages[0].data.twist.angular.x == 4.0
    assert messages[0].data.twist.angular.y == 5.0
    assert messages[0].data.twist.angular.z == 6.0
    assert messages[0].data.covariance == [0.0] * 36


def test_geometry_msgs_twist_with_covariance_stamped():
    msgtype = "geometry_msgs/TwistWithCovarianceStamped"
    schema = dedent("""
        std_msgs/Header header
        TwistWithCovariance twist
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/TwistWithCovariance
        Twist twist
        float64[36] covariance
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.

        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "twist": {
                "twist": {
                    "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                    "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
                },
                "covariance": [0.0] * 36
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.twist.twist.linear.x == 1.0
    assert messages[0].data.twist.twist.linear.y == 2.0
    assert messages[0].data.twist.twist.linear.z == 3.0
    assert messages[0].data.twist.twist.angular.x == 4.0
    assert messages[0].data.twist.twist.angular.y == 5.0
    assert messages[0].data.twist.twist.angular.z == 6.0
    assert len(messages[0].data.twist.covariance) == 36


def test_geometry_msgs_vector3():
    msgtype = "geometry_msgs/Vector3"
    schema = dedent("""
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"x": 1.0, "y": 2.0, "z": 3.0}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.x == 1.0
    assert messages[0].data.y == 2.0
    assert messages[0].data.z == 3.0


def test_geometry_msgs_vector3_stamped():
    msgtype = "geometry_msgs/Vector3Stamped"
    schema = dedent("""
        std_msgs/Header header
        Vector3 vector
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "vector": {"x": 1.0, "y": 2.0, "z": 3.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.vector.x == 1.0
    assert messages[0].data.vector.y == 2.0
    assert messages[0].data.vector.z == 3.0


def test_geometry_msgs_velocity_stamped():
    msgtype = "geometry_msgs/VelocityStamped"
    schema = dedent("""
        std_msgs/Header header
        string body_frame_id
        string reference_frame_id
        Twist velocity
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Twist
        # This expresses velocity in free space broken into its linear and angular parts.

        Vector3 linear
        Vector3 angular
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)
    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "velocity": {
                "linear": {"x": 1.0, "y": 2.0, "z": 3.0},
                "angular": {"x": 4.0, "y": 5.0, "z": 6.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.velocity.linear.x == 1.0
    assert messages[0].data.velocity.linear.y == 2.0
    assert messages[0].data.velocity.linear.z == 3.0
    assert messages[0].data.velocity.angular.x == 4.0
    assert messages[0].data.velocity.angular.y == 5.0
    assert messages[0].data.velocity.angular.z == 6.0


def test_geometry_msgs_wrench():
    msgtype = "geometry_msgs/Wrench"
    schema = dedent("""
        # This represents force in free space, separated into its linear and angular parts.

        Vector3 force
        Vector3 torque
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "force": {"x": 1.0, "y": 2.0, "z": 3.0},
            "torque": {"x": 4.0, "y": 5.0, "z": 6.0}
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.force.x == 1.0
    assert messages[0].data.force.y == 2.0
    assert messages[0].data.force.z == 3.0
    assert messages[0].data.torque.x == 4.0
    assert messages[0].data.torque.y == 5.0
    assert messages[0].data.torque.z == 6.0


def test_geometry_msgs_wrench_stamped():
    msgtype = "geometry_msgs/WrenchStamped"
    schema = dedent("""
        std_msgs/Header header
        Wrench wrench
        ================================================================================
        MSG: std_msgs/Header
        # Standard metadata for higher-level stamped data types.
        # This is generally used to communicate timestamped data
        # in a particular coordinate frame.

        # Two-integer timestamp that is expressed as seconds and nanoseconds.
        builtin_interfaces/Time stamp

        # Transform frame with which this data is associated.
        string frame_id
        ================================================================================
        MSG: geometry_msgs/Wrench
        Vector3 force
        Vector3 torque
        ================================================================================
        MSG: geometry_msgs/Vector3
        # This represents a vector in free space.

        # This is semantically different than a point.
        # A vector is always anchored at the origin.
        # When a transform is applied to a vector, only the rotational component is applied.

        float64 x
        float64 y
        float64 z
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "header": {
                "stamp": {"sec": 123, "nanosec": 456789},
                "frame_id": "test_frame"
            },
            "wrench": {
                "force": {"x": 1.0, "y": 2.0, "z": 3.0},
                "torque": {"x": 4.0, "y": 5.0, "z": 6.0}
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
    assert messages[0].data.header.frame_id == "test_frame"
    assert messages[0].data.wrench.force.x == 1.0
    assert messages[0].data.wrench.force.y == 2.0
    assert messages[0].data.wrench.force.z == 3.0
    assert messages[0].data.wrench.torque.x == 4.0
    assert messages[0].data.wrench.torque.y == 5.0
    assert messages[0].data.wrench.torque.z == 6.0
