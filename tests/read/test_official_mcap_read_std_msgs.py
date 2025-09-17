"""Test reading std_msgs messages written with the official MCAP writer."""

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


def test_std_msgs_bool():
    msgtype = "std_msgs/Bool"
    schema = dedent("""
        bool data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": True}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data is True


def test_std_msgs_byte():
    msgtype = "std_msgs/Byte"
    schema = dedent("""
        byte data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 42}  # b'\x2a'
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == b'\x2a'


def test_std_msgs_bytemultiarray():
    msgtype = "std_msgs/ByteMultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        byte[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 3, "stride": 3}],
                "data_offset": 0
            },
            "data": [1, 2, 3]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 3
    assert messages[0].data.layout.dim[0].stride == 3
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [b'\x01', b'\x02', b'\x03']


def test_std_msgs_char():
    msgtype = "std_msgs/Char"
    schema = dedent("""
        char data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 65}  # ASCII 'A'
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 'A'


def test_std_msgs_colorrgba():
    msgtype = "std_msgs/ColorRGBA"
    schema = dedent("""
        float32 r
        float32 g
        float32 b
        float32 a
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"r": 1.0, "g": 0.5, "b": 0.25, "a": 0.125}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.r == 1.0
    assert messages[0].data.g == 0.5
    assert messages[0].data.b == 0.25
    assert messages[0].data.a == 0.125


def test_std_msgs_empty():
    msgtype = "std_msgs/Empty"
    schema = dedent("""
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1


def test_std_msgs_float32():
    msgtype = "std_msgs/Float32"
    schema = dedent("""
        float32 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 3.14}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert abs(messages[0].data.data - 3.14) < 0.001


def test_std_msgs_float32multiarray():
    msgtype = "std_msgs/Float32MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        float32[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [1.5, 2.5]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert abs(messages[0].data.data[0] - 1.5) < 0.001
    assert abs(messages[0].data.data[1] - 2.5) < 0.001


def test_std_msgs_float64():
    msgtype = "std_msgs/Float64"
    schema = dedent("""
        float64 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 2.71828}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 2.71828


def test_std_msgs_float64multiarray():
    msgtype = "std_msgs/Float64MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        float64[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [3.14159, 2.71828]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.data_offset == 0
    assert messages[0].data.data[0] == 3.14159
    assert messages[0].data.data[1] == 2.71828


def test_std_msgs_header():
    msgtype = "std_msgs/Header"
    schema = dedent("""
        builtin_interfaces/Time stamp
        string frame_id
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "stamp": {"sec": 123, "nanosec": 456789},
            "frame_id": "test_frame"
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.stamp.sec == 123
    assert messages[0].data.stamp.nanosec == 456789
    assert messages[0].data.frame_id == "test_frame"


def test_std_msgs_int16():
    msgtype = "std_msgs/Int16"
    schema = dedent("""
        int16 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": -1234}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == -1234


def test_std_msgs_int16multiarray():
    msgtype = "std_msgs/Int16MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        int16[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 3, "stride": 3}],
                "data_offset": 0
            },
            "data": [-100, 0, 100]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-100, 0, 100]


def test_std_msgs_int32():
    msgtype = "std_msgs/Int32"
    schema = dedent("""
        int32 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": -123456}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == -123456


def test_std_msgs_int32multiarray():
    msgtype = "std_msgs/Int32MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        int32[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [-1000, 2000]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-1000, 2000]


def test_std_msgs_int64():
    msgtype = "std_msgs/Int64"
    schema = dedent("""
        int64 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": -9876543210}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == -9876543210


def test_std_msgs_int64multiarray():
    msgtype = "std_msgs/Int64MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        int64[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [-9223372036854775807, 9223372036854775807]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-9223372036854775807, 9223372036854775807]


def test_std_msgs_int8():
    msgtype = "std_msgs/Int8"
    schema = dedent("""
        int8 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": -42}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == -42


def test_std_msgs_int8multiarray():
    msgtype = "std_msgs/Int8MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        int8[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 3, "stride": 3}],
                "data_offset": 0
            },
            "data": [-128, 0, 127]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-128, 0, 127]


def test_std_msgs_multiarraydimension():
    msgtype = "std_msgs/MultiArrayDimension"
    schema = dedent("""
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "label": "x",
            "size": 10,
            "stride": 40
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.label == "x"
    assert messages[0].data.size == 10
    assert messages[0].data.stride == 40


def test_std_msgs_multiarraylayout():
    msgtype = "std_msgs/MultiArrayLayout"
    schema = dedent("""
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "dim": [
                {"label": "x", "size": 10, "stride": 40},
                {"label": "y", "size": 4, "stride": 4}
            ],
            "data_offset": 0
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert len(messages[0].data.dim) == 2
    assert messages[0].data.dim[0].label == "x"
    assert messages[0].data.dim[0].size == 10
    assert messages[0].data.dim[0].stride == 40
    assert messages[0].data.dim[1].label == "y"
    assert messages[0].data.dim[1].size == 4
    assert messages[0].data.dim[1].stride == 4
    assert messages[0].data.data_offset == 0


def test_std_msgs_string():
    msgtype = "std_msgs/String"
    schema = dedent("""
        string data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": "Hello, World!"}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == "Hello, World!"


def test_std_msgs_uint16():
    msgtype = "std_msgs/UInt16"
    schema = dedent("""
        uint16 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 1234}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 1234


def test_std_msgs_uint16multiarray():
    msgtype = "std_msgs/UInt16MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        uint16[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [0, 65535]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 65535]


def test_std_msgs_uint32():
    msgtype = "std_msgs/UInt32"
    schema = dedent("""
        uint32 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 123456}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 123456


def test_std_msgs_uint32multiarray():
    msgtype = "std_msgs/UInt32MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        uint32[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [0, 4294967295]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 4294967295]


def test_std_msgs_uint64():
    msgtype = "std_msgs/UInt64"
    schema = dedent("""
        uint64 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 9876543210}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 9876543210


def test_std_msgs_uint64multiarray():
    msgtype = "std_msgs/UInt64MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        uint64[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 2, "stride": 2}],
                "data_offset": 0
            },
            "data": [0, 18446744073709551615]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 18446744073709551615]


def test_std_msgs_uint8():
    msgtype = "std_msgs/UInt8"
    schema = dedent("""
        uint8 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 255}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 255


def test_std_msgs_uint8multiarray():
    msgtype = "std_msgs/UInt8MultiArray"
    schema = dedent("""
        std_msgs/MultiArrayLayout layout
        uint8[] data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        std_msgs/MultiArrayDimension[] dim
        uint32 data_offset
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        string label
        uint32 size
        uint32 stride
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {
            "layout": {
                "dim": [{"label": "x", "size": 3, "stride": 3}],
                "data_offset": 0
            },
            "data": [0, 128, 255]
        }
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 128, 255]
