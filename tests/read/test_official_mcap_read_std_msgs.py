"""Test reading std_msgs messages written with the official MCAP writer."""

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


def test_std_msgs_bool(typestore: Typestore):
    msgtype = "std_msgs/msg/Bool"
    msg = create_message(typestore, msgtype, seed=1)

    schema = (
        "bool data\n"
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


def test_std_msgs_byte(typestore: Typestore):
    msgtype = "std_msgs/msg/Byte"
    msg = create_message(typestore, msgtype, seed=2)

    schema = (
        "byte data\n"
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


def test_std_msgs_bytemultiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/ByteMultiArray"
    msg = create_message(typestore, msgtype, seed=3)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "byte[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_char(typestore: Typestore):
    msgtype = "std_msgs/msg/Char"
    msg = create_message(typestore, msgtype, seed=4)

    schema = (
        "char data\n"
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


def test_std_msgs_colorrgba(typestore: Typestore):
    msgtype = "std_msgs/msg/ColorRGBA"
    msg = create_message(typestore, msgtype, seed=5)

    schema = (
        "float32 r\n"
        "float32 g\n"
        "float32 b\n"
        "float32 a\n"
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


def test_std_msgs_empty(typestore: Typestore):
    msgtype = "std_msgs/msg/Empty"
    msg = create_message(typestore, msgtype, seed=6)

    schema = (
        ""
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


def test_std_msgs_float32(typestore: Typestore):
    msgtype = "std_msgs/msg/Float32"
    msg = create_message(typestore, msgtype, seed=7)

    schema = (
        "float32 data\n"
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


def test_std_msgs_float32multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Float32MultiArray"
    msg = create_message(typestore, msgtype, seed=8)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "float32[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_float64(typestore: Typestore):
    msgtype = "std_msgs/msg/Float64"
    msg = create_message(typestore, msgtype, seed=9)

    schema = (
        "float64 data\n"
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


def test_std_msgs_float64multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Float64MultiArray"
    msg = create_message(typestore, msgtype, seed=10)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "float64[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_header(typestore: Typestore):
    msgtype = "std_msgs/msg/Header"
    msg = create_message(typestore, msgtype, seed=11)

    schema = (
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


def test_std_msgs_int16(typestore: Typestore):
    msgtype = "std_msgs/msg/Int16"
    msg = create_message(typestore, msgtype, seed=12)

    schema = (
        "int16 data\n"
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


def test_std_msgs_int16multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Int16MultiArray"
    msg = create_message(typestore, msgtype, seed=13)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "int16[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_int32(typestore: Typestore):
    msgtype = "std_msgs/msg/Int32"
    msg = create_message(typestore, msgtype, seed=14)

    schema = (
        "int32 data\n"
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


def test_std_msgs_int32multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Int32MultiArray"
    msg = create_message(typestore, msgtype, seed=15)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "int32[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_int64(typestore: Typestore):
    msgtype = "std_msgs/msg/Int64"
    msg = create_message(typestore, msgtype, seed=16)

    schema = (
        "int64 data\n"
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


def test_std_msgs_int64multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Int64MultiArray"
    msg = create_message(typestore, msgtype, seed=17)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "int64[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_int8(typestore: Typestore):
    msgtype = "std_msgs/msg/Int8"
    msg = create_message(typestore, msgtype, seed=18)

    schema = (
        "int8 data\n"
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


def test_std_msgs_int8multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/Int8MultiArray"
    msg = create_message(typestore, msgtype, seed=19)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "int8[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_multiarraydimension(typestore: Typestore):
    msgtype = "std_msgs/msg/MultiArrayDimension"
    msg = create_message(typestore, msgtype, seed=20)

    schema = (
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_multiarraylayout(typestore: Typestore):
    msgtype = "std_msgs/msg/MultiArrayLayout"
    msg = create_message(typestore, msgtype, seed=21)

    schema = (
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_string(typestore: Typestore):
    msgtype = "std_msgs/msg/String"
    msg = create_message(typestore, msgtype, seed=22)

    schema = (
        "string data\n"
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


def test_std_msgs_uint16(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt16"
    msg = create_message(typestore, msgtype, seed=23)

    schema = (
        "uint16 data\n"
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


def test_std_msgs_uint16multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt16MultiArray"
    msg = create_message(typestore, msgtype, seed=24)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "uint16[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_uint32(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt32"
    msg = create_message(typestore, msgtype, seed=25)

    schema = (
        "uint32 data\n"
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


def test_std_msgs_uint32multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt32MultiArray"
    msg = create_message(typestore, msgtype, seed=26)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "uint32[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_uint64(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt64"
    msg = create_message(typestore, msgtype, seed=27)

    schema = (
        "uint64 data\n"
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


def test_std_msgs_uint64multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt64MultiArray"
    msg = create_message(typestore, msgtype, seed=28)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "uint64[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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


def test_std_msgs_uint8(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt8"
    msg = create_message(typestore, msgtype, seed=29)

    schema = (
        "uint8 data\n"
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


def test_std_msgs_uint8multiarray(typestore: Typestore):
    msgtype = "std_msgs/msg/UInt8MultiArray"
    msg = create_message(typestore, msgtype, seed=30)

    schema = (
        "std_msgs/MultiArrayLayout layout\n"
        "uint8[] data\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayLayout\n"
        "std_msgs/MultiArrayDimension[] dim\n"
        "uint32 data_offset\n"
        "================================================================================\n"
        "MSG: std_msgs/MultiArrayDimension\n"
        "string label\n"
        "uint32 size\n"
        "uint32 stride\n"
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

