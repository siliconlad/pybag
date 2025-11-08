"""Test the reading of sensor_msgs messages."""
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pytest
from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from pybag.mcap_reader import McapFileReader


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob('*.mcap'))


def _write_rosbags(
    temp_dir: str,
    msg,
    typestore,
    topic: str = "/rosbags",
    *,
    timestamp: int = 0,
) -> tuple[Path, int]:
    with Writer(Path(temp_dir) / "rosbags", version=9, storage_plugin=StoragePlugin.MCAP) as writer:
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


def test_bool_rosbags(typestore: Typestore):
    Bool = typestore.types['std_msgs/msg/Bool']

    msg = Bool(data=True)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data is True
        assert type(messages[0].data.data) == bool


def test_byte_rosbags(typestore: Typestore):
    Byte = typestore.types['std_msgs/msg/Byte']

    msg = Byte(data=1)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == b'\x01'
        assert type(messages[0].data.data) == bytes


def test_byte_multi_array_rosbags(typestore: Typestore):
    ByteMultiArray = typestore.types['std_msgs/msg/ByteMultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = ByteMultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([1, -2, 3], dtype=np.int8),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [b'\x01', b'\xfe', b'\x03']


def test_char_rosbags(typestore: Typestore):
    Char = typestore.types['std_msgs/msg/Char']

    msg = Char(data=65)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 'A'
        assert type(messages[0].data.data) == str


def test_color_rgba_rosbags(typestore: Typestore):
    ColorRGBA = typestore.types['std_msgs/msg/ColorRGBA']

    msg = ColorRGBA(r=1.0, g=0.5, b=0.25, a=1.0)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.r == 1.0
        assert messages[0].data.g == 0.5
        assert messages[0].data.b == 0.25
        assert messages[0].data.a == 1.0


def test_empty_rosbags(typestore: Typestore):
    Empty = typestore.types['std_msgs/msg/Empty']

    msg = Empty()
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        # Empty messages should return a dataclass instance, not None
        assert messages[0].data is not None
        assert hasattr(messages[0].data, '__msg_name__')
        assert messages[0].data.__msg_name__ == 'std_msgs/msg/Empty'


def test_float32_rosbags(typestore: Typestore):
    Float32 = typestore.types['std_msgs/msg/Float32']

    msg = Float32(data=2.5)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 2.5
        assert type(messages[0].data.data) == float


def test_float32_multi_array_rosbags(typestore: Typestore):
    Float32MultiArray = typestore.types['std_msgs/msg/Float32MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Float32MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([1.0, 2.0, 3.0], dtype=np.float32),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1.0, 2.0, 3.0]


def test_float64_rosbags(typestore: Typestore):
    Float64 = typestore.types['std_msgs/msg/Float64']

    msg = Float64(data=1.5)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 1.5
        assert type(messages[0].data.data) == float


def test_float64_multi_array_rosbags(typestore: Typestore):
    Float64MultiArray = typestore.types['std_msgs/msg/Float64MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Float64MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)], data_offset=0
        ),
        data=np.array([1.0, 2.0, 3.0], dtype=np.float64),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1.0, 2.0, 3.0]


def test_header_rosbags(typestore: Typestore):
    msg = _make_header(typestore)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.frame_id == 'frame'
        assert messages[0].data.stamp.sec == 1
        assert messages[0].data.stamp.nanosec == 2


def test_int16_rosbags(typestore: Typestore):
    Int16 = typestore.types['std_msgs/msg/Int16']

    msg = Int16(data=-1)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == -1
        assert type(messages[0].data.data) == int


def test_int16_multi_array_rosbags(typestore: Typestore):
    Int16MultiArray = typestore.types['std_msgs/msg/Int16MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Int16MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([-1, 2, 3], dtype=np.int16),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [-1, 2, 3]


def test_int32_rosbags(typestore: Typestore):
    Int32 = typestore.types['std_msgs/msg/Int32']

    msg = Int32(data=-2)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == -2
        assert type(messages[0].data.data) == int


def test_int32_multi_array_rosbags(typestore: Typestore):
    Int32MultiArray = typestore.types['std_msgs/msg/Int32MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Int32MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([-1, 2, 3], dtype=np.int32),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [-1, 2, 3]


def test_int64_rosbags(typestore: Typestore):
    Int64 = typestore.types['std_msgs/msg/Int64']

    msg = Int64(data=-4)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == -4
        assert type(messages[0].data.data) == int


def test_int64_multi_array_rosbags(typestore: Typestore):
    Int64MultiArray = typestore.types['std_msgs/msg/Int64MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Int64MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([-1, 2, 3], dtype=np.int64),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [-1, 2, 3]


def test_int8_rosbags(typestore: Typestore):
    Int8 = typestore.types['std_msgs/msg/Int8']

    msg = Int8(data=-8)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == -8
        assert type(messages[0].data.data) == int


def test_int8_multi_array_rosbags(typestore: Typestore):
    Int8MultiArray = typestore.types['std_msgs/msg/Int8MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = Int8MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([-1, 2, 3], dtype=np.int8),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [-1, 2, 3]


def test_multi_array_dimension_rosbags(typestore: Typestore):
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = MultiArrayDimension(label='dim', size=3, stride=2)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.label == 'dim'
        assert messages[0].data.size == 3
        assert messages[0].data.stride == 2


def test_multi_array_layout_rosbags(typestore: Typestore):
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = MultiArrayLayout(
        dim=[MultiArrayDimension(label='dim', size=2, stride=3)],
        data_offset=1
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
        assert messages[0].data.dim[0].label == 'dim'
        assert messages[0].data.dim[0].size == 2
        assert messages[0].data.dim[0].stride == 3
        assert messages[0].data.data_offset == 1


def test_string_rosbags(typestore: Typestore):
    String = typestore.types['std_msgs/msg/String']

    msg = String(data='hello')
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 'hello'
        assert type(messages[0].data.data) == str


def test_uint16_rosbags(typestore: Typestore):
    UInt16 = typestore.types['std_msgs/msg/UInt16']

    msg = UInt16(data=2)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 2
        assert type(messages[0].data.data) == int


def test_uint16_multi_array_rosbags(typestore: Typestore):
    UInt16MultiArray = typestore.types['std_msgs/msg/UInt16MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = UInt16MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([1, 2, 3], dtype=np.uint16),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1, 2, 3]


def test_uint32_rosbags(typestore: Typestore):
    UInt32 = typestore.types['std_msgs/msg/UInt32']

    msg = UInt32(data=3)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 3
        assert type(messages[0].data.data) == int


def test_uint32_multi_array_rosbags(typestore: Typestore):
    UInt32MultiArray = typestore.types['std_msgs/msg/UInt32MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = UInt32MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([1, 2, 3], dtype=np.uint32),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1, 2, 3]


def test_uint64_rosbags(typestore: Typestore):
    UInt64 = typestore.types['std_msgs/msg/UInt64']

    msg = UInt64(data=5)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 5
        assert type(messages[0].data.data) == int


def test_uint64_multi_array_rosbags(typestore: Typestore):
    UInt64MultiArray = typestore.types['std_msgs/msg/UInt64MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = UInt64MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
        data=np.array([1, 2, 3], dtype=np.uint64),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1, 2, 3]


def test_uint8_rosbags(typestore: Typestore):
    UInt8 = typestore.types['std_msgs/msg/UInt8']

    msg = UInt8(data=1)
    with TemporaryDirectory() as temp_dir:
        mcap_file, channel_id = _write_rosbags(temp_dir, msg, typestore)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages('/rosbags'))

        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == channel_id
        assert messages[0].data.data == 1
        assert type(messages[0].data.data) == int


def test_uint8_multi_array_rosbags(typestore: Typestore):
    UInt8MultiArray = typestore.types['std_msgs/msg/UInt8MultiArray']
    MultiArrayLayout = typestore.types['std_msgs/msg/MultiArrayLayout']
    MultiArrayDimension = typestore.types['std_msgs/msg/MultiArrayDimension']

    msg = UInt8MultiArray(
        layout=MultiArrayLayout(
            dim=[MultiArrayDimension(label='dim', size=3, stride=3)],
            data_offset=0
        ),
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
        assert messages[0].data.layout.dim[0].label == 'dim'
        assert messages[0].data.layout.dim[0].size == 3
        assert messages[0].data.layout.dim[0].stride == 3
        assert messages[0].data.layout.data_offset == 0
        assert list(messages[0].data.data) == [1, 2, 3]
