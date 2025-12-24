"""Test writing std_msgs with pybag and reading with rosbags."""
from pathlib import Path
from tempfile import TemporaryDirectory

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap_writer import McapFileWriter


def _write_pybag(temp_dir: str, msg, topic: str = "/pybag", *, timestamp: int = 0) -> Path:
    mcap_path = Path(temp_dir) / "data.mcap"
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

def test_bool_pybag() -> None:
    msg = std_msgs.Bool(data=True)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Bool'
    assert decoded_msgs[0].data is True


def test_byte_pybag() -> None:
    msg = std_msgs.Byte(data=4)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Byte'
    assert decoded_msgs[0].data == 4


def test_char_pybag() -> None:
    msg = std_msgs.Char(data='A')
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Char'
    assert decoded_msgs[0].data == 65  # ASCII 'A'


def test_color_rgba_pybag() -> None:
    msg = std_msgs.ColorRGBA(r=1.0, g=0.5, b=0.25, a=0.75)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'ColorRGBA'
    assert decoded_msgs[0].r == 1.0
    assert decoded_msgs[0].g == 0.5
    assert decoded_msgs[0].b == 0.25
    assert decoded_msgs[0].a == 0.75


def test_empty_pybag() -> None:
    msg = std_msgs.Empty()
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Empty'


def test_float32_pybag() -> None:
    msg = std_msgs.Float32(data=1.5)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Float32'
    assert decoded_msgs[0].data == 1.5


def test_float64_pybag() -> None:
    msg = std_msgs.Float64(data=3.14159)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Float64'
    assert decoded_msgs[0].data == 3.14159


def test_header_pybag() -> None:
    msg = std_msgs.Header(
        stamp=builtin_interfaces.Time(sec=123, nanosec=456),
        frame_id='test_frame'
    )
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Header'
    assert decoded_msgs[0].stamp.sec == 123
    assert decoded_msgs[0].stamp.nanosec == 456
    assert decoded_msgs[0].frame_id == 'test_frame'


def test_int8_pybag() -> None:
    msg = std_msgs.Int8(data=-128)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int8'
    assert decoded_msgs[0].data == -128


def test_int16_pybag() -> None:
    msg = std_msgs.Int16(data=-32768)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int16'
    assert decoded_msgs[0].data == -32768


def test_int32_pybag() -> None:
    msg = std_msgs.Int32(data=42)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int32'
    assert decoded_msgs[0].data == 42


def test_int64_pybag() -> None:
    msg = std_msgs.Int64(data=-9223372036854775808)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int64'
    assert decoded_msgs[0].data == -9223372036854775808


def test_string_pybag() -> None:
    msg = std_msgs.String(data='hello world')
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'String'
    assert decoded_msgs[0].data == 'hello world'


def test_uint8_pybag() -> None:
    msg = std_msgs.UInt8(data=255)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt8'
    assert decoded_msgs[0].data == 255


def test_uint16_pybag() -> None:
    msg = std_msgs.UInt16(data=65535)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt16'
    assert decoded_msgs[0].data == 65535


def test_uint32_pybag() -> None:
    msg = std_msgs.UInt32(data=4294967295)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt32'
    assert decoded_msgs[0].data == 4294967295


def test_uint64_pybag() -> None:
    msg = std_msgs.UInt64(data=18446744073709551615)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt64'
    assert decoded_msgs[0].data == 18446744073709551615


def test_multi_array_dimension_pybag() -> None:
    msg = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MultiArrayDimension'
    assert decoded_msgs[0].label == 'test'
    assert decoded_msgs[0].size == 10
    assert decoded_msgs[0].stride == 5


def test_multi_array_layout_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    msg = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'MultiArrayLayout'
    assert len(decoded_msgs[0].dim) == 2
    assert decoded_msgs[0].dim[0].label == 'test'
    assert decoded_msgs[0].dim[0].size == 10
    assert decoded_msgs[0].dim[0].stride == 5
    assert decoded_msgs[0].dim[1].label == 'test'
    assert decoded_msgs[0].dim[1].size == 10
    assert decoded_msgs[0].dim[1].stride == 5
    assert decoded_msgs[0].data_offset == 0


def test_byte_multi_array_pybag() -> None:
    # No dim
    layout = std_msgs.MultiArrayLayout(dim=[], data_offset=0)
    msg = std_msgs.ByteMultiArray(layout=layout, data=[b'\x01', b'\x02', b'\x03', b'\x04'])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'ByteMultiArray'
    assert len(decoded_msgs[0].layout.dim) == 0
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 4
    assert list(decoded_msgs[0].data) == [1, 2, 3, 4]

    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.ByteMultiArray(layout=layout, data=[b'\x01', b'\x02', b'\x03', b'\x04'])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'ByteMultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 4
    assert list(decoded_msgs[0].data) == [1, 2, 3, 4]


def test_float32_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Float32MultiArray(layout=layout, data=[1.0, 2.0, 3.0])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Float32MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [1.0, 2.0, 3.0]


def test_float64_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Float64MultiArray(layout=layout, data=[1.1, 2.2, 3.3])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Float64MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [1.1, 2.2, 3.3]


def test_int8_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Int8MultiArray(layout=layout, data=[-128, 0, 127])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int8MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [-128, 0, 127]


def test_int16_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Int16MultiArray(layout=layout, data=[-32768, 0, 32767])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int16MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [-32768, 0, 32767]


def test_int32_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Int32MultiArray(layout=layout, data=[-2147483648, 0, 2147483647])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int32MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [-2147483648, 0, 2147483647]


def test_int64_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.Int64MultiArray(layout=layout, data=[-9223372036854775808, 0, 9223372036854775807])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'Int64MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [-9223372036854775808, 0, 9223372036854775807]


def test_uint8_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.UInt8MultiArray(layout=layout, data=[0, 128, 255])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt8MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [0, 128, 255]


def test_uint16_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.UInt16MultiArray(layout=layout, data=[0, 32768, 65535])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt16MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [0, 32768, 65535]


def test_uint32_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.UInt32MultiArray(layout=layout, data=[0, 2147483648, 4294967295])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt32MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [0, 2147483648, 4294967295]


def test_uint64_multi_array_pybag() -> None:
    dim = std_msgs.MultiArrayDimension(label='test', size=10, stride=5)
    layout = std_msgs.MultiArrayLayout(dim=[dim, dim], data_offset=0)
    msg = std_msgs.UInt64MultiArray(layout=layout, data=[0, 9223372036854775808, 18446744073709551615])
    decoded_msgs = _roundtrip_write(msg)
    assert len(decoded_msgs) == 1
    assert decoded_msgs[0].__name__ == 'UInt64MultiArray'
    assert len(decoded_msgs[0].layout.dim) == 2
    assert decoded_msgs[0].layout.dim[0].label == 'test'
    assert decoded_msgs[0].layout.dim[0].size == 10
    assert decoded_msgs[0].layout.dim[0].stride == 5
    assert decoded_msgs[0].layout.dim[1].label == 'test'
    assert decoded_msgs[0].layout.dim[1].size == 10
    assert decoded_msgs[0].layout.dim[1].stride == 5
    assert decoded_msgs[0].layout.data_offset == 0
    assert len(decoded_msgs[0].data) == 3
    assert list(decoded_msgs[0].data) == [0, 9223372036854775808, 18446744073709551615]
