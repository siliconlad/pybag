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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_byte_multi_array():
    msgtype = "std_msgs/ByteMultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        byte[]            data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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
    schema = ""

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
    # Empty messages should return a dataclass instance, not None
    assert messages[0].data is not None
    assert hasattr(messages[0].data, '__msg_name__')
    assert messages[0].data.__msg_name__ == 'std_msgs/Empty'


def test_std_msgs_float32():
    msgtype = "std_msgs/Float32"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        float32 data
    """)

    with TemporaryDirectory() as temp_dir:
        msg = {"data": 3.5}
        path = _write_mcap(temp_dir, msg, msgtype, schema)
        with McapFileReader.from_file(path) as reader:
            messages = list(reader.messages("/rosbags"))

    assert len(messages) == 1
    assert messages[0].log_time == 0
    assert messages[0].publish_time == 0
    assert messages[0].sequence == 0
    assert messages[0].channel_id == 1
    assert messages[0].data.data == 3.5


def test_std_msgs_float32_multi_array():
    msgtype = "std_msgs/Float32MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        float32[]         data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert messages[0].data.data[0] == 1.5
    assert messages[0].data.data[1] == 2.5


def test_std_msgs_float64():
    msgtype = "std_msgs/Float64"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_float64_multi_array():
    msgtype = "std_msgs/Float64MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        float64[]         data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert messages[0].data.data[0] == 3.14159
    assert messages[0].data.data[1] == 2.71828


def test_std_msgs_header():
    msgtype = "std_msgs/Header"
    schema = dedent("""
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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_int16_multi_array():
    msgtype = "std_msgs/Int16MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        int16[]           data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 3
    assert messages[0].data.layout.dim[0].stride == 3
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-100, 0, 100]


def test_std_msgs_int32():
    msgtype = "std_msgs/Int32"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_int32_multi_array():
    msgtype = "std_msgs/Int32MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        int32[]           data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert list(messages[0].data.data) == [-1000, 2000]


def test_std_msgs_int64():
    msgtype = "std_msgs/Int64"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_int64_multi_array():
    msgtype = "std_msgs/Int64MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        int64[]           data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-9223372036854775807, 9223372036854775807]


def test_std_msgs_int8():
    msgtype = "std_msgs/Int8"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_int8_multi_array():
    msgtype = "std_msgs/Int8MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        int8[]            data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 3
    assert messages[0].data.layout.dim[0].stride == 3
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [-128, 0, 127]


def test_std_msgs_multi_array_dimension():
    msgtype = "std_msgs/MultiArrayDimension"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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


def test_std_msgs_multi_array_layout():
    msgtype = "std_msgs/MultiArrayLayout"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_uint16_multi_array():
    msgtype = "std_msgs/UInt16MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        uint16[]            data        # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 65535]


def test_std_msgs_uint32():
    msgtype = "std_msgs/UInt32"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_uint32_multi_array():
    msgtype = "std_msgs/UInt32MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        uint32[]          data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 4294967295]


def test_std_msgs_uint64():
    msgtype = "std_msgs/UInt64"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_uint64_multi_array():
    msgtype = "std_msgs/UInt64MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        uint64[]          data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 2
    assert messages[0].data.layout.dim[0].stride == 2
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 18446744073709551615]


def test_std_msgs_uint8():
    msgtype = "std_msgs/UInt8"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

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


def test_std_msgs_uint8_multi_array():
    msgtype = "std_msgs/UInt8MultiArray"
    schema = dedent("""
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # Please look at the MultiArrayLayout message definition for
        # documentation on all multiarrays.

        MultiArrayLayout  layout        # specification of data layout
        uint8[]           data          # array of data
        ================================================================================
        MSG: std_msgs/MultiArrayLayout
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        # The multiarray declares a generic multi-dimensional array of a
        # particular data type.  Dimensions are ordered from outer most
        # to inner most.
        #
        # Accessors should ALWAYS be written in terms of dimension stride
        # and specified outer-most dimension first.
        #
        # multiarray(i,j,k) = data[data_offset + dim_stride[1]*i + dim_stride[2]*j + k]
        #
        # A standard, 3-channel 640x480 image with interleaved color channels
        # would be specified as:
        #
        # dim[0].label  = "height"
        # dim[0].size   = 480
        # dim[0].stride = 3*640*480 = 921600  (note dim[0] stride is just size of image)
        # dim[1].label  = "width"
        # dim[1].size   = 640
        # dim[1].stride = 3*640 = 1920
        # dim[2].label  = "channel"
        # dim[2].size   = 3
        # dim[2].stride = 3
        #
        # multiarray(i,j,k) refers to the ith row, jth column, and kth channel.

        MultiArrayDimension[] dim # Array of dimension properties
        uint32 data_offset        # padding bytes at front of data
        ================================================================================
        MSG: std_msgs/MultiArrayDimension
        # This was originally provided as an example message.
        # It is deprecated as of Foxy
        # It is recommended to create your own semantically meaningful message.
        # However if you would like to continue using this please use the equivalent in example_msgs.

        string label   # label of given dimension
        uint32 size    # size of given dimension (in type units)
        uint32 stride  # stride of given dimension
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
    assert len(messages[0].data.layout.dim) == 1
    assert messages[0].data.layout.dim[0].label == "x"
    assert messages[0].data.layout.dim[0].size == 3
    assert messages[0].data.layout.dim[0].stride == 3
    assert messages[0].data.layout.data_offset == 0
    assert list(messages[0].data.data) == [0, 128, 255]
