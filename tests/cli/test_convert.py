"""Tests for the convert CLI command."""

from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag.types as t
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.cli.main import main as cli_main
from pybag.cli.mcap_convert import convert
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import builtin_interfaces


@dataclass(kw_only=True)
class SimpleInt:
    """A simple int32 message compatible with both ROS1 and ROS2."""
    __msg_name__ = 'test_msgs/SimpleInt'
    data: t.int32


@dataclass(kw_only=True)
class SimpleString:
    """A simple string message compatible with both ROS1 and ROS2."""
    __msg_name__ = 'test_msgs/SimpleString'
    data: t.string


@dataclass(kw_only=True)
class CharMessageRos1:
    """Message with ROS1 char field (uint8)."""
    __msg_name__ = 'test_msgs/CharMessage'
    value: t.ros1.char


@dataclass(kw_only=True)
class CharMessageRos2:
    """Message with ROS2 char field (string)."""
    __msg_name__ = 'test_msgs/CharMessage'
    value: t.ros2.char


@dataclass(kw_only=True)
class TimeMessageRos1:
    """Message with ROS1 time field."""
    __msg_name__ = 'test_msgs/TimeMessage'
    stamp: t.ros1.time


@dataclass(kw_only=True)
class DurationMessageRos1:
    """Message with ROS1 duration field."""
    __msg_name__ = 'test_msgs/DurationMessage'
    elapsed: t.ros1.duration


@dataclass(kw_only=True)
class TimeMessageRos2:
    """Message with ROS2 Time field."""
    __msg_name__ = 'test_msgs/TimeMessage'
    stamp: builtin_interfaces.Time


@dataclass(kw_only=True)
class DurationMessageRos2:
    """Message with ROS2 Duration field."""
    __msg_name__ = 'test_msgs/DurationMessage'
    elapsed: builtin_interfaces.Duration



# --- bag to mcap conversion tests ---


def test_convert_bag_to_mcap_basic(tmp_path: Path) -> None:
    """Test bag to mcap conversion with multiple topics."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file with multiple topics
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/topic_a", int(1e9), SimpleInt(data=1))
        writer.write_message("/topic_b", int(2e9), SimpleString(data="hello"))
        writer.write_message("/topic_a", int(3e9), SimpleInt(data=2))

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the mcap content
    with McapFileReader.from_file(mcap_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/topic_a", "/topic_b"}

        msgs_a = list(reader.messages("/topic_a"))
        assert len(msgs_a) == 2
        assert msgs_a[0].data.data == 1
        assert msgs_a[1].data.data == 2

        msgs_b = list(reader.messages("/topic_b"))
        assert len(msgs_b) == 1
        assert msgs_b[0].data.data == "hello"


def test_convert_bag_to_mcap_empty(tmp_path: Path) -> None:
    """Test converting an empty bag file."""
    bag_path = tmp_path / "empty.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create an empty bag file
    with BagFileWriter.open(bag_path) as writer:
        pass  # No messages

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the mcap is valid but empty
    with McapFileReader.from_file(mcap_path) as reader:
        assert reader.get_topics() == []


# --- mcap to bag conversion tests ---


def test_convert_mcap_to_bag_basic(tmp_path: Path) -> None:
    """Test mcap to bag conversion with multiple topics."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an mcap file with multiple topics
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/topic_a", int(1e9), SimpleInt(data=1))
        writer.write_message("/topic_b", int(2e9), SimpleString(data="hello"))
        writer.write_message("/topic_a", int(3e9), SimpleInt(data=2))

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the bag content
    with BagFileReader.from_file(bag_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/topic_a", "/topic_b"}

        msgs_a = list(reader.messages("/topic_a"))
        assert len(msgs_a) == 2
        assert msgs_a[0].data.data == 1
        assert msgs_a[1].data.data == 2

        msgs_b = list(reader.messages("/topic_b"))
        assert len(msgs_b) == 1
        assert msgs_b[0].data.data == "hello"


def test_convert_mcap_to_bag_empty(tmp_path: Path) -> None:
    """Test converting an empty mcap file."""
    mcap_path = tmp_path / "empty.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an empty mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        pass  # No messages

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the bag is valid but empty
    with BagFileReader.from_file(bag_path) as reader:
        assert reader.get_topics() == []


# --- CLI tests ---


def test_cli_convert_bag_to_mcap(tmp_path: Path) -> None:
    """Test CLI conversion from bag to mcap."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/cli_test", int(1e9), SimpleInt(data=99))

    # Convert via CLI
    cli_main(["convert", str(bag_path), "-o", str(mcap_path)])

    # Verify
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/cli_test"))
        assert len(messages) == 1
        assert messages[0].data.data == 99


def test_cli_convert_mcap_to_bag(tmp_path: Path) -> None:
    """Test CLI conversion from mcap to bag."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/cli_test", int(1e9), SimpleInt(data=99))

    # Convert via CLI
    cli_main(["convert", str(mcap_path), "-o", str(bag_path)])

    # Verify
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/cli_test"))
        assert len(messages) == 1
        assert messages[0].data.data == 99


def test_cli_convert_overwrite(tmp_path: Path) -> None:
    """Test CLI conversion with overwrite flag."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    # Create an existing mcap file
    mcap_path.touch()

    # Without overwrite, should fail
    with pytest.raises(ValueError, match="Output file exists"):
        convert(bag_path, mcap_path)

    # With overwrite, should succeed
    convert(bag_path, mcap_path, overwrite=True)
    assert mcap_path.exists()


# --- Error handling tests ---


def test_convert_same_format_error(tmp_path: Path) -> None:
    """Test that converting to the same format raises an error."""
    mcap_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="same"):
        convert(mcap_path, output_path)


def test_convert_same_path_error(tmp_path: Path) -> None:
    """Test that converting to the same path raises an error."""
    mcap_path = tmp_path / "input.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="Input and output paths cannot be the same."):
        convert(mcap_path, mcap_path)


def test_convert_input_not_found(tmp_path: Path) -> None:
    """Test that a missing input file raises an error."""
    with pytest.raises(FileNotFoundError):
        convert(tmp_path / "nonexistent.bag", tmp_path / "output.mcap")


def test_convert_unknown_input_format(tmp_path: Path) -> None:
    """Test that an unknown input format raises an error."""
    input_path = tmp_path / "input.xyz"
    input_path.touch()

    with pytest.raises(ValueError, match="Cannot detect input format"):
        convert(input_path, tmp_path / "output.mcap")


def test_convert_unknown_output_format(tmp_path: Path) -> None:
    """Test that an unknown output format raises an error."""
    bag_path = tmp_path / "input.bag"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="Cannot detect output format"):
        convert(bag_path, tmp_path / "output.xyz")


# --- Compression tests ---


@pytest.mark.parametrize("compression", ["lz4", "zstd"])
def test_convert_bag_to_mcap_with_compression(tmp_path: Path, compression) -> None:
    """Test bag to mcap conversion with different compression options."""
    bag_path = tmp_path / "input.bag"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        for i in range(100):
            writer.write_message("/test", i * int(1e6), SimpleInt(data=i))

    mcap_lz4 = tmp_path / "output_lz4.mcap"
    convert(bag_path, mcap_lz4, mcap_compression=compression, chunk_size=1024)

    with McapFileReader.from_file(mcap_lz4) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 100


def test_convert_mcap_to_bag_with_compression(tmp_path: Path) -> None:
    """Test mcap to bag conversion with bz2 compression."""
    mcap_path = tmp_path / "input.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        for i in range(100):
            writer.write_message("/test", i * int(1e6), SimpleInt(data=i))

    # Convert with bz2 compression
    bag_path = tmp_path / "output.bag"
    convert(mcap_path, bag_path, bag_compression="bz2", chunk_size=1024)

    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 100


# --- Time/Duration/Char type conversion tests ---


def test_convert_bag_to_mcap_with_time(tmp_path: Path) -> None:
    """Test bag to mcap conversion preserves time field values."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create bag with ROS1 time message
    with BagFileWriter.open(bag_path) as writer:
        msg = TimeMessageRos1(stamp=t.ros1.Time(secs=1234567890, nsecs=123456789))
        writer.write_message("/time", int(1e9), msg)

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the time value is preserved (as ROS2 Time)
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/time"))
        assert len(messages) == 1
        assert messages[0].data.stamp.sec == 1234567890
        assert messages[0].data.stamp.nanosec == 123456789


def test_convert_mcap_to_bag_with_time(tmp_path: Path) -> None:
    """Test mcap to bag conversion preserves time field values."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create mcap with ROS2 Time message
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        msg = TimeMessageRos2(stamp=builtin_interfaces.Time(sec=1234567890, nanosec=123456789))
        writer.write_message("/time", int(1e9), msg)

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the time value is preserved (as ROS1 Time)
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/time"))
        assert len(messages) == 1
        assert messages[0].data.stamp.secs == 1234567890
        assert messages[0].data.stamp.nsecs == 123456789


def test_convert_bag_to_mcap_with_duration(tmp_path: Path) -> None:
    """Test bag to mcap conversion preserves duration field values."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create bag with ROS1 duration message
    with BagFileWriter.open(bag_path) as writer:
        msg = DurationMessageRos1(elapsed=t.ros1.Duration(secs=60, nsecs=500000000))
        writer.write_message("/duration", int(1e9), msg)

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the duration value is preserved (as ROS2 Duration)
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/duration"))
        assert len(messages) == 1
        assert messages[0].data.elapsed.sec == 60
        assert messages[0].data.elapsed.nanosec == 500000000


def test_convert_mcap_to_bag_with_duration(tmp_path: Path) -> None:
    """Test mcap to bag conversion preserves duration field values."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create mcap with ROS2 Duration message
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        msg = DurationMessageRos2(elapsed=builtin_interfaces.Duration(sec=60, nanosec=500000000))
        writer.write_message("/duration", int(1e9), msg)

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the duration value is preserved (as ROS1 Duration)
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/duration"))
        assert len(messages) == 1
        assert messages[0].data.elapsed.secs == 60
        assert messages[0].data.elapsed.nsecs == 500000000


def test_convert_bag_to_mcap_with_char(tmp_path: Path) -> None:
    """Test bag to mcap conversion preserves char field values."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create bag with ROS1 char message (uint8)
    with BagFileWriter.open(bag_path) as writer:
        msg = CharMessageRos1(value=65)  # ASCII 'A'
        writer.write_message("/char", int(1e9), msg)

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the char value is preserved (as string in ROS2)
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/char"))
        assert len(messages) == 1
        assert messages[0].data.value == 'A'


def test_convert_mcap_to_bag_with_char(tmp_path: Path) -> None:
    """Test mcap to bag conversion preserves char field values."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create mcap with ROS2 char message (string)
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        msg = CharMessageRos2(value='A')
        writer.write_message("/char", int(1e9), msg)

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the char value is preserved (as uint8 in ROS1)
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/char"))
        assert len(messages) == 1
        assert messages[0].data.value == 65  # ASCII 'A'


def test_convert_roundtrip_char_bag_mcap_bag(tmp_path: Path) -> None:
    """Test that char values are preserved through bag -> mcap -> bag roundtrip."""
    original_bag = tmp_path / "original.bag"
    mcap_path = tmp_path / "intermediate.mcap"
    final_bag = tmp_path / "final.bag"

    # Create original bag with char message
    with BagFileWriter.open(original_bag) as writer:
        msg = CharMessageRos1(value=90)  # ASCII 'Z'
        writer.write_message("/char", int(1e9), msg)

    # Convert bag -> mcap -> bag
    convert(original_bag, mcap_path)
    convert(mcap_path, final_bag)

    # Verify the char value is preserved
    with BagFileReader.from_file(final_bag) as reader:
        messages = list(reader.messages("/char"))
        assert len(messages) == 1
        assert messages[0].data.value == 90


# --- Image message type conversion tests ---


def test_convert_bag_to_mcap_with_compressed_image(tmp_path: Path) -> None:
    """Test bag to mcap conversion with CompressedImage message type."""
    from pybag.ros1.noetic import sensor_msgs, std_msgs

    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Simulated JPEG data (just bytes for testing)
    jpeg_data = [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46]
    header = std_msgs.Header(
        seq=42,
        stamp=t.ros1.Time(secs=1700000000, nsecs=987654321),
        frame_id="compressed_camera"
    )
    msg = sensor_msgs.CompressedImage(
        header=header,
        format="jpeg",
        data=jpeg_data
    )

    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/camera/image/compressed", int(1e9), msg)

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the compressed image data is preserved
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/camera/image/compressed"))
        assert len(messages) == 1
        result = messages[0].data
        assert result.format == "jpeg"
        assert list(result.data) == jpeg_data
        assert result.header.frame_id == "compressed_camera"
        assert result.header.stamp.sec == 1700000000
        assert result.header.stamp.nanosec == 987654321


def test_convert_mcap_to_bag_with_compressed_image(tmp_path: Path) -> None:
    """Test mcap to bag conversion with CompressedImage message type."""
    from pybag.ros2.humble import sensor_msgs, std_msgs

    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Simulated PNG data (just bytes for testing)
    png_data = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
    header = std_msgs.Header(
        stamp=builtin_interfaces.Time(sec=1111111111, nanosec=222222222),
        frame_id="png_camera"
    )
    msg = sensor_msgs.CompressedImage(
        header=header,
        format="png",
        data=png_data
    )

    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/camera/image/compressed", int(1e9), msg)

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the compressed image data is preserved
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/camera/image/compressed"))
        assert len(messages) == 1
        result = messages[0].data
        assert result.format == "png"
        assert list(result.data) == png_data
        assert result.header.frame_id == "png_camera"
        assert result.header.stamp.secs == 1111111111
        assert result.header.stamp.nsecs == 222222222
