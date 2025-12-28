"""Tests for ROS 1 bag file reading and writing roundtrip."""

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

import pybag.types as t
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter


@dataclass(kw_only=True)
class SimpleMessage:
    """A simple test message."""
    __msg_name__ = 'test_msgs/SimpleMessage'
    value: t.int32
    name: t.string


@dataclass(kw_only=True)
class PointMessage:
    """A point message with floating point values."""
    __msg_name__ = 'test_msgs/PointMessage'
    x: t.float64
    y: t.float64
    z: t.float64


@dataclass(kw_only=True)
class ArrayMessage:
    """A message with arrays."""
    __msg_name__ = 'test_msgs/ArrayMessage'
    fixed_array: t.Array[t.int32, Literal[3]]
    dynamic_array: t.Array[t.float64]


@dataclass(kw_only=True)
class TimeMessage:
    """A message with ROS 1 time type."""
    __msg_name__ = 'test_msgs/TimeMessage'
    stamp: t.ros1.time


@dataclass(kw_only=True)
class DurationMessage:
    """A message with ROS 1 duration type."""
    __msg_name__ = 'test_msgs/DurationMessage'
    elapsed: t.ros1.duration


@dataclass(kw_only=True)
class CharMessage:
    """A message with char type."""
    __msg_name__ = 'test_msgs/CharMessage'
    character: t.ros1.char


def test_write_and_read_simple_message(tmp_path: Path):
    """Test writing and reading a simple message."""
    # Write the bag
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = SimpleMessage(value=42, name="hello")
        writer.write_message("/test", 1_000_000_000, msg)

    # Read the bag
    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        topics = reader.get_topics()
        assert ["/test"] == topics

        messages = list(reader.messages("/test"))
        assert len(messages) == 1

        assert messages[0].topic == "/test"
        assert messages[0].log_time == 1_000_000_000
        assert messages[0].data.value == 42
        assert messages[0].data.name == "hello"


def test_write_and_read_multiple_messages(tmp_path: Path):
    """Test writing and reading multiple messages."""
    # Write the bag
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        for i in range(10):
            msg = SimpleMessage(value=i, name=f"msg_{i}")
            writer.write_message("/topic", i * 1000000000, msg)

    # Read the bag
    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        topics = reader.get_topics()
        assert ["/topic"] == topics

        messages = list(reader.messages("/topic"))
        assert len(messages) == 10

        for i, decoded in enumerate(messages):
            assert decoded.data.value == i
            assert decoded.data.name == f"msg_{i}"


def test_write_and_read_multiple_topics(tmp_path: Path):
    """Test writing and reading multiple topics."""
    # Write the bag
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        writer.write_message("/topic_a", 1000, SimpleMessage(value=1, name="a"))
        writer.write_message("/topic_b", 2000, SimpleMessage(value=2, name="b"))
        writer.write_message("/topic_a", 3000, SimpleMessage(value=3, name="c"))

    # Read the bag
    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        topics = reader.get_topics()
        assert set(topics) == {"/topic_a", "/topic_b"}

        msgs_a = list(reader.messages("/topic_a"))
        assert len(msgs_a) == 2

        msgs_b = list(reader.messages("/topic_b"))
        assert len(msgs_b) == 1


def test_write_and_read_point_message(tmp_path: Path):
    """Test writing and reading float64 values."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = PointMessage(x=1.5, y=2.5, z=3.5)
        writer.write_message("/point", 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/point"))
        assert len(messages) == 1

        point = messages[0].data
        assert point.x == pytest.approx(1.5)
        assert point.y == pytest.approx(2.5)
        assert point.z == pytest.approx(3.5)


def test_write_and_read_array_message(tmp_path: Path):
    """Test writing and reading array fields."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = ArrayMessage(
            fixed_array=[1, 2, 3],
            dynamic_array=[1.1, 2.2, 3.3, 4.4]
        )
        writer.write_message("/arrays", 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/arrays"))
        assert len(messages) == 1

        arr = messages[0].data
        assert arr.fixed_array == [1, 2, 3]
        assert len(arr.dynamic_array) == 4
        assert arr.dynamic_array[0] == pytest.approx(1.1)
        assert arr.dynamic_array[1] == pytest.approx(2.2)
        assert arr.dynamic_array[2] == pytest.approx(3.3)
        assert arr.dynamic_array[3] == pytest.approx(4.4)


def test_time_filtering(tmp_path: Path):
    """Test filtering messages by time."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        for i in range(5):
            msg = SimpleMessage(value=i, name=f"msg_{i}")
            writer.write_message("/topic", i * 1_000_000_000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        # Filter by start time
        msgs = list(reader.messages("/topic", start_time=2_000_000_000))
        assert len(msgs) == 3
        assert msgs[0].data.value == 2
        assert msgs[1].data.value == 3
        assert msgs[2].data.value == 4

        # Filter by end time
        msgs = list(reader.messages("/topic", end_time=2_000_000_000))
        assert len(msgs) == 3
        assert msgs[0].data.value == 0
        assert msgs[1].data.value == 1
        assert msgs[2].data.value == 2

        # Filter by both
        msgs = list(reader.messages("/topic", start_time=1_000_000_000, end_time=3_000_000_000))
        assert len(msgs) == 3
        assert msgs[0].data.value == 1
        assert msgs[1].data.value == 2
        assert msgs[2].data.value == 3


def test_topic_glob_pattern(tmp_path: Path):
    """Test topic filtering with glob patterns."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        writer.write_message("/sensor/camera", 1000, SimpleMessage(value=1, name="cam"))
        writer.write_message("/sensor/lidar", 2000, SimpleMessage(value=2, name="lidar"))
        writer.write_message("/other", 3000, SimpleMessage(value=3, name="other"))

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        # Match all sensor topics
        msgs = list(reader.messages("/sensor/*"))
        assert len(msgs) == 2
        assert msgs[0].data.value == 1
        assert msgs[1].data.value == 2


def test_get_message_count(tmp_path: Path):
    """Test getting message count for a topic."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        for i in range(5):
            writer.write_message("/count_test", i * 1000, SimpleMessage(value=i, name="x"))

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        count = reader.get_message_count("/count_test")
        assert count == 5


def test_start_end_time(tmp_path: Path):
    """Test getting bag start and end times."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        writer.write_message("/time_test", 1000000000, SimpleMessage(value=1, name="first"))
        writer.write_message("/time_test", 5000000000, SimpleMessage(value=5, name="last"))

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        assert reader.start_time == 1000000000
        assert reader.end_time == 5000000000


@pytest.mark.parametrize("compression", ['none', 'bz2'])
def test_compression_roundtrip(tmp_path: Path, compression):
    """Test writing and reading with different compression options."""
    with BagFileWriter.open(tmp_path / 'test.bag', compression=compression) as writer:
        for i in range(10):
            msg = SimpleMessage(value=i, name=f"compressed_{i}")
            writer.write_message("/compressed", i * 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/compressed"))
        assert len(messages) == 10

        for i, m in enumerate(messages):
            assert m.data.value == i


def test_write_and_read_time_message(tmp_path: Path):
    """Test writing and reading a message with ROS 1 time type."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = TimeMessage(stamp=t.ros1.Time(secs=1234567890, nsecs=123456789))
        writer.write_message("/time", 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/time"))
        assert len(messages) == 1

        assert messages[0].data.stamp == t.ros1.Time(secs=1234567890, nsecs=123456789)
        assert messages[0].data.stamp.secs == 1234567890
        assert messages[0].data.stamp.nsecs == 123456789


def test_write_and_read_duration_message(tmp_path: Path):
    """Test writing and reading a message with ROS 1 duration type."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = DurationMessage(elapsed=t.ros1.Duration(secs=-100, nsecs=500000000))
        writer.write_message("/duration", 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/duration"))
        assert len(messages) == 1

        assert messages[0].data.elapsed == t.ros1.Duration(secs=-100, nsecs=500000000)
        assert messages[0].data.elapsed.secs == -100
        assert messages[0].data.elapsed.nsecs == 500000000


def test_write_and_read_char_message(tmp_path: Path):
    """Test writing and reading a message with ROS 1 char type (uint8)."""
    with BagFileWriter.open(tmp_path / 'test.bag') as writer:
        msg = CharMessage(character=65)  # 'A' as uint8
        writer.write_message("/char", 1000, msg)

    with BagFileReader.from_file(tmp_path / 'test.bag') as reader:
        messages = list(reader.messages("/char"))
        assert len(messages) == 1

        assert messages[0].data.character == 65
