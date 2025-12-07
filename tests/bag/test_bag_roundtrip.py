"""Tests for ROS 1 bag file reading and writing roundtrip."""

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

import pybag
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter


@dataclass(kw_only=True)
class SimpleMessage:
    """A simple test message."""
    __msg_name__ = 'test_msgs/SimpleMessage'
    value: pybag.int32
    name: pybag.string


@dataclass(kw_only=True)
class PointMessage:
    """A point message with floating point values."""
    __msg_name__ = 'test_msgs/PointMessage'
    x: pybag.float64
    y: pybag.float64
    z: pybag.float64


@dataclass(kw_only=True)
class ArrayMessage:
    """A message with arrays."""
    __msg_name__ = 'test_msgs/ArrayMessage'
    fixed_array: pybag.Array[pybag.int32, Literal[3]]
    dynamic_array: pybag.Array[pybag.float64]


class TestBagWriterReader:
    """Test bag file writing and reading."""

    def test_write_and_read_simple_message(self):
        """Test writing and reading a simple message."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write the bag
            with BagFileWriter.open(temp_path) as writer:
                msg = SimpleMessage(value=42, name="hello")
                writer.write_message("/test", 1000000000, msg)

            # Read the bag
            with BagFileReader.from_file(temp_path) as reader:
                topics = reader.get_topics()
                assert "/test" in topics

                messages = list(reader.messages("/test"))
                assert len(messages) == 1

                decoded = messages[0]
                assert decoded.topic == "/test"
                assert decoded.log_time == 1000000000
                assert decoded.data.value == 42
                assert decoded.data.name == "hello"
        finally:
            temp_path.unlink()

    def test_write_and_read_multiple_messages(self):
        """Test writing and reading multiple messages."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write the bag
            with BagFileWriter.open(temp_path) as writer:
                for i in range(10):
                    msg = SimpleMessage(value=i, name=f"msg_{i}")
                    writer.write_message("/topic", i * 1000000000, msg)

            # Read the bag
            with BagFileReader.from_file(temp_path) as reader:
                messages = list(reader.messages("/topic"))
                assert len(messages) == 10

                for i, decoded in enumerate(messages):
                    assert decoded.data.value == i
                    assert decoded.data.name == f"msg_{i}"
        finally:
            temp_path.unlink()

    def test_write_and_read_multiple_topics(self):
        """Test writing and reading multiple topics."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write the bag
            with BagFileWriter.open(temp_path) as writer:
                writer.write_message("/topic_a", 1000, SimpleMessage(value=1, name="a"))
                writer.write_message("/topic_b", 2000, SimpleMessage(value=2, name="b"))
                writer.write_message("/topic_a", 3000, SimpleMessage(value=3, name="c"))

            # Read the bag
            with BagFileReader.from_file(temp_path) as reader:
                topics = reader.get_topics()
                assert set(topics) == {"/topic_a", "/topic_b"}

                msgs_a = list(reader.messages("/topic_a"))
                assert len(msgs_a) == 2

                msgs_b = list(reader.messages("/topic_b"))
                assert len(msgs_b) == 1
        finally:
            temp_path.unlink()

    def test_write_and_read_point_message(self):
        """Test writing and reading float64 values."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                msg = PointMessage(x=1.5, y=2.5, z=3.5)
                writer.write_message("/point", 1000, msg)

            with BagFileReader.from_file(temp_path) as reader:
                messages = list(reader.messages("/point"))
                assert len(messages) == 1

                point = messages[0].data
                assert point.x == pytest.approx(1.5)
                assert point.y == pytest.approx(2.5)
                assert point.z == pytest.approx(3.5)
        finally:
            temp_path.unlink()

    def test_write_and_read_array_message(self):
        """Test writing and reading array fields."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                msg = ArrayMessage(
                    fixed_array=[1, 2, 3],
                    dynamic_array=[1.1, 2.2, 3.3, 4.4]
                )
                writer.write_message("/arrays", 1000, msg)

            with BagFileReader.from_file(temp_path) as reader:
                messages = list(reader.messages("/arrays"))
                assert len(messages) == 1

                arr = messages[0].data
                assert arr.fixed_array == [1, 2, 3]
                assert len(arr.dynamic_array) == 4
                assert arr.dynamic_array[0] == pytest.approx(1.1)
        finally:
            temp_path.unlink()

    def test_time_filtering(self):
        """Test filtering messages by time."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                for i in range(5):
                    msg = SimpleMessage(value=i, name=f"msg_{i}")
                    writer.write_message("/topic", i * 1000000000, msg)

            with BagFileReader.from_file(temp_path) as reader:
                # Filter by start time
                msgs = list(reader.messages("/topic", start_time=2000000000))
                assert len(msgs) == 3
                assert msgs[0].data.value == 2

                # Filter by end time
                msgs = list(reader.messages("/topic", end_time=2000000000))
                assert len(msgs) == 3
                assert msgs[-1].data.value == 2

                # Filter by both
                msgs = list(reader.messages(
                    "/topic",
                    start_time=1000000000,
                    end_time=3000000000
                ))
                assert len(msgs) == 3
                assert msgs[0].data.value == 1
                assert msgs[-1].data.value == 3
        finally:
            temp_path.unlink()

    def test_topic_glob_pattern(self):
        """Test topic filtering with glob patterns."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                writer.write_message("/sensor/camera", 1000, SimpleMessage(value=1, name="cam"))
                writer.write_message("/sensor/lidar", 2000, SimpleMessage(value=2, name="lidar"))
                writer.write_message("/other", 3000, SimpleMessage(value=3, name="other"))

            with BagFileReader.from_file(temp_path) as reader:
                # Match all sensor topics
                msgs = list(reader.messages("/sensor/*"))
                assert len(msgs) == 2
                values = {m.data.value for m in msgs}
                assert values == {1, 2}
        finally:
            temp_path.unlink()

    def test_get_message_count(self):
        """Test getting message count for a topic."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                for i in range(5):
                    writer.write_message("/count_test", i * 1000, SimpleMessage(value=i, name="x"))

            with BagFileReader.from_file(temp_path) as reader:
                count = reader.get_message_count("/count_test")
                assert count == 5
        finally:
            temp_path.unlink()

    def test_start_end_time(self):
        """Test getting bag start and end times."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path) as writer:
                writer.write_message("/time_test", 1000000000, SimpleMessage(value=1, name="first"))
                writer.write_message("/time_test", 5000000000, SimpleMessage(value=5, name="last"))

            with BagFileReader.from_file(temp_path) as reader:
                assert reader.start_time == 1000000000
                assert reader.end_time == 5000000000
        finally:
            temp_path.unlink()


class TestBagCompression:
    """Test bag file compression options."""

    @pytest.mark.parametrize("compression", ['none', 'bz2'])
    def test_compression_roundtrip(self, compression):
        """Test writing and reading with different compression options."""
        with tempfile.NamedTemporaryFile(suffix='.bag', delete=False) as f:
            temp_path = Path(f.name)

        try:
            with BagFileWriter.open(temp_path, compression=compression) as writer:
                for i in range(10):
                    msg = SimpleMessage(value=i, name=f"compressed_{i}")
                    writer.write_message("/compressed", i * 1000, msg)

            with BagFileReader.from_file(temp_path) as reader:
                messages = list(reader.messages("/compressed"))
                assert len(messages) == 10

                for i, m in enumerate(messages):
                    assert m.data.value == i
        finally:
            temp_path.unlink()
