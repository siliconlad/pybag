"""Tests for ROS1 profile support."""

from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag
from pybag.mcap_writer import McapFileWriter
from pybag.mcap_reader import McapFileReader
from pybag.serialize import MessageSerializerFactory
from pybag.deserialize import MessageDeserializerFactory


@dataclass
class SimpleMessage:
    __msg_name__ = 'test_msgs/SimpleMessage'
    x: pybag.float64
    y: pybag.float64
    name: pybag.string


def test_ros1_profile_serializer() -> None:
    """Test that ROS1 profile creates correct serializer."""
    serializer = MessageSerializerFactory.from_profile('ros1')
    assert serializer is not None
    assert serializer.message_encoding == 'ros1'
    assert serializer.schema_encoding == 'ros1msg'


def test_ros1_profile_deserializer() -> None:
    """Test that ROS1 profile creates correct deserializer."""
    deserializer = MessageDeserializerFactory.from_profile('ros1')
    assert deserializer is not None


def test_ros1_write_and_read(tmp_path: Path) -> None:
    """Test writing and reading MCAP file with ROS1 profile."""
    file_path = tmp_path / "test_ros1.mcap"

    # Write some messages
    with McapFileWriter.open(file_path, profile="ros1") as writer:
        msg1 = SimpleMessage(x=1.0, y=2.0, name="first")
        msg2 = SimpleMessage(x=3.0, y=4.0, name="second")

        writer.write_message("/test", 1000, msg1)
        writer.write_message("/test", 2000, msg2)

    # Read them back
    with McapFileReader.from_file(file_path) as reader:
        assert reader.profile == "ros1"

        messages = list(reader.messages("/test"))
        assert len(messages) == 2

        assert messages[0].log_time == 1000
        assert messages[0].data.x == 1.0
        assert messages[0].data.y == 2.0
        assert messages[0].data.name == "first"

        assert messages[1].log_time == 2000
        assert messages[1].data.x == 3.0
        assert messages[1].data.y == 4.0
        assert messages[1].data.name == "second"


def test_ros1_invalid_profile() -> None:
    """Test that invalid profile raises error."""
    with pytest.raises(ValueError, match="Unknown encoding type"):
        McapFileWriter.open("test.mcap", profile="invalid_profile")
