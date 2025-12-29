"""Tests to verify type compatibility issues when converting between bag and mcap formats.

These tests document the differences between ROS1 and ROS2 message formats:
1. `char` type: uint8 in ROS1 vs single-character string in ROS2
2. `time`/`duration` types: primitive types in ROS1 (secs/nsecs) vs message types in ROS2 (sec/nanosec)
"""

from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag.types as t
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.cli.mcap_convert import convert
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import builtin_interfaces
from pybag.translate import (
    ros1_duration_to_ros2,
    ros1_time_to_ros2,
    ros2_duration_to_ros1,
    ros2_time_to_ros1,
    translate_ros1_to_ros2,
    translate_ros2_to_ros1
)

# =============================================================================
# Test Message Definitions
# =============================================================================


@dataclass(kw_only=True)
class CharMessageRos1:
    """Message with a char field (ROS1 style - uint8)."""
    __msg_name__ = 'test_msgs/CharMessage'
    value: t.ros1.char  # ROS1 char is uint8


@dataclass(kw_only=True)
class CharMessageRos2:
    """Message with a char field (ROS2 style - single char string)."""
    __msg_name__ = 'test_msgs/CharMessage'
    value: t.ros2.char  # ROS2 char is str


@dataclass(kw_only=True)
class TimeMessage:
    """Message with a time field to test time type compatibility (ROS1 style)."""
    __msg_name__ = 'test_msgs/TimeMessage'
    stamp: t.ros1.time  # ROS1 primitive: Time(secs, nsecs)


@dataclass(kw_only=True)
class DurationMessage:
    """Message with a duration field to test duration type compatibility (ROS1 style)."""
    __msg_name__ = 'test_msgs/DurationMessage'
    elapsed: t.ros1.duration  # ROS1 primitive: Duration(secs, nsecs)


@dataclass(kw_only=True)
class TimeMessageRos2:
    """Message with a Time field using ROS2 builtin_interfaces."""
    __msg_name__ = 'test_msgs/TimeMessageRos2'
    stamp: builtin_interfaces.Time


@dataclass(kw_only=True)
class DurationMessageRos2:
    """Message with a Duration field using ROS2 builtin_interfaces."""
    __msg_name__ = 'test_msgs/DurationMessageRos2'
    elapsed: builtin_interfaces.Duration


# =============================================================================
# Translation Layer Unit Tests
# =============================================================================


class TestTranslationFunctions:
    """Tests for the individual translation functions."""

    def test_ros1_time_to_ros2(self) -> None:
        """Test converting ROS1 Time to ROS2 Time."""
        ros1_time = t.ros1.Time(secs=1234567890, nsecs=123456789)
        ros2_time = ros1_time_to_ros2(ros1_time)

        assert isinstance(ros2_time, builtin_interfaces.Time)
        assert ros2_time.sec == 1234567890
        assert ros2_time.nanosec == 123456789

    def test_ros2_time_to_ros1(self) -> None:
        """Test converting ROS2 Time to ROS1 Time."""
        ros2_time = builtin_interfaces.Time(sec=1234567890, nanosec=123456789)
        ros1_time = ros2_time_to_ros1(ros2_time)

        assert isinstance(ros1_time, t.ros1.Time)
        assert ros1_time.secs == 1234567890
        assert ros1_time.nsecs == 123456789

    def test_ros1_duration_to_ros2(self) -> None:
        """Test converting ROS1 Duration to ROS2 Duration."""
        ros1_duration = t.ros1.Duration(secs=60, nsecs=500000000)
        ros2_duration = ros1_duration_to_ros2(ros1_duration)

        assert isinstance(ros2_duration, builtin_interfaces.Duration)
        assert ros2_duration.sec == 60
        assert ros2_duration.nanosec == 500000000

    def test_ros2_duration_to_ros1(self) -> None:
        """Test converting ROS2 Duration to ROS1 Duration."""
        ros2_duration = builtin_interfaces.Duration(sec=60, nanosec=500000000)
        ros1_duration = ros2_duration_to_ros1(ros2_duration)

        assert isinstance(ros1_duration, t.ros1.Duration)
        assert ros1_duration.secs == 60
        assert ros1_duration.nsecs == 500000000

    def test_translate_ros1_message_to_ros2(self) -> None:
        """Test translating a complete ROS1 message to ROS2 format."""
        ros1_msg = TimeMessage(stamp=t.ros1.Time(secs=1234567890, nsecs=123456789))
        translated = translate_ros1_to_ros2(ros1_msg)

        # The message should be the same type but with converted stamp
        assert type(translated).__name__ == 'TimeMessage'
        assert isinstance(translated.stamp, builtin_interfaces.Time)
        assert translated.stamp.sec == 1234567890
        assert translated.stamp.nanosec == 123456789

    def test_translate_ros2_message_to_ros1(self) -> None:
        """Test translating a complete ROS2 message to ROS1 format."""
        ros2_msg = TimeMessageRos2(stamp=builtin_interfaces.Time(sec=1234567890, nanosec=123456789))
        translated = translate_ros2_to_ros1(ros2_msg)

        # The message should be the same type but with converted stamp
        assert type(translated).__name__ == 'TimeMessageRos2'
        assert isinstance(translated.stamp, t.ros1.Time)
        assert translated.stamp.secs == 1234567890
        assert translated.stamp.nsecs == 123456789

    def test_time_roundtrip_ros1_ros2_ros1(self) -> None:
        """Test that time can be converted ROS1 -> ROS2 -> ROS1 losslessly."""
        original = t.ros1.Time(secs=1234567890, nsecs=123456789)
        ros2 = ros1_time_to_ros2(original)
        back_to_ros1 = ros2_time_to_ros1(ros2)

        assert back_to_ros1 == original

    def test_duration_roundtrip_ros1_ros2_ros1(self) -> None:
        """Test that duration can be converted ROS1 -> ROS2 -> ROS1 losslessly."""
        original = t.ros1.Duration(secs=60, nsecs=500000000)
        ros2 = ros1_duration_to_ros2(original)
        back_to_ros1 = ros2_duration_to_ros1(ros2)

        assert back_to_ros1 == original
