"""Tests for ROS 1 message MD5 sum computation.

Known MD5 sums from official ROS 1 can be verified with `rosmsg md5 <type>`
"""

import hashlib
from dataclasses import dataclass
from typing import Literal

import pytest

import pybag.types as t
from pybag.schema.ros1msg import Ros1MsgSchemaEncoder, compute_md5sum


class TestComputeMD5Sum:
    """Test the compute_md5sum function."""

    def test_simple_message(self):
        """Test MD5 for a simple message with only builtins."""
        # Message like std_msgs/Int32: int32 data
        msg_def = "int32 data"
        md5 = compute_md5sum(msg_def, "std_msgs/Int32")
        expected_md5 = "da5909fbe378aeaf85e547e830cc1bb7"
        assert md5 == expected_md5

    def test_string_message(self):
        """Test MD5 for std_msgs/String."""
        msg_def = "string data"
        md5 = compute_md5sum(msg_def, "std_msgs/String")
        expected_md5 = "992ce8a1687cec8c8bd883ec73ca41d1"
        assert md5 == expected_md5

    def test_multifield_message(self):
        """Test MD5 for message with multiple fields."""
        # Like geometry_msgs/Vector3
        msg_def = "float64 x\nfloat64 y\nfloat64 z"
        md5 = compute_md5sum(msg_def, "geometry_msgs/Vector3")
        expected_md5 = "4a842b65f413084dc2b10fb484ea7f17"
        assert md5 == expected_md5

    def test_header_message(self):
        """Test MD5 for std_msgs/Header."""
        msg_def = "uint32 seq\ntime stamp\nstring frame_id"
        md5 = compute_md5sum(msg_def, "std_msgs/Header")
        expected_md5 = "2176decaecbce78abc3b96ef049fabed"
        assert md5 == expected_md5

    def test_message_with_constant(self):
        """Test MD5 for message with constants.

        Constants should appear first in the MD5 text.

        Example: a hypothetical message
            uint8 TYPE_A=1
            uint8 TYPE_B=2
            uint8 type
        """
        # Order in msg file: constant, constant, field
        msg_def = "uint8 type\nuint8 TYPE_A=1\nuint8 TYPE_B=2"

        # According to ROS 1 algorithm, constants come first in original order
        # Expected MD5 text: "uint8 TYPE_A=1\nuint8 TYPE_B=2\nuint8 type"
        expected_text = "uint8 TYPE_A=1\nuint8 TYPE_B=2\nuint8 type"
        expected_md5 = hashlib.md5(expected_text.encode()).hexdigest()

        md5 = compute_md5sum(msg_def, "test_msgs/ConstantMsg")
        assert md5 == expected_md5

    def test_message_with_nested_type(self):
        """Test MD5 for message with a nested type.

        For nested types, the MD5 of the nested message should replace the type name.
        """
        # Header MD5 = 2176decaecbce78abc3b96ef049fabed
        # For a message that includes Header:
        # The MD5 text should be: "2176decaecbce78abc3b96ef049fabed header"
        header_md5 = "2176decaecbce78abc3b96ef049fabed"
        expected_text = f"{header_md5} header"
        expected_md5 = hashlib.md5(expected_text.encode()).hexdigest()

        msg_def = f"""std_msgs/Header header
================================================================================
MSG: std_msgs/Header
uint32 seq
time stamp
string frame_id"""
        md5 = compute_md5sum(msg_def, "test_msgs/WithHeader")
        assert md5 == expected_md5

    def test_message_with_array(self):
        """Test MD5 for message with array fields.

        Array notation should be preserved in the MD5 text.
        """
        # A message with dynamic array
        msg_def = "float64[] data"
        expected_text = "float64[] data"
        expected_md5 = hashlib.md5(expected_text.encode()).hexdigest()

        md5 = compute_md5sum(msg_def, "test_msgs/Float64Array")
        assert md5 == expected_md5

    def test_message_with_fixed_array(self):
        """Test MD5 for message with fixed-size array fields."""
        msg_def = "float64[3] position"
        expected_text = "float64[3] position"
        expected_md5 = hashlib.md5(expected_text.encode()).hexdigest()

        md5 = compute_md5sum(msg_def, "test_msgs/FixedArray")
        assert md5 == expected_md5

    def test_comments_stripped(self):
        """Test that comments are stripped from MD5 text."""
        msg_def = "int32 data  # this is a comment"
        # Comment should be stripped
        expected_text = "int32 data"
        expected_md5 = hashlib.md5(expected_text.encode()).hexdigest()

        md5 = compute_md5sum(msg_def, "test_msgs/WithComment")
        assert md5 == expected_md5


class TestSchemaEncoderMD5:
    """Test MD5 computation through the schema encoder."""

    def test_simple_dataclass_md5(self):
        """Test MD5 for a dataclass message matches expected."""
        @dataclass(kw_only=True)
        class SimpleInt:
            __msg_name__ = 'test_msgs/SimpleInt'
            data: t.int32

        encoder = Ros1MsgSchemaEncoder()
        msg_def = encoder.encode(SimpleInt).decode('utf-8')
        md5 = compute_md5sum(msg_def, SimpleInt.__msg_name__)

        # Should match std_msgs/Int32 MD5
        expected_md5 = "da5909fbe378aeaf85e547e830cc1bb7"
        assert md5 == expected_md5

    def test_vector3_like_dataclass_md5(self):
        """Test MD5 for a Vector3-like dataclass."""
        @dataclass(kw_only=True)
        class Vector3:
            __msg_name__ = 'geometry_msgs/Vector3'
            x: t.float64
            y: t.float64
            z: t.float64

        encoder = Ros1MsgSchemaEncoder()
        msg_def = encoder.encode(Vector3).decode('utf-8')
        md5 = compute_md5sum(msg_def, Vector3.__msg_name__)

        expected_md5 = "4a842b65f413084dc2b10fb484ea7f17"
        assert md5 == expected_md5
