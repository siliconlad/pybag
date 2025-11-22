"""Tests for ROS1 encoding."""

import pytest

from pybag.encoding.ros1 import Ros1Decoder, Ros1Encoder


def test_ros1_primitives() -> None:
    """Test encoding and decoding of primitive types."""
    encoder = Ros1Encoder()

    # Test bool
    encoder.bool(True)
    encoder.bool(False)

    # Test integers
    encoder.int8(-128)
    encoder.uint8(255)
    encoder.int16(-32768)
    encoder.uint16(65535)
    encoder.int32(-2147483648)
    encoder.uint32(4294967295)
    encoder.int64(-9223372036854775808)
    encoder.uint64(18446744073709551615)

    # Test floats
    encoder.float32(3.14)
    encoder.float64(3.14159265359)

    # Test string
    encoder.string("hello")

    # Decode and verify
    data = encoder.save()
    decoder = Ros1Decoder(data)

    assert decoder.bool() is True
    assert decoder.bool() is False

    assert decoder.int8() == -128
    assert decoder.uint8() == 255
    assert decoder.int16() == -32768
    assert decoder.uint16() == 65535
    assert decoder.int32() == -2147483648
    assert decoder.uint32() == 4294967295
    assert decoder.int64() == -9223372036854775808
    assert decoder.uint64() == 18446744073709551615

    assert abs(decoder.float32() - 3.14) < 0.01
    assert abs(decoder.float64() - 3.14159265359) < 0.0000001

    assert decoder.string() == "hello"


def test_ros1_string() -> None:
    """Test string encoding/decoding."""
    encoder = Ros1Encoder()
    encoder.string("test string")
    encoder.string("")
    encoder.string("with unicode: 你好")

    data = encoder.save()
    decoder = Ros1Decoder(data)

    assert decoder.string() == "test string"
    assert decoder.string() == ""
    assert decoder.string() == "with unicode: 你好"


def test_ros1_array() -> None:
    """Test array encoding/decoding."""
    encoder = Ros1Encoder()
    encoder.array("int32", [1, 2, 3, 4, 5])
    encoder.array("float64", [1.1, 2.2, 3.3])

    data = encoder.save()
    decoder = Ros1Decoder(data)

    assert decoder.array("int32", 5) == [1, 2, 3, 4, 5]
    result = decoder.array("float64", 3)
    assert len(result) == 3
    assert abs(result[0] - 1.1) < 0.0001
    assert abs(result[1] - 2.2) < 0.0001
    assert abs(result[2] - 3.3) < 0.0001


def test_ros1_sequence() -> None:
    """Test sequence encoding/decoding."""
    encoder = Ros1Encoder()
    encoder.sequence("int32", [10, 20, 30])
    encoder.sequence("string", ["hello", "world"])
    encoder.sequence("float32", [])

    data = encoder.save()
    decoder = Ros1Decoder(data)

    assert decoder.sequence("int32") == [10, 20, 30]
    assert decoder.sequence("string") == ["hello", "world"]
    assert decoder.sequence("float32") == []


def test_ros1_encoding() -> None:
    """Test that encoding() returns correct value."""
    assert Ros1Encoder.encoding() == "ros1"
