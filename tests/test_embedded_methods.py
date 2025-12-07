"""Tests for embedded encode/decode methods in message classes."""

import pytest

import pybag.types as t
from pybag.encoding.cdr import CdrDecoder, CdrEncoder
from pybag.ros2.humble import builtin_interfaces, geometry_msgs, std_msgs


def test_simple_message_decode():
    """Test decoding a simple message with embedded decode() method."""
    # Create encoder with Time data
    encoder = CdrEncoder(little_endian=True)
    encoder._payload.align(4)
    encoder._payload.write((123).to_bytes(4, 'little', signed=True))  # sec
    encoder._payload.align(4)
    encoder._payload.write((456789).to_bytes(4, 'little', signed=False))  # nanosec
    data = encoder.save()

    # Decode using embedded method
    decoder = CdrDecoder(data)
    time_msg = builtin_interfaces.Time.decode(decoder)

    assert time_msg.sec == 123
    assert time_msg.nanosec == 456789


def test_simple_message_encode():
    """Test encoding a simple message with embedded encode() method."""
    # Create message
    time_msg = builtin_interfaces.Time(sec=123, nanosec=456789)

    # Encode using embedded method
    encoder = CdrEncoder(little_endian=True)
    time_msg.encode(encoder)
    data = encoder.save()

    # Verify by decoding
    decoder = CdrDecoder(data)
    result = builtin_interfaces.Time.decode(decoder)

    assert result.sec == 123
    assert result.nanosec == 456789


def test_message_roundtrip_point():
    """Test encode/decode roundtrip for Point message."""
    # Create message
    point = geometry_msgs.Point(x=1.5, y=2.5, z=3.5)

    # Encode
    encoder = CdrEncoder(little_endian=True)
    point.encode(encoder)
    data = encoder.save()

    # Decode
    decoder = CdrDecoder(data)
    result = geometry_msgs.Point.decode(decoder)

    assert result.x == 1.5
    assert result.y == 2.5
    assert result.z == 3.5


def test_message_with_nested_type():
    """Test message with nested complex type (Header with Time)."""
    # Import at runtime needs to work
    from pybag.ros2.humble import builtin_interfaces

    # Create Header with Time
    header = std_msgs.Header(
        stamp=t.Complex[builtin_interfaces.Time](sec=100, nanosec=200),
        frame_id="map"
    )

    # Encode
    encoder = CdrEncoder(little_endian=True)
    header.encode(encoder)
    data = encoder.save()

    # Decode
    decoder = CdrDecoder(data)
    result = std_msgs.Header.decode(decoder)

    assert result.stamp.sec == 100
    assert result.stamp.nanosec == 200
    assert result.frame_id == "map"


def test_integration_with_serializer():
    """Test that MessageSerializer uses encode() method."""
    from pybag.serialize import MessageSerializerFactory

    serializer = MessageSerializerFactory.from_profile("ros2")
    assert serializer is not None

    # Create a message
    point = geometry_msgs.Point(x=1.0, y=2.0, z=3.0)

    # Serialize (should use embedded encode method)
    data = serializer.serialize_message(point)

    # Verify data is not empty
    assert len(data) > 0


def test_integration_with_deserializer():
    """Test that MessageDeserializer uses decode() method."""
    from pybag.deserialize import MessageDeserializerFactory
    from pybag.mcap.records import MessageRecord, SchemaRecord
    from pybag.serialize import MessageSerializerFactory

    # Create and serialize a message
    serializer = MessageSerializerFactory.from_profile("ros2")
    point = geometry_msgs.Point(x=1.0, y=2.0, z=3.0)
    data = serializer.serialize_message(point)
    schema_data = serializer.serialize_schema(type(point))

    # Create schema and message records
    schema = SchemaRecord(
        id=1,
        name="geometry_msgs/msg/Point",
        encoding="ros2msg",
        data=schema_data
    )

    message = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data
    )

    # Deserialize (should use embedded decode method)
    deserializer = MessageDeserializerFactory.from_profile("ros2")
    assert deserializer is not None

    result = deserializer.deserialize_message(message, schema)

    # Verify
    assert result.x == 1.0
    assert result.y == 2.0
    assert result.z == 3.0
