"""Tests for pre-compiled message encoders/decoders."""

import pytest

import pybag.types as t
from pybag import precompiled
from pybag.encoding.cdr import CdrDecoder, CdrEncoder
from pybag.ros2.humble import builtin_interfaces, geometry_msgs, std_msgs
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder


def test_precompiled_decoder_available():
    """Test that pre-compiled decoders are available for standard messages."""
    # Test a few common message types
    assert precompiled.get_decoder('std_msgs/msg/Header') is not None
    assert precompiled.get_decoder('geometry_msgs/msg/Point') is not None
    assert precompiled.get_decoder('geometry_msgs/msg/Pose') is not None
    assert precompiled.get_decoder('sensor_msgs/msg/Image') is not None


def test_precompiled_encoder_available():
    """Test that pre-compiled encoders are available for standard messages."""
    # Test a few common message types
    assert precompiled.get_encoder('std_msgs/msg/Header') is not None
    assert precompiled.get_encoder('geometry_msgs/msg/Point') is not None
    assert precompiled.get_encoder('geometry_msgs/msg/Pose') is not None
    assert precompiled.get_encoder('sensor_msgs/msg/Image') is not None


def test_precompiled_not_available_for_custom_messages():
    """Test that pre-compiled functions return None for non-standard messages."""
    assert precompiled.get_decoder('custom_msgs/msg/CustomMessage') is None
    assert precompiled.get_encoder('custom_msgs/msg/CustomMessage') is None


def test_precompiled_header_roundtrip():
    """Test serialization and deserialization of Header message."""
    # Create a Header message
    header = std_msgs.Header(
        stamp=t.Complex[builtin_interfaces.Time](sec=123, nanosec=456789),
        frame_id="base_link"
    )

    # Get pre-compiled encoder and decoder
    encoder_func = precompiled.get_encoder('std_msgs/msg/Header')
    decoder_func = precompiled.get_decoder('std_msgs/msg/Header')

    assert encoder_func is not None
    assert decoder_func is not None

    # Serialize
    enc = CdrEncoder(little_endian=True)
    encoder_func(enc, header)
    data = enc.save()

    # Deserialize
    dec = CdrDecoder(data)
    result = decoder_func(dec)

    # Verify
    assert result.stamp.sec == 123
    assert result.stamp.nanosec == 456789
    assert result.frame_id == "base_link"


def test_precompiled_point_roundtrip():
    """Test serialization and deserialization of Point message."""
    # Create a Point message
    point = geometry_msgs.Point(x=1.5, y=2.5, z=3.5)

    # Get pre-compiled encoder and decoder
    encoder_func = precompiled.get_encoder('geometry_msgs/msg/Point')
    decoder_func = precompiled.get_decoder('geometry_msgs/msg/Point')

    assert encoder_func is not None
    assert decoder_func is not None

    # Serialize
    enc = CdrEncoder(little_endian=True)
    encoder_func(enc, point)
    data = enc.save()

    # Deserialize
    dec = CdrDecoder(data)
    result = decoder_func(dec)

    # Verify
    assert result.x == 1.5
    assert result.y == 2.5
    assert result.z == 3.5


def test_precompiled_pose_roundtrip():
    """Test serialization and deserialization of Pose message with nested types."""
    # Create a Pose message
    pose = geometry_msgs.Pose(
        position=t.Complex[geometry_msgs.Point](x=1.0, y=2.0, z=3.0),
        orientation=t.Complex[geometry_msgs.Quaternion](x=0.0, y=0.0, z=0.0, w=1.0)
    )

    # Get pre-compiled encoder and decoder
    encoder_func = precompiled.get_encoder('geometry_msgs/msg/Pose')
    decoder_func = precompiled.get_decoder('geometry_msgs/msg/Pose')

    assert encoder_func is not None
    assert decoder_func is not None

    # Serialize
    enc = CdrEncoder(little_endian=True)
    encoder_func(enc, pose)
    data = enc.save()

    # Deserialize
    dec = CdrDecoder(data)
    result = decoder_func(dec)

    # Verify
    assert result.position.x == 1.0
    assert result.position.y == 2.0
    assert result.position.z == 3.0
    assert result.orientation.x == 0.0
    assert result.orientation.y == 0.0
    assert result.orientation.z == 0.0
    assert result.orientation.w == 1.0


def test_precompiled_pose_stamped_roundtrip():
    """Test serialization and deserialization of PoseStamped message with deeply nested types."""
    # Create a PoseStamped message
    pose_stamped = geometry_msgs.PoseStamped(
        header=t.Complex[std_msgs.Header](
            stamp=t.Complex[builtin_interfaces.Time](sec=100, nanosec=200),
            frame_id="map"
        ),
        pose=t.Complex[geometry_msgs.Pose](
            position=t.Complex[geometry_msgs.Point](x=1.0, y=2.0, z=3.0),
            orientation=t.Complex[geometry_msgs.Quaternion](x=0.0, y=0.0, z=0.0, w=1.0)
        )
    )

    # Get pre-compiled encoder and decoder
    encoder_func = precompiled.get_encoder('geometry_msgs/msg/PoseStamped')
    decoder_func = precompiled.get_decoder('geometry_msgs/msg/PoseStamped')

    assert encoder_func is not None
    assert decoder_func is not None

    # Serialize
    enc = CdrEncoder(little_endian=True)
    encoder_func(enc, pose_stamped)
    data = enc.save()

    # Deserialize
    dec = CdrDecoder(data)
    result = decoder_func(dec)

    # Verify
    assert result.header.stamp.sec == 100
    assert result.header.stamp.nanosec == 200
    assert result.header.frame_id == "map"
    assert result.pose.position.x == 1.0
    assert result.pose.position.y == 2.0
    assert result.pose.position.z == 3.0
    assert result.pose.orientation.x == 0.0
    assert result.pose.orientation.y == 0.0
    assert result.pose.orientation.z == 0.0
    assert result.pose.orientation.w == 1.0


def test_integration_with_serializer():
    """Test that the MessageSerializer uses pre-compiled functions."""
    from pybag.serialize import MessageSerializerFactory

    serializer = MessageSerializerFactory.from_profile("ros2")
    assert serializer is not None

    # Create a message
    point = geometry_msgs.Point(x=1.0, y=2.0, z=3.0)

    # Serialize (should use pre-compiled encoder)
    data = serializer.serialize_message(point)

    # Verify data is not empty
    assert len(data) > 0


def test_integration_with_deserializer():
    """Test that the MessageDeserializer uses pre-compiled functions."""
    from pybag.mcap.records import MessageRecord, SchemaRecord
    from pybag.deserialize import MessageDeserializerFactory
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

    # Deserialize (should use pre-compiled decoder)
    deserializer = MessageDeserializerFactory.from_profile("ros2")
    assert deserializer is not None

    result = deserializer.deserialize_message(message, schema)

    # Verify
    assert result.x == 1.0
    assert result.y == 2.0
    assert result.z == 3.0
