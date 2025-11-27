"""Integration tests for JSON serialization and deserialization."""

from dataclasses import dataclass
from typing import Annotated

import pytest

from pybag.deserialize import JsonMessageDeserializer, MessageDeserializerFactory
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder, Ros2MsgSchemaEncoder
from pybag.serialize import JsonMessageSerializer, MessageSerializerFactory
from pybag.types import (
    Array,
    Complex,
    Message,
    bool as msg_bool,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    string,
    uint8,
    uint16,
    uint32,
    uint64,
)


# Test message types ---------------------------------------------------------


@dataclass(kw_only=True)
class SimplePoint(Message):
    __msg_name__ = "test_msgs/SimplePoint"
    x: float64
    y: float64
    z: float64


@dataclass(kw_only=True)
class AllPrimitives(Message):
    __msg_name__ = "test_msgs/AllPrimitives"
    bool_val: msg_bool
    int8_val: int8
    uint8_val: uint8
    int16_val: int16
    uint16_val: uint16
    int32_val: int32
    uint32_val: uint32
    int64_val: int64
    uint64_val: uint64
    float32_val: float32
    float64_val: float64
    string_val: string


@dataclass(kw_only=True)
class WithArray(Message):
    __msg_name__ = "test_msgs/WithArray"
    values: Annotated[list[float64], ("array", float64, 3)]


@dataclass(kw_only=True)
class WithSequence(Message):
    __msg_name__ = "test_msgs/WithSequence"
    values: Annotated[list[int32], ("array", int32, None)]


@dataclass(kw_only=True)
class Nested(Message):
    __msg_name__ = "test_msgs/Nested"
    point: Annotated[SimplePoint, ("complex", "test_msgs/SimplePoint")]
    label: string


# Factory tests --------------------------------------------------------------


def test_serializer_factory_from_profile_json() -> None:
    """Test that factory creates JsonMessageSerializer for 'json' profile."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None
    assert isinstance(serializer, JsonMessageSerializer)
    assert serializer.message_encoding == "json"


def test_deserializer_factory_from_profile_json() -> None:
    """Test that factory creates JsonMessageDeserializer for 'json' profile."""
    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    assert isinstance(deserializer, JsonMessageDeserializer)


def test_serializer_factory_from_channel_json() -> None:
    """Test factory creates JsonMessageSerializer for JSON channel."""
    channel = ChannelRecord(
        id=1,
        schema_id=1,
        topic="/test",
        message_encoding="json",
        metadata={},
    )
    schema = SchemaRecord(
        id=1,
        name="test_msgs/SimplePoint",
        encoding="ros2msg",
        data=b"float64 x\nfloat64 y\nfloat64 z\n",
    )
    serializer = MessageSerializerFactory.from_channel(channel, schema)
    assert serializer is not None
    assert isinstance(serializer, JsonMessageSerializer)


def test_deserializer_factory_from_channel_json() -> None:
    """Test factory creates JsonMessageDeserializer for JSON channel."""
    channel = ChannelRecord(
        id=1,
        schema_id=1,
        topic="/test",
        message_encoding="json",
        metadata={},
    )
    schema = SchemaRecord(
        id=1,
        name="test_msgs/SimplePoint",
        encoding="ros2msg",
        data=b"float64 x\nfloat64 y\nfloat64 z\n",
    )
    deserializer = MessageDeserializerFactory.from_channel(channel, schema)
    assert deserializer is not None
    assert isinstance(deserializer, JsonMessageDeserializer)


# Roundtrip tests ------------------------------------------------------------


def test_roundtrip_simple_point() -> None:
    """Test roundtrip serialization of a simple message."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = SimplePoint(x=1.0, y=2.0, z=3.0)
    data = serializer.serialize_message(msg)

    # Create schema and message records for deserialization
    schema = SchemaRecord(
        id=1,
        name="test_msgs/SimplePoint",
        encoding="ros2msg",
        data=serializer.serialize_schema(SimplePoint),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.x == 1.0
    assert decoded.y == 2.0
    assert decoded.z == 3.0


def test_roundtrip_all_primitives() -> None:
    """Test roundtrip serialization of all primitive types."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = AllPrimitives(
        bool_val=True,
        int8_val=-42,
        uint8_val=200,
        int16_val=-1000,
        uint16_val=50000,
        int32_val=-100000,
        uint32_val=3000000000,
        int64_val=-9000000000000000000,
        uint64_val=18000000000000000000,
        float32_val=3.14,
        float64_val=2.718281828459045,
        string_val="hello world",
    )
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/AllPrimitives",
        encoding="ros2msg",
        data=serializer.serialize_schema(AllPrimitives),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.bool_val is True
    assert decoded.int8_val == -42
    assert decoded.uint8_val == 200
    assert decoded.int16_val == -1000
    assert decoded.uint16_val == 50000
    assert decoded.int32_val == -100000
    assert decoded.uint32_val == 3000000000
    assert decoded.int64_val == -9000000000000000000
    assert decoded.uint64_val == 18000000000000000000
    assert abs(decoded.float32_val - 3.14) < 1e-6
    assert decoded.float64_val == 2.718281828459045
    assert decoded.string_val == "hello world"


def test_roundtrip_with_array() -> None:
    """Test roundtrip serialization of message with fixed-size array."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = WithArray(values=[1.0, 2.0, 3.0])
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/WithArray",
        encoding="ros2msg",
        data=serializer.serialize_schema(WithArray),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.values == [1.0, 2.0, 3.0]


def test_roundtrip_with_sequence() -> None:
    """Test roundtrip serialization of message with variable-size sequence."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = WithSequence(values=[10, 20, 30, 40, 50])
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/WithSequence",
        encoding="ros2msg",
        data=serializer.serialize_schema(WithSequence),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.values == [10, 20, 30, 40, 50]


def test_roundtrip_nested_message() -> None:
    """Test roundtrip serialization of nested message."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = Nested(
        point=SimplePoint(x=1.0, y=2.0, z=3.0),
        label="test",
    )
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/Nested",
        encoding="ros2msg",
        data=serializer.serialize_schema(Nested),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.point.x == 1.0
    assert decoded.point.y == 2.0
    assert decoded.point.z == 3.0
    assert decoded.label == "test"


def test_roundtrip_empty_string() -> None:
    """Test roundtrip with empty string."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = AllPrimitives(
        bool_val=False,
        int8_val=0,
        uint8_val=0,
        int16_val=0,
        uint16_val=0,
        int32_val=0,
        uint32_val=0,
        int64_val=0,
        uint64_val=0,
        float32_val=0.0,
        float64_val=0.0,
        string_val="",
    )
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/AllPrimitives",
        encoding="ros2msg",
        data=serializer.serialize_schema(AllPrimitives),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.string_val == ""


def test_roundtrip_empty_sequence() -> None:
    """Test roundtrip with empty sequence."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg = WithSequence(values=[])
    data = serializer.serialize_message(msg)

    schema = SchemaRecord(
        id=1,
        name="test_msgs/WithSequence",
        encoding="ros2msg",
        data=serializer.serialize_schema(WithSequence),
    )
    message_record = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=data,
    )

    deserializer = MessageDeserializerFactory.from_profile("json")
    assert deserializer is not None
    decoded = deserializer.deserialize_message(message_record, schema)

    assert decoded.values == []


def test_serializer_caches_compiled_functions() -> None:
    """Test that the serializer caches compiled functions."""
    serializer = MessageSerializerFactory.from_profile("json")
    assert serializer is not None

    msg1 = SimplePoint(x=1.0, y=2.0, z=3.0)
    msg2 = SimplePoint(x=4.0, y=5.0, z=6.0)

    # Both serializations should use the same compiled function
    data1 = serializer.serialize_message(msg1)
    data2 = serializer.serialize_message(msg2)

    assert len(serializer._compiled) == 1


def test_deserializer_caches_compiled_functions() -> None:
    """Test that the deserializer caches compiled functions."""
    serializer = MessageSerializerFactory.from_profile("json")
    deserializer = MessageDeserializerFactory.from_profile("json")
    assert serializer is not None
    assert deserializer is not None

    schema = SchemaRecord(
        id=1,
        name="test_msgs/SimplePoint",
        encoding="ros2msg",
        data=serializer.serialize_schema(SimplePoint),
    )

    msg1 = SimplePoint(x=1.0, y=2.0, z=3.0)
    msg2 = SimplePoint(x=4.0, y=5.0, z=6.0)

    record1 = MessageRecord(
        channel_id=1,
        sequence=0,
        log_time=0,
        publish_time=0,
        data=serializer.serialize_message(msg1),
    )
    record2 = MessageRecord(
        channel_id=1,
        sequence=1,
        log_time=0,
        publish_time=0,
        data=serializer.serialize_message(msg2),
    )

    # Both deserializations should use the same compiled function
    decoded1 = deserializer.deserialize_message(record1, schema)
    decoded2 = deserializer.deserialize_message(record2, schema)

    assert len(deserializer._compiled) == 1
