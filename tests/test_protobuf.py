"""Tests for protobuf support in MCAP files."""

import tempfile
from pathlib import Path

import pytest
from google.protobuf import message_factory
from google.protobuf.descriptor_pb2 import (
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
)
from google.protobuf.descriptor_pool import DescriptorPool

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


def create_point_message():
    """Create a Point message type dynamically."""
    # Create file descriptor
    file_proto = FileDescriptorProto()
    file_proto.name = "test_message.proto"
    file_proto.package = "pybag.test"
    file_proto.syntax = "proto3"

    # Create Point message descriptor
    point_msg = DescriptorProto()
    point_msg.name = "Point"

    # Add fields
    x_field = point_msg.field.add()
    x_field.name = "x"
    x_field.number = 1
    x_field.type = FieldDescriptorProto.TYPE_DOUBLE
    x_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    y_field = point_msg.field.add()
    y_field.name = "y"
    y_field.number = 2
    y_field.type = FieldDescriptorProto.TYPE_DOUBLE
    y_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    z_field = point_msg.field.add()
    z_field.name = "z"
    z_field.number = 3
    z_field.type = FieldDescriptorProto.TYPE_DOUBLE
    z_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    file_proto.message_type.append(point_msg)

    # Build the message class
    pool = DescriptorPool()
    pool.Add(file_proto)

    descriptor = pool.FindMessageTypeByName("pybag.test.Point")
    # Use message_factory.GetMessageClass for protobuf 6.x+
    return message_factory.GetMessageClass(descriptor)


def create_header_message():
    """Create a Header message type dynamically."""
    # Create file descriptor
    file_proto = FileDescriptorProto()
    file_proto.name = "test_header.proto"
    file_proto.package = "pybag.test"
    file_proto.syntax = "proto3"

    # Create Header message descriptor
    header_msg = DescriptorProto()
    header_msg.name = "Header"

    # Add seq field
    seq_field = header_msg.field.add()
    seq_field.name = "seq"
    seq_field.number = 1
    seq_field.type = FieldDescriptorProto.TYPE_UINT32
    seq_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    # Add timestamp field
    timestamp_field = header_msg.field.add()
    timestamp_field.name = "timestamp"
    timestamp_field.number = 2
    timestamp_field.type = FieldDescriptorProto.TYPE_INT64
    timestamp_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    # Add frame_id field
    frame_id_field = header_msg.field.add()
    frame_id_field.name = "frame_id"
    frame_id_field.number = 3
    frame_id_field.type = FieldDescriptorProto.TYPE_STRING
    frame_id_field.label = FieldDescriptorProto.LABEL_OPTIONAL

    file_proto.message_type.append(header_msg)

    # Build the message class
    pool = DescriptorPool()
    pool.Add(file_proto)

    descriptor = pool.FindMessageTypeByName("pybag.test.Header")
    # Use message_factory.GetMessageClass for protobuf 6.x+
    return message_factory.GetMessageClass(descriptor)


def test_write_and_read_protobuf_point():
    """Test writing and reading a simple protobuf Point message."""
    Point = create_point_message()

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        # Write protobuf messages to MCAP file
        with McapFileWriter.open(tmp_path, profile="protobuf") as writer:
            # Write some points
            for i in range(5):
                point = Point()
                point.x = float(i)
                point.y = float(i * 2)
                point.z = float(i * 3)
                writer.write_message("/points", i * 1000000000, point)

        # Read the messages back
        with McapFileReader.from_file(tmp_path) as reader:
            assert reader.profile == "protobuf"
            topics = reader.get_topics()
            assert "/points" in topics

            messages = list(reader.messages("/points"))
            assert len(messages) == 5

            # Verify the first message
            first_msg = messages[0]
            assert first_msg.log_time == 0
            assert hasattr(first_msg.data, "x")
            assert hasattr(first_msg.data, "y")
            assert hasattr(first_msg.data, "z")
            assert first_msg.data.x == 0.0
            assert first_msg.data.y == 0.0
            assert first_msg.data.z == 0.0

            # Verify the last message
            last_msg = messages[4]
            assert last_msg.log_time == 4 * 1000000000
            assert last_msg.data.x == 4.0
            assert last_msg.data.y == 8.0
            assert last_msg.data.z == 12.0

    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)


def test_write_and_read_protobuf_header():
    """Test writing and reading a protobuf Header message with string fields."""
    Header = create_header_message()

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        # Write protobuf messages to MCAP file
        with McapFileWriter.open(tmp_path, profile="protobuf") as writer:
            for i in range(3):
                header = Header()
                header.seq = i
                header.timestamp = i * 1000000000
                header.frame_id = f"frame_{i}"
                writer.write_message("/headers", i * 1000000000, header)

        # Read the messages back
        with McapFileReader.from_file(tmp_path) as reader:
            messages = list(reader.messages("/headers"))
            assert len(messages) == 3

            # Verify messages
            for i, msg in enumerate(messages):
                assert msg.data.seq == i
                assert msg.data.timestamp == i * 1000000000
                assert msg.data.frame_id == f"frame_{i}"

    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)


def test_write_multiple_protobuf_topics():
    """Test writing multiple topics with different protobuf message types."""
    Point = create_point_message()
    Header = create_header_message()

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        # Write messages to multiple topics
        with McapFileWriter.open(tmp_path, profile="protobuf") as writer:
            # Write points
            for i in range(3):
                point = Point()
                point.x = float(i)
                point.y = float(i * 2)
                point.z = float(i * 3)
                writer.write_message("/points", i * 1000000000, point)

            # Write headers
            for i in range(2):
                header = Header()
                header.seq = i
                header.timestamp = i * 1000000000
                header.frame_id = f"frame_{i}"
                writer.write_message("/headers", i * 1000000000, header)

        # Read the messages back
        with McapFileReader.from_file(tmp_path) as reader:
            topics = reader.get_topics()
            assert "/points" in topics
            assert "/headers" in topics

            points = list(reader.messages("/points"))
            assert len(points) == 3

            headers = list(reader.messages("/headers"))
            assert len(headers) == 2

    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)


def test_protobuf_message_count():
    """Test getting message counts for protobuf topics."""
    Point = create_point_message()

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        # Write messages
        with McapFileWriter.open(tmp_path, profile="protobuf") as writer:
            for i in range(10):
                point = Point()
                point.x = float(i)
                writer.write_message("/points", i * 1000000000, point)

        # Check message count
        with McapFileReader.from_file(tmp_path) as reader:
            count = reader.get_message_count("/points")
            assert count == 10

    finally:
        # Clean up
        tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
