"""Benchmark Rust vs Python message serialization/deserialization."""
import random
from tempfile import TemporaryDirectory
from pathlib import Path

import pytest
from pytest_benchmark.fixture import BenchmarkFixture

# Python implementation
from pybag.ros2.humble.builtin_interfaces import Time
from pybag.ros2.humble.geometry_msgs import (
    Point,
    Pose,
    PoseWithCovariance,
    Quaternion,
    Twist,
    TwistWithCovariance,
    Vector3
)
from pybag.ros2.humble.nav_msgs import Odometry
from pybag.ros2.humble.std_msgs import Header
from pybag.mcap_writer import McapFileWriter
from pybag.mcap_reader import McapFileReader


def _generate_odometry_messages(count: int = 1000, seed: int = 0) -> list[Odometry]:
    """Generate test odometry messages."""
    rng = random.Random(seed)

    messages: list[Odometry] = []
    for i in range(count):
        timestamp = int(i * 1_500_000_000)
        msg = Odometry(
            header=Header(
                stamp=Time(
                    sec=timestamp // 1_000_000_000,
                    nanosec=timestamp % 1_000_000_000,
                ),
                frame_id="map",
            ),
            child_frame_id="base_link",
            pose=PoseWithCovariance(
                pose=Pose(
                    position=Point(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    orientation=Quaternion(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                        w=rng.random(),
                    ),
                ),
                covariance=[rng.random() for _ in range(36)],
            ),
            twist=TwistWithCovariance(
                twist=Twist(
                    linear=Vector3(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                    angular=Vector3(
                        x=rng.random(),
                        y=rng.random(),
                        z=rng.random(),
                    ),
                ),
                covariance=[rng.random() for _ in range(36)],
            ),
        )
        messages.append(msg)
    return messages


def test_python_write_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark Python implementation writing 1000 messages."""
    messages = _generate_odometry_messages(1000)

    def write_messages():
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "python.mcap"
            writer = McapFileWriter.open(path)
            writer.add_channel("/odom", Odometry)
            for i, msg in enumerate(messages):
                timestamp = int(i * 1_500_000_000)
                writer.write_message("/odom", timestamp, msg)
            writer.close()

    benchmark(write_messages)


def test_rust_write_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark Rust implementation writing 1000 messages."""
    messages = _generate_odometry_messages(1000)

    # Import Rust implementations
    from pybag.serialize_rust import MessageSerializerFactory
    from pybag.encoding.cdr_rust import CdrEncoder

    # Monkey-patch to use Rust implementation
    import pybag.mcap_writer as mcap_writer_module
    original_factory = mcap_writer_module.MessageSerializerFactory
    mcap_writer_module.MessageSerializerFactory = MessageSerializerFactory

    try:
        def write_messages():
            with TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "rust.mcap"
                writer = McapFileWriter.open(path)
                writer.add_channel("/odom", Odometry)
                for i, msg in enumerate(messages):
                    timestamp = int(i * 1_500_000_000)
                    writer.write_message("/odom", timestamp, msg)
                writer.close()

        benchmark(write_messages)
    finally:
        # Restore original
        mcap_writer_module.MessageSerializerFactory = original_factory


def test_python_read_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark Python implementation reading 1000 messages."""
    messages = _generate_odometry_messages(1000)

    # Create test file
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "python.mcap"
        writer = McapFileWriter.open(path)
        writer.add_channel("/odom", Odometry)
        for i, msg in enumerate(messages):
            timestamp = int(i * 1_500_000_000)
            writer.write_message("/odom", timestamp, msg)
        writer.close()

        def read_messages():
            reader = McapFileReader.open(path)
            msgs = list(reader.messages("/odom"))
            reader.close()
            return len(msgs)

        result = benchmark(read_messages)
        assert result == 1000


def test_rust_read_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark Rust implementation reading 1000 messages."""
    messages = _generate_odometry_messages(1000)

    # Import Rust implementation
    from pybag.deserialize_rust import MessageDeserializerFactory

    # Create test file
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "rust.mcap"
        writer = McapFileWriter.open(path)
        writer.add_channel("/odom", Odometry)
        for i, msg in enumerate(messages):
            timestamp = int(i * 1_500_000_000)
            writer.write_message("/odom", timestamp, msg)
        writer.close()

        # Monkey-patch to use Rust implementation
        import pybag.mcap_reader as mcap_reader_module
        original_factory = mcap_reader_module.MessageDeserializerFactory
        mcap_reader_module.MessageDeserializerFactory = MessageDeserializerFactory

        try:
            def read_messages():
                reader = McapFileReader.open(path)
                msgs = list(reader.messages("/odom"))
                reader.close()
                return len(msgs)

            result = benchmark(read_messages)
            assert result == 1000
        finally:
            # Restore original
            mcap_reader_module.MessageDeserializerFactory = original_factory


def test_python_serialization_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark only the serialization part (Python)."""
    from pybag.serialize import MessageSerializerFactory

    messages = _generate_odometry_messages(100)
    serializer = MessageSerializerFactory.from_profile("ros2")

    def serialize_all():
        return [serializer.serialize_message(msg) for msg in messages]

    benchmark(serialize_all)


def test_rust_serialization_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark only the serialization part (Rust)."""
    from pybag.serialize_rust import MessageSerializerFactory

    messages = _generate_odometry_messages(100)
    serializer = MessageSerializerFactory.from_profile("ros2")

    def serialize_all():
        return [serializer.serialize_message(msg) for msg in messages]

    benchmark(serialize_all)


def test_python_deserialization_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark only the deserialization part (Python)."""
    from pybag.serialize import MessageSerializerFactory
    from pybag.deserialize import MessageDeserializerFactory
    from pybag.mcap.records import MessageRecord, SchemaRecord

    messages = _generate_odometry_messages(100)
    serializer = MessageSerializerFactory.from_profile("ros2")
    schema_data = serializer.serialize_schema(Odometry)

    # Serialize messages
    serialized = [serializer.serialize_message(msg) for msg in messages]

    # Create mock records
    schema_record = SchemaRecord(id=1, name="nav_msgs/msg/Odometry", encoding="ros2msg", data=schema_data)
    message_records = [
        MessageRecord(channel_id=1, sequence=i, log_time=i, publish_time=i, data=data)
        for i, data in enumerate(serialized)
    ]

    deserializer = MessageDeserializerFactory.from_profile("ros2")

    def deserialize_all():
        return [deserializer.deserialize_message(msg_record, schema_record) for msg_record in message_records]

    benchmark(deserialize_all)


def test_rust_deserialization_only(benchmark: BenchmarkFixture) -> None:
    """Benchmark only the deserialization part (Rust)."""
    from pybag.serialize import MessageSerializerFactory
    from pybag.deserialize_rust import MessageDeserializerFactory
    from pybag.mcap.records import MessageRecord, SchemaRecord

    messages = _generate_odometry_messages(100)
    serializer = MessageSerializerFactory.from_profile("ros2")
    schema_data = serializer.serialize_schema(Odometry)

    # Serialize messages
    serialized = [serializer.serialize_message(msg) for msg in messages]

    # Create mock records
    schema_record = SchemaRecord(id=1, name="nav_msgs/msg/Odometry", encoding="ros2msg", data=schema_data)
    message_records = [
        MessageRecord(channel_id=1, sequence=i, log_time=i, publish_time=i, data=data)
        for i, data in enumerate(serialized)
    ]

    deserializer = MessageDeserializerFactory.from_profile("ros2")

    def deserialize_all():
        return [deserializer.deserialize_message(msg_record, schema_record) for msg_record in message_records]

    benchmark(deserialize_all)
