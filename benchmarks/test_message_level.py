"""Benchmark message-level Rust deserialization vs Python."""
import random
import time

# Python implementation
from pybag.ros2.humble.builtin_interfaces import Time
from pybag.ros2.humble.geometry_msgs import (
    Point, Pose, PoseWithCovariance, Quaternion, Twist, TwistWithCovariance, Vector3
)
from pybag.ros2.humble.nav_msgs import Odometry
from pybag.ros2.humble.std_msgs import Header
from pybag.serialize import MessageSerializerFactory
from pybag.deserialize import MessageDeserializerFactory
from pybag.pybag_rust import deserialize_odometry as rust_deserialize_odometry


def generate_odometry_messages(count: int = 1000, seed: int = 0) -> list[Odometry]:
    """Generate test odometry messages."""
    rng = random.Random(seed)
    messages: list[Odometry] = []
    for i in range(count):
        timestamp = int(i * 1_500_000_000)
        msg = Odometry(
            header=Header(
                stamp=Time(sec=timestamp // 1_000_000_000, nanosec=timestamp % 1_000_000_000),
                frame_id="map",
            ),
            child_frame_id="base_link",
            pose=PoseWithCovariance(
                pose=Pose(
                    position=Point(x=rng.random(), y=rng.random(), z=rng.random()),
                    orientation=Quaternion(x=rng.random(), y=rng.random(), z=rng.random(), w=rng.random()),
                ),
                covariance=[rng.random() for _ in range(36)],
            ),
            twist=TwistWithCovariance(
                twist=Twist(
                    linear=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
                    angular=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
                ),
                covariance=[rng.random() for _ in range(36)],
            ),
        )
        messages.append(msg)
    return messages


def benchmark_python(messages: list[Odometry], serialized_data: list[bytes]):
    """Benchmark Python deserializer with Rust CDR decoder (field-level)."""
    from pybag.deserialize import MessageDeserializer
    from pybag.encoding.cdr import CdrDecoder
    from pybag.schema.ros2msg import Ros2MsgSchemaDecoder
    from pybag.mcap.records import MessageRecord, SchemaRecord

    # Set up deserializer
    deserializer = MessageDeserializer(Ros2MsgSchemaDecoder(), CdrDecoder)

    schema_record = SchemaRecord(
        id=1,
        name="nav_msgs/msg/Odometry",
        encoding="ros2msg",
        data=b""
    )

    # Warm up
    for data in serialized_data[:10]:
        msg_record = MessageRecord(channel_id=1, sequence=0, log_time=0, publish_time=0, data=data)
        _ = deserializer.deserialize_message(msg_record, schema_record)

    # Benchmark
    start = time.perf_counter()
    for data in serialized_data:
        msg_record = MessageRecord(channel_id=1, sequence=0, log_time=0, publish_time=0, data=data)
        _ = deserializer.deserialize_message(msg_record, schema_record)
    end = time.perf_counter()

    total_time = end - start
    avg_time_ms = (total_time / len(serialized_data)) * 1000
    ops_per_sec = len(serialized_data) / total_time

    print(f"\nPython (with Python CDR, field-level):")
    print(f"  Messages: {len(serialized_data)}")
    print(f"  Total time: {total_time:.4f}s")
    print(f"  Avg time per message: {avg_time_ms:.4f} ms")
    print(f"  Throughput: {ops_per_sec:.1f} ops/s")

    return avg_time_ms


def benchmark_rust(messages: list[Odometry], serialized_data: list[bytes]):
    """Benchmark Rust message-level deserializer."""
    # Warm up
    for data in serialized_data[:10]:
        _ = rust_deserialize_odometry(data)

    # Benchmark
    start = time.perf_counter()
    for data in serialized_data:
        _ = rust_deserialize_odometry(data)
    end = time.perf_counter()

    total_time = end - start
    avg_time_ms = (total_time / len(serialized_data)) * 1000
    ops_per_sec = len(serialized_data) / total_time

    print(f"\nRust (message-level):")
    print(f"  Messages: {len(serialized_data)}")
    print(f"  Total time: {total_time:.4f}s")
    print(f"  Avg time per message: {avg_time_ms:.4f} ms")
    print(f"  Throughput: {ops_per_sec:.1f} ops/s")

    return avg_time_ms


def main():
    print("=" * 60)
    print("MESSAGE-LEVEL RUST DESERIALIZATION BENCHMARK")
    print("=" * 60)

    iterations = 10000

    # Generate messages
    print(f"\nGenerating {iterations} Odometry messages...")
    messages = generate_odometry_messages(iterations)

    # Serialize all messages using Python serializer
    print("Serializing messages...")
    from pybag.serialize import MessageSerializer
    from pybag.encoding.cdr import CdrEncoder
    from pybag.schema.ros2msg import Ros2MsgSchemaEncoder

    serializer = MessageSerializer(Ros2MsgSchemaEncoder(), CdrEncoder)
    serialized_data = []
    for msg in messages:
        data = serializer.serialize_message(msg)
        serialized_data.append(data)

    # Test Python deserializer
    python_time = benchmark_python(messages, serialized_data)

    # Test Rust message-level deserializer
    rust_time = benchmark_rust(messages, serialized_data)

    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON:")
    print("=" * 60)
    print(f"Python (field-level):  {python_time:.4f} ms/msg")
    print(f"Rust (message-level):  {rust_time:.4f} ms/msg")
    speedup = python_time / rust_time
    if speedup > 1:
        print(f"Speedup:               {speedup:.2f}x FASTER")
    else:
        print(f"Speedup:               {1/speedup:.2f}x SLOWER")
    print("=" * 60)


if __name__ == "__main__":
    main()
