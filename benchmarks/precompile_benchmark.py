#!/usr/bin/env python3
"""Benchmark to measure the performance improvement from pre-compilation.

This script compares the time to serialize/deserialize messages for the first time
with and without pre-compilation.
"""

import importlib
import sys
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Any

import pybag.types as t
from pybag.encoding.cdr import CdrDecoder, CdrEncoder
from pybag.ros2.humble import builtin_interfaces, geometry_msgs, std_msgs
from pybag.schema.compiler import compile_schema, compile_serializer
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder


def benchmark_without_precompile(message_type: type, message: Any) -> tuple[float, float]:
    """Benchmark serialization/deserialization without pre-compilation."""
    # Force fresh compilation by creating new encoder instance
    encoder_instance = Ros2MsgSchemaEncoder()

    # Measure serialization time (includes compilation)
    start = time.perf_counter()
    schema, sub_schemas = encoder_instance.parse_schema(message_type)
    serializer = compile_serializer(schema, sub_schemas)
    enc = CdrEncoder(little_endian=True)
    serializer(enc, message)
    data = enc.save()
    serialize_time = time.perf_counter() - start

    # Measure deserialization time (includes compilation)
    start = time.perf_counter()
    schema, sub_schemas = encoder_instance.parse_schema(message_type)
    deserializer = compile_schema(schema, sub_schemas)
    dec = CdrDecoder(data)
    result = deserializer(dec)
    deserialize_time = time.perf_counter() - start

    return serialize_time, deserialize_time


def benchmark_with_precompile(message_type: type, message: Any) -> tuple[float, float]:
    """Benchmark serialization/deserialization with pre-compilation."""
    from pybag import precompiled

    encoder_instance = Ros2MsgSchemaEncoder()
    schema, sub_schemas = encoder_instance.parse_schema(message_type)

    # Get pre-compiled functions
    precompiled_encoder = precompiled.get_encoder(schema.name)
    precompiled_decoder = precompiled.get_decoder(schema.name)

    if precompiled_encoder is None or precompiled_decoder is None:
        raise ValueError(f"No pre-compiled functions for {schema.name}")

    # Measure serialization time (no compilation needed)
    start = time.perf_counter()
    enc = CdrEncoder(little_endian=True)
    precompiled_encoder(enc, message)
    data = enc.save()
    serialize_time = time.perf_counter() - start

    # Measure deserialization time (no compilation needed)
    start = time.perf_counter()
    dec = CdrDecoder(data)
    result = precompiled_decoder(dec)
    deserialize_time = time.perf_counter() - start

    return serialize_time, deserialize_time


def main():
    """Run benchmarks for various message types."""
    print("=" * 80)
    print("Pre-compilation Performance Benchmark")
    print("=" * 80)
    print()

    # Test cases with different message types
    test_cases = [
        ("std_msgs/msg/Header", std_msgs.Header(
            stamp=t.Complex[builtin_interfaces.Time](sec=123, nanosec=456789),
            frame_id="base_link"
        )),
        ("geometry_msgs/msg/Point", geometry_msgs.Point(x=1.0, y=2.0, z=3.0)),
        ("geometry_msgs/msg/Pose", geometry_msgs.Pose(
            position=t.Complex[geometry_msgs.Point](x=1.0, y=2.0, z=3.0),
            orientation=t.Complex[geometry_msgs.Quaternion](x=0.0, y=0.0, z=0.0, w=1.0)
        )),
        ("geometry_msgs/msg/PoseStamped", geometry_msgs.PoseStamped(
            header=t.Complex[std_msgs.Header](
                stamp=t.Complex[builtin_interfaces.Time](sec=123, nanosec=456789),
                frame_id="base_link"
            ),
            pose=t.Complex[geometry_msgs.Pose](
                position=t.Complex[geometry_msgs.Point](x=1.0, y=2.0, z=3.0),
                orientation=t.Complex[geometry_msgs.Quaternion](x=0.0, y=0.0, z=0.0, w=1.0)
            )
        )),
    ]

    results = []

    for msg_name, message in test_cases:
        message_type = type(message)
        print(f"Testing: {msg_name}")
        print("-" * 80)

        # Run multiple iterations and take the average
        iterations = 10
        without_precompile_times = []
        with_precompile_times = []

        for i in range(iterations):
            # Clear module cache to force recompilation
            if 'pybag.schema.compiler' in sys.modules:
                importlib.reload(sys.modules['pybag.schema.compiler'])

            serialize_time, deserialize_time = benchmark_without_precompile(message_type, message)
            without_precompile_times.append((serialize_time, deserialize_time))

        for i in range(iterations):
            serialize_time, deserialize_time = benchmark_with_precompile(message_type, message)
            with_precompile_times.append((serialize_time, deserialize_time))

        # Calculate averages
        avg_without_serialize = sum(t[0] for t in without_precompile_times) / iterations
        avg_without_deserialize = sum(t[1] for t in without_precompile_times) / iterations
        avg_with_serialize = sum(t[0] for t in with_precompile_times) / iterations
        avg_with_deserialize = sum(t[1] for t in with_precompile_times) / iterations

        # Calculate speedup
        serialize_speedup = avg_without_serialize / avg_with_serialize if avg_with_serialize > 0 else 0
        deserialize_speedup = avg_without_deserialize / avg_with_deserialize if avg_with_deserialize > 0 else 0

        results.append({
            'name': msg_name,
            'without_serialize': avg_without_serialize,
            'without_deserialize': avg_without_deserialize,
            'with_serialize': avg_with_serialize,
            'with_deserialize': avg_with_deserialize,
            'serialize_speedup': serialize_speedup,
            'deserialize_speedup': deserialize_speedup,
        })

        print(f"  Without pre-compilation:")
        print(f"    Serialize:   {avg_without_serialize * 1000:.3f} ms")
        print(f"    Deserialize: {avg_without_deserialize * 1000:.3f} ms")
        print(f"  With pre-compilation:")
        print(f"    Serialize:   {avg_with_serialize * 1000:.3f} ms")
        print(f"    Deserialize: {avg_with_deserialize * 1000:.3f} ms")
        print(f"  Speedup:")
        print(f"    Serialize:   {serialize_speedup:.2f}x faster")
        print(f"    Deserialize: {deserialize_speedup:.2f}x faster")
        print()

    # Summary
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print()
    print(f"{'Message Type':<40} {'Serialize Speedup':>20} {'Deserialize Speedup':>20}")
    print("-" * 80)
    for result in results:
        print(f"{result['name']:<40} {result['serialize_speedup']:>19.2f}x {result['deserialize_speedup']:>19.2f}x")
    print()

    avg_serialize_speedup = sum(r['serialize_speedup'] for r in results) / len(results)
    avg_deserialize_speedup = sum(r['deserialize_speedup'] for r in results) / len(results)

    print(f"Average speedup: Serialize {avg_serialize_speedup:.2f}x, Deserialize {avg_deserialize_speedup:.2f}x")
    print()


if __name__ == '__main__':
    main()
