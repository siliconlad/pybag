#!/usr/bin/env python3
"""
Standalone benchmark comparison between Python pybag and Rust pybag_rs implementations.

This script creates test MCAP files and measures the performance of reading and
decoding messages using both implementations.
"""

import statistics
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, List, Tuple

# Ensure we can import from the benchmarks directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap_reader import McapFileReader
import pybag_rs


def benchmark_function(func: Callable, iterations: int = 5, warmup: int = 1) -> Tuple[float, float, float]:
    """
    Benchmark a function and return timing statistics.

    Returns:
        Tuple of (mean, min, max) execution times in seconds.
    """
    # Warmup
    for _ in range(warmup):
        func()

    # Measure
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)

    return statistics.mean(times), min(times), max(times)


def read_with_pybag_python(mcap: Path) -> int:
    """Read and decode messages using the pure Python pybag implementation."""
    count = 0
    with McapFileReader.from_file(mcap) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                _ = message.data  # Access data to force decoding
                count += 1
    return count


def read_with_pybag_rust(mcap: Path) -> int:
    """Read and decode messages using the Rust pybag_rs implementation."""
    count = 0
    with pybag_rs.PyMcapFileReader.from_file(str(mcap)) as reader:
        for topic in reader.get_topics():
            for message in reader.messages(topic):
                _ = message.data  # Access data to force decoding
                count += 1
    return count


def format_time(seconds: float) -> str:
    """Format time in a human-readable way."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.3f} s"


def run_benchmark(name: str, message_counts: List[int]) -> None:
    """Run benchmarks for different message counts."""
    print(f"\n{'=' * 70}")
    print(f"BENCHMARK: {name}")
    print(f"{'=' * 70}")

    results = []

    for msg_count in message_counts:
        print(f"\n--- {msg_count:,} messages ---")

        with TemporaryDirectory() as tmpdir:
            # Create test MCAP file
            print(f"Creating test MCAP file...")
            mcap = create_test_mcap(Path(tmpdir) / "test", message_count=msg_count)
            file_size = mcap.stat().st_size
            print(f"File size: {file_size / 1024 / 1024:.2f} MB")

            # Benchmark Python implementation
            print(f"\nBenchmarking Python implementation...")
            py_mean, py_min, py_max = benchmark_function(
                lambda: read_with_pybag_python(mcap),
                iterations=5,
                warmup=1
            )
            py_throughput = msg_count / py_mean

            # Benchmark Rust implementation
            print(f"Benchmarking Rust implementation...")
            rs_mean, rs_min, rs_max = benchmark_function(
                lambda: read_with_pybag_rust(mcap),
                iterations=5,
                warmup=1
            )
            rs_throughput = msg_count / rs_mean

            # Calculate speedup
            speedup = py_mean / rs_mean if rs_mean > 0 else float('inf')

            results.append({
                'msg_count': msg_count,
                'file_size_mb': file_size / 1024 / 1024,
                'py_mean': py_mean,
                'py_min': py_min,
                'py_max': py_max,
                'py_throughput': py_throughput,
                'rs_mean': rs_mean,
                'rs_min': rs_min,
                'rs_max': rs_max,
                'rs_throughput': rs_throughput,
                'speedup': speedup,
            })

    # Print summary table
    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 70}")
    print(f"\n{'Messages':<12} {'Python (mean)':<15} {'Rust (mean)':<15} {'Speedup':<10} {'Rust Throughput':<20}")
    print(f"{'-' * 72}")

    for r in results:
        print(f"{r['msg_count']:<12,} "
              f"{format_time(r['py_mean']):<15} "
              f"{format_time(r['rs_mean']):<15} "
              f"{r['speedup']:.2f}x{'':<5} "
              f"{r['rs_throughput']:,.0f} msg/s")

    print(f"\n{'=' * 70}")
    print("DETAILED RESULTS")
    print(f"{'=' * 70}")

    for r in results:
        print(f"\n{r['msg_count']:,} messages ({r['file_size_mb']:.2f} MB):")
        print(f"  Python:  mean={format_time(r['py_mean'])}, min={format_time(r['py_min'])}, max={format_time(r['py_max'])}")
        print(f"           throughput={r['py_throughput']:,.0f} msg/s")
        print(f"  Rust:    mean={format_time(r['rs_mean'])}, min={format_time(r['rs_min'])}, max={format_time(r['rs_max'])}")
        print(f"           throughput={r['rs_throughput']:,.0f} msg/s")
        print(f"  Speedup: {r['speedup']:.2f}x faster")


def main():
    print("=" * 70)
    print("PYBAG vs PYBAG_RS (Rust) BENCHMARK COMPARISON")
    print("=" * 70)
    print("\nThis benchmark compares the performance of:")
    print("  - pybag: Pure Python MCAP reader")
    print("  - pybag_rs: Rust implementation with Python bindings")
    print("\nTest data: Odometry messages (nav_msgs/msg/Odometry)")

    # Run benchmarks with various message counts
    run_benchmark(
        "Reading and Decoding MCAP Messages",
        message_counts=[100, 1000, 5000, 10000]
    )

    print("\n" + "=" * 70)
    print("BENCHMARK COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
