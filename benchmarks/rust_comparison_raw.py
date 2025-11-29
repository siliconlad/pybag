#!/usr/bin/env python3
"""
Benchmark comparison for raw MCAP reading (without CDR decoding).

This tests the MCAP file parsing performance separately from CDR decoding.
"""

import statistics
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmarks.benchmark_utils import create_test_mcap
from pybag.mcap.record_reader import McapRecordReaderFactory


def benchmark_function(func: Callable, iterations: int = 5, warmup: int = 1) -> Tuple[float, float, float]:
    """Benchmark a function and return timing statistics."""
    for _ in range(warmup):
        func()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)

    return statistics.mean(times), min(times), max(times)


def read_raw_with_pybag_python(mcap: Path) -> int:
    """Read raw messages using the pure Python pybag implementation."""
    count = 0
    reader = McapRecordReaderFactory.from_file(mcap)
    try:
        for channel_id in reader.get_channels().keys():
            for msg in reader.get_messages([channel_id], None, None, in_log_time_order=True, in_reverse=False):
                _ = msg.data  # Access raw data
                count += 1
    finally:
        reader.close()
    return count


def format_time(seconds: float) -> str:
    """Format time in a human-readable way."""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.2f} µs"
    elif seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds:.3f} s"


def main():
    print("=" * 70)
    print("PYBAG RAW READING BENCHMARK")
    print("=" * 70)
    print("\nThis benchmark tests MCAP file parsing (without CDR decoding)")
    print("Test data: Odometry messages (nav_msgs/msg/Odometry)")

    message_counts = [100, 1000, 5000, 10000]
    results = []

    for msg_count in message_counts:
        print(f"\n--- {msg_count:,} messages ---")

        with TemporaryDirectory() as tmpdir:
            print(f"Creating test MCAP file...")
            mcap = create_test_mcap(Path(tmpdir) / "test", message_count=msg_count)
            file_size = mcap.stat().st_size
            print(f"File size: {file_size / 1024 / 1024:.2f} MB")

            print(f"Benchmarking Python raw reading...")
            py_mean, py_min, py_max = benchmark_function(
                lambda: read_raw_with_pybag_python(mcap),
                iterations=5,
                warmup=1
            )
            py_throughput = msg_count / py_mean

            results.append({
                'msg_count': msg_count,
                'file_size_mb': file_size / 1024 / 1024,
                'py_mean': py_mean,
                'py_throughput': py_throughput,
            })

    # Print summary
    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY (Raw Reading - No Decoding)")
    print(f"{'=' * 70}")
    print(f"\n{'Messages':<12} {'Time (mean)':<15} {'Throughput':<20}")
    print(f"{'-' * 47}")

    for r in results:
        print(f"{r['msg_count']:<12,} "
              f"{format_time(r['py_mean']):<15} "
              f"{r['py_throughput']:,.0f} msg/s")


if __name__ == "__main__":
    main()
