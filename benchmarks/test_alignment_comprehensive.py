"""Comprehensive benchmark for BytesReader alignment operations.

Runs multiple trials to get statistically significant results comparing
modulo-based alignment vs bitwise alignment.
"""

import timeit
import statistics
from typing import Callable


class BytesReaderModulo:
    """BytesReader with modulo-based alignment (current implementation)."""

    def __init__(self, data: bytes):
        self._data = data
        self._position = 0

    def align(self, size: int) -> "BytesReaderModulo":
        if self._position % size > 0:
            self._position += size - (self._position % size)
        return self

    def read(self, size: int) -> bytes:
        result = self._data[self._position : self._position + size]
        self._position += size
        return result

    def tell(self) -> int:
        return self._position

    def seek_from_start(self, offset: int) -> int:
        self._position = offset
        return self._position


class BytesReaderBitwise:
    """BytesReader with bitwise alignment (optimized implementation)."""

    def __init__(self, data: bytes):
        self._data = data
        self._position = 0

    def align(self, size: int) -> "BytesReaderBitwise":
        # Faster bit-based alignment for power-of-2 sizes (2, 4, 8)
        remainder = self._position & (size - 1)
        if remainder:
            self._position += size - remainder
        return self

    def read(self, size: int) -> bytes:
        result = self._data[self._position : self._position + size]
        self._position += size
        return result

    def tell(self) -> int:
        return self._position

    def seek_from_start(self, offset: int) -> int:
        self._position = offset
        return self._position


def benchmark_alignment(reader_class: type, data: bytes, iterations: int = 100000) -> float:
    """Benchmark alignment operations.

    Args:
        reader_class: The BytesReader class to benchmark
        data: The data to read from
        iterations: Number of iterations to run

    Returns:
        Time in seconds for all iterations
    """
    def run():
        reader = reader_class(data)
        # Simulate typical CDR decoding pattern
        for _ in range(100):
            # Read a byte (unaligned)
            reader.read(1)
            # Align to 2 bytes
            reader.align(2)
            # Read a uint16
            reader.read(2)
            # Align to 4 bytes
            reader.align(4)
            # Read a uint32
            reader.read(4)
            # Align to 8 bytes
            reader.align(8)
            # Read a float64
            reader.read(8)
            # Reset for next iteration
            reader.seek_from_start(0)

    return timeit.timeit(run, number=iterations)


def main():
    """Run comprehensive alignment benchmarks."""
    print("=" * 80)
    print("Comprehensive BytesReader Alignment Benchmark")
    print("=" * 80)
    print()

    # Create test data
    data = b"\x00" * 10000
    iterations = 50000
    num_trials = 10

    print(f"Benchmark configuration:")
    print(f"  Data size: {len(data)} bytes")
    print(f"  Iterations per trial: {iterations:,}")
    print(f"  Number of trials: {num_trials}")
    print()

    # Run multiple trials for each approach
    print("Running benchmarks...")
    modulo_times = []
    bitwise_times = []

    for trial in range(num_trials):
        print(f"  Trial {trial + 1}/{num_trials}...", end=" ", flush=True)

        # Benchmark modulo
        time_mod = benchmark_alignment(BytesReaderModulo, data, iterations)
        modulo_times.append(time_mod)

        # Benchmark bitwise
        time_bit = benchmark_alignment(BytesReaderBitwise, data, iterations)
        bitwise_times.append(time_bit)

        print(f"modulo={time_mod:.4f}s, bitwise={time_bit:.4f}s, diff={((time_mod-time_bit)/time_mod*100):+.2f}%")

    print()

    # Calculate statistics
    mod_mean = statistics.mean(modulo_times)
    mod_stdev = statistics.stdev(modulo_times)
    mod_median = statistics.median(modulo_times)

    bit_mean = statistics.mean(bitwise_times)
    bit_stdev = statistics.stdev(bitwise_times)
    bit_median = statistics.median(bitwise_times)

    # Calculate improvement
    mean_speedup = (mod_mean - bit_mean) / mod_mean * 100
    median_speedup = (mod_median - bit_median) / mod_median * 100
    mean_factor = mod_mean / bit_mean

    print("=" * 80)
    print("Results:")
    print("=" * 80)
    print()
    print("Modulo-based alignment:")
    print(f"  Mean:   {mod_mean:.4f}s ± {mod_stdev:.4f}s")
    print(f"  Median: {mod_median:.4f}s")
    print(f"  Min:    {min(modulo_times):.4f}s")
    print(f"  Max:    {max(modulo_times):.4f}s")
    print()
    print("Bitwise alignment:")
    print(f"  Mean:   {bit_mean:.4f}s ± {bit_stdev:.4f}s")
    print(f"  Median: {bit_median:.4f}s")
    print(f"  Min:    {min(bitwise_times):.4f}s")
    print(f"  Max:    {max(bitwise_times):.4f}s")
    print()
    print("Performance comparison:")
    print(f"  Mean improvement:   {mean_factor:.3f}x faster ({mean_speedup:+.2f}%)")
    print(f"  Median improvement: {median_speedup:+.2f}%")
    print("=" * 80)

    # Determine if the improvement is significant
    if abs(mean_speedup) < 1.0:
        print()
        print("Conclusion: No significant performance difference detected.")
        print("The improvement is within measurement noise (<1%).")
    elif mean_speedup > 0:
        print()
        print(f"Conclusion: Bitwise alignment is {mean_speedup:.2f}% faster on average.")
        print("This optimization is beneficial.")
    else:
        print()
        print(f"Conclusion: Modulo alignment is {-mean_speedup:.2f}% faster on average.")
        print("The proposed optimization is not beneficial.")


if __name__ == "__main__":
    main()
