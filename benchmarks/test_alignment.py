"""Benchmark for BytesReader alignment operations.

Compares the performance of modulo-based alignment vs bitwise alignment
for power-of-2 sizes (2, 4, 8 bytes).
"""

import timeit
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


def benchmark_alignment(reader_class: type, data: bytes, alignment_sizes: list[int], iterations: int = 100000) -> float:
    """Benchmark alignment operations.

    Args:
        reader_class: The BytesReader class to benchmark
        data: The data to read from
        alignment_sizes: List of alignment sizes to test (e.g., [2, 4, 8])
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


def test_alignment_correctness():
    """Verify that both alignment implementations produce identical results."""
    data = b"\x00" * 1000

    test_cases = [
        (0, 2), (1, 2), (2, 2), (3, 2),
        (0, 4), (1, 4), (2, 4), (3, 4), (4, 4),
        (0, 8), (1, 8), (2, 8), (3, 8), (4, 8), (5, 8), (6, 8), (7, 8), (8, 8),
    ]

    print("Testing alignment correctness...")
    for position, alignment_size in test_cases:
        reader_mod = BytesReaderModulo(data)
        reader_bit = BytesReaderBitwise(data)

        reader_mod.seek_from_start(position)
        reader_bit.seek_from_start(position)

        reader_mod.align(alignment_size)
        reader_bit.align(alignment_size)

        pos_mod = reader_mod.tell()
        pos_bit = reader_bit.tell()

        if pos_mod != pos_bit:
            print(f"  FAIL: position={position}, alignment={alignment_size}, modulo={pos_mod}, bitwise={pos_bit}")
            return False

    print("  ✓ All alignment tests passed!")
    return True


def main():
    """Run alignment benchmarks."""
    print("=" * 80)
    print("BytesReader Alignment Benchmark")
    print("=" * 80)
    print()

    # First verify correctness
    if not test_alignment_correctness():
        print("Correctness test failed! Aborting benchmark.")
        return
    print()

    # Create test data
    data = b"\x00" * 10000
    alignment_sizes = [2, 4, 8]
    iterations = 10000

    print(f"Benchmark configuration:")
    print(f"  Data size: {len(data)} bytes")
    print(f"  Alignment sizes: {alignment_sizes}")
    print(f"  Iterations: {iterations:,}")
    print()

    # Benchmark modulo-based alignment
    print("Running modulo-based alignment benchmark...")
    time_modulo = benchmark_alignment(BytesReaderModulo, data, alignment_sizes, iterations)
    print(f"  Time: {time_modulo:.4f} seconds")
    print()

    # Benchmark bitwise alignment
    print("Running bitwise alignment benchmark...")
    time_bitwise = benchmark_alignment(BytesReaderBitwise, data, alignment_sizes, iterations)
    print(f"  Time: {time_bitwise:.4f} seconds")
    print()

    # Calculate speedup
    speedup = (time_modulo - time_bitwise) / time_modulo * 100
    improvement_factor = time_modulo / time_bitwise

    print("=" * 80)
    print("Results:")
    print("=" * 80)
    print(f"Modulo-based alignment:  {time_modulo:.4f} seconds")
    print(f"Bitwise alignment:       {time_bitwise:.4f} seconds")
    print(f"Improvement:             {improvement_factor:.2f}x faster ({speedup:+.2f}%)")
    print("=" * 80)


if __name__ == "__main__":
    main()
