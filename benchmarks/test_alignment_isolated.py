"""Isolated benchmark for alignment operations only.

Tests pure alignment performance without mixing in read operations.
"""

import timeit
import statistics


def benchmark_modulo_alignment(iterations: int) -> float:
    """Benchmark modulo-based alignment."""
    def run():
        position = 0
        for _ in range(1000):
            # Align to various sizes with different positions
            for size in [2, 4, 8]:
                position += 1  # Offset by 1 to create misalignment
                if position % size > 0:
                    position += size - (position % size)

    return timeit.timeit(run, number=iterations)


def benchmark_bitwise_alignment(iterations: int) -> float:
    """Benchmark bitwise alignment."""
    def run():
        position = 0
        for _ in range(1000):
            # Align to various sizes with different positions
            for size in [2, 4, 8]:
                position += 1  # Offset by 1 to create misalignment
                remainder = position & (size - 1)
                if remainder:
                    position += size - remainder

    return timeit.timeit(run, number=iterations)


def benchmark_modulo_alignment_always_misaligned(iterations: int) -> float:
    """Benchmark modulo-based alignment with always-misaligned positions."""
    def run():
        for position in range(1000):
            for size in [2, 4, 8]:
                # Simulate misaligned position
                pos = position * 3 + 1
                if pos % size > 0:
                    pos += size - (pos % size)

    return timeit.timeit(run, number=iterations)


def benchmark_bitwise_alignment_always_misaligned(iterations: int) -> float:
    """Benchmark bitwise alignment with always-misaligned positions."""
    def run():
        for position in range(1000):
            for size in [2, 4, 8]:
                # Simulate misaligned position
                pos = position * 3 + 1
                remainder = pos & (size - 1)
                if remainder:
                    pos += size - remainder

    return timeit.timeit(run, number=iterations)


def main():
    """Run isolated alignment benchmarks."""
    print("=" * 80)
    print("Isolated Alignment Operation Benchmark")
    print("=" * 80)
    print()

    iterations = 10000
    num_trials = 10

    print(f"Benchmark configuration:")
    print(f"  Iterations per trial: {iterations:,}")
    print(f"  Number of trials: {num_trials}")
    print()

    # Test 1: Mixed alignment pattern
    print("Test 1: Mixed alignment pattern (some aligned, some not)")
    print("-" * 80)
    modulo_times_1 = []
    bitwise_times_1 = []

    for trial in range(num_trials):
        time_mod = benchmark_modulo_alignment(iterations)
        modulo_times_1.append(time_mod)
        time_bit = benchmark_bitwise_alignment(iterations)
        bitwise_times_1.append(time_bit)
        print(f"  Trial {trial + 1}: modulo={time_mod:.4f}s, bitwise={time_bit:.4f}s, diff={((time_mod-time_bit)/time_mod*100):+.2f}%")

    mod_mean_1 = statistics.mean(modulo_times_1)
    bit_mean_1 = statistics.mean(bitwise_times_1)
    improvement_1 = (mod_mean_1 - bit_mean_1) / mod_mean_1 * 100

    print(f"  Modulo mean:  {mod_mean_1:.4f}s")
    print(f"  Bitwise mean: {bit_mean_1:.4f}s")
    print(f"  Improvement:  {improvement_1:+.2f}%")
    print()

    # Test 2: Always misaligned
    print("Test 2: Always misaligned (worst case for alignment)")
    print("-" * 80)
    modulo_times_2 = []
    bitwise_times_2 = []

    for trial in range(num_trials):
        time_mod = benchmark_modulo_alignment_always_misaligned(iterations)
        modulo_times_2.append(time_mod)
        time_bit = benchmark_bitwise_alignment_always_misaligned(iterations)
        bitwise_times_2.append(time_bit)
        print(f"  Trial {trial + 1}: modulo={time_mod:.4f}s, bitwise={time_bit:.4f}s, diff={((time_mod-time_bit)/time_mod*100):+.2f}%")

    mod_mean_2 = statistics.mean(modulo_times_2)
    bit_mean_2 = statistics.mean(bitwise_times_2)
    improvement_2 = (mod_mean_2 - bit_mean_2) / mod_mean_2 * 100

    print(f"  Modulo mean:  {mod_mean_2:.4f}s")
    print(f"  Bitwise mean: {bit_mean_2:.4f}s")
    print(f"  Improvement:  {improvement_2:+.2f}%")
    print()

    print("=" * 80)
    print("Summary:")
    print("=" * 80)
    print(f"Test 1 (mixed):        {improvement_1:+.2f}%")
    print(f"Test 2 (always mis):   {improvement_2:+.2f}%")
    print()

    avg_improvement = (improvement_1 + improvement_2) / 2
    if avg_improvement > 1.0:
        print(f"Overall: Bitwise is {avg_improvement:.2f}% faster on average.")
        print("Recommendation: Use bitwise alignment.")
    elif avg_improvement < -1.0:
        print(f"Overall: Modulo is {-avg_improvement:.2f}% faster on average.")
        print("Recommendation: Keep modulo alignment.")
    else:
        print(f"Overall: Performance difference is negligible ({avg_improvement:+.2f}%).")
        print("Recommendation: Either approach is fine. Bitwise may be more readable.")


if __name__ == "__main__":
    main()
