"""Microbenchmarks for CDR encoder/decoder primitive operations.

These benchmarks measure the raw performance of encoding and decoding
primitive types with the CDR encoder/decoder. This is useful for
measuring the impact of optimizations like removing repeated endianness checks.
"""
import pytest
from pybag.encoding.cdr import CdrDecoder, CdrEncoder


@pytest.fixture
def sample_data():
    """Generate sample data for benchmarking."""
    # Create an encoder and encode various primitive types
    encoder = CdrEncoder(little_endian=True)

    # Encode multiple values of each type to simulate realistic usage
    for _ in range(100):
        encoder.int8(42)
        encoder.uint8(255)
        encoder.int16(-1000)
        encoder.uint16(65000)
        encoder.int32(-100000)
        encoder.uint32(4000000000)
        encoder.int64(-1000000000000)
        encoder.uint64(10000000000000)
        encoder.float32(3.14159)
        encoder.float64(2.718281828)
        encoder.bool(True)
        encoder.string("benchmark")

    return encoder.save()


@pytest.fixture
def float_array_data():
    """Generate CDR-encoded float array data for benchmarking."""
    encoder = CdrEncoder(little_endian=True)
    # Encode a large array of floats (common in robotics messages)
    encoder.array('float64', [float(i) for i in range(1000)])
    return encoder.save()


def test_decode_primitives(benchmark, sample_data):
    """Benchmark decoding of primitive types."""
    def decode():
        decoder = CdrDecoder(sample_data)
        for _ in range(100):
            decoder.int8()
            decoder.uint8()
            decoder.int16()
            decoder.uint16()
            decoder.int32()
            decoder.uint32()
            decoder.int64()
            decoder.uint64()
            decoder.float32()
            decoder.float64()
            decoder.bool()
            decoder.string()

    benchmark(decode)


def test_encode_primitives(benchmark):
    """Benchmark encoding of primitive types."""
    def encode():
        encoder = CdrEncoder(little_endian=True)
        for _ in range(100):
            encoder.int8(42)
            encoder.uint8(255)
            encoder.int16(-1000)
            encoder.uint16(65000)
            encoder.int32(-100000)
            encoder.uint32(4000000000)
            encoder.int64(-1000000000000)
            encoder.uint64(10000000000000)
            encoder.float32(3.14159)
            encoder.float64(2.718281828)
            encoder.bool(True)
            encoder.string("benchmark")
        return encoder.save()

    benchmark(encode)


def test_decode_float_array(benchmark, float_array_data):
    """Benchmark decoding of a large float array."""
    def decode():
        decoder = CdrDecoder(float_array_data)
        return decoder.array('float64', 1000)

    benchmark(decode)


def test_encode_float_array(benchmark):
    """Benchmark encoding of a large float array."""
    def encode():
        encoder = CdrEncoder(little_endian=True)
        encoder.array('float64', [float(i) for i in range(1000)])
        return encoder.save()

    benchmark(encode)


def test_decode_int32_sequence(benchmark):
    """Benchmark decoding of int32 values (common type in messages)."""
    # Create test data
    encoder = CdrEncoder(little_endian=True)
    for _ in range(10000):
        encoder.int32(12345678)
    data = encoder.save()

    def decode():
        decoder = CdrDecoder(data)
        for _ in range(10000):
            decoder.int32()

    benchmark(decode)


def test_encode_int32_sequence(benchmark):
    """Benchmark encoding of int32 values (common type in messages)."""
    def encode():
        encoder = CdrEncoder(little_endian=True)
        for _ in range(10000):
            encoder.int32(12345678)
        return encoder.save()

    benchmark(encode)


def test_decode_float64_sequence(benchmark):
    """Benchmark decoding of float64 values (common in robotics)."""
    # Create test data
    encoder = CdrEncoder(little_endian=True)
    for _ in range(5000):
        encoder.float64(3.141592653589793)
    data = encoder.save()

    def decode():
        decoder = CdrDecoder(data)
        for _ in range(5000):
            decoder.float64()

    benchmark(decode)


def test_encode_float64_sequence(benchmark):
    """Benchmark encoding of float64 values (common in robotics)."""
    def encode():
        encoder = CdrEncoder(little_endian=True)
        for _ in range(5000):
            encoder.float64(3.141592653589793)
        return encoder.save()

    benchmark(encode)


def test_decode_mixed_endianness_big(benchmark):
    """Benchmark decoding with big-endian data."""
    # Create test data with big-endian encoding
    encoder = CdrEncoder(little_endian=False)
    for _ in range(1000):
        encoder.int32(12345678)
        encoder.float64(3.141592653589793)
    data = encoder.save()

    def decode():
        decoder = CdrDecoder(data)
        for _ in range(1000):
            decoder.int32()
            decoder.float64()

    benchmark(decode)


def test_encode_mixed_endianness_big(benchmark):
    """Benchmark encoding with big-endian data."""
    def encode():
        encoder = CdrEncoder(little_endian=False)
        for _ in range(1000):
            encoder.int32(12345678)
            encoder.float64(3.141592653589793)
        return encoder.save()

    benchmark(encode)
