"""Quick test to verify Rust CDR encoder/decoder works correctly."""
import sys

# Use installed package, not src
from pybag.encoding.cdr import CdrEncoder as PythonCdrEncoder, CdrDecoder as PythonCdrDecoder
from pybag.encoding.cdr_rust import CdrEncoder as RustCdrEncoder, CdrDecoder as RustCdrDecoder

# Test primitive types
def test_primitives():
    print("Testing primitive types...")

    # Python encoder
    py_enc = PythonCdrEncoder(little_endian=True)
    py_enc.bool(True)
    py_enc.int8(-42)
    py_enc.uint8(200)
    py_enc.int16(-1234)
    py_enc.uint16(5678)
    py_enc.int32(-123456)
    py_enc.uint32(654321)
    py_enc.int64(-123456789)
    py_enc.uint64(987654321)
    py_enc.float32(3.14159)
    py_enc.float64(2.718281828)
    py_enc.string("Hello, World!")
    py_data = py_enc.save()

    # Rust encoder
    rust_enc = RustCdrEncoder(little_endian=True)
    rust_enc.bool(True)
    rust_enc.int8(-42)
    rust_enc.uint8(200)
    rust_enc.int16(-1234)
    rust_enc.uint16(5678)
    rust_enc.int32(-123456)
    rust_enc.uint32(654321)
    rust_enc.int64(-123456789)
    rust_enc.uint64(987654321)
    rust_enc.float32(3.14159)
    rust_enc.float64(2.718281828)
    rust_enc.string("Hello, World!")
    rust_data = rust_enc.save()

    print(f"Python encoded: {len(py_data)} bytes")
    print(f"Rust encoded: {len(rust_data)} bytes")

    if py_data == rust_data:
        print("✓ Encoders produce identical output!")
    else:
        print("✗ Encoders produce different output!")
        print(f"Python: {py_data.hex()}")
        print(f"Rust:   {rust_data.hex()}")
        return False

    # Test decoding with Rust decoder
    rust_dec = RustCdrDecoder(rust_data)
    assert rust_dec.bool() == True
    assert rust_dec.int8() == -42
    assert rust_dec.uint8() == 200
    assert rust_dec.int16() == -1234
    assert rust_dec.uint16() == 5678
    assert rust_dec.int32() == -123456
    assert rust_dec.uint32() == 654321
    assert rust_dec.int64() == -123456789
    assert rust_dec.uint64() == 987654321
    assert abs(rust_dec.float32() - 3.14159) < 0.0001
    assert abs(rust_dec.float64() - 2.718281828) < 0.0000001
    assert rust_dec.string() == "Hello, World!"

    print("✓ Rust decoder works correctly!")

    # Test decoding Python data with Rust decoder
    py_dec_rust = RustCdrDecoder(py_data)
    assert py_dec_rust.bool() == True
    assert py_dec_rust.int8() == -42
    assert py_dec_rust.uint8() == 200
    assert py_dec_rust.int16() == -1234
    assert py_dec_rust.uint16() == 5678
    assert py_dec_rust.int32() == -123456
    assert py_dec_rust.uint32() == 654321
    assert py_dec_rust.int64() == -123456789
    assert py_dec_rust.uint64() == 987654321
    assert abs(py_dec_rust.float32() - 3.14159) < 0.0001
    assert abs(py_dec_rust.float64() - 2.718281828) < 0.0000001
    assert py_dec_rust.string() == "Hello, World!"

    print("✓ Rust decoder can decode Python-encoded data!")

    # Test decoding Rust data with Python decoder
    rust_dec_py = PythonCdrDecoder(rust_data)
    assert rust_dec_py.bool() == True
    assert rust_dec_py.int8() == -42
    assert rust_dec_py.uint8() == 200
    assert rust_dec_py.int16() == -1234
    assert rust_dec_py.uint16() == 5678
    assert rust_dec_py.int32() == -123456
    assert rust_dec_py.uint32() == 654321
    assert rust_dec_py.int64() == -123456789
    assert rust_dec_py.uint64() == 987654321
    assert abs(rust_dec_py.float32() - 3.14159) < 0.0001
    assert abs(rust_dec_py.float64() - 2.718281828) < 0.0000001
    assert rust_dec_py.string() == "Hello, World!"

    print("✓ Python decoder can decode Rust-encoded data!")

    return True


def test_arrays():
    print("\nTesting arrays...")

    # Test float array
    float_values = [1.0, 2.0, 3.0, 4.0, 5.0]

    py_enc = PythonCdrEncoder(little_endian=True)
    py_enc.array('float64', float_values)
    py_data = py_enc.save()

    rust_enc = RustCdrEncoder(little_endian=True)
    rust_enc.array('float64', float_values)
    rust_data = rust_enc.save()

    if py_data == rust_data:
        print("✓ Array encoding matches!")
    else:
        print("✗ Array encoding differs!")
        return False

    # Decode with Rust
    rust_dec = RustCdrDecoder(rust_data)
    decoded = rust_dec.array('float64', 5)
    assert decoded == float_values, f"Expected {float_values}, got {decoded}"

    print("✓ Array decoding works!")
    return True


if __name__ == '__main__':
    try:
        if test_primitives() and test_arrays():
            print("\n✓ All tests passed!")
            sys.exit(0)
        else:
            print("\n✗ Some tests failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
