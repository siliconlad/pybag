"""Direct comparison benchmark between string-based and AST-based compilation."""
import time
from pybag.schema import Schema, SchemaField, Primitive, Array, Sequence, Complex
from pybag.schema import compiler as string_compiler
from pybag.schema import compiler_ast as ast_compiler
from pybag.encoding.cdr import CdrDecoder, CdrEncoder
import struct

# Create test schemas of varying complexity
def create_simple_schema():
    """Simple schema with 3 primitive fields."""
    return Schema(
        name="SimpleMessage",
        fields={
            "x": SchemaField(type=Primitive(type="float64")),
            "y": SchemaField(type=Primitive(type="float64")),
            "z": SchemaField(type=Primitive(type="float64")),
        }
    )

def create_medium_schema():
    """Medium complexity schema with various field types."""
    return Schema(
        name="MediumMessage",
        fields={
            "timestamp": SchemaField(type=Primitive(type="uint64")),
            "x": SchemaField(type=Primitive(type="float64")),
            "y": SchemaField(type=Primitive(type="float64")),
            "z": SchemaField(type=Primitive(type="float64")),
            "vx": SchemaField(type=Primitive(type="float32")),
            "vy": SchemaField(type=Primitive(type="float32")),
            "vz": SchemaField(type=Primitive(type="float32")),
            "id": SchemaField(type=Primitive(type="uint32")),
            "active": SchemaField(type=Primitive(type="bool")),
        }
    )

def create_complex_schema():
    """Complex schema with arrays and sequences."""
    return Schema(
        name="ComplexMessage",
        fields={
            "timestamp": SchemaField(type=Primitive(type="uint64")),
            "position": SchemaField(type=Array(type=Primitive(type="float64"), length=3)),
            "velocity": SchemaField(type=Array(type=Primitive(type="float64"), length=3)),
            "acceleration": SchemaField(type=Array(type=Primitive(type="float64"), length=3)),
            "covariance": SchemaField(type=Array(type=Primitive(type="float64"), length=36)),
            "labels": SchemaField(type=Sequence(type=Primitive(type="uint32"))),
        }
    )

def benchmark_compilation(schema, name, iterations=1000):
    """Benchmark compilation time for both approaches."""
    sub_schemas = {}

    # Warmup
    string_compiler.compile_schema(schema, sub_schemas)
    ast_compiler.compile_schema(schema, sub_schemas)

    # Benchmark string-based
    start = time.perf_counter()
    for _ in range(iterations):
        string_compiler.compile_schema(schema, sub_schemas)
    string_time = time.perf_counter() - start

    # Benchmark AST-based
    start = time.perf_counter()
    for _ in range(iterations):
        ast_compiler.compile_schema(schema, sub_schemas)
    ast_time = time.perf_counter() - start

    print(f"\n{name} Schema Compilation ({iterations} iterations):")
    print(f"  String-based: {string_time*1000:.2f} ms ({string_time/iterations*1000000:.2f} µs per compilation)")
    print(f"  AST-based:    {ast_time*1000:.2f} ms ({ast_time/iterations*1000000:.2f} µs per compilation)")
    print(f"  Speedup:      {string_time/ast_time:.2f}x {'(AST faster)' if ast_time < string_time else '(String faster)'}")

    return string_time, ast_time

def benchmark_runtime(schema, test_data, name, iterations=10000):
    """Benchmark runtime performance of compiled decoders."""
    sub_schemas = {}

    # Compile with both approaches
    string_decoder = string_compiler.compile_schema(schema, sub_schemas)
    ast_decoder = ast_compiler.compile_schema(schema, sub_schemas)

    # Warmup
    for _ in range(100):
        decoder = CdrDecoder(test_data)
        string_decoder(decoder)
        decoder = CdrDecoder(test_data)
        ast_decoder(decoder)

    # Benchmark string-based decoder
    start = time.perf_counter()
    for _ in range(iterations):
        decoder = CdrDecoder(test_data)
        result = string_decoder(decoder)
    string_time = time.perf_counter() - start

    # Benchmark AST-based decoder
    start = time.perf_counter()
    for _ in range(iterations):
        decoder = CdrDecoder(test_data)
        result = ast_decoder(decoder)
    ast_time = time.perf_counter() - start

    print(f"\n{name} Schema Runtime ({iterations} iterations):")
    print(f"  String-based: {string_time*1000:.2f} ms ({string_time/iterations*1000000:.2f} µs per decode)")
    print(f"  AST-based:    {ast_time*1000:.2f} ms ({ast_time/iterations*1000000:.2f} µs per decode)")
    print(f"  Speedup:      {string_time/ast_time:.2f}x {'(AST faster)' if ast_time < string_time else '(String faster)'}")

    return string_time, ast_time

def main():
    print("=" * 80)
    print("PyBag Schema Compiler Benchmark: String-based vs AST-based")
    print("=" * 80)

    # Test 1: Simple schema compilation
    simple_schema = create_simple_schema()
    benchmark_compilation(simple_schema, "Simple", iterations=1000)

    # Test 2: Medium schema compilation
    medium_schema = create_medium_schema()
    benchmark_compilation(medium_schema, "Medium", iterations=1000)

    # Test 3: Complex schema compilation
    complex_schema = create_complex_schema()
    benchmark_compilation(complex_schema, "Complex", iterations=1000)

    # Test 4: Simple schema runtime
    simple_data = b'\x00\x00\x00\x00' + struct.pack('>ddd', 1.0, 2.0, 3.0)
    benchmark_runtime(simple_schema, simple_data, "Simple", iterations=10000)

    # Test 5: Medium schema runtime
    medium_data = b'\x00\x00\x00\x00' + struct.pack('>QdddfffI?',
        123456789, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 42, True)
    benchmark_runtime(medium_schema, medium_data, "Medium", iterations=10000)

    # Test 6: Complex schema runtime
    complex_data = b'\x00\x00\x00\x00' + struct.pack('>Q', 123456789)  # timestamp
    complex_data += struct.pack('>ddd', 1.0, 2.0, 3.0)  # position
    complex_data += struct.pack('>ddd', 4.0, 5.0, 6.0)  # velocity
    complex_data += struct.pack('>ddd', 7.0, 8.0, 9.0)  # acceleration
    complex_data += struct.pack('>36d', *[float(i) for i in range(36)])  # covariance
    complex_data += struct.pack('>I', 3)  # labels length
    complex_data += struct.pack('>III', 1, 2, 3)  # labels
    benchmark_runtime(complex_schema, complex_data, "Complex", iterations=10000)

    print("\n" + "=" * 80)
    print("Benchmark Complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()
