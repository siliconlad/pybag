"""Quick test to verify AST compiler works."""
import struct
from pybag.schema import Schema, SchemaField, Primitive
from pybag.schema import compiler as string_compiler
from pybag.schema import compiler_ast as ast_compiler
from pybag.encoding.cdr import CdrDecoder, CdrEncoder

# Create a simple test schema
test_schema = Schema(
    name="Point",
    fields={
        "x": SchemaField(type=Primitive(type="float64")),
        "y": SchemaField(type=Primitive(type="float64")),
        "z": SchemaField(type=Primitive(type="float64")),
    }
)

sub_schemas: dict[str, Schema] = {}

# Test string compiler
print("Testing string-based compiler...")
string_decoder = string_compiler.compile_schema(test_schema, sub_schemas)
string_encoder = string_compiler.compile_serializer(test_schema, sub_schemas)

# Create test data - CDR format has 4-byte header: [0x00, endianness, 0x00, 0x00]
cdr_header = b'\x00\x00\x00\x00'  # Big endian (second byte = 0)
test_payload = struct.pack('>ddd', 1.0, 2.0, 3.0)
test_data = cdr_header + test_payload

decoder = CdrDecoder(test_data)
result1 = string_decoder(decoder)
print(f"String decoder result: x={result1.x}, y={result1.y}, z={result1.z}")

# Encode it back
encoder = CdrEncoder()
string_encoder(encoder, result1)
encoded_data = encoder.save()
print(f"String encoder result: {encoded_data.hex()}")
print(f"Original data:         {test_data.hex()}")
print(f"Match: {encoded_data == test_data}")

# Test AST compiler
print("\nTesting AST-based compiler...")
ast_decoder = ast_compiler.compile_schema(test_schema, sub_schemas)
ast_encoder = ast_compiler.compile_serializer(test_schema, sub_schemas)

# Decode with AST
decoder2 = CdrDecoder(test_data)
result2 = ast_decoder(decoder2)
print(f"AST decoder result: x={result2.x}, y={result2.y}, z={result2.z}")

# Encode it back
encoder2 = CdrEncoder()
ast_encoder(encoder2, result2)
encoded_data2 = encoder2.save()
print(f"AST encoder result: {encoded_data2.hex()}")
print(f"Original data:      {test_data.hex()}")
print(f"Match: {encoded_data2 == test_data}")

# Compare results
print(f"\nDecoders match: {result1.x == result2.x and result1.y == result2.y and result1.z == result2.z}")
print(f"Encoders match: {encoded_data == encoded_data2}")

print("\n✅ All basic tests passed!")
