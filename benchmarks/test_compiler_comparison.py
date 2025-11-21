"""Benchmark comparing string-based vs AST-based schema compilation."""
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap_reader import McapFileReader
from pybag.schema import compiler as string_compiler
from pybag.schema import compiler_ast as ast_compiler

from .benchmark_utils import create_test_mcap


def test_string_compiler_compilation_time(benchmark: BenchmarkFixture) -> None:
    """Benchmark the time to compile schemas using string-based approach."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        def compile_all_schemas():
            with McapFileReader.from_file(mcap) as reader:
                # Access schemas to trigger compilation
                for topic in reader.get_topics():
                    schema = reader._get_schema(topic.schema_id)
                    # Force recompilation by clearing cache
                    reader._decoders.clear()
                    reader._encoders.clear()
                    # Get decoder (triggers compilation)
                    reader._get_decoder(schema)

        benchmark(compile_all_schemas)


def test_ast_compiler_compilation_time(benchmark: BenchmarkFixture) -> None:
    """Benchmark the time to compile schemas using AST-based approach."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        def compile_all_schemas():
            with McapFileReader.from_file(mcap) as reader:
                # Access schemas to trigger compilation
                for topic in reader.get_topics():
                    schema = reader._get_schema(topic.schema_id)
                    # Temporarily replace compiler with AST version
                    original_compile = string_compiler.compile_schema
                    string_compiler.compile_schema = ast_compiler.compile_schema

                    # Force recompilation by clearing cache
                    reader._decoders.clear()
                    reader._encoders.clear()
                    # Get decoder (triggers compilation)
                    reader._get_decoder(schema)

                    # Restore original compiler
                    string_compiler.compile_schema = original_compile

        benchmark(compile_all_schemas)


def test_string_compiler_decode_runtime(benchmark: BenchmarkFixture) -> None:
    """Benchmark runtime performance of string-compiled decoders."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        def decode_all_messages():
            with McapFileReader.from_file(mcap) as reader:
                count = 0
                for topic in reader.get_topics():
                    for message in reader.messages(topic):
                        _ = message.data
                        count += 1
                return count

        benchmark(decode_all_messages)


def test_ast_compiler_decode_runtime(benchmark: BenchmarkFixture) -> None:
    """Benchmark runtime performance of AST-compiled decoders."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        # Monkey-patch the compiler before loading
        original_compile = string_compiler.compile_schema
        string_compiler.compile_schema = ast_compiler.compile_schema

        try:
            def decode_all_messages():
                with McapFileReader.from_file(mcap) as reader:
                    count = 0
                    for topic in reader.get_topics():
                        for message in reader.messages(topic):
                            _ = message.data
                            count += 1
                    return count

            benchmark(decode_all_messages)
        finally:
            # Restore original compiler
            string_compiler.compile_schema = original_compile


def test_string_compiler_encode_runtime(benchmark: BenchmarkFixture) -> None:
    """Benchmark runtime performance of string-compiled encoders."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        # First, collect all messages
        messages_to_encode = []
        with McapFileReader.from_file(mcap) as reader:
            for topic in reader.get_topics():
                for message in reader.messages(topic):
                    messages_to_encode.append((reader._get_encoder(reader._get_schema(topic.schema_id)), message.data))

        def encode_all_messages():
            from pybag.encoding import MessageEncoder
            for encoder_func, data in messages_to_encode:
                encoder = MessageEncoder()
                encoder_func(encoder, data)
                _ = encoder.payload

        benchmark(encode_all_messages)


def test_ast_compiler_encode_runtime(benchmark: BenchmarkFixture) -> None:
    """Benchmark runtime performance of AST-compiled encoders."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test")

        # Monkey-patch both compilers
        original_compile_decoder = string_compiler.compile_schema
        original_compile_encoder = string_compiler.compile_serializer
        string_compiler.compile_schema = ast_compiler.compile_schema
        string_compiler.compile_serializer = ast_compiler.compile_serializer

        try:
            # First, collect all messages
            messages_to_encode = []
            with McapFileReader.from_file(mcap) as reader:
                for topic in reader.get_topics():
                    for message in reader.messages(topic):
                        messages_to_encode.append((reader._get_encoder(reader._get_schema(topic.schema_id)), message.data))

            def encode_all_messages():
                from pybag.encoding import MessageEncoder
                for encoder_func, data in messages_to_encode:
                    encoder = MessageEncoder()
                    encoder_func(encoder, data)
                    _ = encoder.payload

            benchmark(encode_all_messages)
        finally:
            # Restore original compilers
            string_compiler.compile_schema = original_compile_decoder
            string_compiler.compile_serializer = original_compile_encoder


def test_compiler_method_comparison(benchmark: BenchmarkFixture) -> None:
    """Direct comparison of compilation methods for a simple schema."""
    from pybag.schema import Schema, SchemaField, Primitive
    from pybag.encoding import MessageDecoder

    # Create a simple test schema
    test_schema = Schema(
        name="TestMessage",
        fields={
            "x": SchemaField(type=Primitive(type="float64")),
            "y": SchemaField(type=Primitive(type="float64")),
            "z": SchemaField(type=Primitive(type="float64")),
            "timestamp": SchemaField(type=Primitive(type="uint64")),
        }
    )

    sub_schemas: dict[str, Schema] = {}

    def compile_with_string():
        return string_compiler.compile_schema(test_schema, sub_schemas)

    def compile_with_ast():
        return ast_compiler.compile_schema(test_schema, sub_schemas)

    # Benchmark string compilation
    benchmark.group = "compilation"
    benchmark(compile_with_string)
