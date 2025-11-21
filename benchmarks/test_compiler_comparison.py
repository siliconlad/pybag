"""Benchmark comparison between pure Python and Cython schema compilation."""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator
import time

from pytest_benchmark.fixture import BenchmarkFixture

from pybag.mcap_reader import McapFileReader
from pybag.deserialize import MessageDeserializer
from pybag.schema.compiler import compile_schema, compile_serializer
from pybag.schema.cython_decoder import compile_schema_cython, compile_serializer_cython
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder
from pybag.encoding.cdr import CdrDecoder, CdrEncoder

from .benchmark_utils import create_test_mcap


# Monkey-patch the deserializer to use different compilers
def read_with_python_compiler(mcap: Path) -> Iterator[Any]:
    """Read messages using pure Python compiled decoder."""
    # Patch compile_schema temporarily
    import pybag.deserialize as deserialize_module
    original_compile = deserialize_module.compile_schema
    deserialize_module.compile_schema = compile_schema

    try:
        with McapFileReader.from_file(mcap) as reader:
            for topic in reader.get_topics():
                for message in reader.messages(topic):
                    yield message.data
    finally:
        deserialize_module.compile_schema = original_compile


def read_with_cython_compiler(mcap: Path) -> Iterator[Any]:
    """Read messages using Cython compiled decoder."""
    # Patch compile_schema temporarily
    import pybag.deserialize as deserialize_module
    original_compile = deserialize_module.compile_schema
    deserialize_module.compile_schema = compile_schema_cython

    try:
        with McapFileReader.from_file(mcap) as reader:
            for topic in reader.get_topics():
                for message in reader.messages(topic):
                    yield message.data
    finally:
        deserialize_module.compile_schema = original_compile


def test_python_compiler_read_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark pure Python decoder reading 1000 messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)
        benchmark(lambda: deque(read_with_python_compiler(mcap), maxlen=0))


def test_cython_compiler_read_1000_messages(benchmark: BenchmarkFixture) -> None:
    """Benchmark Cython decoder reading 1000 messages."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1000)
        benchmark(lambda: deque(read_with_cython_compiler(mcap), maxlen=0))


def test_python_compiler_compilation_speed(benchmark: BenchmarkFixture) -> None:
    """Benchmark pure Python schema compilation time."""
    # Create a test mcap to get a real schema
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1)

        # Extract schema
        with McapFileReader.from_file(mcap) as reader:
            # Force a message read to trigger schema parsing
            for topic in reader.get_topics():
                for message in reader.messages(topic):
                    break
                break

            # Get the cached schema from the deserializer
            deserializer = reader._message_deserializer
            schema_id = list(deserializer._compiled.keys())[0]

            # Now extract the raw schema to re-parse it
            from pybag.mcap.record_reader import McapRecordReaderFactory
            record_reader = McapRecordReaderFactory.from_file(mcap)
            schemas = record_reader.get_schemas()
            schema_record = list(schemas.values())[0]

            # Parse the schema
            schema_decoder = Ros2MsgSchemaDecoder()
            schema, sub_schemas = schema_decoder.parse_schema(schema_record.data.decode())

            # Benchmark just the compilation step
            def compile_it():
                return compile_schema(schema, sub_schemas)

            benchmark(compile_it)


def test_cython_compiler_compilation_speed(benchmark: BenchmarkFixture) -> None:
    """Benchmark Cython schema compilation time."""
    # Create a test mcap to get a real schema
    with TemporaryDirectory() as tmpdir:
        mcap = create_test_mcap(Path(tmpdir) / "test", message_count=1)

        # Extract schema
        with McapFileReader.from_file(mcap) as reader:
            # Force a message read to trigger schema parsing
            for topic in reader.get_topics():
                for message in reader.messages(topic):
                    break
                break

            # Get the cached schema from the deserializer
            deserializer = reader._message_deserializer
            schema_id = list(deserializer._compiled.keys())[0]

            # Now extract the raw schema to re-parse it
            from pybag.mcap.record_reader import McapRecordReaderFactory
            record_reader = McapRecordReaderFactory.from_file(mcap)
            schemas = record_reader.get_schemas()
            schema_record = list(schemas.values())[0]

            # Parse the schema
            schema_decoder = Ros2MsgSchemaDecoder()
            schema, sub_schemas = schema_decoder.parse_schema(schema_record.data.decode())

            # Benchmark just the compilation step
            def compile_it():
                return compile_schema_cython(schema, sub_schemas)

            benchmark(compile_it)
