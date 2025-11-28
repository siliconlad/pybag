"""Tests for bug fixes in pybag."""
import os
import tempfile
from dataclasses import dataclass
from typing import Annotated, Literal

import pytest

import pybag.types as t
from pybag.encoding.cdr import CdrDecoder, CdrEncoder
from pybag.mcap.records import SchemaRecord
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder
from pybag.types import Message


class TestBug1WstringDecoding:
    """Bug 1: CdrDecoder missing wstring() method."""

    def test_cdr_decoder_has_wstring_method(self):
        """CdrDecoder should have a wstring() method."""
        assert hasattr(CdrDecoder, 'wstring'), "CdrDecoder missing wstring() method"

    def test_wstring_roundtrip(self):
        """Test encoding and decoding wstring values."""
        encoder = CdrEncoder()
        test_string = "Hello, World! 你好世界"
        encoder.wstring(test_string)

        data = encoder.save()
        decoder = CdrDecoder(data)
        result = decoder.wstring()

        assert result == test_string

    def test_wstring_empty(self):
        """Test encoding and decoding empty wstring."""
        encoder = CdrEncoder()
        encoder.wstring("")

        data = encoder.save()
        decoder = CdrDecoder(data)
        result = decoder.wstring()

        assert result == ""


class TestBug2FixedArrayValidation:
    """Bug 2: Fixed array size not validated during write."""

    def test_fixed_array_undersized_raises_error(self):
        """Writing an undersized fixed array should raise an error."""
        @dataclass
        class TestFixedArray(Message):
            __msg_name__ = 'test_msgs/msg/TestFixedArray'
            data: Annotated[list[Annotated[t.int32, ('int32',)]], ('array', Annotated[t.int32, ('int32',)], Literal[5])]

        with tempfile.NamedTemporaryFile(suffix='.mcap', delete=False) as f:
            temp_path = f.name

        try:
            writer = McapFileWriter.open(temp_path)
            msg = TestFixedArray(data=[1, 2])  # Only 2 elements instead of 5

            # Should raise an error about array size mismatch
            with pytest.raises(ValueError, match=r"[Aa]rray.*size|[Ll]ength"):
                writer.write_message('/test', 1000000000, msg)
            writer.close()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_fixed_array_oversized_raises_error(self):
        """Writing an oversized fixed array should raise an error."""
        @dataclass
        class TestFixedArray(Message):
            __msg_name__ = 'test_msgs/msg/TestFixedArray'
            data: Annotated[list[Annotated[t.int32, ('int32',)]], ('array', Annotated[t.int32, ('int32',)], Literal[5])]

        with tempfile.NamedTemporaryFile(suffix='.mcap', delete=False) as f:
            temp_path = f.name

        try:
            writer = McapFileWriter.open(temp_path)
            msg = TestFixedArray(data=[1, 2, 3, 4, 5, 6, 7])  # 7 elements instead of 5

            # Should raise an error about array size mismatch
            with pytest.raises(ValueError, match=r"[Aa]rray.*size|[Ll]ength"):
                writer.write_message('/test', 1000000000, msg)
            writer.close()
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_fixed_array_correct_size_succeeds(self):
        """Writing a correctly sized fixed array should succeed."""
        @dataclass
        class TestFixedArray(Message):
            __msg_name__ = 'test_msgs/msg/TestFixedArray'
            data: Annotated[list[Annotated[t.int32, ('int32',)]], ('array', Annotated[t.int32, ('int32',)], Literal[5])]

        with tempfile.NamedTemporaryFile(suffix='.mcap', delete=False) as f:
            temp_path = f.name

        try:
            writer = McapFileWriter.open(temp_path)
            msg = TestFixedArray(data=[1, 2, 3, 4, 5])  # Exactly 5 elements
            writer.write_message('/test', 1000000000, msg)
            writer.close()

            # Verify we can read it back
            reader = McapFileReader.from_file(temp_path)
            messages = list(reader.messages('/test'))
            assert len(messages) == 1
            assert messages[0].data.data == [1, 2, 3, 4, 5]
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestBug3ByteTypeDefaultValue:
    """Bug 3: byte type causes TypeError when parsing default values."""

    def test_byte_constant_parsing(self):
        """Parsing a schema with byte constants should not raise TypeError."""
        diagnostic_status_schema = SchemaRecord(
            id=1,
            name='diagnostic_msgs/msg/DiagnosticStatus',
            encoding='ros2msg',
            data=b'byte OK=0\nbyte WARN=1\nbyte ERROR=2\nbyte STALE=3\nbyte level\nstring name\nstring message\nstring hardware_id\ndiagnostic_msgs/KeyValue[] values\n================================================================================\nMSG: diagnostic_msgs/KeyValue\nstring key\nstring value\n'
        )

        decoder = Ros2MsgSchemaDecoder()
        # This should not raise TypeError
        schema, sub_schemas = decoder.parse_schema(diagnostic_status_schema)

        # Verify the constants were parsed correctly
        assert 'OK' in schema.fields
        assert 'WARN' in schema.fields
        assert 'ERROR' in schema.fields
        assert 'STALE' in schema.fields

    def test_byte_field_with_default_parsing(self):
        """Parsing a schema with byte field default should not raise TypeError."""
        schema = SchemaRecord(
            id=1,
            name='test_msgs/msg/ByteTest',
            encoding='ros2msg',
            data=b'byte value 42\n'
        )

        decoder = Ros2MsgSchemaDecoder()
        # This should not raise TypeError
        parsed_schema, _ = decoder.parse_schema(schema)

        # Verify the default value was parsed correctly
        assert 'value' in parsed_schema.fields
        assert parsed_schema.fields['value'].default == 42

    def test_byte_array_default_parsing(self):
        """Parsing a schema with byte array default should work."""
        schema = SchemaRecord(
            id=1,
            name='test_msgs/msg/ByteArrayTest',
            encoding='ros2msg',
            data=b'byte[3] data [1, 2, 3]\n'
        )

        decoder = Ros2MsgSchemaDecoder()
        parsed_schema, _ = decoder.parse_schema(schema)

        assert 'data' in parsed_schema.fields
        assert parsed_schema.fields['data'].default == [1, 2, 3]
