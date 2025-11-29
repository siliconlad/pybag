from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

import pybag.types as t
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.types import Message


@dataclass
class FixedArray(Message):
    __msg_name__ = 'test_msgs/msg/FixedArray'
    data: t.Array[t.int32, Literal[5]]


@dataclass
class WStringMessage(Message):
    __msg_name__ = 'test_msgs/msg/WStringMessage'
    data: t.wstring


def test_fixed_array_undersized_raises_error(tmp_path: Path):
    """Writing an undersized fixed array should raise an error."""
    mcap_file = tmp_path / 'test.mcap'
    with McapFileWriter.open(mcap_file) as writer:
        msg = FixedArray(data=[1, 2])  # Only 2 elements instead of 5
        # Should raise an error about array size mismatch
        with pytest.raises(ValueError, match=r"[Aa]rray.*size|[Ll]ength"):
            writer.write_message('/test', 1000000000, msg)


def test_fixed_array_oversized_raises_error(tmp_path: Path):
    """Writing an oversized fixed array should raise an error."""
    mcap_file = tmp_path / 'test.mcap'
    with McapFileWriter.open(mcap_file) as writer:
        msg = FixedArray(data=[1, 2, 3, 4, 5, 6, 7])
        # Should raise an error about array size mismatch
        with pytest.raises(ValueError, match=r"[Aa]rray.*size|[Ll]ength"):
            writer.write_message('/test', 1000000000, msg)


def test_fixed_array_correct_size_succeeds(tmp_path: Path):
    """Writing a correctly sized fixed array should succeed."""
    mcap_file = tmp_path / 'test.mcap'
    with McapFileWriter.open(mcap_file) as writer:
        msg = FixedArray(data=[1, 2, 3, 4, 5])  # Exactly 5 elements
        writer.write_message('/test', 1000000000, msg)

    # Verify we can read it back
    with McapFileReader.from_file(mcap_file) as reader:
        messages = list(reader.messages('/test'))
        assert len(messages) == 1
        assert messages[0].data.data == [1, 2, 3, 4, 5]


def test_wstring_with_unicode(tmp_path: Path):
    """Writing a wstring with multi-byte Unicode characters should succeed."""
    mcap_file = tmp_path / 'test.mcap'
    test_string = "Hello 世界"  # Contains multi-byte Unicode characters

    with McapFileWriter.open(mcap_file) as writer:
        msg = WStringMessage(data=test_string)
        writer.write_message('/test', 1000000000, msg)

    # Verify we can read it back
    with McapFileReader.from_file(mcap_file) as reader:
        messages = list(reader.messages('/test'))
        assert len(messages) == 1
        assert messages[0].data.data == test_string


def test_wstring_empty(tmp_path: Path):
    """Writing an empty wstring should succeed."""
    mcap_file = tmp_path / 'test.mcap'

    with McapFileWriter.open(mcap_file) as writer:
        msg = WStringMessage(data="")
        writer.write_message('/test', 1000000000, msg)

    # Verify we can read it back
    with McapFileReader.from_file(mcap_file) as reader:
        messages = list(reader.messages('/test'))
        assert len(messages) == 1
        assert messages[0].data.data == ""


def test_wstring_ascii_only(tmp_path: Path):
    """Writing a wstring with only ASCII characters should succeed."""
    mcap_file = tmp_path / 'test.mcap'
    test_string = "Hello World"

    with McapFileWriter.open(mcap_file) as writer:
        msg = WStringMessage(data=test_string)
        writer.write_message('/test', 1000000000, msg)

    # Verify we can read it back
    with McapFileReader.from_file(mcap_file) as reader:
        messages = list(reader.messages('/test'))
        assert len(messages) == 1
        assert messages[0].data.data == test_string
