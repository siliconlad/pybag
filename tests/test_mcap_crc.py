"""Tests for CRC functionality in src/pybag/mcap/crc.py."""
import tempfile
import zlib
from io import BytesIO
from pathlib import Path

import pytest

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.io.raw_reader import BytesReader, FileReader
from pybag.mcap.crc import (
    DEFAULT_CRC_CHUNK_SIZE,
    McapInvalidCrcError,
    assert_crc,
    assert_data_crc,
    assert_summary_crc,
    compute_crc,
    compute_crc_batched,
    validate_crc,
    validate_data_crc,
    validate_summary_crc
)
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)
from pybag.mcap_writer import McapFileWriter


class TestBasicCrcFunctions:
    """Test basic CRC functions: compute_crc, validate_crc, assert_crc."""

    def test_compute_crc_empty_data(self):
        """Test CRC computation with empty data."""
        result = compute_crc(b"")
        assert result == 0
        assert result == zlib.crc32(b"")

    def test_compute_crc_binary_data(self):
        """Test CRC computation with binary data."""
        data = bytes(range(256))
        result = compute_crc(data)
        expected = zlib.crc32(data)
        assert result == expected

    def test_compute_crc_with_start_value(self):
        """Test incremental CRC computation with start_value."""
        data1 = b"hello"
        data2 = b" world"

        # Compute CRC incrementally
        crc1 = compute_crc(data1)
        crc2 = compute_crc(data2, start_value=crc1)

        # Compute CRC all at once
        crc_combined = compute_crc(data1 + data2)

        assert crc2 == crc_combined

    def test_validate_crc_valid(self):
        """Test validate_crc returns True for matching CRC."""
        data = b"test data"
        crc = zlib.crc32(data)
        assert validate_crc(data, crc) is True

    def test_validate_crc_invalid(self):
        """Test validate_crc returns False for non-matching CRC."""
        data = b"test data"
        wrong_crc = zlib.crc32(b"different data")
        assert validate_crc(data, wrong_crc) is False

    def test_assert_crc_valid(self):
        """Test assert_crc does not raise exception for valid CRC."""
        data = b"test data"
        crc = zlib.crc32(data)
        assert_crc(data, crc)

    def test_assert_crc_invalid(self):
        """Test assert_crc raises McapInvalidCrcError for invalid CRC."""
        data = b"test data"
        wrong_crc = zlib.crc32(b"different data")

        with pytest.raises(McapInvalidCrcError, match="Invalid CRC for data"):
            assert_crc(data, wrong_crc)


class TestBatchedCrcComputation:
    """Test compute_crc_batched function."""

    def test_batched_crc_empty_data(self):
        """Test batched CRC with empty data."""
        reader = BytesReader(b"")
        batched_crc = compute_crc_batched(reader, 0)
        assert batched_crc == 0

    def test_batched_crc_matches_regular(self):
        """Test that batched CRC matches non-batched CRC for same data."""
        data = b"x" * 10000
        reader = BytesReader(data)

        batched_crc = compute_crc_batched(reader, len(data))
        regular_crc = compute_crc(data)

        assert batched_crc == regular_crc

    def test_batched_crc_small_chunk_size(self):
        """Test batched CRC with chunk size smaller than data."""
        data = b"hello world this is a test"
        reader = BytesReader(data)

        # Use small chunk size to ensure multiple reads
        batched_crc = compute_crc_batched(reader, len(data), chunk_size=5)
        regular_crc = compute_crc(data)

        assert batched_crc == regular_crc

    def test_batched_crc_large_chunk_size(self):
        """Test batched CRC with chunk size larger than data."""
        data = b"small data"
        reader = BytesReader(data)

        batched_crc = compute_crc_batched(reader, len(data), chunk_size=1000)
        regular_crc = compute_crc(data)

        assert batched_crc == regular_crc

    def test_batched_crc_partial_read(self):
        """Test batched CRC with partial read of available data."""
        data = b"x" * 10000
        reader = BytesReader(data)

        # Only read first 11 bytes
        batched_crc = compute_crc_batched(reader, 11, chunk_size=5)
        regular_crc = compute_crc(data[:11])

        assert batched_crc == regular_crc
        assert reader.tell() == 11

    def test_batched_crc_premature_end_of_data(self):
        """Test batched CRC when reader has less data than requested."""
        data = b"short data"
        reader = BytesReader(data)

        # Request more bytes than available
        batched_crc = compute_crc_batched(reader, len(data) * 2, chunk_size=5)
        regular_crc = compute_crc(data)

        # Should compute CRC of only available data
        assert batched_crc == regular_crc


class TestFileLevelCrcValidation:
    """Test file-level CRC validation functions."""

    def test_validate_data_crc_valid_file(self):
        """Test validate_data_crc returns True for valid MCAP file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "valid.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="hello"))
                writer.write_message("/test", 2, std_msgs.String(data="world"))

            with FileReader(path) as reader:
                original_pos = reader.tell()
                result = validate_data_crc(reader)
                assert reader.tell() == original_pos
                assert result is True

    def test_validate_data_crc_with_provided_footer(self):
        """Test validate_data_crc with footer parameter provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "valid.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="hello"))

            # First read the footer
            with FileReader(path) as reader:
                reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
                footer = McapRecordParser.parse_footer(reader)
                result = validate_data_crc(reader, footer=footer)
                assert result is True

    def test_assert_data_crc_raises_on_corruption(self):
        """Test that assert_data_crc raises exception when CRC is invalid."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="test"))
                writer.write_message("/test", 2, std_msgs.String(data="test"))
                writer.write_message("/test", 3, std_msgs.String(data="test"))

            # Corrupt the file by modifying some bytes in the data section
            data = bytearray(path.read_bytes())
            data[20] ^= 0xFF
            path.write_bytes(data)

            with FileReader(path) as reader:
                with pytest.raises(McapInvalidCrcError, match="Invalid CRC for data"):
                    assert_data_crc(reader)

    def test_validate_summary_crc_valid_file(self):
        """Test validate_summary_crc returns True for valid MCAP file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "valid.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="hello"))
                writer.write_message("/test", 2, std_msgs.String(data="world"))

            with FileReader(path) as reader:
                original_pos = reader.tell()
                result = validate_summary_crc(reader)
                assert reader.tell() == original_pos
                assert result is True

    def test_validate_summary_crc_with_provided_footer(self):
        """Test validate_summary_crc with footer parameter provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "valid.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="hello"))

            # First read the footer
            with FileReader(path) as reader:
                reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
                footer = McapRecordParser.parse_footer(reader)
                result = validate_summary_crc(reader, footer=footer)
                assert result is True

    def test_assert_summary_crc_raises_on_corruption(self):
        """Test that assert_summary_crc raises exception when CRC is invalid."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1, std_msgs.String(data="test"))
                writer.write_message("/test", 2, std_msgs.String(data="test"))
                writer.write_message("/test", 3, std_msgs.String(data="test"))

            # Corrupt the file by modifying some bytes in the summary section
            data = bytearray(path.read_bytes())
            data[-50] ^= 0xFF
            path.write_bytes(data)

            with FileReader(path) as reader:
                with pytest.raises(McapInvalidCrcError, match="Invalid CRC for summary"):
                    assert_summary_crc(reader)
