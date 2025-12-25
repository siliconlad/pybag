"""Tests for BagFileWriter header and index handling."""

import struct
from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag
from pybag.bag.record_parser import BagRecordParser
from pybag.bag.records import BagRecordType, IndexDataRecord
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.io.raw_reader import FileReader


@dataclass(kw_only=True)
class SimpleMessage:
    """A simple test message."""
    __msg_name__ = 'test_msgs/SimpleMessage'
    value: pybag.int32
    name: pybag.string


@dataclass(kw_only=True)
class OtherMessage:
    """Another test message type."""
    __msg_name__ = 'test_msgs/OtherMessage'
    data: pybag.float64


class TestBagHeaderOffsets:
    """Tests that verify the bag header is correctly written with proper offsets."""

    def test_bag_header_has_nonzero_index_pos(self, tmp_path: Path):
        """Test that the bag header's index_pos is non-zero after close.

        The bag header should point to the index section where connection
        and chunk info records are stored.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            msg = SimpleMessage(value=42, name="hello")
            writer.write_message("/test", 1_000_000_000, msg)

        # Read the raw file and check the header
        with FileReader(bag_path) as reader:
            version = BagRecordParser.parse_version(reader)
            assert version == '2.0'

            result = BagRecordParser.parse_record(reader)
            assert result is not None
            op, header = result
            assert op == BagRecordType.BAG_HEADER

            # The index_pos should be non-zero and point past the chunks
            assert header.index_pos > 0, "index_pos should be non-zero"

    def test_bag_header_has_correct_conn_count(self, tmp_path: Path):
        """Test that the bag header has the correct connection count."""
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/topic1", 1000, SimpleMessage(value=1, name="a"))
            writer.write_message("/topic2", 2000, SimpleMessage(value=2, name="b"))
            writer.write_message("/topic1", 3000, SimpleMessage(value=3, name="c"))

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            assert header.conn_count == 2, "Should have 2 connections"

    def test_bag_header_has_correct_chunk_count(self, tmp_path: Path):
        """Test that the bag header has the correct chunk count."""
        bag_path = tmp_path / 'test.bag'

        # Use small chunk size to force multiple chunks
        with BagFileWriter.open(bag_path, chunk_size=100) as writer:
            for i in range(50):
                msg = SimpleMessage(value=i, name=f"message_{i}")
                writer.write_message("/test", i * 1000, msg)

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            # Should have multiple chunks due to small chunk_size
            assert header.chunk_count >= 1, "Should have at least one chunk"

    def test_reader_finds_topics_after_write(self, tmp_path: Path):
        """Test that BagFileReader can find topics from a written bag.

        This is the key integration test - if index_pos is wrong, the reader
        won't be able to find the connection records in the index section.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/sensor/camera", 1000, SimpleMessage(value=1, name="cam"))
            writer.write_message("/sensor/lidar", 2000, SimpleMessage(value=2, name="lidar"))

        with BagFileReader.from_file(bag_path) as reader:
            topics = reader.get_topics()
            assert len(topics) == 2, f"Expected 2 topics, got {len(topics)}"
            assert set(topics) == {"/sensor/camera", "/sensor/lidar"}

    def test_reader_can_read_messages_after_write(self, tmp_path: Path):
        """Test that BagFileReader can read messages from a written bag.

        This verifies the complete roundtrip works.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/test", 1_000_000_000, SimpleMessage(value=42, name="hello"))

        with BagFileReader.from_file(bag_path) as reader:
            messages = list(reader.messages("/test"))
            assert len(messages) == 1, f"Expected 1 message, got {len(messages)}"
            assert messages[0].data.value == 42
            assert messages[0].data.name == "hello"

    def test_index_pos_points_to_valid_index_record(self, tmp_path: Path):
        """Test that seeking to index_pos gives a valid index section record.

        The index section starts with INDEX_DATA records, followed by
        CONNECTION records, then CHUNK_INFO records.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/test", 1000, SimpleMessage(value=1, name="test"))

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            # Seek to index_pos and read the record there
            reader.seek_from_start(header.index_pos)
            index_result = BagRecordParser.parse_record(reader)
            assert index_result is not None

            op, _ = index_result
            # First record in index section should be INDEX_DATA
            assert op == BagRecordType.INDEX_DATA, f"Expected INDEX_DATA, got {op}"


class TestIndexDataRecords:
    """Tests that verify INDEX_DATA records are properly written."""

    def test_index_section_contains_index_data_records(self, tmp_path: Path):
        """Test that the index section contains INDEX_DATA records.

        ROS 1 bag format requires INDEX_DATA records for random access
        to messages within chunks.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/test", 1_000_000_000, SimpleMessage(value=1, name="a"))
            writer.write_message("/test", 2_000_000_000, SimpleMessage(value=2, name="b"))

        # Parse the index section and look for INDEX_DATA records
        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            # Seek to index section
            reader.seek_from_start(header.index_pos)

            # Collect all record types in the index section
            record_types = []
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break
                op, _ = result
                record_types.append(op)

            # Should have INDEX_DATA records
            assert BagRecordType.INDEX_DATA in record_types, \
                f"INDEX_DATA not found in index section. Found: {record_types}"

    def test_index_data_has_correct_entry_count(self, tmp_path: Path):
        """Test that INDEX_DATA records have the correct number of entries."""
        bag_path = tmp_path / 'test.bag'

        # Write 5 messages to a single topic
        with BagFileWriter.open(bag_path) as writer:
            for i in range(5):
                writer.write_message("/test", i * 1_000_000_000, SimpleMessage(value=i, name=f"msg{i}"))

        # Parse and find the INDEX_DATA record
        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            reader.seek_from_start(header.index_pos)

            index_data_records = []
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break
                op, record = result
                if op == BagRecordType.INDEX_DATA:
                    index_data_records.append(record)

            # Should have at least one INDEX_DATA record
            assert len(index_data_records) >= 1

            # Total entries across all INDEX_DATA records should equal message count
            total_entries = sum(r.count for r in index_data_records)
            assert total_entries == 5, f"Expected 5 index entries, got {total_entries}"

    def test_index_data_per_connection(self, tmp_path: Path):
        """Test that INDEX_DATA records are created per connection."""
        bag_path = tmp_path / 'test.bag'

        # Write messages to two different topics
        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/topic_a", 1_000_000_000, SimpleMessage(value=1, name="a"))
            writer.write_message("/topic_a", 2_000_000_000, SimpleMessage(value=2, name="b"))
            writer.write_message("/topic_b", 3_000_000_000, OtherMessage(data=3.14))

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            reader.seek_from_start(header.index_pos)

            index_data_records: list[IndexDataRecord] = []
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break
                op, record = result
                if op == BagRecordType.INDEX_DATA:
                    index_data_records.append(record)

            # Should have INDEX_DATA for both connections
            conn_ids = {r.conn for r in index_data_records}
            assert len(conn_ids) == 2, f"Expected 2 connections in index, got {len(conn_ids)}"

    def test_index_data_entries_have_valid_offsets(self, tmp_path: Path):
        """Test that INDEX_DATA entries have valid chunk offsets."""
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/test", 1_000_000_000, SimpleMessage(value=1, name="a"))
            writer.write_message("/test", 2_000_000_000, SimpleMessage(value=2, name="b"))

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            reader.seek_from_start(header.index_pos)

            index_data_records: list[IndexDataRecord] = []
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break
                op, record = result
                if op == BagRecordType.INDEX_DATA:
                    index_data_records.append(record)

            assert len(index_data_records) >= 1, "No INDEX_DATA records found"

            for idx_record in index_data_records:
                # Each entry should have (time_sec, time_nsec, offset)
                for time_sec, time_nsec, offset in idx_record.entries:
                    # Offset should be non-negative (relative to chunk start)
                    assert offset >= 0, f"Invalid offset: {offset}"
                    # Time values should be reasonable
                    assert time_sec >= 0, f"Invalid time_sec: {time_sec}"
                    assert 0 <= time_nsec < 1_000_000_000, f"Invalid time_nsec: {time_nsec}"

    def test_index_data_with_multiple_chunks(self, tmp_path: Path):
        """Test that INDEX_DATA records are written for each chunk."""
        bag_path = tmp_path / 'test.bag'

        # Use small chunk size to force multiple chunks
        with BagFileWriter.open(bag_path, chunk_size=100) as writer:
            for i in range(20):
                writer.write_message("/test", i * 1_000_000_000, SimpleMessage(value=i, name=f"msg{i}"))

        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None
            _, header = result

            # Should have multiple chunks
            assert header.chunk_count > 1, f"Expected multiple chunks, got {header.chunk_count}"

            reader.seek_from_start(header.index_pos)

            index_data_records: list[IndexDataRecord] = []
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break
                op, record = result
                if op == BagRecordType.INDEX_DATA:
                    index_data_records.append(record)

            # Should have INDEX_DATA records (one per chunk per connection)
            assert len(index_data_records) >= header.chunk_count, \
                f"Expected at least {header.chunk_count} INDEX_DATA records, got {len(index_data_records)}"

            # Total entries should equal total messages
            total_entries = sum(r.count for r in index_data_records)
            assert total_entries == 20, f"Expected 20 total entries, got {total_entries}"
