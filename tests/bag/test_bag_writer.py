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

    def test_bag_header_has_correct_values(self, tmp_path: Path):
        """Test that the bag header's index_pos is non-zero after close.

        The bag header should point to the index section where connection
        and chunk info records are stored.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path, chunk_size=100) as writer:
            writer.write_message("/topic1", 1000, SimpleMessage(value=1, name="a"))
            writer.write_message("/topic2", 2000, SimpleMessage(value=2, name="b"))
            writer.write_message("/topic1", 3000, SimpleMessage(value=3, name="c"))

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
            # Should have two conenctions
            assert header.conn_count == 2, "Should have 2 connections"
            # Should have multiple chunks due to small chunk_size
            assert header.chunk_count >= 1, "Should have at least one chunk"

    def test_index_pos_points_to_valid_index_record(self, tmp_path: Path):
        """Test that seeking to index_pos gives a valid index section record.

        The index section starts with CONNECTION records, followed by CHUNK_INFO records.
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
            # First record in index section should be CONNECTION
            assert op == BagRecordType.CONNECTION, f"Expected CONNECTION, got {op}"


class TestIndexDataRecords:
    """Tests that verify INDEX_DATA records are properly written."""

    def test_contains_index_data_records(self, tmp_path: Path):
        """Test that the index section contains INDEX_DATA records.

        ROS 1 bag format requires INDEX_DATA records for random access
        to messages within chunks.
        """
        bag_path = tmp_path / 'test.bag'

        with BagFileWriter.open(bag_path) as writer:
            writer.write_message("/topic_a", 1_000_000_000, SimpleMessage(value=1, name="a"))
            writer.write_message("/topic_a", 2_000_000_000, SimpleMessage(value=2, name="b"))
            writer.write_message("/topic_b", 3_000_000_000, OtherMessage(data=3.14))

        # Parse the index section and look for INDEX_DATA records
        with FileReader(bag_path) as reader:
            BagRecordParser.parse_version(reader)
            result = BagRecordParser.parse_record(reader)
            assert result is not None

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
            assert total_entries == 3, f"Expected 3 index entries, got {total_entries}"

            # Should have INDEX_DATA for both connections
            conn_ids = {r.conn for r in index_data_records}
            assert len(conn_ids) == 2, f"Expected 2 connections in index, got {len(conn_ids)}"
