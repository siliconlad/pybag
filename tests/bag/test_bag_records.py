import struct

import pytest

from pybag.bag.records import ChunkInfoRecord, IndexDataRecord


def test_connection_counts_max_uint32():
    """Test connection counts with maximum uint32 values."""
    max_uint32 = 2**32 - 1  # 4294967295

    data = struct.pack('<II', max_uint32, max_uint32)

    record = ChunkInfoRecord(
        ver=1,
        chunk_pos=0,
        start_time=0,
        end_time=0,
        count=1,
        data=data,
    )

    counts = record.connection_counts
    assert max_uint32 in counts
    assert counts[max_uint32] == max_uint32


def test_index_entries_max_offset():
    """Test index entries with maximum uint32 offset."""
    max_offset = 2**32 - 1  # 4294967295

    data = struct.pack('<III', 0, 0, max_offset)

    record = IndexDataRecord(ver=1, conn=0, count=1, data=data)
    entries = record.entries

    timestamp_ns, offset = entries[0]
    assert offset == max_offset
