"""ROS 1 bag file format support.

This module provides reading and writing support for ROS 1 .bag files
(format version 2.0).
"""

from pybag.bag.record_parser import BagRecordParser, MalformedBag
from pybag.bag.record_writer import BagRecordWriter
from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ChunkRecord,
    ConnectionHeader,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord
)

__all__ = [
    # Records
    'BagHeaderRecord',
    'BagRecordType',
    'ChunkInfoRecord',
    'ChunkRecord',
    'ConnectionHeader',
    'ConnectionRecord',
    'IndexDataRecord',
    'MessageDataRecord',
    # Parser
    'BagRecordParser',
    'MalformedBag',
    # Writer
    'BagRecordWriter',
]
