"""Writer for ROS 1 bag file format 2.0 records."""

import bz2
import struct
from typing import Literal

# Number of nanoseconds in one second
NSEC_PER_SEC = 1_000_000_000

from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord
)
from pybag.io.raw_writer import BaseWriter

# ROS 1 bag format version string
BAG_VERSION = b'#ROSBAG V2.0\n'


class BagRecordWriter:
    """Writer for ROS 1 bag file records."""

    def __init__(self, writer: BaseWriter):
        """Initialize the record writer.

        Args:
            writer: The underlying writer for binary output.
        """
        self._writer = writer

    def write_version(self) -> int:
        """Write the bag file version string.

        Returns:
            Number of bytes written.
        """
        return self._writer.write(BAG_VERSION)

    @staticmethod
    def _encode_header_field(name: str, value: bytes) -> bytes:
        """Encode a single header field.

        Format: field_len (4 bytes) | name=value
        """
        field_data = name.encode('ascii') + b'=' + value
        return struct.pack('<I', len(field_data)) + field_data

    @staticmethod
    def _encode_ros_time(nanoseconds: int) -> bytes:
        """Encode a timestamp as ROS time format (sec, nsec).

        ROS time is stored as two 32-bit unsigned integers:
        - secs: seconds since epoch
        - nsecs: nanoseconds within the second

        Args:
            nanoseconds: Timestamp in nanoseconds since epoch.

        Returns:
            8 bytes encoding the time as (secs, nsecs).
        """
        secs = nanoseconds // NSEC_PER_SEC
        nsecs = nanoseconds % NSEC_PER_SEC
        return struct.pack('<II', secs, nsecs)

    def _write_record(
        self,
        op: int,
        header_fields: list[tuple[str, bytes]],
        data: bytes
    ) -> int:
        """Write a complete record.

        Format: header_len | header | data_len | data

        Args:
            op: The operation code.
            header_fields: List of (name, value) tuples for header fields.
            data: The record data.

        Returns:
            Number of bytes written.
        """
        # Build header with op field first
        header = self._encode_header_field('op', struct.pack('<B', op))
        for name, value in header_fields:
            header += self._encode_header_field(name, value)

        # Write: header_len | header | data_len | data
        result = 0
        result += self._writer.write(struct.pack('<i', len(header)))
        result += self._writer.write(header)
        result += self._writer.write(struct.pack('<i', len(data)))
        result += self._writer.write(data)
        return result

    def write_bag_header(
        self,
        bag_header: BagHeaderRecord,
    ) -> int:
        """Write a bag header record.

        Args:
            bag_header: The bag header record to write

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('index_pos', struct.pack('<q', bag_header.index_pos)),
            ('conn_count', struct.pack('<i', bag_header.conn_count)),
            ('chunk_count', struct.pack('<i', bag_header.chunk_count)),
        ]
        return self._write_record(BagRecordType.BAG_HEADER, header_fields, bag_header.data)

    def write_connection(self, conn: ConnectionRecord) -> int:
        """Write a connection record.

        Args:
            conn: The connection record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('conn', struct.pack('<i', conn.conn)),
            ('topic', conn.topic.encode('utf-8')),
        ]
        return self._write_record(BagRecordType.CONNECTION, header_fields, conn.data)

    def write_message_data(self, msg: MessageDataRecord) -> int:
        """Write a message data record.

        Args:
            msg: The message data record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('conn', struct.pack('<i', msg.conn)),
            ('time', self._encode_ros_time(msg.time)),
        ]
        return self._write_record(BagRecordType.MSG_DATA, header_fields, msg.data)

    def write_chunk(
        self,
        data: bytes,
        compression: Literal['none', 'bz2'] = 'none'
    ) -> int:
        """Write a chunk record.

        Args:
            data: The uncompressed chunk data.
            compression: Compression algorithm to use.

        Returns:
            Number of bytes written.
        """
        uncompressed_size = len(data)

        if compression == 'none':
            compressed_data = data
        elif compression == 'bz2':
            compressed_data = bz2.compress(data)
        else:
            raise ValueError(f'Unknown compression type: {compression}')

        header_fields = [
            ('compression', compression.encode('ascii')),
            ('size', struct.pack('<i', uncompressed_size)),
        ]
        return self._write_record(BagRecordType.CHUNK, header_fields, compressed_data)

    def write_index_data(self, index: IndexDataRecord) -> int:
        """Write an index data record.

        Args:
            index: The index data record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('ver', struct.pack('<i', index.ver)),
            ('conn', struct.pack('<i', index.conn)),
            ('count', struct.pack('<i', index.count)),
        ]
        return self._write_record(BagRecordType.INDEX_DATA, header_fields, index.data)

    def write_chunk_info(self, chunk_info: ChunkInfoRecord) -> int:
        """Write a chunk info record.

        Args:
            chunk_info: The chunk info record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('ver', struct.pack('<i', chunk_info.ver)),
            ('chunk_pos', struct.pack('<q', chunk_info.chunk_pos)),
            ('start_time', self._encode_ros_time(chunk_info.start_time)),
            ('end_time', self._encode_ros_time(chunk_info.end_time)),
            ('count', struct.pack('<i', chunk_info.count)),
        ]

        return self._write_record(BagRecordType.CHUNK_INFO, header_fields, chunk_info.data)

    def tell(self) -> int:
        """Get the current position in the output stream.

        Returns:
            Current byte offset.
        """
        return self._writer.tell()

    def close(self) -> None:
        """Close the underlying writer."""
        self._writer.close()
