"""Writer for ROS 1 bag file format 2.0 records."""

import bz2
import struct
from typing import Literal

from pybag.bag.records import (
    BagRecordType,
    ChunkInfoRecord,
    ConnectionRecord,
    MessageDataRecord
)
from pybag.io.raw_writer import BaseWriter, BytesWriter

# Try to import lz4 for LZ4 compression support
try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

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
        result += self._writer.write(struct.pack('<I', len(header)))
        result += self._writer.write(header)
        result += self._writer.write(struct.pack('<I', len(data)))
        result += self._writer.write(data)
        return result

    def write_bag_header(
        self,
        index_pos: int,
        conn_count: int,
        chunk_count: int
    ) -> int:
        """Write a bag header record.

        Args:
            index_pos: Offset to first index record.
            conn_count: Number of connections.
            chunk_count: Number of chunks.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('index_pos', struct.pack('<Q', index_pos)),
            ('conn_count', struct.pack('<I', conn_count)),
            ('chunk_count', struct.pack('<I', chunk_count)),
        ]
        # Bag header has no data section (empty)
        return self._write_record(BagRecordType.BAG_HEADER, header_fields, b'')

    def write_connection(self, conn: ConnectionRecord) -> int:
        """Write a connection record.

        Args:
            conn: The connection record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('conn', struct.pack('<I', conn.conn)),
            ('topic', conn.topic.encode('utf-8')),
        ]

        # Build the data section with connection details
        data_buffer = BytesWriter()
        data_buffer.write(self._encode_header_field('type', conn.msg_type.encode('utf-8')))
        data_buffer.write(self._encode_header_field('md5sum', conn.md5sum.encode('ascii')))
        data_buffer.write(self._encode_header_field(
            'message_definition',
            conn.message_definition.encode('utf-8')
        ))
        if conn.callerid is not None:
            data_buffer.write(self._encode_header_field('callerid', conn.callerid.encode('utf-8')))
        if conn.latching is not None:
            data_buffer.write(self._encode_header_field('latching', conn.latching.encode('ascii')))

        return self._write_record(BagRecordType.CONNECTION, header_fields, data_buffer.as_bytes())

    def write_message_data(self, msg: MessageDataRecord) -> int:
        """Write a message data record.

        Args:
            msg: The message data record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('conn', struct.pack('<I', msg.conn)),
            ('time', struct.pack('<II', msg.time_sec, msg.time_nsec)),
        ]
        return self._write_record(BagRecordType.MSG_DATA, header_fields, msg.data)

    def write_chunk(
        self,
        data: bytes,
        compression: Literal['none', 'bz2', 'lz4'] = 'none'
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
        elif compression == 'lz4':
            if not HAS_LZ4:
                raise ValueError('lz4 compression not available. Install with: pip install lz4')
            compressed_data = lz4.frame.compress(data)
        else:
            raise ValueError(f'Unknown compression type: {compression}')

        header_fields = [
            ('compression', compression.encode('ascii')),
            ('size', struct.pack('<I', uncompressed_size)),
        ]
        return self._write_record(BagRecordType.CHUNK, header_fields, compressed_data)

    def write_index_data(
        self,
        conn: int,
        entries: list[tuple[int, int, int]]
    ) -> int:
        """Write an index data record.

        Args:
            conn: Connection ID.
            entries: List of (time_sec, time_nsec, offset) tuples.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('ver', struct.pack('<I', 1)),  # Version 1
            ('conn', struct.pack('<I', conn)),
            ('count', struct.pack('<I', len(entries))),
        ]

        # Build index data: (time_sec, time_nsec, offset) for each entry
        data_buffer = BytesWriter()
        for time_sec, time_nsec, offset in entries:
            data_buffer.write(struct.pack('<III', time_sec, time_nsec, offset))

        return self._write_record(BagRecordType.INDEX_DATA, header_fields, data_buffer.as_bytes())

    def write_chunk_info(self, chunk_info: ChunkInfoRecord) -> int:
        """Write a chunk info record.

        Args:
            chunk_info: The chunk info record to write.

        Returns:
            Number of bytes written.
        """
        header_fields = [
            ('ver', struct.pack('<I', chunk_info.ver)),
            ('chunk_pos', struct.pack('<Q', chunk_info.chunk_pos)),
            ('start_time', struct.pack('<II', chunk_info.start_time_sec, chunk_info.start_time_nsec)),
            ('end_time', struct.pack('<II', chunk_info.end_time_sec, chunk_info.end_time_nsec)),
            ('count', struct.pack('<I', chunk_info.count)),
        ]

        # Build connection counts data
        data_buffer = BytesWriter()
        for conn_id, msg_count in chunk_info.connection_counts.items():
            data_buffer.write(struct.pack('<II', conn_id, msg_count))

        return self._write_record(BagRecordType.CHUNK_INFO, header_fields, data_buffer.as_bytes())

    def tell(self) -> int:
        """Get the current position in the output stream.

        Returns:
            Current byte offset.
        """
        return self._writer.tell()

    def close(self) -> None:
        """Close the underlying writer."""
        self._writer.close()
