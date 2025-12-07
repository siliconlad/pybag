"""Parser for ROS 1 bag file format 2.0 records."""

import bz2
import logging
import struct
from typing import Any

from pybag.io.raw_reader import BaseReader, BytesReader
from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ChunkRecord,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord,
)

logger = logging.getLogger(__name__)

# ROS 1 bag format version string
BAG_VERSION = b'#ROSBAG V2.0\n'

# Try to import lz4 for LZ4 compression support
try:
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False


class MalformedBag(Exception):
    """The ROS 1 bag file does not conform to the specification."""
    def __init__(self, error_message: str):
        super().__init__(error_message)


class BagRecordParser:
    """Parser for ROS 1 bag file records."""

    @classmethod
    def parse_version(cls, file: BaseReader) -> str:
        """Parse and validate the bag file version string.

        Returns:
            The version string (e.g., "2.0").

        Raises:
            MalformedBag: If the version string is invalid.
        """
        version_line = file.read(len(BAG_VERSION))
        if not version_line.startswith(b'#ROSBAG V'):
            raise MalformedBag(f'Invalid bag file version: {version_line!r}')
        # Extract version number (e.g., "2.0")
        version = version_line[9:-1].decode('ascii')  # Skip "#ROSBAG V" and newline
        return version

    @classmethod
    def _parse_header_field(cls, data: bytes, offset: int) -> tuple[int, str, bytes]:
        """Parse a single header field.

        Format: field_len (4 bytes) | name=value

        Args:
            data: The header bytes.
            offset: Current offset in the data.

        Returns:
            Tuple of (new_offset, field_name, field_value).
        """
        field_len = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        field_data = data[offset:offset + field_len]
        offset += field_len

        # Split on first '=' character
        eq_pos = field_data.find(b'=')
        if eq_pos == -1:
            raise MalformedBag(f'Invalid header field: no = found in {field_data!r}')

        name = field_data[:eq_pos].decode('ascii')
        value = field_data[eq_pos + 1:]
        return offset, name, value

    @classmethod
    def _parse_header(cls, file: BaseReader) -> dict[str, bytes]:
        """Parse a record header.

        Format: header_len (4 bytes) | header_data

        Returns:
            Dictionary mapping field names to raw byte values.
        """
        header_len = struct.unpack('<I', file.read(4))[0]
        header_data = file.read(header_len)

        fields: dict[str, bytes] = {}
        offset = 0
        while offset < len(header_data):
            offset, name, value = cls._parse_header_field(header_data, offset)
            fields[name] = value

        return fields

    @classmethod
    def _parse_data(cls, file: BaseReader) -> bytes:
        """Parse record data.

        Format: data_len (4 bytes) | data

        Returns:
            The raw data bytes.
        """
        data_len = struct.unpack('<I', file.read(4))[0]
        return file.read(data_len)

    @classmethod
    def parse_record(cls, file: BaseReader) -> tuple[int, Any] | None:
        """Parse the next record from the file.

        Returns:
            Tuple of (record_type, record_data) or None if EOF.
        """
        # Try to read header length
        header_len_bytes = file.read(4)
        if len(header_len_bytes) < 4:
            return None  # EOF

        # Seek back and parse full record
        file.seek_from_current(-4)

        header = cls._parse_header(file)
        data = cls._parse_data(file)

        # Get the operation type
        if 'op' not in header:
            raise MalformedBag('Record header missing op field')

        op = struct.unpack('<B', header['op'])[0]
        record = cls._parse_record_by_type(op, header, data)
        return op, record

    @classmethod
    def _parse_record_by_type(
        cls,
        op: int,
        header: dict[str, bytes],
        data: bytes
    ) -> Any:
        """Parse a record based on its operation type."""
        if op == BagRecordType.BAG_HEADER:
            return cls._parse_bag_header(header, data)
        elif op == BagRecordType.CHUNK:
            return cls._parse_chunk(header, data)
        elif op == BagRecordType.CONNECTION:
            return cls._parse_connection(header, data)
        elif op == BagRecordType.MSG_DATA:
            return cls._parse_message_data(header, data)
        elif op == BagRecordType.INDEX_DATA:
            return cls._parse_index_data(header, data)
        elif op == BagRecordType.CHUNK_INFO:
            return cls._parse_chunk_info(header, data)
        else:
            logger.warning(f'Unknown record type: {op}')
            return None

    @classmethod
    def _parse_bag_header(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> BagHeaderRecord:
        """Parse a bag header record."""
        index_pos = struct.unpack('<Q', header['index_pos'])[0]
        conn_count = struct.unpack('<I', header['conn_count'])[0]
        chunk_count = struct.unpack('<I', header['chunk_count'])[0]
        return BagHeaderRecord(index_pos, conn_count, chunk_count)

    @classmethod
    def _parse_chunk(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> ChunkRecord:
        """Parse a chunk record."""
        compression = header['compression'].decode('ascii')
        size = struct.unpack('<I', header['size'])[0]
        return ChunkRecord(compression, size, data)

    @classmethod
    def _parse_connection(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> ConnectionRecord:
        """Parse a connection record.

        The connection header contains basic info, and the data section
        contains the full connection header from the original publisher.
        """
        conn = struct.unpack('<I', header['conn'])[0]
        topic = header['topic'].decode('utf-8')

        # Parse the data section which contains the message type info
        data_fields: dict[str, bytes] = {}
        offset = 0
        while offset < len(data):
            offset, name, value = cls._parse_header_field(data, offset)
            data_fields[name] = value

        msg_type = data_fields['type'].decode('utf-8')
        md5sum = data_fields['md5sum'].decode('ascii')
        message_definition = data_fields.get('message_definition', b'').decode('utf-8')

        callerid = None
        if 'callerid' in data_fields:
            callerid = data_fields['callerid'].decode('utf-8')

        latching = None
        if 'latching' in data_fields:
            latching = data_fields['latching'].decode('ascii')

        return ConnectionRecord(
            conn=conn,
            topic=topic,
            msg_type=msg_type,
            md5sum=md5sum,
            message_definition=message_definition,
            callerid=callerid,
            latching=latching,
        )

    @classmethod
    def _parse_message_data(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> MessageDataRecord:
        """Parse a message data record."""
        conn = struct.unpack('<I', header['conn'])[0]
        time_sec, time_nsec = struct.unpack('<II', header['time'])
        return MessageDataRecord(conn, time_sec, time_nsec, data)

    @classmethod
    def _parse_index_data(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> IndexDataRecord:
        """Parse an index data record."""
        ver = struct.unpack('<I', header['ver'])[0]
        conn = struct.unpack('<I', header['conn'])[0]
        count = struct.unpack('<I', header['count'])[0]

        # Parse index entries: (time_sec, time_nsec, offset)
        # Each entry is 12 bytes: 4 + 4 + 4
        entries: list[tuple[int, int, int]] = []
        for i in range(count):
            offset = i * 12
            time_sec, time_nsec, chunk_offset = struct.unpack_from('<III', data, offset)
            entries.append((time_sec, time_nsec, chunk_offset))

        return IndexDataRecord(ver, conn, count, entries)

    @classmethod
    def _parse_chunk_info(
        cls,
        header: dict[str, bytes],
        data: bytes
    ) -> ChunkInfoRecord:
        """Parse a chunk info record."""
        ver = struct.unpack('<I', header['ver'])[0]
        chunk_pos = struct.unpack('<Q', header['chunk_pos'])[0]
        start_time_sec, start_time_nsec = struct.unpack('<II', header['start_time'])
        end_time_sec, end_time_nsec = struct.unpack('<II', header['end_time'])
        count = struct.unpack('<I', header['count'])[0]

        # Parse connection counts from data section
        # Format: conn_id (4 bytes) + count (4 bytes) for each connection
        connection_counts: dict[int, int] = {}
        num_entries = len(data) // 8
        for i in range(num_entries):
            offset = i * 8
            conn_id, msg_count = struct.unpack_from('<II', data, offset)
            connection_counts[conn_id] = msg_count

        return ChunkInfoRecord(
            ver=ver,
            chunk_pos=chunk_pos,
            start_time_sec=start_time_sec,
            start_time_nsec=start_time_nsec,
            end_time_sec=end_time_sec,
            end_time_nsec=end_time_nsec,
            count=count,
            connection_counts=connection_counts,
        )

    @classmethod
    def decompress_chunk(cls, chunk: ChunkRecord) -> bytes:
        """Decompress chunk data based on compression type.

        Args:
            chunk: The chunk record to decompress.

        Returns:
            The decompressed data.

        Raises:
            MalformedBag: If compression type is unsupported.
        """
        if chunk.compression == 'none':
            return chunk.data
        elif chunk.compression == 'bz2':
            return bz2.decompress(chunk.data)
        elif chunk.compression == 'lz4':
            if not HAS_LZ4:
                raise MalformedBag('lz4 compression not available. Install with: pip install lz4')
            return lz4.frame.decompress(chunk.data)
        else:
            raise MalformedBag(f'Unknown compression type: {chunk.compression}')

    @classmethod
    def parse_chunk_records(
        cls,
        chunk_data: bytes
    ) -> list[tuple[int, Any]]:
        """Parse all records from decompressed chunk data.

        Args:
            chunk_data: Decompressed chunk data.

        Returns:
            List of (record_type, record) tuples.
        """
        reader = BytesReader(chunk_data)
        records: list[tuple[int, Any]] = []

        while reader.tell() < reader.size():
            result = cls.parse_record(reader)
            if result is None:
                break
            records.append(result)

        return records
