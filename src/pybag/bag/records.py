"""Data structures for ROS 1 bag file format 2.0."""

import struct
from dataclasses import dataclass
from enum import IntEnum

# Number of nanoseconds in one second
NSEC_PER_SEC = 1_000_000_000


class BagRecordType(IntEnum):
    """Operation codes for ROS 1 bag record types."""
    MSG_DEF = 0x01        # Message definition (deprecated in 2.0, but still valid)
    MSG_DATA = 0x02       # Message data
    BAG_HEADER = 0x03     # Bag header (file header)
    INDEX_DATA = 0x04     # Index data
    CHUNK = 0x05          # Compressed chunk
    CHUNK_INFO = 0x06     # Chunk info (metadata about a chunk)
    CONNECTION = 0x07     # Connection info


@dataclass(slots=True)
class BagHeaderRecord:
    """Bag file header containing metadata about the bag file.

    Attributes:
        index_pos: Offset in bytes from the beginning of the file to the first
                   index record (or connection record in the summary).
        conn_count: Number of unique connections in the bag.
        chunk_count: Number of chunks in the bag.
        data: The raw data bytes of the record.
    """
    index_pos: int
    conn_count: int
    chunk_count: int
    # The bag header record is padded out by filling data with ASCII space characters (0x20)
    # so that additional information can be added after the bag file is recorded.
    # Currently, this padding is such that the header is 4096 bytes long.
    data: bytes = b' ' * 4096


@dataclass(slots=True)
class ConnectionHeader:
    """Parsed connection header information.

    Attributes:
        topic: Topic name for this connection.
        type: Message type (e.g., 'std_msgs/String').
        md5sum: MD5 hash of the message definition.
        message_definition: Full text of the message definition including
                           all dependent types.
        callerid: Optional caller ID of the publisher.
        latching: Whether this topic is latched ('1') or not ('0').
    """
    topic: str
    type: str
    md5sum: str
    message_definition: str
    callerid: str | None = None
    latching: str | None = None


@dataclass(slots=True)
class ConnectionRecord:
    """Connection record containing topic and message type information.

    Attributes:
        conn: Unique connection identifier.
        topic: Topic name for this connection (from header).
        data: Raw connection header data bytes.
    """
    conn: int
    topic: str
    data: bytes

    @property
    def connection_header(self) -> ConnectionHeader:
        """Parse and return the connection header from the data field.

        Returns:
            ConnectionHeader with the parsed fields.
        """
        fields: dict[str, bytes] = {}
        offset = 0
        while offset < len(self.data):
            field_len = struct.unpack_from('<i', self.data, offset)[0]
            offset += 4
            field_data = self.data[offset:offset + field_len]
            offset += field_len

            eq_pos = field_data.find(b'=')
            if eq_pos != -1:
                name = field_data[:eq_pos].decode('ascii')
                value = field_data[eq_pos + 1:]
                fields[name] = value

        return ConnectionHeader(
            topic=fields['topic'].decode('utf-8'),
            type=fields['type'].decode('utf-8'),
            md5sum=fields['md5sum'].decode('ascii'),
            message_definition=fields.get('message_definition', b'').decode('utf-8'),
            callerid=fields['callerid'].decode('utf-8') if 'callerid' in fields else None,
            latching=fields['latching'].decode('ascii') if 'latching' in fields else None,
        )


@dataclass(slots=True)
class MessageDataRecord:
    """Message data record containing a serialized ROS message.

    Attributes:
        conn: Connection ID this message belongs to.
        time: Timestamp as nanoseconds since epoch.
        data: Serialized message data in rosmsg format.
    """
    conn: int
    time: int
    data: bytes

    @property
    def time_sec(self) -> int:
        """Get timestamp seconds component."""
        return self.time // 1_000_000_000

    @property
    def time_nsec(self) -> int:
        """Get timestamp nanoseconds component."""
        return self.time % 1_000_000_000


@dataclass(slots=True)
class ChunkRecord:
    """Chunk record containing compressed messages and connections.

    Attributes:
        compression: Compression algorithm ('none', 'bz2', or 'lz4').
        size: Uncompressed size in bytes.
        data: Compressed (or uncompressed) record data.
    """
    compression: str
    size: int
    data: bytes


@dataclass(slots=True)
class ChunkInfoRecord:
    """Chunk info record containing metadata about a chunk.

    Attributes:
        ver: Version of chunk info format (typically 1).
        chunk_pos: Offset in bytes of the chunk record.
        start_time: Start time of messages in chunk as nanoseconds since epoch.
        end_time: End time of messages in chunk as nanoseconds since epoch.
        count: Number of connections in the chunk (length of connection_counts).
        data: Raw connection counts data (pairs of conn_id, msg_count).
    """
    ver: int
    chunk_pos: int
    start_time: int
    end_time: int
    count: int
    data: bytes

    @property
    def start_time_sec(self) -> int:
        """Get start timestamp seconds component."""
        return self.start_time // 1_000_000_000

    @property
    def start_time_nsec(self) -> int:
        """Get start timestamp nanoseconds component."""
        return self.start_time % 1_000_000_000

    @property
    def end_time_sec(self) -> int:
        """Get end timestamp seconds component."""
        return self.end_time // 1_000_000_000

    @property
    def end_time_nsec(self) -> int:
        """Get end timestamp nanoseconds component."""
        return self.end_time % 1_000_000_000

    @property
    def connection_counts(self) -> dict[int, int]:
        """Parse and return connection counts from the data field.

        Returns:
            Dictionary mapping connection ID to message count.
        """
        counts: dict[int, int] = {}
        num_entries = len(self.data) // 8
        for i in range(num_entries):
            offset = i * 8
            conn_id, msg_count = struct.unpack_from('<ii', self.data, offset)
            counts[conn_id] = msg_count
        return counts


@dataclass(slots=True)
class IndexDataRecord:
    """Index data record for random access to messages.

    Attributes:
        ver: Version of index format (typically 1).
        conn: Connection ID this index is for.
        count: Number of entries in the index.
        data: Raw index entries data.
    """
    ver: int
    conn: int
    count: int
    data: bytes

    @property
    def entries(self) -> list[tuple[int, int]]:
        """Parse and return index entries from the data field.

        Returns:
            List of (timestamp_ns, offset) tuples.
            Timestamp is in nanoseconds since epoch.
            Offset is relative to the start of the chunk data.
        """
        result: list[tuple[int, int]] = []
        for i in range(self.count):
            offset = i * 12
            # ROS time format: two uint32 (secs, nsecs) + int32 offset
            secs, nsecs, chunk_offset = struct.unpack_from('<IIi', self.data, offset)
            timestamp_ns = secs * NSEC_PER_SEC + nsecs
            result.append((timestamp_ns, chunk_offset))
        return result
