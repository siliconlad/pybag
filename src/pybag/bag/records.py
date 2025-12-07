"""Data structures for ROS 1 bag file format 2.0."""

from dataclasses import dataclass
from enum import IntEnum


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
    """
    index_pos: int
    conn_count: int
    chunk_count: int


@dataclass(slots=True)
class ConnectionRecord:
    """Connection record containing topic and message type information.

    Attributes:
        conn: Unique connection identifier.
        topic: Topic name for this connection.
        msg_type: Message type (e.g., 'std_msgs/String').
        md5sum: MD5 hash of the message definition.
        message_definition: Full text of the message definition including
                           all dependent types.
        callerid: Optional caller ID of the publisher.
        latching: Whether this topic is latched ('1') or not ('0').
    """
    conn: int
    topic: str
    msg_type: str
    md5sum: str
    message_definition: str
    callerid: str | None = None
    latching: str | None = None


@dataclass(slots=True)
class MessageDataRecord:
    """Message data record containing a serialized ROS message.

    Attributes:
        conn: Connection ID this message belongs to.
        time_sec: Timestamp seconds component.
        time_nsec: Timestamp nanoseconds component.
        data: Serialized message data in rosmsg format.
    """
    conn: int
    time_sec: int
    time_nsec: int
    data: bytes

    @property
    def time_ns(self) -> int:
        """Get timestamp as nanoseconds since epoch."""
        return self.time_sec * 1_000_000_000 + self.time_nsec


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
        start_time_sec: Start time seconds of messages in chunk.
        start_time_nsec: Start time nanoseconds of messages in chunk.
        end_time_sec: End time seconds of messages in chunk.
        end_time_nsec: End time nanoseconds of messages in chunk.
        count: Number of messages in the chunk.
        connection_counts: Dictionary mapping connection ID to message count.
    """
    ver: int
    chunk_pos: int
    start_time_sec: int
    start_time_nsec: int
    end_time_sec: int
    end_time_nsec: int
    count: int
    connection_counts: dict[int, int]

    @property
    def start_time_ns(self) -> int:
        """Get start timestamp as nanoseconds since epoch."""
        return self.start_time_sec * 1_000_000_000 + self.start_time_nsec

    @property
    def end_time_ns(self) -> int:
        """Get end timestamp as nanoseconds since epoch."""
        return self.end_time_sec * 1_000_000_000 + self.end_time_nsec


@dataclass(slots=True)
class IndexDataRecord:
    """Index data record for random access to messages.

    Attributes:
        ver: Version of index format (typically 1).
        conn: Connection ID this index is for.
        count: Number of entries in the index.
        entries: List of (time_sec, time_nsec, offset) tuples.
                 Offset is relative to the start of the chunk data.
    """
    ver: int
    conn: int
    count: int
    entries: list[tuple[int, int, int]]  # (time_sec, time_nsec, offset)
