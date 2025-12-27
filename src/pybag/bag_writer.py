"""High-level writer for ROS 1 bag files."""

import logging
import struct
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Literal

from pybag.bag.record_writer import BagRecordWriter
from pybag.bag.records import (
    ChunkInfoRecord,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord
)
from pybag.encoding.rosmsg import RosMsgEncoder
from pybag.io.raw_writer import BaseWriter, BytesWriter, FileWriter
from pybag.schema.ros1_compiler import compile_ros1_serializer
from pybag.schema.ros1msg import Ros1MsgSchemaEncoder, compute_md5sum
from pybag.types import Message

logger = logging.getLogger(__name__)

# TODO: Chunk compression

class BagFileWriter:
    """High-level writer for ROS 1 bag files.

    This class provides an API similar to McapFileWriter for creating
    ROS 1 bag files.

    Example:
        with BagFileWriter.open("recording.bag") as writer:
            writer.write_message("/topic", 1000000000, msg)
    """

    def __init__(
        self,
        writer: BaseWriter,
        *,
        compression: Literal['none', 'bz2'] = 'none',
        chunk_size: int = 1024 * 1024,  # 1MB default chunk size
    ):
        """Initialize the bag writer.

        Args:
            writer: The underlying binary writer.
            compression: Compression algorithm for chunks.
            chunk_size: Target size for chunks in bytes.
        """
        self._writer = writer
        self._compression = compression
        self._chunk_size = chunk_size
        self._record_writer = BagRecordWriter(writer)

        # Schema encoder
        self._schema_encoder = Ros1MsgSchemaEncoder()

        # Tracking state
        self._next_conn_id = 0
        self._topics: dict[str, int] = {}  # topic -> conn_id
        self._connections: dict[int, ConnectionRecord] = {}
        self._message_types: dict[type[Message], tuple[str, str]] = {}  # type -> (msg_def, md5sum)
        self._serializers: dict[type[Message], Callable[[Any, Any], None]] = {}

        # Current chunk state
        self._chunk_buffer = BytesWriter()
        self._chunk_record_writer = BagRecordWriter(self._chunk_buffer)
        self._chunk_start_time: int | None = None
        self._chunk_end_time: int | None = None
        self._chunk_message_counts: dict[int, int] = {}
        # Index entries for current chunk: conn_id -> [(time_sec, time_nsec, offset)]
        self._chunk_index_entries: dict[int, list[tuple[int, int]]] = {}

        # Chunk info records (for summary)
        self._chunk_infos: list[ChunkInfoRecord] = []

        # Write initial file structure
        self._write_header()

    @classmethod
    def open(
        cls,
        file_path: str | Path,
        *,
        compression: Literal['none', 'bz2'] = 'none',
        chunk_size: int = 1024 * 1024,
    ) -> "BagFileWriter":
        """Create a writer for a file.

        Args:
            file_path: Path to the output bag file.
            compression: Compression algorithm for chunks.
            chunk_size: Target size for chunks in bytes.

        Returns:
            A new BagFileWriter instance.
        """
        return cls(
            FileWriter(file_path),
            compression=compression,
            chunk_size=chunk_size,
        )

    @staticmethod
    def _encode_header_field(name: str, value: bytes) -> bytes:
        """Encode a single header field.

        Format: field_len (4 bytes) | name=value
        """
        field_data = name.encode('ascii') + b'=' + value
        return struct.pack('<i', len(field_data)) + field_data

    def _write_header(self) -> None:
        """Write the initial file header.

        We write a placeholder header that will be updated when we close.
        """
        self._record_writer.write_version()
        self._header_pos = self._record_writer.tell()
        # Write placeholder header with zeros (will be updated on close)
        self._record_writer.write_bag_header(
            index_pos=0,
            conn_count=0,
            chunk_count=0,
        )

    def _get_message_info(self, message_type: type[Message]) -> tuple[str, str]:
        """Get or create message definition and MD5 sum.

        Args:
            message_type: The message class.

        Returns:
            Tuple of (message_definition, md5sum).
        """
        # TODO: Link with pre-encoded message types for common ones?
        if message_type not in self._message_types:
            msg_def = self._schema_encoder.encode(message_type).decode('utf-8')
            msg_name = message_type.__msg_name__
            md5sum = compute_md5sum(msg_def, msg_name)
            self._message_types[message_type] = (msg_def, md5sum)
        return self._message_types[message_type]

    def _get_serializer(self, message_type: type[Message]) -> Callable[[Any, Any], None]:
        """Get or create a serializer for a message type.

        Args:
            message_type: The message class.

        Returns:
            A callable that serializes messages.
        """
        if message_type not in self._serializers:
            schema, sub_schemas = self._schema_encoder.parse_schema(message_type)
            self._serializers[message_type] = compile_ros1_serializer(schema, sub_schemas)
        return self._serializers[message_type]

    def add_connection(self, topic: str, message_type: type[Message]) -> int:
        """Add a connection (topic) to the bag file.

        Args:
            topic: The topic name.
            message_type: The message type class.

        Returns:
            The connection ID.
        """
        # TODO: Also check the message type of topic?
        if topic in self._topics:
            return self._topics[topic]

        conn_id = self._next_conn_id
        self._next_conn_id += 1

        msg_def, md5sum = self._get_message_info(message_type)
        msg_type = message_type.__msg_name__

        # Build the connection data (connection header fields)
        data_buffer = BytesWriter()
        # Two topic fields exist (in the record and connection headers).
        # This is because messages can be written to the bag file on a topic different
        # from where they were originally published
        data_buffer.write(self._encode_header_field('topic', topic.encode('utf-8')))
        data_buffer.write(self._encode_header_field('type', msg_type.encode('utf-8')))
        data_buffer.write(self._encode_header_field('md5sum', md5sum.encode('ascii')))
        data_buffer.write(self._encode_header_field('message_definition', msg_def.encode('utf-8')))

        connection = ConnectionRecord(
            conn=conn_id,
            topic=topic,
            data=data_buffer.as_bytes(),
        )
        self._connections[conn_id] = connection
        self._topics[topic] = conn_id

        # Write connection record to current chunk
        self._chunk_record_writer.write_connection(connection)

        return conn_id

    def write_message(
        self,
        topic: str,
        timestamp: int,
        message: Message,
    ) -> None:
        """Write a message to the bag file.

        Args:
            topic: The topic name.
            timestamp: The timestamp in nanoseconds since epoch.
            message: The message to write.
        """
        message_type = type(message)

        # Ensure connection exists
        conn_id = self.add_connection(topic, message_type)

        # Update chunk time bounds
        if self._chunk_start_time is None:
            self._chunk_start_time = timestamp
        self._chunk_end_time = timestamp if self._chunk_end_time is None else max(self._chunk_end_time, timestamp)

        # Track message count per connection
        self._chunk_message_counts[conn_id] = self._chunk_message_counts.get(conn_id, 0) + 1

        # Record the offset within the chunk buffer before writing
        msg_offset = self._chunk_buffer.size()

        # Serialize the message
        serializer = self._get_serializer(message_type)
        encoder = RosMsgEncoder()
        serializer(encoder, message)
        data = encoder.save()

        # Write message to chunk buffer
        msg_record = MessageDataRecord(conn=conn_id, time=timestamp, data=data)
        self._chunk_record_writer.write_message_data(msg_record)

        # Track index entry for this message
        if conn_id not in self._chunk_index_entries:
            self._chunk_index_entries[conn_id] = []
        self._chunk_index_entries[conn_id].append((timestamp, msg_offset))

        # Check if we should flush the chunk
        if self._chunk_buffer.size() >= self._chunk_size:
            self._flush_chunk()

    def _flush_chunk(self) -> None:
        """Flush the current chunk to disk."""
        if self._chunk_buffer.size() == 0:
            return

        chunk_data = self._chunk_buffer.as_bytes()
        chunk_pos = self._record_writer.tell()

        # Write the chunk
        # TODO: Chunk compression
        self._record_writer.write_chunk(chunk_data, self._compression)

        # Create chunk info
        total_messages = sum(self._chunk_message_counts.values())

        # Build connection counts data
        conn_counts_buffer = BytesWriter()
        for conn_id, msg_count in self._chunk_message_counts.items():
            conn_counts_buffer.write(struct.pack('<II', conn_id, msg_count))

        chunk_info = ChunkInfoRecord(
            ver=1,
            chunk_pos=chunk_pos,
            start_time=self._chunk_start_time if self._chunk_start_time is not None else 0,
            end_time=self._chunk_end_time if self._chunk_end_time is not None else 0,
            count=total_messages,
            data=conn_counts_buffer.as_bytes(),
        )
        self._chunk_infos.append(chunk_info)

        for conn_id, entries in self._chunk_index_entries.items():
            # Build index data: (time, offset) for each entry
            index_data_buffer = BytesWriter()
            for time, offset in entries:
                index_data_buffer.write(struct.pack('<qi', time, offset))
            # Write the index records
            index_record = IndexDataRecord(
                ver=1,
                conn=conn_id,
                count=len(entries),
                data=index_data_buffer.as_bytes(),
            )
            self._record_writer.write_index_data(index_record)

        # Reset chunk state
        self._chunk_buffer.clear()
        self._chunk_start_time = None
        self._chunk_end_time = None
        self._chunk_message_counts.clear()
        self._chunk_index_entries.clear()

    def close(self) -> None:
        """Finalize and close the bag file."""
        # Flush any remaining chunk data
        self._flush_chunk()

        # Record the index position (where index data, connections and chunk infos start)
        index_pos = self._record_writer.tell()

        # Write all connection records
        for conn in self._connections.values():
            self._record_writer.write_connection(conn)

        # Write all chunk info records
        for chunk_info in self._chunk_infos:
            self._record_writer.write_chunk_info(chunk_info)

        # Seek back to the header position and rewrite with correct values
        self._writer.seek_from_start(self._header_pos)
        self._record_writer.write_bag_header(
            index_pos=index_pos,
            conn_count=len(self._connections),
            chunk_count=len(self._chunk_infos),
        )

        self._record_writer.close()

    def __enter__(self) -> "BagFileWriter":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None
    ) -> None:
        """Context manager exit."""
        self.close()
