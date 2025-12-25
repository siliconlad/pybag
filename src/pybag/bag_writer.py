"""High-level writer for ROS 1 bag files."""

import logging
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Literal

from pybag.bag.record_writer import BagRecordWriter
from pybag.bag.records import (
    ChunkInfoRecord,
    ConnectionRecord,
    MessageDataRecord
)
from pybag.encoding.rosmsg import RosmsgEncoder
from pybag.io.raw_writer import BaseWriter, BytesWriter, FileWriter
from pybag.schema.ros1_compiler import compile_ros1_serializer
from pybag.schema.ros1msg import Ros1MsgSchemaEncoder, compute_md5sum
from pybag.types import Message

logger = logging.getLogger(__name__)


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
        compression: Literal['none', 'bz2', 'lz4'] = 'none',
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
        self._chunk_start_time_sec: int | None = None
        self._chunk_start_time_nsec: int | None = None
        self._chunk_end_time_sec: int | None = None
        self._chunk_end_time_nsec: int | None = None
        self._chunk_message_counts: dict[int, int] = {}
        # Index entries for current chunk: conn_id -> [(time_sec, time_nsec, offset)]
        self._chunk_index_entries: dict[int, list[tuple[int, int, int]]] = {}

        # Chunk info records (for summary)
        self._chunk_infos: list[ChunkInfoRecord] = []
        # Index data for all chunks: list of (conn_id, entries) per chunk
        self._all_index_data: list[list[tuple[int, list[tuple[int, int, int]]]]] = []

        # Write initial file structure
        self._write_header()

    @classmethod
    def open(
        cls,
        file_path: str | Path,
        *,
        compression: Literal['none', 'bz2', 'lz4'] = 'none',
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
        if topic in self._topics:
            return self._topics[topic]

        conn_id = self._next_conn_id
        self._next_conn_id += 1

        msg_def, md5sum = self._get_message_info(message_type)
        msg_type = message_type.__msg_name__

        connection = ConnectionRecord(
            conn=conn_id,
            topic=topic,
            msg_type=msg_type,
            md5sum=md5sum,
            message_definition=msg_def,
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

        # Serialize the message
        serializer = self._get_serializer(message_type)
        encoder = RosmsgEncoder()
        serializer(encoder, message)
        data = encoder.save()

        # Convert timestamp
        time_sec = timestamp // 1_000_000_000
        time_nsec = timestamp % 1_000_000_000

        # Update chunk time bounds
        if self._chunk_start_time_sec is None:
            self._chunk_start_time_sec = time_sec
            self._chunk_start_time_nsec = time_nsec
        self._chunk_end_time_sec = time_sec
        self._chunk_end_time_nsec = time_nsec

        # Track message count per connection
        self._chunk_message_counts[conn_id] = self._chunk_message_counts.get(conn_id, 0) + 1

        # Record the offset within the chunk buffer before writing
        msg_offset = self._chunk_buffer.size()

        # Write message to chunk buffer
        msg_record = MessageDataRecord(
            conn=conn_id,
            time_sec=time_sec,
            time_nsec=time_nsec,
            data=data,
        )
        self._chunk_record_writer.write_message_data(msg_record)

        # Track index entry for this message
        if conn_id not in self._chunk_index_entries:
            self._chunk_index_entries[conn_id] = []
        self._chunk_index_entries[conn_id].append((time_sec, time_nsec, msg_offset))

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
        self._record_writer.write_chunk(chunk_data, self._compression)

        # Create chunk info
        total_messages = sum(self._chunk_message_counts.values())
        chunk_info = ChunkInfoRecord(
            ver=1,
            chunk_pos=chunk_pos,
            start_time_sec=self._chunk_start_time_sec or 0,
            start_time_nsec=self._chunk_start_time_nsec or 0,
            end_time_sec=self._chunk_end_time_sec or 0,
            end_time_nsec=self._chunk_end_time_nsec or 0,
            count=total_messages,
            connection_counts=dict(self._chunk_message_counts),
        )
        self._chunk_infos.append(chunk_info)

        # Save index entries for this chunk
        chunk_index_data: list[tuple[int, list[tuple[int, int, int]]]] = []
        for conn_id, entries in self._chunk_index_entries.items():
            chunk_index_data.append((conn_id, list(entries)))
        self._all_index_data.append(chunk_index_data)

        # Reset chunk state
        self._chunk_buffer.clear()
        self._chunk_start_time_sec = None
        self._chunk_start_time_nsec = None
        self._chunk_end_time_sec = None
        self._chunk_end_time_nsec = None
        self._chunk_message_counts.clear()
        self._chunk_index_entries.clear()

    def close(self) -> None:
        """Finalize and close the bag file."""
        # Flush any remaining chunk data
        self._flush_chunk()

        # Record the index position (where index data, connections and chunk infos start)
        index_pos = self._record_writer.tell()

        # Write INDEX_DATA records for each chunk
        for chunk_index_data in self._all_index_data:
            for conn_id, entries in chunk_index_data:
                self._record_writer.write_index_data(conn_id, entries)

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
