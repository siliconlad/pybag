"""High-level writer for ROS 1 bag files."""

import logging
import struct
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Literal

# Number of nanoseconds in one second
NSEC_PER_SEC = 1_000_000_000

from pybag.bag.record_parser import BagRecordParser
from pybag.bag.record_writer import BagRecordWriter
from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord
)
from pybag.encoding.rosmsg import RosMsgEncoder
from pybag.io.raw_reader import FileReader
from pybag.io.raw_writer import BaseWriter, BytesWriter, FileWriter
from pybag.schema.ros1_compiler import compile_ros1_serializer
from pybag.schema.ros1msg import (
    Ros1MsgSchemaDecoder,
    Ros1MsgSchemaEncoder,
    compute_md5sum
)
from pybag.types import Message, SchemaText

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
        mode: Literal['w', 'a'] = 'w',
        compression: Literal['none', 'bz2'] = 'none',
        chunk_size: int | None = None,
    ):
        """Initialize the bag writer.

        Args:
            writer: The underlying binary writer.
            mode: The mode to open the file in: 'w' for write (creates new file),
                  'a' for append (adds to existing file). In append mode, the file
                  must already exist and be a valid ROS 1 bag file. Existing
                  messages, connections, and chunk info are preserved; new chunks
                  are appended and the index section is rebuilt on close.
            compression: Compression algorithm for chunks.
            chunk_size: Target size for chunks in bytes.
        """
        self._writer = writer
        self._mode = mode
        self._compression = compression
        self._chunk_size = chunk_size or (1024 * 1024)  # 1MB
        self._record_writer = BagRecordWriter(writer)

        # Schema encoder and decoder
        self._schema_encoder = Ros1MsgSchemaEncoder()
        self._schema_decoder = Ros1MsgSchemaDecoder()

        # Tracking state
        self._next_conn_id = 0
        self._topics: dict[str, int] = {}  # topic -> conn_id
        self._connections: dict[int, ConnectionRecord] = {}
        self._message_types: dict[type[Message], tuple[str, str]] = {}  # type -> (msg_def, md5sum)

        # Pre-compiled serializers for topics with explicit schemas
        # Maps topic -> compiled serializer function
        self._topic_serializers: dict[str, Callable[[Any, Any], None]] = {}

        # Current chunk state
        self._chunk_buffer = BytesWriter()
        self._chunk_record_writer = BagRecordWriter(self._chunk_buffer)
        self._chunk_start_time: int | None = None
        self._chunk_end_time: int | None = None
        self._chunk_message_counts: dict[int, int] = {}
        # Index entries for current chunk: conn_id -> [(time, offset)]
        self._chunk_index_entries: dict[int, list[tuple[int, int]]] = {}

        # Chunk info records (for summary)
        self._chunk_infos: list[ChunkInfoRecord] = []

        # Initialize based on mode
        if mode == 'w':
            # Write initial file structure for new files
            self._write_header()
        elif mode == 'a':
            # Load existing state and prepare for appending
            self._load_existing_state()
        else:
            raise ValueError(f"Invalid mode: {mode!r}. Must be 'w' or 'a'.")

    @classmethod
    def open(
        cls,
        file_path: str | Path,
        *,
        mode: Literal['w', 'a'] = 'w',
        compression: Literal['none', 'bz2'] = 'none',
        chunk_size: int | None = None,
    ) -> "BagFileWriter":
        """Create a writer for a file.

        Args:
            file_path: Path to the output bag file.
            mode: The mode to open the file in: 'w' for write (creates new file),
                  'a' for append (adds to existing file). In append mode, the file
                  must already exist and be a valid ROS 1 bag file. Existing
                  messages, connections, and chunk info are preserved; new chunks
                  are appended and the index section is rebuilt on close.
            compression: Compression algorithm for chunks.
            chunk_size: Target size for chunks in bytes.

        Returns:
            A new BagFileWriter instance.
        """
        # Use 'wb' for write mode (truncates), 'r+b' for append mode (read/write)
        file_mode = 'wb' if mode == 'w' else 'r+b'
        return cls(
            FileWriter(file_path, mode=file_mode),
            mode=mode,
            compression=compression,
            chunk_size=chunk_size,
        )

    @staticmethod
    def _encode_header_field(name: str, value: bytes) -> bytes:
        """Encode a single header field.

        Format: field_len (4 bytes) | name=value
        """
        field_data = name.encode('ascii') + b'=' + value
        return struct.pack('<I', len(field_data)) + field_data

    def _write_header(self) -> None:
        """Write the initial file header.

        We write a placeholder header that will be updated when we close.
        """
        self._record_writer.write_version()
        self._header_pos = self._record_writer.tell()
        # Write placeholder header with zeros (will be updated on close)
        self._record_writer.write_bag_header(
            BagHeaderRecord(index_pos=0, conn_count=0, chunk_count=0),
        )

    def _load_existing_state(self) -> None:
        """Load state from an existing bag file for append mode.

        This method parses the existing bag file to extract:
        - The bag header (to find index_pos)
        - All connection records
        - All chunk info records

        After loading, it truncates the file at index_pos so new chunks
        can be appended before the index section is rewritten on close.

        Raises:
            TypeError: If the writer is not a FileWriter.
            ValueError: If the bag version is not 2.0, if index_pos is invalid,
                or if no connection records are found at index_pos (which may
                indicate a corrupted or incomplete bag file).
        """
        # Get the file path from the writer (must be a FileWriter for append mode)
        if not isinstance(self._writer, FileWriter):
            raise TypeError("Append mode requires a FileWriter")

        file_path = self._writer.file_path

        # Create a separate reader to parse the existing file
        # We use a reader because BagRecordParser expects BaseReader
        reader = FileReader(file_path)
        try:
            # Parse version
            version = BagRecordParser.parse_version(reader)
            if version != "2.0":
                raise ValueError(f"Unsupported bag version: {version} (must be 2.0)")

            # Record the header position (after version line)
            self._header_pos = reader.tell()

            # Parse bag header
            result = BagRecordParser.parse_record(reader)
            if result is None or result[0] != BagRecordType.BAG_HEADER:
                raise ValueError("Expected bag header record")
            bag_header: BagHeaderRecord = result[1]

            # Seek to the index section to read connections and chunk infos
            reader.seek_from_start(bag_header.index_pos)

            # Parse the index section (connections and chunk infos)
            while True:
                result = BagRecordParser.parse_record(reader)
                if result is None:
                    break

                op, record = result
                if op == BagRecordType.CONNECTION:
                    conn: ConnectionRecord = record
                    self._connections[conn.conn] = conn
                    self._topics[conn.topic] = conn.conn

                    # Compile a serializer for this existing connection
                    parsed_schema, sub_schemas = self._schema_decoder.parse_schema(conn)
                    serializer = compile_ros1_serializer(parsed_schema, sub_schemas)
                    self._topic_serializers[conn.topic] = serializer

                    # Update next_conn_id to avoid collisions
                    if conn.conn >= self._next_conn_id:
                        self._next_conn_id = conn.conn + 1

                elif op == BagRecordType.CHUNK_INFO:
                    self._chunk_infos.append(record)

            # Verify we found connections at index_pos
            if not self._connections and bag_header.conn_count != 0:
                # TODO: Reconstruct from the rest of the file
                raise ValueError(
                    "No connection records found at index_pos. "
                    "The bag file may be corrupted or missing the index section. "
                    "Reconstruction from chunks is not yet implemented."
                )
        finally:
            # Close the reader after parsing
            reader.close()

        # Truncate the file at index_pos to remove the old index section
        # New chunks will be written starting at this position
        self._writer.seek_from_start(bag_header.index_pos)
        self._writer.truncate()

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

    def add_connection(
        self,
        topic: str,
        *,
        schema: SchemaText | type[Message] | Message,
    ) -> int:
        """Add a connection (topic) to the bag file.

        Args:
            topic: The topic name.
            schema: A SchemaText object containing the message type name and
                   schema definition text, or a message class/instance to
                   generate the schema from.

        Returns:
            The connection ID.
        """
        # TODO: Also check the message type of topic (using hash)?
        if topic in self._topics:
            return self._topics[topic]

        # Convert message class or instance to SchemaText
        if isinstance(schema, type) and hasattr(schema, '__msg_name__'):
            schema = SchemaText(
               name=schema.__msg_name__,
               text=self._schema_encoder.encode(schema).decode('utf-8'),
            )
        elif isinstance(schema, Message):
            schema_type = type(schema)
            schema = SchemaText(
               name=schema_type.__msg_name__,
               text=self._schema_encoder.encode(schema_type).decode('utf-8'),
            )

        conn_id = self._next_conn_id
        self._next_conn_id += 1

        # Use provided schema text directly
        msg_def = schema.text
        msg_type_name = schema.name
        md5sum = compute_md5sum(msg_def, msg_type_name)

        # Build the connection data (connection header fields)
        data_buffer = BytesWriter()
        # Two topic fields exist (in the record and connection headers).
        # This is because messages can be written to the bag file on a topic different
        # from where they were originally published
        data_buffer.write(self._encode_header_field('topic', topic.encode('utf-8')))
        data_buffer.write(self._encode_header_field('type', msg_type_name.encode('utf-8')))
        data_buffer.write(self._encode_header_field('md5sum', md5sum.encode('ascii')))
        data_buffer.write(self._encode_header_field('message_definition', msg_def.encode('utf-8')))

        # TODO: Add checks to see if previous topic exists
        connection = ConnectionRecord(
            conn=conn_id,
            topic=topic,
            data=data_buffer.as_bytes(),
        )
        self._connections[conn_id] = connection
        self._topics[topic] = conn_id

        # If explicit schema was provided, compile and store a serializer for this topic
        # This allows us to serialize messages without relying on type annotations
        parsed_schema, sub_schemas = self._schema_decoder.parse_schema(connection)
        serializer = compile_ros1_serializer(parsed_schema, sub_schemas)
        self._topic_serializers[topic] = serializer

        # Write connection record inside the current chunk
        # (connections are also written at index_pos during close for fast lookup)
        self._chunk_record_writer.write_connection(connection)

        return conn_id

    def add_connection_record(self, record: ConnectionRecord) -> int:
        """Add a connection record directly to the bag file.

        This method is useful for recovery operations where you have
        an existing ConnectionRecord from another bag file.

        Args:
            record: A ConnectionRecord to add.

        Returns:
            The connection ID.

        Raises:
            ValueError: If a connection with this ID already exists.
        """
        conn_id = record.conn

        if conn_id in self._connections:
            raise ValueError(f"Connection ID {conn_id} already exists")

        # Track the connection
        self._connections[conn_id] = record
        self._topics[record.topic] = conn_id

        # Update next_conn_id if necessary to avoid collisions
        if conn_id >= self._next_conn_id:
            self._next_conn_id = conn_id + 1

        # Compile and store a serializer for this topic
        parsed_schema, sub_schemas = self._schema_decoder.parse_schema(record)
        serializer = compile_ros1_serializer(parsed_schema, sub_schemas)
        self._topic_serializers[record.topic] = serializer

        # Write connection record inside the current chunk
        # (connections are also written at index_pos during close for fast lookup)
        self._chunk_record_writer.write_connection(record)

        return conn_id

    def write_message(
        self,
        topic: str,
        timestamp: int,
        message: Message,
    ) -> None:
        """Write a message to the bag file.

        Automatically creates the connection (and schema) if it doesn't exist.
        If the connection was pre-registered with add_connection(), uses that schema.

        Args:
            topic: The topic name.
            timestamp: The timestamp in nanoseconds since epoch.
            message: The message to write.
        """
        message_type = type(message)

        # Check if connection already exists (may have been pre-registered)
        if topic in self._topics:
            conn_id = self._topics[topic]
        else:
            # Auto-create connection from message type
            conn_id = self.add_connection(topic, schema=SchemaText(
               name=message_type.__msg_name__,
               text=self._schema_encoder.encode(message_type).decode('utf-8'),
            ))

        # Update chunk time bounds
        if self._chunk_start_time is None:
            self._chunk_start_time = timestamp
        self._chunk_end_time = timestamp if self._chunk_end_time is None else max(self._chunk_end_time, timestamp)

        # Track message count per connection
        self._chunk_message_counts[conn_id] = self._chunk_message_counts.get(conn_id, 0) + 1

        # Record the offset within the chunk buffer before writing
        msg_offset = self._chunk_buffer.size()

        # Serialize the message
        serializer = self._topic_serializers[topic]
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

    def write_message_record(self, record: MessageDataRecord) -> None:
        """Write a pre-serialized message record to the bag file.

        This method is useful for recovery operations or copying messages
        between bag files without re-serializing the message data.

        Args:
            record: A MessageDataRecord containing the connection ID,
                   timestamp, and pre-serialized message data.

        Raises:
            KeyError: If the connection ID has not been registered.
        """
        conn_id = record.conn
        timestamp = record.time

        # Verify connection exists
        if conn_id not in self._connections:
            raise KeyError(f"Connection ID {conn_id} has not been registered")

        # Update chunk time bounds
        if self._chunk_start_time is None:
            self._chunk_start_time = timestamp
        self._chunk_end_time = timestamp if self._chunk_end_time is None else max(self._chunk_end_time, timestamp)

        # Track message count per connection
        self._chunk_message_counts[conn_id] = self._chunk_message_counts.get(conn_id, 0) + 1

        # Record the offset within the chunk buffer before writing
        msg_offset = self._chunk_buffer.size()

        # Write message to chunk buffer (data is already serialized)
        self._chunk_record_writer.write_message_data(record)

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
        self._record_writer.write_chunk(chunk_data, self._compression)

        # Create chunk info
        # Build connection counts data: pairs of (conn_id, msg_count)
        conn_counts_buffer = BytesWriter()
        for conn_id, msg_count in self._chunk_message_counts.items():
            conn_counts_buffer.write(struct.pack('<II', conn_id, msg_count))

        # count field is the number of connections in this chunk, not total messages
        chunk_info = ChunkInfoRecord(
            ver=1,
            chunk_pos=chunk_pos,
            start_time=self._chunk_start_time if self._chunk_start_time is not None else 0,
            end_time=self._chunk_end_time if self._chunk_end_time is not None else 0,
            count=len(self._chunk_message_counts),
            data=conn_counts_buffer.as_bytes(),
        )
        self._chunk_infos.append(chunk_info)

        for conn_id, entries in self._chunk_index_entries.items():
            # Build index data: (time_sec, time_nsec, offset) for each entry
            # ROS time format uses two uint32 values (secs, nsecs)
            index_data_buffer = BytesWriter()
            for time_ns, offset in entries:
                secs = time_ns // NSEC_PER_SEC
                nsecs = time_ns % NSEC_PER_SEC
                index_data_buffer.write(struct.pack('<III', secs, nsecs, offset))
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
            BagHeaderRecord(
                index_pos=index_pos,
                conn_count=len(self._connections),
                chunk_count=len(self._chunk_infos),
            ),
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
