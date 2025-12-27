"""High-level reader for ROS 1 bag files."""

import fnmatch
import logging
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable

from pybag.bag.record_parser import BagRecordParser, MalformedBag
from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ChunkRecord,
    ConnectionRecord,
    MessageDataRecord
)
from pybag.encoding.rosmsg import RosMsgDecoder
from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.schema.ros1_compiler import compile_ros1_schema
from pybag.schema.ros1msg import Ros1MsgSchemaDecoder

logger = logging.getLogger(__name__)

# TODO: Do not load all messages at once

@dataclass(slots=True)
class DecodedMessage:
    """A decoded message from a ROS 1 bag file."""
    connection_id: int
    topic: str
    msg_type: str
    log_time: int  # nanoseconds since epoch
    data: Any  # Deserialized message object


class BagFileReader:
    """High-level reader for ROS 1 bag files.

    This class provides an API similar to McapFileReader for reading
    ROS 1 bag files.

    Example:
        with BagFileReader.from_file("recording.bag") as reader:
            for msg in reader.messages("/camera/image"):
                print(msg.log_time, msg.data)
    """

    def __init__(self, reader: BaseReader):
        """Initialize the bag reader.

        Args:
            reader: The underlying binary reader.
        """
        self._reader = reader
        self._version: str | None = None
        self._bag_header: BagHeaderRecord | None = None
        self._connections: dict[int, ConnectionRecord] = {}
        self._chunk_infos: list[ChunkInfoRecord] = []

        # Schema decoder for message definitions
        self._schema_decoder = Ros1MsgSchemaDecoder()
        self._compiled_schemas: dict[int, Callable] = {}

        # Parse the file structure
        self._parse_file()

    @staticmethod
    def from_file(file_path: Path | str) -> 'BagFileReader':
        """Create a reader from a file path.

        Args:
            file_path: Path to the bag file.

        Returns:
            A new BagFileReader instance.
        """
        reader = FileReader(file_path)
        return BagFileReader(reader)

    @staticmethod
    def from_bytes(data: bytes) -> 'BagFileReader':
        """Create a reader from bytes.

        Args:
            data: The bag file data.

        Returns:
            A new BagFileReader instance.
        """
        reader = BytesReader(data)
        return BagFileReader(reader)

    def _parse_file(self) -> None:
        """Parse the bag file structure."""
        # Parse version
        self._version = BagRecordParser.parse_version(self._reader)
        if self._version != '2.0':
            raise MalformedBag(f'Unsupported bag version: {self._version} (must be 2.0)')

        # Parse bag header
        result = BagRecordParser.parse_record(self._reader)
        if result is None or result[0] != BagRecordType.BAG_HEADER:
            raise MalformedBag(f'Expected bag header record, got {result}')
        self._bag_header = result[1]

        # Seek to index section
        if self._bag_header.index_pos > 0:
            self._reader.seek_from_start(self._bag_header.index_pos)

        # Parse index section (connections and chunk infos)
        while True:
            result = BagRecordParser.parse_record(self._reader)
            if result is None:
                break

            op, record = result
            if op == BagRecordType.CONNECTION:
                self._connections[record.conn] = record
            elif op == BagRecordType.CHUNK_INFO:
                self._chunk_infos.append(record)

    @property
    def version(self) -> str:
        """Get the bag file version."""
        return self._version or ''

    def get_topics(self) -> list[str]:
        """Get all topics in the bag file.

        Returns:
            List of topic names.
        """
        return list(set(c.topic for c in self._connections.values()))

    def get_message_count(self, topic: str) -> int:
        """Get the number of messages for a topic.

        Args:
            topic: The topic name.

        Returns:
            Number of messages for the topic.
        """
        # Find connection IDs for this topic
        conn_ids = [c.conn for c in self._connections.values() if c.topic == topic]
        if not conn_ids:
            raise ValueError(f'Topic {topic} not found in bag file')

        count = 0
        for chunk_info in self._chunk_infos:
            for conn_id in conn_ids:
                count += chunk_info.connection_counts.get(conn_id, 0)
        return count

    @property
    def start_time(self) -> int:
        """Get the start time of the bag file in nanoseconds since epoch."""
        return min([ci.start_time for ci in self._chunk_infos], default=0)

    @property
    def end_time(self) -> int:
        """Get the end time of the bag file in nanoseconds since epoch."""
        return max([ci.end_time for ci in self._chunk_infos], default=0)

    def _expand_topics(self, topic: str | list[str]) -> list[str]:
        """Expand topic patterns to list of concrete topic names.

        Args:
            topic: Topic pattern (string or list of strings).

        Returns:
            Deduplicated list of concrete topic names that exist in the file.
        """
        available_topics = self.get_topics()
        topic_patterns = [topic] if isinstance(topic, str) else topic

        matched_topics = set()
        for pattern in topic_patterns:
            matches = fnmatch.filter(available_topics, pattern)
            matched_topics.update(matches)
        return list(matched_topics)

    def _get_deserializer(self, conn_id: int) -> Callable:
        """Get or create a deserializer for a connection.

        Args:
            conn_id: The connection ID.

        Returns:
            A callable that deserializes message data.
        """
        if conn_id not in self._compiled_schemas:
            conn = self._connections[conn_id]
            schema, sub_schemas = self._schema_decoder.parse_schema(conn)
            self._compiled_schemas[conn_id] = compile_ros1_schema(schema, sub_schemas)
        return self._compiled_schemas[conn_id]

    def _deserialize_message(
        self,
        msg: MessageDataRecord,
        conn: ConnectionRecord
    ) -> Any:
        """Deserialize a message.

        Args:
            msg: The message data record.
            conn: The connection record.

        Returns:
            The deserialized message object.
        """
        deserializer = self._get_deserializer(conn.conn)
        return deserializer(RosMsgDecoder(msg.data))

    def messages(
        self,
        topic: str | list[str],
        start_time: int | None = None,
        end_time: int | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
        *,
        in_log_time_order: bool = True
    ) -> Generator[DecodedMessage, None, None]:
        """Iterate over messages in the bag file.

        Args:
            topic: Topic(s) to filter by. Can be:
                - Single topic string (e.g., "/camera")
                - Glob pattern (e.g., "/sensor/*")
                - List of topics/patterns
            start_time: Start time to filter by (nanoseconds). If None, start from beginning.
            end_time: End time to filter by (nanoseconds). If None, read to end.
            filter: Callable to filter messages. If None, all messages are returned.
            in_log_time_order: Return messages in log time order if True.

        Yields:
            DecodedMessage objects from matching topics.
        """
        concrete_topics = self._expand_topics(topic)
        if not concrete_topics:
            return

        # Get connection IDs for requested topics
        conn_ids_to_topics: dict[int, str] = {}
        for conn in self._connections.values():
            if conn.topic in concrete_topics:
                conn_ids_to_topics[conn.conn] = conn.topic

        if not conn_ids_to_topics:
            logging.warning("No matching topics found")
            return

        # Sort chunk infos by start time if needed
        chunk_infos = self._chunk_infos
        if in_log_time_order:
            chunk_infos = sorted(chunk_infos, key=lambda ci: ci.start_time)

        # Collect messages (optionally sorted by time)
        all_messages: list[tuple[int, MessageDataRecord, ConnectionRecord]] = []

        for chunk_info in chunk_infos:
            # Skip chunks outside the time range
            if start_time is not None and chunk_info.end_time < start_time:
                continue
            if end_time is not None and chunk_info.start_time > end_time:
                continue

            # Check if this chunk has any relevant connections
            has_relevant_conn = any(
                conn_id in conn_ids_to_topics
                for conn_id in chunk_info.connection_counts.keys()
            )
            if not has_relevant_conn:
                continue

            # Read and decompress the chunk
            self._reader.seek_from_start(chunk_info.chunk_pos)
            result = BagRecordParser.parse_record(self._reader)
            if result is None or result[0] != BagRecordType.CHUNK:
                logger.warning(f'Expected chunk at position {chunk_info.chunk_pos}, got {result}')
                continue

            chunk: ChunkRecord = result[1]
            chunk_data = BagRecordParser.decompress_chunk(chunk)

            # Parse records from the chunk
            chunk_records = BagRecordParser.parse_chunk_records(chunk_data)

            for op, record in chunk_records:
                if op != BagRecordType.MSG_DATA:
                    continue

                msg: MessageDataRecord = record
                if msg.conn not in conn_ids_to_topics:
                    continue

                # Time filtering
                log_time = msg.time
                if start_time is not None and log_time < start_time:
                    continue
                if end_time is not None and log_time > end_time:
                    continue

                conn = self._connections[msg.conn]
                all_messages.append((log_time, msg, conn))

        # Sort by time if requested
        if in_log_time_order:
            all_messages.sort(key=lambda x: x[0])

        # Yield decoded messages
        for log_time, msg, conn in all_messages:
            decoded_data = self._deserialize_message(msg, conn)
            conn_header = conn.connection_header
            decoded = DecodedMessage(
                connection_id=msg.conn,
                topic=conn.topic,
                msg_type=conn_header.type,
                log_time=log_time,
                data=decoded_data,
            )
            if filter is None or filter(decoded):
                yield decoded

    def close(self) -> None:
        """Close the bag reader and release all resources."""
        self._reader.close()

    def __enter__(self) -> "BagFileReader":
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
