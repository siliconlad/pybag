"""High-level reader for ROS 1 bag files."""

import fnmatch
import heapq
import logging
import struct
from collections.abc import Generator
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Iterator

from pybag.bag.record_parser import BagRecordParser, MalformedBag
from pybag.bag.records import (
    BagHeaderRecord,
    BagRecordType,
    ChunkInfoRecord,
    ChunkRecord,
    ConnectionRecord,
    IndexDataRecord,
    MessageDataRecord
)
from pybag.encoding.rosmsg import RosMsgDecoder
from pybag.io.raw_reader import BaseReader, BytesReader, FileReader
from pybag.schema.ros1_compiler import compile_ros1_schema
from pybag.schema.ros1msg import Ros1MsgSchemaDecoder

logger = logging.getLogger(__name__)


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

    def __init__(self, reader: BaseReader, *, chunk_cache_size: int = 1):
        """Initialize the bag reader.

        Args:
            reader: The underlying binary reader.
            chunk_cache_size: Number of decompressed chunks to cache in memory.
        """
        self._reader = reader
        self._version: str | None = None
        self._bag_header: BagHeaderRecord | None = None
        self._connections: dict[int, ConnectionRecord] = {}
        self._chunk_infos: list[ChunkInfoRecord] = []

        # Schema decoder for message definitions
        self._schema_decoder = Ros1MsgSchemaDecoder()
        self._compiled_schemas: dict[int, Callable] = {}

        # Cache for index records: (chunk_pos, conn_id) -> IndexDataRecord
        self._index_cache: dict[tuple[int, int], IndexDataRecord] = {}

        # LRU cache for decompressed chunks
        self._decompress_chunk_cached = lru_cache(maxsize=chunk_cache_size)(
            self._decompress_chunk_impl
        )

        # Parse the file structure
        self._parse_file()

    @staticmethod
    def from_file(
        file_path: Path | str, *, chunk_cache_size: int = 1
    ) -> "BagFileReader":
        """Create a reader from a file path.

        Args:
            file_path: Path to the bag file.
            chunk_cache_size: Number of decompressed chunks to cache in memory.

        Returns:
            A new BagFileReader instance.
        """
        reader = FileReader(file_path)
        return BagFileReader(reader, chunk_cache_size=chunk_cache_size)

    @staticmethod
    def from_bytes(data: bytes, *, chunk_cache_size: int = 1) -> "BagFileReader":
        """Create a reader from bytes.

        Args:
            data: The bag file data.
            chunk_cache_size: Number of decompressed chunks to cache in memory.

        Returns:
            A new BagFileReader instance.
        """
        reader = BytesReader(data)
        return BagFileReader(reader, chunk_cache_size=chunk_cache_size)

    def _parse_file(self) -> None:
        """Parse the bag file structure."""
        # Parse version
        self._version = BagRecordParser.parse_version(self._reader)
        if self._version != "2.0":
            raise MalformedBag(f"Unsupported bag version: {self._version} (must be 2.0)")

        # Parse bag header
        result = BagRecordParser.parse_record(self._reader)
        if result is None or result[0] != BagRecordType.BAG_HEADER:
            raise MalformedBag(f"Expected bag header record, got {result}")
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
        return self._version or ""

    def get_topics(self) -> list[str]:
        """Get all topics in the bag file.

        Returns:
            List of topic names.
        """
        return list(set(c.topic for c in self._connections.values()))

    def get_connections(self) -> list[ConnectionRecord]:
        """Get all connection records in the bag file.

        Connection records contain metadata about each topic including
        the message type and message definition.

        Returns:
            List of ConnectionRecord objects.
        """
        return list(self._connections.values())

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
            raise ValueError(f"Topic {topic} not found in bag file")

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

    def _deserialize_message(self, msg: MessageDataRecord, conn: ConnectionRecord) -> Any:
        """Deserialize a message.

        Args:
            msg: The message data record.
            conn: The connection record.

        Returns:
            The deserialized message object.
        """
        deserializer = self._get_deserializer(conn.conn)
        return deserializer(RosMsgDecoder(msg.data))

    def _read_index_records_for_chunk(
        self, chunk_info: ChunkInfoRecord, conn_ids: set[int]
    ) -> dict[int, IndexDataRecord]:
        """Read index records for a chunk, using cache when available.

        This method skips the chunk data and reads the index records that follow it.
        Index records are cached for subsequent reads.

        Args:
            chunk_info: The chunk info record.
            conn_ids: Set of connection IDs to read indexes for.

        Returns:
            Dictionary mapping connection ID to IndexDataRecord.
        """
        result: dict[int, IndexDataRecord] = {}
        uncached_conn_ids: set[int] = set()

        # Check cache first
        for conn_id in conn_ids:
            cache_key = (chunk_info.chunk_pos, conn_id)
            if cache_key in self._index_cache:
                result[conn_id] = self._index_cache[cache_key]
            elif conn_id in chunk_info.connection_counts:
                uncached_conn_ids.add(conn_id)

        # If all needed indexes are cached, return early
        if not uncached_conn_ids:
            return result

        # Seek to chunk position and skip the chunk record to get to index records
        self._reader.seek_from_start(chunk_info.chunk_pos)

        # Read header length and skip the header
        header_len_bytes = self._reader.read(4)
        if len(header_len_bytes) < 4:
            logging.warning("Incomplete header length bytes")
            return result
        header_len = struct.unpack("<i", header_len_bytes)[0]
        self._reader.seek_from_current(header_len)

        # Read data length and skip the data
        data_len_bytes = self._reader.read(4)
        if len(data_len_bytes) < 4:
            return result
        data_len = struct.unpack("<i", data_len_bytes)[0]
        self._reader.seek_from_current(data_len)

        # Now read index records for connections in this chunk
        # Index records immediately follow the chunk
        connections_in_chunk = set(chunk_info.connection_counts.keys())
        indexes_to_read = len(connections_in_chunk)

        for _ in range(indexes_to_read):
            record_result = BagRecordParser.parse_record(self._reader)
            if record_result is None:
                break

            op, record = record_result
            if op != BagRecordType.INDEX_DATA:
                # We've moved past the index records
                break

            index_record: IndexDataRecord = record
            conn_id = index_record.conn

            # Cache this index record
            cache_key = (chunk_info.chunk_pos, conn_id)
            self._index_cache[cache_key] = index_record

            # Add to result if it's one we need
            if conn_id in conn_ids:
                result[conn_id] = index_record

        return result

    def _decompress_chunk_impl(self, chunk_pos: int) -> bytes:
        """Internal implementation for chunk decompression (cached).

        Args:
            chunk_pos: Chunk position in the file (used as cache key).

        Returns:
            Decompressed chunk data.
        """
        self._reader.seek_from_start(chunk_pos)
        record_result = BagRecordParser.parse_record(self._reader)
        if record_result is None or record_result[0] != BagRecordType.CHUNK:
            raise MalformedBag(f"Expected chunk at position {chunk_pos}")
        chunk: ChunkRecord = record_result[1]
        return BagRecordParser.decompress_chunk(chunk)

    def _get_relevant_chunks(
        self,
        conn_ids: set[int],
        start_time: int | None,
        end_time: int | None,
    ) -> list[ChunkInfoRecord]:
        """Get chunks that are relevant for the given query.

        Args:
            conn_ids: Set of connection IDs to filter by.
            start_time: Start time filter (nanoseconds).
            end_time: End time filter (nanoseconds).

        Returns:
            List of relevant ChunkInfoRecords sorted by start_time.
        """
        relevant_chunks: list[ChunkInfoRecord] = []

        for chunk_info in self._chunk_infos:
            # Skip chunks outside the time range
            if start_time is not None and chunk_info.end_time < start_time:
                continue
            if end_time is not None and chunk_info.start_time > end_time:
                continue

            # Check if this chunk has any relevant connections
            has_relevant_conn = any(
                conn_id in conn_ids for conn_id in chunk_info.connection_counts.keys()
            )
            if not has_relevant_conn:
                continue

            relevant_chunks.append(chunk_info)
        return relevant_chunks

    def _chunk_message_iterator(
        self,
        chunk_info: ChunkInfoRecord,
        conn_ids: set[int],
        start_time: int | None,
        end_time: int | None,
        chunk_idx: int = 0,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Iterator[tuple[int, int, int, int]]:
        """Create an iterator that yields message references for a chunk.

        Uses the index records to yield (timestamp, chunk_pos, offset, conn_id) tuples
        in sorted order without loading all messages into memory.

        Args:
            chunk_info: The chunk info record.
            conn_ids: Set of connection IDs to include.
            start_time: Start time filter (nanoseconds).
            end_time: End time filter (nanoseconds).
            chunk_idx: Priority of the chunk

        Yields:
            Tuples of (timestamp, chunk_idx, offset, conn_id).
        """
        # Get index records for this chunk
        index_records = self._read_index_records_for_chunk(chunk_info, conn_ids)
        if not index_records:
            return

        # Collect all message references from the index records
        message_refs: list[tuple[int, int, int]] = []  # (timestamp, offset)
        for conn_id, index_record in index_records.items():
            for timestamp, offset in index_record.entries:
                # Apply time filtering
                if start_time is not None and timestamp < start_time:
                    continue
                if end_time is not None and timestamp > end_time:
                    continue
                message_refs.append((timestamp, offset, conn_id))

        # Sort by timestamp, then offset for determinism
        if in_log_time_order:
            message_refs.sort(key=lambda x: (x[0], x[1]))
        else:
            message_refs.sort(key=lambda x: x[1])

        if in_reverse:
            message_refs.reverse()

        # Yield the references
        for timestamp, offset, conn_id in message_refs:
            yield timestamp, chunk_idx, offset, conn_id

    def messages(
        self,
        topic: str | list[str],
        start_time: int | None = None,
        end_time: int | None = None,
        filter: Callable[[DecodedMessage], bool] | None = None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[DecodedMessage, None, None]:
        """Iterate over messages in the bag file.

        This method uses a lazy generator approach that only loads one message
        at a time, utilizing index records for efficient random access.

        Args:
            topic: Topic(s) to filter by. Can be:
                - Single topic string (e.g., "/camera")
                - Glob pattern (e.g., "/sensor/*")
                - List of topics/patterns
            start_time: Start time to filter by (nanoseconds). If None, start from beginning.
            end_time: End time to filter by (nanoseconds). If None, read to end.
            filter: Callable to filter messages. If None, all messages are returned.
            in_log_time_order: Return messages in log time order if True.
            in_reverse: Return messages in reverse order if True. When combined with
                in_log_time_order=True, returns messages in reverse chronological order.
                When in_log_time_order=False, returns messages in reverse file order.

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
        conn_ids = set(conn_ids_to_topics.keys())

        # Get relevant chunks
        relevant_chunks = self._get_relevant_chunks(conn_ids, start_time, end_time)
        if not relevant_chunks:
            return

        has_overlapping_chunks = self._has_overlapping_chunks(relevant_chunks)
        if in_log_time_order and has_overlapping_chunks:
            yield from self._get_messages_with_overlaps(
                relevant_chunks, conn_ids, start_time, end_time, filter,
                in_reverse=in_reverse,
            )
        else:
            yield from self._get_messages_sequential(
                relevant_chunks,
                conn_ids,
                start_time,
                end_time,
                msg_filter=filter,
                in_log_time_order=in_log_time_order,
                in_reverse=in_reverse,
            )

    def _has_overlapping_chunks(self, chunks: list[ChunkInfoRecord]) -> bool:
        """Check if chunks have overlapping time ranges."""

        if len(chunks) <= 1:
            return False

        chunks = sorted(chunks, key=lambda ci: ci.start_time)
        for i in range(len(chunks) - 1):
            if chunks[i].end_time >= chunks[i + 1].start_time:
                return True
        return False

    def _get_messages_sequential(
        self,
        chunks: list[ChunkInfoRecord],
        conn_ids: set[int],
        start_time: int | None,
        end_time: int | None,
        msg_filter: Callable[[DecodedMessage], bool] | None,
        *,
        in_log_time_order: bool = True,
        in_reverse: bool = False,
    ) -> Generator[DecodedMessage, None, None]:
        """Get messages from non-overlapping chunks sequentially.

        Args:
            chunks: List of chunk info records in written order.
            conn_ids: Set of connection IDs to include.
            start_time: Start time filter.
            end_time: End time filter.
            msg_filter: Optional message filter.
            in_log_time_order: Whether to sort messages within each chunk by timestamp.
            in_reverse: Whether to yield messages in reverse chronological order.

        Yields:
            DecodedMessage objects in appropriate order.
        """
        chunks = sorted(chunks, key=lambda ci: ci.start_time if in_log_time_order else ci.chunk_pos)
        chunks = list(reversed(chunks)) if in_reverse else chunks

        for chunk_info in chunks:
            chunk_data = self._decompress_chunk_cached(chunk_info.chunk_pos)
            reader = BytesReader(chunk_data)

            for timestamp, _, offset, conn_id in self._chunk_message_iterator(
                chunk_info, conn_ids, start_time, end_time,
                in_log_time_order=in_log_time_order,
                in_reverse=in_reverse,
            ):
                reader.seek_from_start(offset)
                record_result = BagRecordParser.parse_record(reader)
                if record_result is None or record_result[0] != BagRecordType.MSG_DATA:
                    logging.warning(f"Invalid message data record at offset {offset}")
                    continue

                msg: MessageDataRecord = record_result[1]
                conn = self._connections[msg.conn]

                decoded_data = self._deserialize_message(msg, conn)
                conn_header = conn.connection_header
                decoded = DecodedMessage(
                    connection_id=msg.conn,
                    topic=conn.topic,
                    msg_type=conn_header.type,
                    log_time=timestamp,
                    data=decoded_data,
                )

                if msg_filter is None or msg_filter(decoded):
                    yield decoded

    def _get_messages_with_overlaps(
        self,
        chunks: list[ChunkInfoRecord],
        conn_ids: set[int],
        start_time: int | None,
        end_time: int | None,
        msg_filter: Callable[[DecodedMessage], bool] | None,
        *,
        in_reverse: bool = False,
    ) -> Generator[DecodedMessage, None, None]:
        """Get messages from overlapping chunks using heap-based merge.

        Args:
            chunks: List of chunk info records.
            conn_ids: Set of connection IDs to include.
            start_time: Start time filter.
            end_time: End time filter.
            msg_filter: Optional message filter.
            in_reverse: Whether to yield messages in reverse chronological order.

        Yields:
            DecodedMessage objects in appropriate order.
        """
        logger.warning("Detected time-overlapping chunks. Reading performance is affected!")

        chunks = sorted(chunks, key=lambda ci: ci.start_time)
        chunks = list(reversed(chunks)) if in_reverse else chunks

        # Create iterators for all chunks
        chunk_iterators = [
            self._chunk_message_iterator(
                chunk_info, conn_ids, start_time, end_time,
                chunk_idx=i,
                in_reverse=in_reverse,
            ) for i, chunk_info in enumerate(chunks)
        ]

        # Merge using heapq
        heapq_key = lambda x: (-x[0], x[1]) if in_reverse else (x[0], x[1])
        for timestamp, chunk_idx, offset, conn_id in heapq.merge(*chunk_iterators, key=heapq_key):
            chunk_info = chunks[chunk_idx]
            chunk_data = self._decompress_chunk_cached(chunk_info.chunk_pos)
            reader = BytesReader(chunk_data)
            reader.seek_from_start(offset)

            record_result = BagRecordParser.parse_record(reader)
            if record_result is None or record_result[0] != BagRecordType.MSG_DATA:
                continue

            msg: MessageDataRecord = record_result[1]
            conn = self._connections[msg.conn]

            decoded_data = self._deserialize_message(msg, conn)
            conn_header = conn.connection_header
            decoded = DecodedMessage(
                connection_id=msg.conn,
                topic=conn.topic,
                msg_type=conn_header.type,
                log_time=timestamp,
                data=decoded_data,
            )

            if msg_filter is None or msg_filter(decoded):
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
        tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        self.close()
