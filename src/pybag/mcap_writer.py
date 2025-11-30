"""Utilities for writing MCAP files."""

import logging
from pathlib import Path
from typing import Literal

from pybag.io.raw_reader import FileReader
from pybag.io.raw_writer import AppendFileWriter, BaseWriter, FileWriter
from pybag.mcap.crc import compute_crc
from pybag.mcap.record_parser import (
    DATA_END_SIZE,
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE
)
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import (
    AttachmentIndexRecord,
    AttachmentRecord,
    ChannelRecord,
    ChunkIndexRecord,
    MessageRecord,
    MetadataIndexRecord,
    MetadataRecord,
    SchemaRecord,
    StatisticsRecord
)
from pybag.mcap.summary import McapChunkedSummary, McapNonChunkedSummary
from pybag.serialize import MessageSerializer, MessageSerializerFactory
from pybag.types import Message

logger = logging.getLogger(__name__)


class McapFileWriter:
    """High level writer for producing MCAP files.

    This class provides a convenient API for writing messages to MCAP files.
    It automatically creates and manages schemas and channels based on the
    messages written. Internally, it delegates to low-level record writers.
    """

    def __init__(
        self,
        writer: BaseWriter,
        *,
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = None,
    ) -> None:
        """Initialize a high-level MCAP file writer.

        Args:
            writer: The underlying writer to write binary data to.
            profile: The MCAP profile to use (default: "ros2").
            chunk_size: If provided, creates chunks of approximately this size in bytes. If None, writes without chunking.
            chunk_compression: Compression algorithm for chunks ("lz4" or "zstd" or None for no compression).
        """
        # Create the low-level record writer via factory
        self._record_writer = McapRecordWriterFactory.create_writer(
            writer,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=profile,
        )

        # Get message serializer for this profile
        self._profile = profile
        message_serializer = MessageSerializerFactory.from_profile(self._profile)
        if message_serializer is None:
            raise ValueError(f"Unknown encoding type: {self._profile}")
        self._message_serializer: MessageSerializer = message_serializer

        # High-level tracking
        self._next_schema_id = 1  # Schema ID must be non-zero
        self._next_channel_id = 1
        self._topics: dict[str, int] = {}  # topic -> channel_id
        self._schemas: dict[type[Message], int] = {}  # type -> schema_id
        self._schema_names: dict[str, int] = {}  # schema name -> schema_id
        self._sequences: dict[int, int] = {}  # channel_id -> sequence number

    def __enter__(self) -> "McapFileWriter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Context manager exit."""
        self.close()

    @classmethod
    def open(
        cls,
        file_path: str | Path,
        *,
        mode: Literal["w", "a"] = "w",
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = "lz4",
    ) -> "McapFileWriter":
        """Create a writer backed by a file on disk.

        Args:
            file_path: The path to the file to write to.
            mode: The mode to open the file in. 'w' for write (default), 'a' for append.
                  In append mode, the file must already exist and be a valid MCAP file.
            profile: The profile to use for the MCAP file.
            chunk_size: The size of the chunk to write to in bytes.
                       If None, writes without chunking.
            chunk_compression: The compression to use for the chunk.

        Returns:
            A writer backed by a file on disk.
        """
        if mode == "w":
            return cls(
                FileWriter(file_path),
                profile=profile,
                chunk_size=chunk_size,
                chunk_compression=chunk_compression,
            )
        elif mode == "a":
            return cls._open_append(
                file_path,
                profile=profile,
                chunk_size=chunk_size,
                chunk_compression=chunk_compression,
            )
        else:
            raise ValueError(f"Invalid mode: {mode}. Use 'w' for write or 'a' for append.")

    @classmethod
    def _open_append(
        cls,
        file_path: str | Path,
        *,
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = "lz4",
    ) -> "McapFileWriter":
        """Open an existing MCAP file for appending.

        Args:
            file_path: The path to the existing MCAP file.
            profile: The profile to use (should match existing file).
            chunk_size: The size of the chunk to write to in bytes.
            chunk_compression: The compression to use for the chunk.

        Returns:
            A writer configured to append to the existing file.
        """
        file_path = Path(file_path)

        # Read the existing file to load summary information
        with FileReader(file_path) as reader:
            # Try chunked summary first, fall back to non-chunked
            try:
                summary: McapChunkedSummary | McapNonChunkedSummary = McapChunkedSummary(
                    reader,
                    enable_crc_check=False,
                    enable_reconstruction='missing',
                )
                is_chunked = True
            except Exception:
                # Fall back to non-chunked summary
                summary = McapNonChunkedSummary(
                    reader,
                    enable_crc_check=False,
                    enable_reconstruction='missing',
                )
                is_chunked = False

            # Load existing data
            schemas = summary.get_schemas()
            channels = summary.get_channels()
            statistics = summary.get_statistics()
            if statistics is None:
                statistics = StatisticsRecord(
                    message_count=0,
                    schema_count=len(schemas),
                    channel_count=len(channels),
                    attachment_count=0,
                    metadata_count=0,
                    chunk_count=0,
                    message_start_time=0,
                    message_end_time=0,
                    channel_message_counts={},
                )

            # Get chunk indexes for chunked files
            chunk_indexes: list[ChunkIndexRecord] = []
            if is_chunked and isinstance(summary, McapChunkedSummary):
                chunk_indexes = summary.get_chunk_indexes()

            # Get attachment and metadata indexes
            attachment_indexes_dict = summary.get_attachment_indexes()
            metadata_indexes_dict = summary.get_metadata_indexes()

            # Flatten the dictionaries to lists
            attachment_indexes: list[AttachmentIndexRecord] = []
            for indexes in attachment_indexes_dict.values():
                attachment_indexes.extend(indexes)

            metadata_indexes: list[MetadataIndexRecord] = []
            for indexes in metadata_indexes_dict.values():
                metadata_indexes.extend(indexes)

            # Calculate the data end position
            # The data section ends at footer.summary_start - DATA_END_SIZE
            # or if there's no summary, it ends at file_size - FOOTER_SIZE - MAGIC_BYTES_SIZE - DATA_END_SIZE
            footer = summary._footer
            if footer.summary_start != 0:
                data_end_position = footer.summary_start - DATA_END_SIZE
            else:
                reader.seek_from_end(0)
                file_size = reader.tell()
                data_end_position = file_size - FOOTER_SIZE - MAGIC_BYTES_SIZE - DATA_END_SIZE

        # Create append writer
        append_writer = AppendFileWriter(file_path)
        record_writer = McapRecordWriterFactory.create_append_writer(
            append_writer,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            schemas=schemas,
            channels=channels,
            statistics=statistics,
            chunk_indexes=chunk_indexes,
            attachment_indexes=attachment_indexes,
            metadata_indexes=metadata_indexes,
            data_end_position=data_end_position,
        )

        # Create the high-level writer instance
        instance = cls.__new__(cls)

        # Initialize the record writer directly (bypass __init__)
        instance._record_writer = record_writer
        instance._profile = profile

        # Get message serializer
        message_serializer = MessageSerializerFactory.from_profile(profile)
        if message_serializer is None:
            raise ValueError(f"Unknown encoding type: {profile}")
        instance._message_serializer = message_serializer

        # Initialize tracking from existing data
        # Find the next schema and channel IDs
        instance._next_schema_id = max(schemas.keys(), default=0) + 1
        instance._next_channel_id = max(channels.keys(), default=0) + 1

        # Build topic -> channel_id mapping
        instance._topics = {ch.topic: ch.id for ch in channels.values()}

        # Build schema name -> schema_id mapping (for matching existing schemas)
        instance._schema_names = {sch.name: sch.id for sch in schemas.values()}

        # We cannot directly map type[Message] -> schema_id without the original types
        # So we start with an empty mapping; new types will get new IDs
        instance._schemas = {}

        # Initialize sequence numbers from existing message counts
        instance._sequences = {
            channel_id: count
            for channel_id, count in statistics.channel_message_counts.items()
        }

        return instance

    def add_channel(self, topic: str, channel_type: type[Message]) -> int:
        """Add a channel to the MCAP output.

        If the topic already exists, returns the existing channel ID.
        Otherwise, creates a new schema (if needed) and channel.

        Args:
            topic: The topic name.
            channel_type: The type of the messages to be written to the channel.

        Returns:
            The channel ID.
        """
        # Check if topic already exists
        if (channel_id := self._topics.get(topic)) is not None:
            return channel_id

        # Check that the channel type has a __msg_name__ attribute
        if not hasattr(channel_type, '__msg_name__'):
            raise ValueError(f"Channel type {channel_type} needs a __msg_name__ attribute")

        # Register the schema if it's not already registered
        # First check by type (for newly created schemas in this session)
        if (schema_id := self._schemas.get(channel_type)) is None:
            # Then check by name (for schemas loaded from existing file in append mode)
            schema_name = channel_type.__msg_name__
            if (schema_id := self._schema_names.get(schema_name)) is None:
                # Schema doesn't exist, create a new one
                schema_id = self._next_schema_id
                self._next_schema_id += 1

                schema_record = SchemaRecord(
                    id=schema_id,
                    name=schema_name,
                    encoding=self._message_serializer.schema_encoding,
                    data=self._message_serializer.serialize_schema(channel_type),  # type: ignore[arg-type]
                )

                self._record_writer.write_schema(schema_record)
                self._schema_names[schema_name] = schema_id

            # Track by type as well for future lookups in this session
            self._schemas[channel_type] = schema_id  # type: ignore[assignment]

        # Register the channel
        channel_id = self._next_channel_id
        self._next_channel_id += 1
        channel_record = ChannelRecord(
            id=channel_id,
            schema_id=schema_id,
            topic=topic,
            message_encoding=self._message_serializer.message_encoding,
            metadata={},
        )

        self._record_writer.write_channel(channel_record)
        self._topics[topic] = channel_id
        self._sequences[channel_id] = 0

        return channel_id

    def write_message(
        self,
        topic: str,
        timestamp: int,
        message: Message,
        publish_time: int | None = None
    ) -> None:
        """Write a message to a topic at a given timestamp.

        Automatically creates the channel (and schema) if it doesn't exist.

        Args:
            topic: The topic name.
            timestamp: The log timestamp of the message (nanoseconds).
            message: The message to write.
            publish_time: The publish timestamp (nanoseconds). If None, defaults to timestamp.
        """
        # Ensure the channel exists
        channel_id = self.add_channel(topic, type(message))

        # Get and increment sequence number
        sequence = self._sequences[channel_id]
        self._sequences[channel_id] = sequence + 1

        # Use publish_time if provided, otherwise default to timestamp (log_time)
        actual_publish_time = publish_time if publish_time is not None else timestamp

        # Create message record
        record = MessageRecord(
            channel_id=channel_id,
            sequence=sequence,
            log_time=timestamp,
            publish_time=actual_publish_time,
            data=self._message_serializer.serialize_message(message),
        )

        # Delegate to low-level writer
        self._record_writer.write_message(record)

    def flush_chunk(self) -> None:
        """Flush the current chunk if using a chunked writer.

        For chunked writers, this forces the current chunk to be written even if
        it hasn't reached the size threshold. This is useful when switching topics
        to ensure that each chunk only contains messages from one topic.
        For non-chunked writers, this is a no-op.
        """
        self._record_writer.flush_chunk()

    # TODO: Smarter API (e.g. auto-encode text, auto media_type)?
    def write_attachment(
        self,
        name: str,
        data: bytes,
        media_type: str = "application/octet-stream",
        log_time: int | None = None,
        create_time: int | None = None,
        *,
        do_compute_crc = True,
    ) -> None:
        """Write an attachment to the MCAP file.

        Attachments are auxiliary files embedded in the MCAP, such as calibration data,
        configuration files, or other metadata files.

        Args:
            name: Name of the attachment (e.g., "camera_calibration.yaml").
            data: Binary data of the attachment.
            media_type: MIME type of the attachment.
            log_time: Log timestamp (nanoseconds). If None, defaults to 0.
            create_time: Creation timestamp (nanoseconds). If None, defaults to log_time.
            compute_crc: Whether to compute the crc value on the data or not
        """
        actual_log_time = log_time if log_time is not None else 0
        actual_create_time = create_time if create_time is not None else actual_log_time

        record = AttachmentRecord(
            log_time=actual_log_time,
            create_time=actual_create_time,
            name=name,
            media_type=media_type,
            data=data,
            crc=compute_crc(data) if do_compute_crc else 0,
        )
        self._record_writer.write_attachment(record)

    def write_metadata(
        self,
        name: str,
        metadata: dict[str, str]
    ) -> None:
        """Write a metadata record to the MCAP file.

        Args:
            name: Name/identifier for this metadata record.
            metadata: Dictionary of key-value pairs (both strings).
        """
        record = MetadataRecord(
            name=name,
            metadata=metadata,
        )
        self._record_writer.write_metadata(record)

    def close(self) -> None:
        """Finalize the MCAP file by writing summary section and footer.

        Delegates to the low-level record writer to handle all finalization.
        """
        self._record_writer.close()
