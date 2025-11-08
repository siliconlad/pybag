"""Utilities for writing MCAP files."""

import logging
from pathlib import Path
from typing import Literal

from pybag.io.raw_writer import BaseWriter, FileWriter
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import ChannelRecord, MessageRecord, SchemaRecord
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
        profile: str = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["lz4", "zstd"] | None = "lz4",
    ) -> "McapFileWriter":
        """Create a writer backed by a file on disk.

        Args:
            file_path: The path to the file to write to.
            profile: The profile to use for the MCAP file.
            chunk_size: The size of the chunk to write to in bytes.
                       If None, writes without chunking.
            chunk_compression: The compression to use for the chunk.

        Returns:
            A writer backed by a file on disk.
        """
        return cls(
            FileWriter(file_path),
            profile=profile,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
        )

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

        # Register the schema if it's not already registered
        if (schema_id := self._schemas.get(channel_type)) is None:
            schema_id = self._next_schema_id
            self._next_schema_id += 1

            # Check that the channel type has a __msg_name__ attribute
            if not hasattr(channel_type, '__msg_name__'):
                raise ValueError(f"Channel type {channel_type} needs a __msg_name__ attribute")

            schema_record = SchemaRecord(
                id=schema_id,
                name=channel_type.__msg_name__,
                encoding=self._message_serializer.schema_encoding,
                data=self._message_serializer.serialize_schema(channel_type),  # type: ignore[arg-type]
            )

            self._record_writer.write_schema(schema_record)
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

    def close(self) -> None:
        """Finalize the MCAP file by writing summary section and footer.

        Delegates to the low-level record writer to handle all finalization.
        """
        self._record_writer.close()
