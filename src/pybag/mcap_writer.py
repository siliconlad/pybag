"""Utilities for writing MCAP files."""

import logging
from pathlib import Path
from typing import Any, Callable, Literal

from pybag.encoding import MessageEncoder
from pybag.encoding.cdr import CdrEncoder
from pybag.encoding.rosmsg import RosMsgEncoder
from pybag.io.raw_reader import FileReader
from pybag.io.raw_writer import BaseWriter, FileWriter
from pybag.mcap.crc import compute_crc
from pybag.mcap.record_writer import McapRecordWriterFactory
from pybag.mcap.records import (
    AttachmentRecord,
    ChannelRecord,
    MessageRecord,
    MetadataRecord,
    SchemaRecord
)
from pybag.mcap.summary import (
    McapChunkedSummary,
    McapNonChunkedSummary,
    McapSummary,
    McapSummaryFactory
)
from pybag.schema.compiler import compile_serializer
from pybag.schema.ros1_compiler import compile_ros1_serializer
from pybag.schema.ros1msg import Ros1McapSchemaDecoder, Ros1MsgSchemaEncoder
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder, Ros2MsgSchemaEncoder
from pybag.serialize import MessageSerializer, MessageSerializerFactory
from pybag.types import Message, SchemaText

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
        summary: McapSummary,
        *,
        mode: Literal['w', 'a'] = 'w',
        profile: Literal['ros1', 'ros2'] = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["none", "lz4", "zstd"] | None = "none",
    ) -> None:
        """Initialize a high-level MCAP file writer.

        Args:
            writer: The underlying writer to write binary data to.
            mode: The mode to open the file in: 'a' for append, 'w' for write.
                  In append mode, the file must already exist and be a valid MCAP file.
            profile: The MCAP profile to use (default: "ros2").
            chunk_size: If provided, creates chunks of approximately this size in bytes. If None, writes without chunking.
            chunk_compression: Compression algorithm for chunks ("lz4" or "zstd" or None for no compression).
        """
        # Get message serializer for this profile
        self._profile = profile
        message_serializer = MessageSerializerFactory.from_profile(self._profile)
        if message_serializer is None:
            raise ValueError(f"Unknown encoding type: {self._profile}")
        self._message_serializer: MessageSerializer = message_serializer

        self._summary = summary
        if not chunk_size and isinstance(self._summary, McapChunkedSummary):
            logging.warning(f'File is chunked so ignoring chunk_size: {chunk_size}')
            chunk_size = 1024 * 1024  # 1MB

        # Create the low-level record writer via factory
        self._record_writer = McapRecordWriterFactory.create_writer(
            writer,
            self._summary,
            mode=mode,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            profile=self._profile,
        )

        # Pre-compiled serializers for topics with explicit schemas
        # Maps topic -> compiled serializer function
        self._topic_serializers: dict[str, Callable[[MessageEncoder, Any], None]] = {}

        # Schema decoder/encoder and compiler based on profile
        # TODO: This should be integrated with message_serializer
        if self._profile == "ros1":
            self._schema_decoder = Ros1McapSchemaDecoder()
            self._schema_encoder = Ros1MsgSchemaEncoder()
            self._schema_compiler = compile_ros1_serializer
            self._create_encoder: Callable[[], MessageEncoder] = RosMsgEncoder
        elif self._profile == "ros2":
            self._schema_decoder = Ros2MsgSchemaDecoder()
            self._schema_encoder = Ros2MsgSchemaEncoder()
            self._schema_compiler = compile_serializer
            self._create_encoder = lambda: CdrEncoder(little_endian=True)
        else:
            raise ValueError(f'Unsupported profile: {self._profile}')

        # TODO: Use Summary instead
        self._written_schemas: dict[int, SchemaRecord] = {}

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
        mode: Literal['w', 'a'] = 'w',
        profile: Literal['ros1', 'ros2'] = "ros2",
        chunk_size: int | None = None,
        chunk_compression: Literal["none", "lz4", "zstd"] | None = "lz4",
    ) -> "McapFileWriter":
        """Create a writer backed by a file on disk.

        Args:
            file_path: The path to the file to write to.
            mode: The mode to open the file in: 'a' for append, 'w' for write.
                  In append mode, the file must already exist and be a valid MCAP file.
            profile: The profile to use for the MCAP file.
            chunk_size: The size of the chunk to write to in bytes.
                       If None, writes without chunking.
            chunk_compression: The compression to use for the chunk.

        Returns:
            A writer backed by a file on disk.
        """
        return cls(
            FileWriter(file_path, mode='wb' if mode == 'w' else 'r+b'),
            mode=mode,
            profile=profile,
            chunk_size=chunk_size,
            chunk_compression=chunk_compression,
            summary=McapSummaryFactory.create_summary(
                file=FileReader(file_path) if mode == 'a' else None,
                chunk_size=chunk_size,
            ),
        )

    def add_channel(
        self,
        topic: str,
        *,
        schema: SchemaText | type[Message] | Message,
    ) -> int:
        """Add a channel to the MCAP output.

        If the topic already exists, returns the existing channel ID.
        Otherwise, creates a new schema (if needed) and channel.

        Args:
            topic: The topic name.
            schema: A SchemaText object containing the message type name and
                   schema definition text, or a message class/instance to
                   generate the schema from.

        Returns:
            The channel ID.
        """
        # Check if topic already exists
        if (channel_id := self._summary.get_channel_id(topic)) is not None:
            return channel_id

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

        schema_hash = hash(schema.text + schema.name)
        if schema_hash not in self._written_schemas:
            schema_data = schema.text.encode('utf-8')
            schema_name = schema.name
            schema_record = SchemaRecord(
                id=self._summary.next_schema_id(),
                name=schema_name,
                encoding=self._message_serializer.schema_encoding,
                data=schema_data,
            )
            self._record_writer.write_schema(schema_record)
            self._written_schemas[schema_hash] = schema_record
        else:
            schema_record = self._written_schemas[schema_hash]

        # Parse the schema text and compile a serializer for this topic
        # This allows us to serialize messages without relying on type annotations
        parsed_schema, sub_schemas = self._schema_decoder.parse_schema(schema_record)
        serializer = self._schema_compiler(parsed_schema, sub_schemas)
        self._topic_serializers[topic] = serializer

        # Register the channel
        channel_id = self._summary.next_channel_id()
        channel_record = ChannelRecord(
            id=channel_id,
            schema_id=schema_record.id,
            topic=topic,
            message_encoding=self._message_serializer.message_encoding,
            metadata={},
        )
        self._record_writer.write_channel(channel_record)

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
        If the channel was pre-registered with add_channel(), uses that schema.

        Args:
            topic: The topic name.
            timestamp: The log timestamp of the message (nanoseconds).
            message: The message to write.
            publish_time: The publish timestamp (nanoseconds). If None, defaults to timestamp.
        """
        # Check if channel already exists (may have been pre-registered)
        channel_id = self._summary.get_channel_id(topic)
        if channel_id is None:
            channel_id = self.add_channel(topic, schema=message)

        # Get and increment sequence number
        sequence = self._summary.next_sequence_id(channel_id)

        # Use publish_time if provided, otherwise default to timestamp (log_time)
        actual_publish_time = publish_time if publish_time is not None else timestamp

        # Serialize the message
        if topic not in self._topic_serializers:
            data = self._message_serializer.serialize_message(message)
        else:
            serializer = self._topic_serializers[topic]
            encoder = self._create_encoder()
            serializer(encoder, message)
            data = encoder.save()

        # Create message record
        record = MessageRecord(
            channel_id=channel_id,
            sequence=sequence,
            log_time=timestamp,
            publish_time=actual_publish_time,
            data=data,
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
