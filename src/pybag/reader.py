"""Unified reader for MCAP and ROS 1 bag files."""

from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Literal

from pybag.bag_reader import BagFileReader
from pybag.bag_reader import DecodedMessage as BagDecodedMessage
from pybag.mcap.records import AttachmentRecord, MetadataRecord
from pybag.mcap_reader import DecodedMessage as McapDecodedMessage
from pybag.mcap_reader import McapFileReader


@dataclass(slots=True)
class DecodedMessage:
    """A decoded message from either MCAP or ROS 1 bag file.

    This unified message format provides a common interface for messages
    from both file formats.

    Attributes:
        topic: The topic name the message was published on.
        msg_type: The message type name (e.g., "std_msgs/msg/String").
        log_time: Timestamp in nanoseconds when the message was logged.
        data: The deserialized message object.
    """

    topic: str
    msg_type: str
    log_time: int
    data: Any


def _detect_format(path: Path) -> Literal['mcap', 'bag']:
    """Detect file format from extension.

    Args:
        path: Path to the file.

    Returns:
        The detected format ('mcap' or 'bag').

    Raises:
        ValueError: If the file extension is not recognized.
    """
    # TODO: Read magic bytes to determine?
    suffix = path.suffix.lower()
    if suffix == '.mcap':
        return 'mcap'
    elif suffix == '.bag':
        return 'bag'
    else:
        raise ValueError(f"Unknown file extension '{suffix}'. Expected '.mcap' or '.bag'.")


class Reader:
    """Unified reader for MCAP and ROS 1 bag files.

    This class provides a common interface for reading messages from both
    MCAP and ROS 1 bag files. The file format is automatically detected
    from the file extension.

    Example:
        with Reader.from_file("recording.mcap") as reader:
            for msg in reader.messages("/camera/image"):
                print(msg.log_time, msg.data)

        with Reader.from_file("recording.bag") as reader:
            for msg in reader.messages("/camera/image"):
                print(msg.log_time, msg.data)
    """

    def __init__(
        self,
        reader: McapFileReader | BagFileReader,
        file_format: Literal['mcap', 'bag'],
    ):
        """Initialize the unified reader.

        Args:
            reader: The underlying format-specific reader.
            file_format: The file format ('mcap' or 'bag').
        """
        self._reader = reader
        self._format = file_format

    @staticmethod
    def from_file(
        path: Path | str,
        *,
        enable_crc_check: bool = False,
        chunk_cache_size: int = 1,
    ) -> 'Reader':
        """Create a reader from a file path.

        The file format is automatically detected from the file extension.

        Args:
            path: Path to the MCAP or bag file.
            enable_crc_check: Enable CRC validation (MCAP only).
            chunk_cache_size: Number of decompressed chunks to cache (bag only).

        Returns:
            A new Reader instance.

        Raises:
            ValueError: If the file extension is not recognized.
        """
        path = Path(path)
        file_format = _detect_format(path)

        if file_format == 'mcap':
            reader = McapFileReader.from_file(path, enable_crc_check=enable_crc_check)
        else:
            reader = BagFileReader.from_file(path, chunk_cache_size=chunk_cache_size)

        return Reader(reader, file_format)

    @property
    def format(self) -> Literal['mcap', 'bag']:
        """Get the file format.

        Returns:
            The file format ('mcap' or 'bag').
        """
        return self._format

    @property
    def start_time(self) -> int:
        """Get the start time of the file in nanoseconds since epoch."""
        return self._reader.start_time

    @property
    def end_time(self) -> int:
        """Get the end time of the file in nanoseconds since epoch."""
        return self._reader.end_time

    def get_topics(self) -> list[str]:
        """Get all topics in the file.

        Returns:
            List of topic names.
        """
        return self._reader.get_topics()

    def get_message_count(self, topic: str) -> int:
        """Get the number of messages for a topic.

        Args:
            topic: The topic name.

        Returns:
            Number of messages for the topic.
        """
        return self._reader.get_message_count(topic)

    def _convert_mcap_message(self, msg: McapDecodedMessage) -> DecodedMessage:
        """Convert an MCAP message to the unified format."""
        return DecodedMessage(
            topic=msg.topic,
            msg_type=msg.msg_type,
            log_time=msg.log_time,
            data=msg.data,
        )

    def _convert_bag_message(self, msg: BagDecodedMessage) -> DecodedMessage:
        """Convert a bag message to the unified format."""
        return DecodedMessage(
            topic=msg.topic,
            msg_type=msg.msg_type,
            log_time=msg.log_time,
            data=msg.data,
        )

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
        """Iterate over messages in the file.

        Args:
            topic: Topic(s) to filter by. Can be:
                - Single topic string (e.g., "/camera")
                - Glob pattern (e.g., "/sensor/*")
                - List of topics/patterns
            start_time: Start time to filter by (nanoseconds). If None, start from beginning.
            end_time: End time to filter by (nanoseconds). If None, read to end.
            filter: Callable to filter messages. If None, all messages are returned.
            in_log_time_order: Return messages in log time order if True.
            in_reverse: Return messages in reverse order.

        Yields:
            DecodedMessage objects from matching topics.

        Raises:
            NotImplementedError: If in_reverse=True for bag files.
        """
        if filter is not None:
            if self._format == 'mcap':
                convert_filter = lambda msg: filter(self._convert_mcap_message(msg))
            elif self._format == 'bag':
                convert_filter = lambda msg: filter(self._convert_bag_message(msg))
            else:
                raise ValueError(f"Unsupported format: {self._format}")

        for msg in self._reader.messages(
            topic,
            start_time=start_time,
            end_time=end_time,
            filter=convert_filter if filter else None,
            in_log_time_order=in_log_time_order,
            in_reverse=in_reverse,
        ):
            if self._format == 'mcap':
                mcap_msg: McapDecodedMessage = msg  # type: ignore
                yield self._convert_mcap_message(mcap_msg)
            elif self._format == 'bag':
                bag_msg: BagDecodedMessage = msg  # type: ignore
                yield self._convert_bag_message(bag_msg)
            else:
                raise ValueError(f"Unsupported format: {self._format}")

    def get_attachments(self, name: str | None = None) -> list[AttachmentRecord]:
        """Get attachments from the file.

        For MCAP files, returns the attachments stored in the file.
        For bag files, returns an empty list (attachments not supported).

        Args:
            name: Optional name filter. If None, returns all attachments.

        Returns:
            List of AttachmentRecord objects.
        """
        if self._format == 'mcap':
            mcap_reader: McapFileReader = self._reader  # type: ignore
            return mcap_reader.get_attachments(name)
        else:
            return []

    def get_metadata(self, name: str | None = None) -> list[MetadataRecord]:
        """Get metadata records from the file.

        For MCAP files, returns the metadata records stored in the file.
        For bag files, returns an empty list (metadata records not supported).

        Args:
            name: Optional name filter. If None, returns all metadata records.

        Returns:
            List of MetadataRecord objects.
        """
        if self._format == 'mcap':
            mcap_reader: McapFileReader = self._reader  # type: ignore
            return mcap_reader.get_metadata(name)
        else:
            return []

    def close(self) -> None:
        """Close the reader and release all resources."""
        self._reader.close()

    def __enter__(self) -> 'Reader':
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
