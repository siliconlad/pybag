from pathlib import Path
from typing import Iterator, Any

from pybag.mcap.raw_reader import BaseReader, FileReader, BytesReader
from pybag.mcap.record_reader import McapRecordReader

# GLOBAL TODOs:
# - TODO: Add logging
# - TODO: Add tests with mcaps
# - TODO: Parse ros2idl messages
# - TODO: Control seeking behaviour
# - TODO: Improve performance by batching the reads (maybe)
# - TODO: Support arrays in ros messages
# - TODO: Figure out how to make schema encoding work with cdr


class McapFileReader:
    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP reader.

        Args:
            file: The file to read from.
        """
        self._file: BaseReader = file
        self._version = McapRecordReader.read_magic_bytes(self._file)


    def read_record(self) -> Iterator[tuple[int, Any]]:
        """Read the next record in the MCAP file."""
        return McapRecordReader.read_record(self._file)


    @staticmethod
    def from_file(file_path: Path | str) -> 'McapFileReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapFileReader(FileReader(file_path))


    @staticmethod
    def from_bytes(data: bytes) -> 'McapFileReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapFileReader(BytesReader(data))


    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

   # Schema Management

    # def get_schemas(self) -> dict[int, SchemaInfo]:
    #     """
    #     Get all schemas defined in the MCAP file.

    #     Returns:
    #         A dictionary mapping schema IDs to SchemaInfo objects.
    #     """
    #     pass

    # def get_schema(self, schema_id: int) -> SchemaInfo | None:
    #     """
    #     Get a schema by its ID.
    #     """

    # # Channel Management

    # def get_channels(self) -> dict[int, ChannelInfo]:
    #     """
    #     Get all channels/topics in the MCAP file.

    #     Returns:
    #         A dictionary mapping channel IDs to channel information.
    #     """
    #     pass

    # def get_topics(self) -> list[str]:
    #     """
    #     Get all topics in the MCAP file.

    #     Returns:
    #         A list of topic names.
    #     """
    #     pass

    # def get_channel_by_id(self, channel_id: int) -> ChannelInfo | None:
    #     """
    #     Get channel information by its ID.

    #     Args:
    #         channel_id: The ID of the channel.

    #     Returns:
    #         The channel information or None if the channel does not exist.
    #     """
    #     pass

    # # Time Management

    # def get_start_time(self) -> float:
    #     """
    #     Get the start time of the MCAP file in seconds since epoch.
    #     """
    #     pass

    # def get_start_time_ns(self) -> int:
    #     """
    #     Get the start time of the MCAP file in nanoseconds since epoch.
    #     """
    #     pass

    # def get_end_time(self) -> float:
    #     """
    #     Get the end time of the MCAP file in seconds since epoch.
    #     """
    #     pass

    # def get_end_time_ns(self) -> int:
    #     """
    #     Get the end time of the MCAP file in nanoseconds since epoch.
    #     """
    #     pass

    # # Message Management

    # def get_message_counts(self) -> dict[int, int]:
    #     """
    #     Get the number of messages in the MCAP file for each channel.

    #     Returns:
    #         A dictionary mapping channel IDs to the number of messages.
    #     """
    #     pass

    # def messages(
    #         self,
    #         topics: list[str] | str | None = None,
    #         start_time: float | None = None,
    #         end_time: float | None = None,
    #         decode: bool = True,
    #     ) -> Iterator[tuple[ChannelInfo, int, Any]]:
    #         """
    #         Iterate over messages in the MCAP file.

    #         Args:
    #             topics: Topics to filter by. If None, all topics are included.
    #             start_time: Start time to filter by. If None, start from the beginning of the file.
    #             end_time: End time to filter by. If None, read to the end of the file.
    #             decode: If True, the message data will be decoded.

    #         Returns:
    #             An iterator over tuples of (channel_info, sequence, message_data).
    #         """
    #         pass

    # # Index Management

    # def build_index(self) -> None:
    #     """Build an index for the MCAP file."""
    #     pass


    # def is_indexed(self) -> bool:
    #     """
    #     Check if the message index has been built.

    #     Returns:
    #         True if the index is available, False otherwise.
    #     """
    #     pass

    # # File Information

    # def get_summary(self) -> dict[str, str]:
    #     """Get a summary of the MCAP file."""
    #     pass

    # def get_file_size(self) -> int:
    #     """Get the size of the MCAP file in bytes."""
    #     pass

    # def path(self) -> Path:
    #     """Return the path of the MCAP file."""
    #     return self._file
