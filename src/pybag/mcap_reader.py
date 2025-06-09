import logging
from pathlib import Path
from functools import cache
from collections import namedtuple

from pybag.mcap.records import StatisticsRecord, SchemaRecord, ChannelRecord, FooterRecord
from pybag.io.raw_reader import BaseReader, FileReader, BytesReader
from pybag.mcap.record_reader import McapRecordReader, MAGIC_BYTES_SIZE, FOOTER_SIZE, McapRecordType

# GLOBAL TODOs:
# - TODO: Add tests with mcaps
# - TODO: Parse ros2idl messages
# - TODO: Control seeking beaviour
# - TODO: Improve performance by batching the reads (maybe)
# - TODO: Figure out how to make schema encoding work with cdr
# - TODO: Do something with CRC
# - TODO: Generate summary section of mcap file
logger = logging.getLogger(__name__)


class McapFileRandomAccessReader:
    """Class to efficiently get records from an MCAP file."""

    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP reader.

        Args:
            file: The file to read from.
        """
        self._file: BaseReader = file
        self._version = McapRecordReader.read_magic_bytes(self._file)
        logger.debug(f'MCAP version: {self._version}')

        # Check magic bytes at the end of the file
        self._file.seek_from_end(MAGIC_BYTES_SIZE)
        mcap_version = McapRecordReader.read_magic_bytes(self._file)
        assert self._version == mcap_version

        # Check footer at the end of the file
        footer = self.get_footer()

        # Summary section start
        self._summary_start = footer.summary_start
        logger.debug(f'Summary start: {self._summary_start}')
        if self._summary_start == 0:
            logger.warning('No summary section detected in MCAP')
            raise ValueError('No summary section detected in MCAP')

        # Summary offset section start
        self._summary_offset_start = footer.summary_offset_start
        logger.debug(f'Summary offset start: {self._summary_offset_start}')
        if self._summary_offset_start == 0:
            logger.error('No summary offset section detected in MCAP')
            raise ValueError('No summary offset section detected in MCAP')

        self._summary_offset = {}
        Offset = namedtuple('Offset', ['group_start', 'group_length'])
        self._file.seek_from_start(self._summary_offset_start)
        while McapRecordReader.peek_record(self._file) == McapRecordType.SUMMARY_OFFSET:
            record = McapRecordReader.parse_summary_offset(self._file)
            self._summary_offset[record.group_opcode] = Offset(record.group_start, record.group_length)

    # Helpful Constructors

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapFileRandomAccessReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapFileRandomAccessReader(FileReader(file_path))


    @staticmethod
    def from_bytes(data: bytes) -> 'McapFileRandomAccessReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapFileRandomAccessReader(BytesReader(data))

    # Destructors

    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()

    # Statistics Management

    @cache
    def get_footer(self) -> FooterRecord:
        """Get the footer record from the MCAP file."""
        self._file.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordReader.parse_footer(self._file)

    @cache
    def get_statistics(self) -> StatisticsRecord:
        """Get the statistics record from the MCAP file."""
        self._file.seek_from_start(self._summary_offset[McapRecordType.STATISTICS].group_start)
        return McapRecordReader.parse_statistics(self._file)

    @cache
    def get_start_time(self) -> int:
        """
        Get the start time of the MCAP file in nanoseconds since epoch.
        """
        return self.get_statistics().message_start_time

    @cache
    def get_end_time(self) -> int:
        """
        Get the end time of the MCAP file in nanoseconds since epoch.
        """
        return self.get_statistics().message_end_time

    @cache
    def get_message_counts(self) -> dict[int, int]:
        """
        Get the number of messages in the MCAP file for each channel.

        Returns:
            A dictionary mapping channel IDs to the number of messages.
        """
        return self.get_statistics().channel_message_counts

    # Schema Management

    @cache
    def get_schemas(self) -> dict[int, SchemaRecord]:
        """
        Get all schemas defined in the MCAP file.

        Returns:
            A dictionary mapping schema IDs to SchemaInfo objects.
        """
        self._file.seek_from_start(self._summary_offset[McapRecordType.SCHEMA].group_start)
        schemas = {}
        while McapRecordReader.peek_record(self._file) == McapRecordType.SCHEMA:
            schema = McapRecordReader.parse_schema(self._file)
            schemas[schema.id] = schema
        return schemas

    @cache
    def get_schema(self, schema_id: int) -> SchemaRecord | None:
        """Get a schema by its ID."""
        return self.get_schemas().get(schema_id)

    # Channel Management

    @cache
    def get_channels(self) -> dict[int, ChannelRecord]:
        """
        Get all channels/topics in the MCAP file.

        Returns:
            A dictionary mapping channel IDs to channel information.
        """
        self._file.seek_from_start(self._summary_offset[McapRecordType.CHANNEL].group_start)
        channels = {}
        while McapRecordReader.peek_record(self._file) == McapRecordType.CHANNEL:
            channel = McapRecordReader.parse_channel(self._file)
            channels[channel.id] = channel
        return channels

    @cache
    def get_channel_by_id(self, channel_id: int) -> ChannelRecord | None:
        """
        Get channel information by its ID.

        Args:
            channel_id: The ID of the channel.

        Returns:
            The channel information or None if the channel does not exist.
        """
        return self.get_channels().get(channel_id)

    # Data Management

    # TODO: Implement methods to deal with:
    #    - Message
    #    - Chunk
    #    - Message Index
    #    - Chunk Index


class McapFileSequentialReader:
    """Class to read messages from an MCAP file sequentially."""

    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP file sequential reader.
        """
        self._file = file


class McapIndexBuilder:
    """Class to build an index for an MCAP file."""

    @staticmethod
    def build(reader: McapFileSequentialReader) -> McapFileRandomAccessReader:
        """Build an index for the MCAP file."""
        # TODO: Implement


class McapReader:
    """Class to read messages from an MCAP file."""

    def __init__(self, file: BaseReader):
        """
        Initialize the MCAP reader.

        Args:
            file: The file to read from.
        """
        pass

    @staticmethod
    def from_file(file_path: Path | str) -> 'McapReader':
        """
        Create a new MCAP reader from a file.
        """
        return McapReader(FileReader(file_path))


    @staticmethod
    def from_bytes(data: bytes) -> 'McapReader':
        """
        Create a new MCAP reader from a bytes object.
        """
        return McapReader(BytesReader(data))

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


