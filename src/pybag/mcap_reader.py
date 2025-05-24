from pathlib import Path
from typing import Iterator, Any, Callable
import struct
import io

from pybag.records import *  # TODO: Make better

# GLOBAL TODOs:
# - TODO: Add logging
# - TODO: Add tests with mcaps


class MalformedMCAP(Exception):
    """The MCAP format does not conform to the specification."""
    def __init__(self, error_msessage: str):
        super().__init__(error_msessage)


class McapFileReader:
    def __init__(self, file_path: Path | str):
        """
        Initialize the MCAP reader.

        Args:
            file_path: The path to the MCAP file.
        """
        self._file_path = Path(file_path).absolute()
        self._file = open(self._file_path, 'rb')
        self._version = self._parse_magic_bytes()


    def close(self) -> None:
        """Close the MCAP file and release all resources."""
        self._file.close()


    def peek_record(self) -> int:
        """Peek at the next record in the MCAP file."""
        return self._file.peek(1)[:1]


    def read_record(self) -> Iterator[tuple[int, Any]]:
        """Read the next record in the MCAP file."""
        while True:
            record_type = int.from_bytes(self.peek_record(), 'little')
            print(f'Peeked at {record_type} record...')
            if record_type == 0:
                break  # EOF
            yield record_type, self._parse_record(record_type)


    def _parse_record(self, record_type: int) -> Any:
        """Parse the next record in the MCAP file."""
        record_name = RecordType(record_type).name.lower()
        print(f'Parsing {record_name} record...')
        return getattr(self, f'_parse_{record_name}')()

    # MCAP Serialization Handlers

    def _parse_uint8(self) -> tuple[int, int]:
        return 1, struct.unpack('<B', self._file.read(1))[0]


    def _parse_uint16(self) -> tuple[int, int]:
        return 2, struct.unpack('<H', self._file.read(2))[0]


    def _parse_uint32(self) -> tuple[int, int]:
        return 4, struct.unpack('<I', self._file.read(4))[0]


    def _parse_uint64(self) -> tuple[int, int]:
        return 8, struct.unpack('<Q', self._file.read(8))[0]


    def _parse_string(self) -> tuple[int, str]:
        string_length_bytes, string_length = self._parse_uint32()
        string = self._file.read(string_length)
        return string_length_bytes + string_length, string.decode()


    def _parse_timestamp(self) -> tuple[int, int]:
        return self._parse_uint64()


    def _parse_bytes(self, size: int) -> tuple[int, bytes]:
        bytes = self._file.read(size)
        return len(bytes), bytes


    def _parse_tuple(self, first_type: str, second_type: str) -> tuple[int, tuple]:
        first_value_length, first_value = getattr(self, f'_parse_{first_type}')()
        second_value_length, second_value = getattr(self, f'_parse_{second_type}')()
        return first_value_length + second_value_length, (first_value, second_value)


    def _parse_map(self, key_type: str, value_type: str) -> tuple[int, dict]:
        map_length_bytes, map_length = self._parse_uint32()
        original_length = map_length

        map_key_value = {}
        while map_length > 0:
            key_length, key = getattr(self, f'_parse_{key_type}')()
            value_length, value = getattr(self, f'_parse_{value_type}')()
            map_key_value[key] = value
            map_length -= key_length + value_length

        if map_length != 0:
            bytes_read = original_length - map_length
            raise MalformedMCAP(f'Read {bytes_read} bytes. Expected {original_length}.')

        return map_length_bytes + map_length, map_key_value


    def _parse_array(self, array_type_parser: Callable[[], tuple[int, Any]]) -> tuple[int, list]:
        array_length_bytes, array_length = self._parse_uint32()
        original_length = array_length
        print(f'Array length: {array_length}')

        array = []
        while array_length > 0:
            value_length, value = array_type_parser()
            array.append(value)
            array_length -= value_length

        if array_length != 0:
            bytes_read = original_length - array_length
            raise MalformedMCAP(f'Read {bytes_read} bytes. Expected {original_length}.')

        return array_length_bytes + array_length, array

    # MCAP Record Handlers

    def _parse_magic_bytes(self) -> str:
        """Parse the magic bytes at the begining/end of the MCAP file."""
        magic = self._file.read(8)
        if magic != b'\x89MCAP\x30\r\n':  # TODO: Support multiple versions
            raise MalformedMCAP(f'Invalid magic bytes: {str(magic)}')
        return chr(magic[5])  # Return the version


    def _parse_header(self) -> HeaderRecord:
        """Parse the header record of an MCAP file."""
        if (record_type := self._file.read(1)) != b'\x01':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()
        _, profile = self._parse_string()
        _, library = self._parse_string()

        return HeaderRecord(profile, library)


    def _parse_footer(self) -> FooterRecord:
        """Parse the footer record of an MCAP file."""
        if (record_type := self._file.read(1)) != b'\x02':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # Footer record length is fixed to 20 bytes
        _, record_length = self._parse_uint64()
        if record_length != 20:
            raise MalformedMCAP(f'Unexpected footer record length ({record_length} bytes).')

        _, summary_start = self._parse_uint64()
        _, summary_offset_start = self._parse_uint64()
        _, summary_crc = self._parse_uint32()

        return FooterRecord(summary_start, summary_offset_start, summary_crc)


    def _parse_schema(self) -> SchemaRecord | None:
        if (record_type := self._file.read(1)) != b'\x03':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _, record_length = self._parse_uint64()

        _, id = self._parse_uint16()
        if id == 0:  # Invalid and should be ignored
            return None

        _, name = self._parse_string()
        _, encoding = self._parse_string()
        _, data_length = self._parse_uint32()
        _, data = self._parse_bytes(data_length)

        return SchemaRecord(id, name, encoding, data)


    def _parse_channel(self) -> ChannelRecord:
        if (record_type := self._file.read(1)) != b'\x04':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _, record_length = self._parse_uint64()

        _, id = self._parse_uint16()
        _, channel_id = self._parse_uint16()
        _, topic = self._parse_string()
        _, message_encoding = self._parse_string()
        _, metadata = self._parse_map("string", "string")

        return ChannelRecord(id, channel_id, topic, message_encoding, metadata)


    def _parse_message(self) -> MessageRecord:
        if (record_type := self._file.read(1)) != b'\x05':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _, record_length = self._parse_uint64()

        _, channel_id = self._parse_uint16()
        _, sequence = self._parse_uint32()
        _, log_time = self._parse_timestamp()
        _, publish_time = self._parse_timestamp()
        # Other fields: 2 + 4 + 8 + 8 = 22 bytes
        _, data = self._parse_bytes(record_length - 22)

        return MessageRecord(channel_id, sequence, log_time, publish_time, data)


    def _parse_chunk(self) -> ChunkRecord:
        if (record_type := self._file.read(1)) != b'\x06':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, message_start_time = self._parse_timestamp()
        _, message_end_time = self._parse_timestamp()
        _, uncompressed_size = self._parse_uint64()
        _, uncompressed_crc = self._parse_uint32()
        _, compression = self._parse_string()
        # TODO: Parse chunks records based on compression algorithm
        _, records_length = self._parse_uint64()
        _, records = self._parse_bytes(records_length)

        return ChunkRecord(
            message_start_time,
            message_end_time,
            uncompressed_size,
            uncompressed_crc,
            compression,
            records
        )


    def _parse_message_index(self) -> MessageIndexRecord:
        if (record_type := self._file.read(1)) != b'\x07':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _, message_index_length = self._parse_uint64()
        print(f'Message index length: {message_index_length}')

        _, channel_id = self._parse_uint16()
        _, records = self._parse_array(lambda: self._parse_tuple("timestamp", "uint64"))

        return MessageIndexRecord(channel_id, records)


    def _parse_chunk_index(self) -> ChunkIndexRecord:
        if (record_type := self._file.read(1)) != b'\x08':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, message_start_time = self._parse_timestamp()
        _, message_end_time = self._parse_timestamp()
        _, chunk_start_offset = self._parse_uint64()
        _, chunk_length = self._parse_uint64()
        _, message_index_offsets = self._parse_map("uint16", "uint64")
        _, message_index_length = self._parse_uint64()
        _, compression = self._parse_string()
        _, compressed_size = self._parse_uint64()
        _, uncompressed_size = self._parse_uint64()

        return ChunkIndexRecord(
            message_start_time,
            message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression,
            compressed_size,
            uncompressed_size
        )


    def _parse_attachment(self) -> AttachmentRecord:
        if (record_type := self._file.read(1)) != b'\x09':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, log_time = self._parse_timestamp()
        _, create_time = self._parse_timestamp()
        _, name = self._parse_string()
        _, media_type = self._parse_string()
        _, data_bytes_length = self._parse_uint64()
        _, data_bytes = self._parse_bytes(data_bytes_length)
        _, crc = self._parse_uint32()

        return AttachmentRecord(log_time, create_time, name, media_type, data_bytes, crc)


    def _parse_metadata(self) -> MetadataRecord:
        if (record_type := self._file.read(1)) != b'\x0C':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, name = self._parse_string()
        print(f'Parsing metadata for {name}...')
        _, metadata = self._parse_map("string", "string")

        return MetadataRecord(name, metadata)


    def _parse_data_end(self) -> DataEndRecord:
        if (record_type := self._file.read(1)) != b'\x0f':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        data_section_crc = self._parse_uint32()
        return DataEndRecord(data_section_crc)


    def _parse_attachment_index(self) -> AttachmentIndexRecord:
        if (record_type := self._file.read(1)) != b'\x0A':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, offset = self._parse_uint64()
        _, length = self._parse_uint64()
        _, log_time = self._parse_timestamp()
        _, create_time = self._parse_timestamp()
        _, data_size = self._parse_uint64()
        _, name = self._parse_string()
        _, media_type = self._parse_string()

        return AttachmentIndexRecord(
            offset,
            length,
            log_time,
            create_time,
            data_size,
            name,
            media_type
        )

    def _parse_metadata_index(self) -> MetadataIndexRecord:
        if (record_type := self._file.read(1)) != b'\x0D':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, offset = self._parse_uint64()
        _, length = self._parse_uint64()
        _, name = self._parse_string()

        return MetadataIndexRecord(offset, length, name)


    def _parse_statistics(self) -> StatisticsRecord:
        if (record_type := self._file.read(1)) != b'\x0B':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, message_count = self._parse_uint64()
        _, schema_count = self._parse_uint16()
        _, channel_count = self._parse_uint32()
        _, attachment_count = self._parse_uint32()
        _, metadata_count = self._parse_uint32()
        _, chunk_count = self._parse_uint32()
        _, message_start_time = self._parse_timestamp()
        _, message_end_time = self._parse_timestamp()
        _, channel_message_counts = self._parse_map("uint16", "uint64")

        return StatisticsRecord(
            message_count,
            schema_count,
            channel_count,
            attachment_count,
            metadata_count,
            chunk_count,
            message_start_time,
            message_end_time,
            channel_message_counts
        )


    def _parse_summary_offset(self) -> SummaryOffsetRecord:
        if (record_type := self._file.read(1)) != b'\x0E':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = self._parse_uint64()

        _, group_opcode = self._parse_uint8()
        _, group_start = self._parse_uint64()
        _, group_length = self._parse_uint64()

        return SummaryOffsetRecord(group_opcode, group_start, group_length)

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
