import struct
from typing import Iterator, Any, Callable
from pybag.mcap.raw_reader import BaseReader
from pybag.mcap.records import *  # TODO: Make better


class MalformedMCAP(Exception):
    """The MCAP format does not conform to the specification."""
    def __init__(self, error_msessage: str):
        super().__init__(error_msessage)


class McapRecordReader:
    @classmethod
    def peek_record(cls, file: BaseReader) -> int:
        """Peek at the next record in the MCAP file."""
        return file.peek(1)[:1]


    @classmethod
    def read_record(cls, file: BaseReader) -> Iterator[tuple[int, Any]]:
        """Read the next record in the MCAP file."""
        while True:
            record_type = int.from_bytes(cls.peek_record(file), 'little')
            print(f'Peeked at {record_type} record...')
            if record_type == 0:
                break  # EOF
            yield record_type, cls._parse_record(record_type, file)


    @classmethod
    def read_magic_bytes(cls, file: BaseReader) -> str:
        """Parse the magic bytes at the begining/end of the MCAP file."""
        magic = file.read(8)
        if magic != b'\x89MCAP\x30\r\n':  # TODO: Support multiple versions
            raise MalformedMCAP(f'Invalid magic bytes: {str(magic)}')
        return chr(magic[5])  # Return the version


    @classmethod
    def _parse_record(cls, record_type: int, file: BaseReader) -> Any:
        """Parse the next record in the MCAP file."""
        record_name = RecordType(record_type).name.lower()
        print(f'Parsing {record_name} record...')
        return getattr(cls, f'_parse_{record_name}')(file)

    # MCAP Serialization Handlers

    @classmethod
    def _parse_uint8(cls, file: BaseReader) -> tuple[int, int]:
        return 1, struct.unpack('<B', file.read(1))[0]


    @classmethod
    def _parse_uint16(cls, file: BaseReader) -> tuple[int, int]:
        return 2, struct.unpack('<H', file.read(2))[0]


    @classmethod
    def _parse_uint32(cls, file: BaseReader) -> tuple[int, int]:
        return 4, struct.unpack('<I', file.read(4))[0]


    @classmethod
    def _parse_uint64(cls, file: BaseReader) -> tuple[int, int]:
        return 8, struct.unpack('<Q', file.read(8))[0]


    @classmethod
    def _parse_string(cls, file: BaseReader) -> tuple[int, str]:
        string_length_bytes, string_length = cls._parse_uint32(file)
        string = file.read(string_length)
        return string_length_bytes + string_length, string.decode()


    @classmethod
    def _parse_timestamp(cls, file: BaseReader) -> tuple[int, int]:
        return cls._parse_uint64(file)


    @classmethod
    def _parse_bytes(cls, file: BaseReader, size: int) -> tuple[int, bytes]:
        bytes = file.read(size)
        return len(bytes), bytes


    @classmethod
    def _parse_tuple(cls, file: BaseReader, first_type: str, second_type: str) -> tuple[int, tuple]:
        first_value_length, first_value = getattr(cls, f'_parse_{first_type}')(file)
        second_value_length, second_value = getattr(cls, f'_parse_{second_type}')(file)
        return first_value_length + second_value_length, (first_value, second_value)


    @classmethod
    def _parse_map(cls, file: BaseReader, key_type: str, value_type: str) -> tuple[int, dict]:
        map_length_bytes, map_length = cls._parse_uint32(file)
        original_length = map_length

        map_key_value = {}
        while map_length > 0:
            key_length, key = getattr(cls, f'_parse_{key_type}')(file)
            value_length, value = getattr(cls, f'_parse_{value_type}')(file)
            map_key_value[key] = value
            map_length -= key_length + value_length

        if map_length != 0:
            bytes_read = original_length - map_length
            raise MalformedMCAP(f'Read {bytes_read} bytes. Expected {original_length}.')

        return map_length_bytes + map_length, map_key_value


    @classmethod
    def _parse_array(
        cls,
        file: BaseReader,
        array_type_parser: Callable[[BaseReader], tuple[int, Any]]
    ) -> tuple[int, list]:
        array_length_bytes, array_length = cls._parse_uint32(file)
        original_length = array_length
        print(f'Array length: {array_length}')

        array = []
        while array_length > 0:
            value_length, value = array_type_parser(file)
            array.append(value)
            array_length -= value_length

        if array_length != 0:
            bytes_read = original_length - array_length
            raise MalformedMCAP(f'Read {bytes_read} bytes. Expected {original_length}.')

        return array_length_bytes + array_length, array

    # MCAP Record Handlers

    @classmethod
    def _parse_header(cls, file: BaseReader) -> HeaderRecord:
        """Parse the header record of an MCAP file."""
        if (record_type := file.read(1)) != b'\x01':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # TODO: Improve performance by batching the reads (maybe)
        _ = cls._parse_uint64(file)
        _, profile = cls._parse_string(file)
        _, library = cls._parse_string(file)

        return HeaderRecord(profile, library)


    @classmethod
    def _parse_footer(cls, file: BaseReader) -> FooterRecord:
        """Parse the footer record of an MCAP file."""
        if (record_type := file.read(1)) != b'\x02':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        # Footer record length is fixed to 20 bytes
        _, record_length = cls._parse_uint64(file)
        if record_length != 20:
            raise MalformedMCAP(f'Unexpected footer record length ({record_length} bytes).')

        _, summary_start = cls._parse_uint64(file)
        _, summary_offset_start = cls._parse_uint64(file)
        _, summary_crc = cls._parse_uint32(file)

        return FooterRecord(summary_start, summary_offset_start, summary_crc)


    @classmethod
    def _parse_schema(cls, file: BaseReader) -> SchemaRecord | None:
        if (record_type := file.read(1)) != b'\x03':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _, record_length = cls._parse_uint64(file)

        _, id = cls._parse_uint16(file)
        if id == 0:  # Invalid and should be ignored
            return None

        _, name = cls._parse_string(file)
        _, encoding = cls._parse_string(file)
        _, data_length = cls._parse_uint32(file)
        _, data = cls._parse_bytes(file, data_length)

        return SchemaRecord(id, name, encoding, data)


    @classmethod
    def _parse_channel(cls, file: BaseReader) -> ChannelRecord:
        if (record_type := file.read(1)) != b'\x04':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _, record_length = cls._parse_uint64(file)

        _, id = cls._parse_uint16(file)
        _, channel_id = cls._parse_uint16(file)
        _, topic = cls._parse_string(file)
        _, message_encoding = cls._parse_string(file)
        _, metadata = cls._parse_map(file, "string", "string")

        return ChannelRecord(id, channel_id, topic, message_encoding, metadata)


    @classmethod
    def _parse_message(cls, file: BaseReader) -> MessageRecord:
        if (record_type := file.read(1)) != b'\x05':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _, record_length = cls._parse_uint64(file)

        _, channel_id = cls._parse_uint16(file)
        _, sequence = cls._parse_uint32(file)
        _, log_time = cls._parse_timestamp(file)
        _, publish_time = cls._parse_timestamp(file)
        # Other fields: 2 + 4 + 8 + 8 = 22 bytes
        _, data = cls._parse_bytes(file, record_length - 22)

        return MessageRecord(channel_id, sequence, log_time, publish_time, data)


    @classmethod
    def _parse_chunk(cls, file: BaseReader) -> ChunkRecord:
        if (record_type := file.read(1)) != b'\x06':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, message_start_time = cls._parse_timestamp(file)
        _, message_end_time = cls._parse_timestamp(file)
        _, uncompressed_size = cls._parse_uint64(file)
        _, uncompressed_crc = cls._parse_uint32(file)
        _, compression = cls._parse_string(file)
        # TODO: Parse chunks records based on compression algorithm
        _, records_length = cls._parse_uint64(file)
        _, records = cls._parse_bytes(file, records_length)

        return ChunkRecord(
            message_start_time,
            message_end_time,
            uncompressed_size,
            uncompressed_crc,
            compression,
            records
        )


    @classmethod
    def _parse_message_index(cls, file: BaseReader) -> MessageIndexRecord:
        if (record_type := file.read(1)) != b'\x07':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _, message_index_length = cls._parse_uint64(file)
        print(f'Message index length: {message_index_length}')

        _, channel_id = cls._parse_uint16(file)
        _, records = cls._parse_array(lambda: cls._parse_tuple(file, "timestamp", "uint64"))

        return MessageIndexRecord(channel_id, records)


    @classmethod
    def _parse_chunk_index(cls, file: BaseReader) -> ChunkIndexRecord:
        if (record_type := file.read(1)) != b'\x08':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, message_start_time = cls._parse_timestamp(file)
        _, message_end_time = cls._parse_timestamp(file)
        _, chunk_start_offset = cls._parse_uint64(file)
        _, chunk_length = cls._parse_uint64(file)
        _, message_index_offsets = cls._parse_map("uint16", "uint64")
        _, message_index_length = cls._parse_uint64(file)
        _, compression = cls._parse_string(file)
        _, compressed_size = cls._parse_uint64(file)
        _, uncompressed_size = cls._parse_uint64(file)

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


    @classmethod
    def _parse_attachment(cls, file: BaseReader) -> AttachmentRecord:
        if (record_type := file.read(1)) != b'\x09':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, log_time = cls._parse_timestamp(file)
        _, create_time = cls._parse_timestamp(file)
        _, name = cls._parse_string(file)
        _, media_type = cls._parse_string(file)
        _, data_bytes_length = cls._parse_uint64(file)
        _, data_bytes = cls._parse_bytes(file, data_bytes_length)
        _, crc = cls._parse_uint32(file)

        return AttachmentRecord(log_time, create_time, name, media_type, data_bytes, crc)


    @classmethod
    def _parse_metadata(cls, file: BaseReader) -> MetadataRecord:
        if (record_type := file.read(1)) != b'\x0C':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, name = cls._parse_string(file)
        print(f'Parsing metadata for {name}...')
        _, metadata = cls._parse_map(file, "string", "string")

        return MetadataRecord(name, metadata)


    @classmethod
    def _parse_data_end(cls, file: BaseReader) -> DataEndRecord:
        if (record_type := file.read(1)) != b'\x0f':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        data_section_crc = cls._parse_uint32(file)
        return DataEndRecord(data_section_crc)


    @classmethod
    def _parse_attachment_index(cls, file: BaseReader) -> AttachmentIndexRecord:
        if (record_type := file.read(1)) != b'\x0A':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, offset = cls._parse_uint64(file)
        _, length = cls._parse_uint64(file)
        _, log_time = cls._parse_timestamp(file)
        _, create_time = cls._parse_timestamp(file)
        _, data_size = cls._parse_uint64(file)
        _, name = cls._parse_string(file)
        _, media_type = cls._parse_string(file)

        return AttachmentIndexRecord(
            offset,
            length,
            log_time,
            create_time,
            data_size,
            name,
            media_type
        )


    @classmethod
    def _parse_metadata_index(cls, file: BaseReader) -> MetadataIndexRecord:
        if (record_type := file.read(1)) != b'\x0D':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, offset = cls._parse_uint64(file)
        _, length = cls._parse_uint64(file)
        _, name = cls._parse_string(file)

        return MetadataIndexRecord(offset, length, name)


    @classmethod
    def _parse_statistics(cls, file: BaseReader) -> StatisticsRecord:
        if (record_type := file.read(1)) != b'\x0B':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, message_count = cls._parse_uint64(file)
        _, schema_count = cls._parse_uint16(file)
        _, channel_count = cls._parse_uint32(file)
        _, attachment_count = cls._parse_uint32(file)
        _, metadata_count = cls._parse_uint32(file)
        _, chunk_count = cls._parse_uint32(file)
        _, message_start_time = cls._parse_timestamp(file)
        _, message_end_time = cls._parse_timestamp(file)
        _, channel_message_counts = cls._parse_map("uint16", "uint64")

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


    @classmethod
    def _parse_summary_offset(cls, file: BaseReader) -> SummaryOffsetRecord:
        if (record_type := file.read(1)) != b'\x0E':
            raise MalformedMCAP(f'Unexpected record type ({record_type}).')

        _ = cls._parse_uint64(file)

        _, group_opcode = cls._parse_uint8(file)
        _, group_start = cls._parse_uint64(file)
        _, group_length = cls._parse_uint64(file)

        return SummaryOffsetRecord(group_opcode, group_start, group_length)
