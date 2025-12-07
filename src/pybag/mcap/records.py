from dataclasses import dataclass, field
from enum import IntEnum


@dataclass
class HeaderRecord:
    profile: str
    library: str


@dataclass
class FooterRecord:
    summary_start: int
    summary_offset_start: int
    summary_crc: int


@dataclass
class ChannelRecord:
    id: int
    schema_id: int
    topic: str
    message_encoding: str
    metadata: dict[str, str]


@dataclass
class SchemaRecord:
    id: int
    name: str
    encoding: str
    data: bytes


@dataclass
class MessageRecord:
    channel_id: int
    sequence: int
    log_time: int
    publish_time: int
    data: bytes


@dataclass
class ChunkRecord:
    message_start_time: int
    message_end_time: int
    uncompressed_size: int
    uncompressed_crc: int
    compression: str
    records: bytes


@dataclass
class MessageIndexRecord:
    channel_id: int
    records: list[tuple[int, int]]


@dataclass
class ChunkIndexRecord:
    message_start_time: int
    message_end_time: int
    chunk_start_offset: int
    chunk_length: int
    message_index_offsets: dict[int, int]
    message_index_length: int
    compression: str
    compressed_size: int
    uncompressed_size: int


@dataclass
class AttachmentRecord:
    log_time: int
    create_time: int
    name: str
    media_type: str
    data: bytes
    crc: int


@dataclass
class MetadataRecord:
    name: str
    metadata: dict[str, str]


@dataclass
class DataEndRecord:
    data_section_crc: int


@dataclass
class AttachmentIndexRecord:
    offset: int
    length: int
    log_time: int
    create_time: int
    data_size: int
    name: str
    media_type: str


@dataclass
class MetadataIndexRecord:
    offset: int
    length: int
    name: str


@dataclass
class StatisticsRecord:
    message_count: int = 0
    schema_count: int = 0
    channel_count: int = 0
    attachment_count: int = 0
    metadata_count: int = 0
    chunk_count: int = 0
    message_start_time: int = 0
    message_end_time: int = 0
    channel_message_counts: dict[int, int] = field(default_factory=dict)


@dataclass
class SummaryOffsetRecord:
    group_opcode: int
    group_start: int
    group_length: int


class RecordType(IntEnum):
    MAGIC_BYTES = 0x89
    HEADER = 0x01
    FOOTER = 0x02
    SCHEMA = 0x03
    CHANNEL = 0x04
    MESSAGE = 0x05
    CHUNK = 0x06
    MESSAGE_INDEX = 0x07
    CHUNK_INDEX = 0x08
    ATTACHMENT = 0x09
    METADATA = 0x0C
    DATA_END = 0x0F
    ATTACHMENT_INDEX = 0x0A
    METADATA_INDEX = 0x0D
    STATISTICS = 0x0B
    SUMMARY_OFFSET = 0x0E
