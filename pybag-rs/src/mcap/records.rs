//! MCAP record types.

use std::collections::HashMap;

/// Record type opcodes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Header = 0x01,
    Footer = 0x02,
    Schema = 0x03,
    Channel = 0x04,
    Message = 0x05,
    Chunk = 0x06,
    MessageIndex = 0x07,
    ChunkIndex = 0x08,
    Attachment = 0x09,
    AttachmentIndex = 0x0A,
    Statistics = 0x0B,
    Metadata = 0x0C,
    MetadataIndex = 0x0D,
    SummaryOffset = 0x0E,
    DataEnd = 0x0F,
}

impl TryFrom<u8> for RecordType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(RecordType::Header),
            0x02 => Ok(RecordType::Footer),
            0x03 => Ok(RecordType::Schema),
            0x04 => Ok(RecordType::Channel),
            0x05 => Ok(RecordType::Message),
            0x06 => Ok(RecordType::Chunk),
            0x07 => Ok(RecordType::MessageIndex),
            0x08 => Ok(RecordType::ChunkIndex),
            0x09 => Ok(RecordType::Attachment),
            0x0A => Ok(RecordType::AttachmentIndex),
            0x0B => Ok(RecordType::Statistics),
            0x0C => Ok(RecordType::Metadata),
            0x0D => Ok(RecordType::MetadataIndex),
            0x0E => Ok(RecordType::SummaryOffset),
            0x0F => Ok(RecordType::DataEnd),
            _ => Err(value),
        }
    }
}

/// MCAP file header.
#[derive(Debug, Clone)]
pub struct HeaderRecord {
    pub profile: String,
    pub library: String,
}

/// MCAP file footer.
#[derive(Debug, Clone)]
pub struct FooterRecord {
    pub summary_start: u64,
    pub summary_offset_start: u64,
    pub summary_crc: u32,
}

/// Schema record.
#[derive(Debug, Clone)]
pub struct SchemaRecord {
    pub id: u16,
    pub name: String,
    pub encoding: String,
    pub data: Vec<u8>,
}

/// Channel record.
#[derive(Debug, Clone)]
pub struct ChannelRecord {
    pub id: u16,
    pub schema_id: u16,
    pub topic: String,
    pub message_encoding: String,
    pub metadata: HashMap<String, String>,
}

/// Message record.
#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub channel_id: u16,
    pub sequence: u32,
    pub log_time: u64,
    pub publish_time: u64,
    pub data: Vec<u8>,
}

/// Chunk record.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub message_start_time: u64,
    pub message_end_time: u64,
    pub uncompressed_size: u64,
    pub uncompressed_crc: u32,
    pub compression: String,
    pub records: Vec<u8>,
}

/// Message index entry.
#[derive(Debug, Clone)]
pub struct MessageIndexEntry {
    pub log_time: u64,
    pub offset: u64,
}

/// Message index record.
#[derive(Debug, Clone)]
pub struct MessageIndexRecord {
    pub channel_id: u16,
    pub records: Vec<MessageIndexEntry>,
}

/// Chunk index record.
#[derive(Debug, Clone)]
pub struct ChunkIndexRecord {
    pub message_start_time: u64,
    pub message_end_time: u64,
    pub chunk_start_offset: u64,
    pub chunk_length: u64,
    pub message_index_offsets: HashMap<u16, u64>,
    pub message_index_length: u64,
    pub compression: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

/// Attachment record.
#[derive(Debug, Clone)]
pub struct AttachmentRecord {
    pub log_time: u64,
    pub create_time: u64,
    pub name: String,
    pub media_type: String,
    pub data: Vec<u8>,
    pub crc: u32,
}

/// Attachment index record.
#[derive(Debug, Clone)]
pub struct AttachmentIndexRecord {
    pub offset: u64,
    pub length: u64,
    pub log_time: u64,
    pub create_time: u64,
    pub data_size: u64,
    pub name: String,
    pub media_type: String,
}

/// Metadata record.
#[derive(Debug, Clone)]
pub struct MetadataRecord {
    pub name: String,
    pub metadata: HashMap<String, String>,
}

/// Metadata index record.
#[derive(Debug, Clone)]
pub struct MetadataIndexRecord {
    pub offset: u64,
    pub length: u64,
    pub name: String,
}

/// Statistics record.
#[derive(Debug, Clone)]
pub struct StatisticsRecord {
    pub message_count: u64,
    pub schema_count: u16,
    pub channel_count: u32,
    pub attachment_count: u32,
    pub metadata_count: u32,
    pub chunk_count: u32,
    pub message_start_time: u64,
    pub message_end_time: u64,
    pub channel_message_counts: HashMap<u16, u64>,
}

/// Summary offset record.
#[derive(Debug, Clone)]
pub struct SummaryOffsetRecord {
    pub group_opcode: u8,
    pub group_start: u64,
    pub group_length: u64,
}

/// Data end record.
#[derive(Debug, Clone)]
pub struct DataEndRecord {
    pub data_section_crc: u32,
}

/// Enum to hold any MCAP record.
#[derive(Debug, Clone)]
pub enum Record {
    Header(HeaderRecord),
    Footer(FooterRecord),
    Schema(SchemaRecord),
    Channel(ChannelRecord),
    Message(MessageRecord),
    Chunk(ChunkRecord),
    MessageIndex(MessageIndexRecord),
    ChunkIndex(ChunkIndexRecord),
    Attachment(AttachmentRecord),
    AttachmentIndex(AttachmentIndexRecord),
    Statistics(StatisticsRecord),
    Metadata(MetadataRecord),
    MetadataIndex(MetadataIndexRecord),
    SummaryOffset(SummaryOffsetRecord),
    DataEnd(DataEndRecord),
}
