//! High-level MCAP file reader.

use crate::error::{PybagError, Result};
use crate::io::{BytesReader, FileReader, Reader};
use crate::mcap::chunk::decompress_chunk;
use crate::mcap::crc::compute_crc;
use crate::mcap::parser::McapRecordParser;
use crate::mcap::records::*;
use std::collections::HashMap;
use std::path::Path;

/// Summary information parsed from an MCAP file.
#[derive(Debug, Default)]
pub struct McapSummary {
    pub schemas: HashMap<u16, SchemaRecord>,
    pub channels: HashMap<u16, ChannelRecord>,
    pub statistics: Option<StatisticsRecord>,
    pub chunk_indices: Vec<ChunkIndexRecord>,
    pub attachment_indices: Vec<AttachmentIndexRecord>,
    pub metadata_indices: Vec<MetadataIndexRecord>,
}

/// High-level MCAP file reader.
pub struct McapReader<R: Reader> {
    reader: R,
    header: HeaderRecord,
    footer: FooterRecord,
    summary: McapSummary,
    enable_crc_check: bool,
    // Cache for topic -> channel_id mapping
    topic_to_channel: HashMap<String, u16>,
}

impl McapReader<FileReader> {
    /// Open an MCAP file for reading.
    pub fn open<P: AsRef<Path>>(path: P, enable_crc_check: bool) -> Result<Self> {
        let reader = FileReader::open(path)?;
        Self::new(reader, enable_crc_check)
    }
}

impl McapReader<BytesReader> {
    /// Create a reader from bytes.
    pub fn from_bytes(data: Vec<u8>, enable_crc_check: bool) -> Result<Self> {
        let reader = BytesReader::new(data);
        Self::new(reader, enable_crc_check)
    }
}

impl<R: Reader> McapReader<R> {
    /// Create a new MCAP reader.
    pub fn new(mut reader: R, enable_crc_check: bool) -> Result<Self> {
        // Parse magic bytes at the beginning
        McapRecordParser::parse_magic_bytes(&mut reader)?;

        // Parse header
        let header = McapRecordParser::parse_header(&mut reader)?;

        // Parse footer (at the end of the file)
        // Footer is 29 bytes: 1 byte opcode + 8 bytes length + 20 bytes content
        // Magic bytes at end: 8 bytes
        reader.seek_from_end(-37)?;
        let footer = McapRecordParser::parse_footer(&mut reader)?;

        // Parse magic bytes at the end
        McapRecordParser::parse_magic_bytes(&mut reader)?;

        // Parse summary section if it exists
        let summary = if footer.summary_start > 0 {
            Self::parse_summary(&mut reader, &footer, enable_crc_check)?
        } else {
            // Fall back to scanning the data section
            Self::scan_data_section(&mut reader)?
        };

        // Build topic -> channel_id cache
        let topic_to_channel: HashMap<String, u16> = summary
            .channels
            .iter()
            .map(|(id, ch)| (ch.topic.clone(), *id))
            .collect();

        Ok(Self {
            reader,
            header,
            footer,
            summary,
            enable_crc_check,
            topic_to_channel,
        })
    }

    /// Get the MCAP profile.
    pub fn profile(&self) -> &str {
        &self.header.profile
    }

    /// Get the header record.
    pub fn header(&self) -> &HeaderRecord {
        &self.header
    }

    /// Get the footer record.
    pub fn footer(&self) -> &FooterRecord {
        &self.footer
    }

    /// Get all schemas.
    pub fn schemas(&self) -> &HashMap<u16, SchemaRecord> {
        &self.summary.schemas
    }

    /// Get a schema by ID.
    pub fn schema(&self, id: u16) -> Option<&SchemaRecord> {
        self.summary.schemas.get(&id)
    }

    /// Get all channels.
    pub fn channels(&self) -> &HashMap<u16, ChannelRecord> {
        &self.summary.channels
    }

    /// Get a channel by ID.
    pub fn channel(&self, id: u16) -> Option<&ChannelRecord> {
        self.summary.channels.get(&id)
    }

    /// Get channel ID by topic name.
    pub fn channel_id_by_topic(&self, topic: &str) -> Option<u16> {
        self.topic_to_channel.get(topic).copied()
    }

    /// Get the schema for a channel.
    pub fn channel_schema(&self, channel_id: u16) -> Option<&SchemaRecord> {
        let channel = self.summary.channels.get(&channel_id)?;
        self.summary.schemas.get(&channel.schema_id)
    }

    /// Get all topic names.
    pub fn topics(&self) -> Vec<&str> {
        self.summary
            .channels
            .values()
            .map(|ch| ch.topic.as_str())
            .collect()
    }

    /// Get statistics.
    pub fn statistics(&self) -> Option<&StatisticsRecord> {
        self.summary.statistics.as_ref()
    }

    /// Get message count for a topic.
    pub fn message_count(&self, topic: &str) -> Option<u64> {
        let channel_id = self.topic_to_channel.get(topic)?;
        self.summary
            .statistics
            .as_ref()
            .and_then(|s| s.channel_message_counts.get(channel_id).copied())
    }

    /// Get start time.
    pub fn start_time(&self) -> Option<u64> {
        self.summary.statistics.as_ref().map(|s| s.message_start_time)
    }

    /// Get end time.
    pub fn end_time(&self) -> Option<u64> {
        self.summary.statistics.as_ref().map(|s| s.message_end_time)
    }

    /// Iterate over all messages, optionally filtered by channel IDs and time range.
    pub fn messages(
        &mut self,
        channel_ids: Option<&[u16]>,
        start_time: Option<u64>,
        end_time: Option<u64>,
        in_log_time_order: bool,
        in_reverse: bool,
    ) -> Result<Vec<MessageRecord>> {
        let mut messages = Vec::new();

        // If we have chunk indices, use them for efficient access
        if !self.summary.chunk_indices.is_empty() {
            // Filter chunk indices by time range
            let mut chunks: Vec<_> = self
                .summary
                .chunk_indices
                .iter()
                .filter(|ci| {
                    let start_ok = start_time.map_or(true, |t| ci.message_end_time >= t);
                    let end_ok = end_time.map_or(true, |t| ci.message_start_time <= t);
                    start_ok && end_ok
                })
                .collect();

            // Sort chunks by time
            if in_reverse {
                chunks.sort_by(|a, b| b.message_start_time.cmp(&a.message_start_time));
            } else {
                chunks.sort_by(|a, b| a.message_start_time.cmp(&b.message_start_time));
            }

            for chunk_index in chunks {
                // Check if this chunk contains any of our channels
                let has_relevant_channel = channel_ids.map_or(true, |ids| {
                    ids.iter()
                        .any(|id| chunk_index.message_index_offsets.contains_key(id))
                });

                if !has_relevant_channel {
                    continue;
                }

                // Read and decompress the chunk
                self.reader.seek(chunk_index.chunk_start_offset)?;
                let chunk = McapRecordParser::parse_chunk(&mut self.reader)?;

                let decompressed = decompress_chunk(
                    &chunk.compression,
                    &chunk.records,
                    chunk.uncompressed_size as usize,
                )?;

                // Verify CRC if enabled
                if self.enable_crc_check && chunk.uncompressed_crc != 0 {
                    let computed = compute_crc(&decompressed);
                    if computed != chunk.uncompressed_crc {
                        return Err(PybagError::CrcMismatch {
                            expected: chunk.uncompressed_crc,
                            computed,
                        });
                    }
                }

                // Parse messages from the chunk
                let mut chunk_reader = BytesReader::new(decompressed);
                while let Some(record_type) = McapRecordParser::peek_record(&mut chunk_reader)? {
                    match RecordType::try_from(record_type) {
                        Ok(RecordType::Message) => {
                            let msg = McapRecordParser::parse_message(&mut chunk_reader)?;

                            // Filter by channel
                            let channel_ok = channel_ids
                                .map_or(true, |ids| ids.contains(&msg.channel_id));

                            // Filter by time
                            let time_ok = start_time.map_or(true, |t| msg.log_time >= t)
                                && end_time.map_or(true, |t| msg.log_time <= t);

                            if channel_ok && time_ok {
                                messages.push(msg);
                            }
                        }
                        Ok(RecordType::Schema) | Ok(RecordType::Channel) => {
                            // Skip schema and channel records in chunks
                            McapRecordParser::skip_record(&mut chunk_reader)?;
                        }
                        _ => {
                            // Skip unknown records
                            McapRecordParser::skip_record(&mut chunk_reader)?;
                        }
                    }
                }
            }
        } else {
            // No chunk indices - scan the data section linearly
            // Skip past header (magic bytes + header record)
            self.reader.seek(8)?; // Past magic bytes
            McapRecordParser::skip_record(&mut self.reader)?; // Skip header

            while let Some(record_type) = McapRecordParser::peek_record(&mut self.reader)? {
                match RecordType::try_from(record_type) {
                    Ok(RecordType::Message) => {
                        let msg = McapRecordParser::parse_message(&mut self.reader)?;

                        let channel_ok =
                            channel_ids.map_or(true, |ids| ids.contains(&msg.channel_id));
                        let time_ok = start_time.map_or(true, |t| msg.log_time >= t)
                            && end_time.map_or(true, |t| msg.log_time <= t);

                        if channel_ok && time_ok {
                            messages.push(msg);
                        }
                    }
                    Ok(RecordType::DataEnd) | Ok(RecordType::Footer) => break,
                    _ => {
                        McapRecordParser::skip_record(&mut self.reader)?;
                    }
                }
            }
        }

        // Sort messages if requested
        if in_log_time_order {
            if in_reverse {
                messages.sort_by(|a, b| b.log_time.cmp(&a.log_time));
            } else {
                messages.sort_by(|a, b| a.log_time.cmp(&b.log_time));
            }
        }

        Ok(messages)
    }

    /// Get all attachments.
    pub fn attachments(&mut self, name_filter: Option<&str>) -> Result<Vec<AttachmentRecord>> {
        let mut attachments = Vec::new();

        for idx in &self.summary.attachment_indices {
            if name_filter.map_or(true, |n| idx.name == n) {
                self.reader.seek(idx.offset)?;
                let attachment = McapRecordParser::parse_attachment(&mut self.reader)?;
                attachments.push(attachment);
            }
        }

        Ok(attachments)
    }

    /// Get all metadata records.
    pub fn metadata(&mut self, name_filter: Option<&str>) -> Result<Vec<MetadataRecord>> {
        let mut metadata = Vec::new();

        for idx in &self.summary.metadata_indices {
            if name_filter.map_or(true, |n| idx.name == n) {
                self.reader.seek(idx.offset)?;
                let meta = McapRecordParser::parse_metadata(&mut self.reader)?;
                metadata.push(meta);
            }
        }

        Ok(metadata)
    }

    // Private methods

    fn parse_summary(reader: &mut R, footer: &FooterRecord, enable_crc_check: bool) -> Result<McapSummary> {
        let mut summary = McapSummary::default();

        // Seek to summary section
        reader.seek(footer.summary_start)?;

        // Parse summary records
        while let Some(record_type) = McapRecordParser::peek_record(reader)? {
            match RecordType::try_from(record_type) {
                Ok(RecordType::Schema) => {
                    if let Some(schema) = McapRecordParser::parse_schema(reader)? {
                        summary.schemas.insert(schema.id, schema);
                    }
                }
                Ok(RecordType::Channel) => {
                    let channel = McapRecordParser::parse_channel(reader)?;
                    summary.channels.insert(channel.id, channel);
                }
                Ok(RecordType::Statistics) => {
                    summary.statistics = Some(McapRecordParser::parse_statistics(reader)?);
                }
                Ok(RecordType::ChunkIndex) => {
                    summary
                        .chunk_indices
                        .push(McapRecordParser::parse_chunk_index(reader)?);
                }
                Ok(RecordType::AttachmentIndex) => {
                    summary
                        .attachment_indices
                        .push(McapRecordParser::parse_attachment_index(reader)?);
                }
                Ok(RecordType::MetadataIndex) => {
                    summary
                        .metadata_indices
                        .push(McapRecordParser::parse_metadata_index(reader)?);
                }
                Ok(RecordType::SummaryOffset) => {
                    // Skip summary offset records
                    McapRecordParser::skip_record(reader)?;
                }
                Ok(RecordType::Footer) => break,
                _ => {
                    // Skip unknown records
                    McapRecordParser::skip_record(reader)?;
                }
            }
        }

        Ok(summary)
    }

    fn scan_data_section(reader: &mut R) -> Result<McapSummary> {
        let mut summary = McapSummary::default();

        // Start after magic bytes
        reader.seek(8)?;

        // Skip header
        McapRecordParser::skip_record(reader)?;

        // Scan for schemas and channels
        while let Some(record_type) = McapRecordParser::peek_record(reader)? {
            match RecordType::try_from(record_type) {
                Ok(RecordType::Schema) => {
                    if let Some(schema) = McapRecordParser::parse_schema(reader)? {
                        summary.schemas.insert(schema.id, schema);
                    }
                }
                Ok(RecordType::Channel) => {
                    let channel = McapRecordParser::parse_channel(reader)?;
                    summary.channels.insert(channel.id, channel);
                }
                Ok(RecordType::DataEnd) | Ok(RecordType::Footer) => break,
                _ => {
                    McapRecordParser::skip_record(reader)?;
                }
            }
        }

        Ok(summary)
    }
}
