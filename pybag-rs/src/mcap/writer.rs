//! High-level MCAP file writer.

use crate::error::Result;
use crate::io::{BytesWriter, FileWriter, Writer};
use crate::mcap::chunk::compress_chunk;
use crate::mcap::crc::compute_crc;
use crate::mcap::parser::MAGIC_BYTES;
use crate::mcap::records::*;
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

/// MCAP file writer.
pub struct McapWriter<W: Writer> {
    writer: W,
    profile: String,
    library: String,
    chunk_size: Option<usize>,
    chunk_compression: Option<String>,
    // Tracking
    schemas: HashMap<u16, SchemaRecord>,
    channels: HashMap<u16, ChannelRecord>,
    chunk_indices: Vec<ChunkIndexRecord>,
    attachment_indices: Vec<AttachmentIndexRecord>,
    metadata_indices: Vec<MetadataIndexRecord>,
    // Chunking state
    chunk_buffer: Vec<u8>,
    chunk_message_start_time: Option<u64>,
    chunk_message_end_time: Option<u64>,
    chunk_message_counts: HashMap<u16, u64>,
    // Statistics
    message_count: u64,
    message_start_time: Option<u64>,
    message_end_time: Option<u64>,
    channel_message_counts: HashMap<u16, u64>,
}

impl McapWriter<FileWriter> {
    /// Create a writer to a file.
    pub fn create<P: AsRef<Path>>(
        path: P,
        profile: &str,
        chunk_size: Option<usize>,
        chunk_compression: Option<&str>,
    ) -> Result<Self> {
        let writer = FileWriter::create(path)?;
        Self::new(
            writer,
            profile,
            chunk_size,
            chunk_compression.map(|s| s.to_string()),
        )
    }
}

impl McapWriter<BytesWriter> {
    /// Create a writer to memory.
    pub fn to_bytes(
        profile: &str,
        chunk_size: Option<usize>,
        chunk_compression: Option<&str>,
    ) -> Result<Self> {
        let writer = BytesWriter::new();
        Self::new(
            writer,
            profile,
            chunk_size,
            chunk_compression.map(|s| s.to_string()),
        )
    }
}

impl<W: Writer> McapWriter<W> {
    /// Create a new MCAP writer.
    pub fn new(
        mut writer: W,
        profile: &str,
        chunk_size: Option<usize>,
        chunk_compression: Option<String>,
    ) -> Result<Self> {
        // Write magic bytes
        writer.write(MAGIC_BYTES)?;

        // Write header
        let header = HeaderRecord {
            profile: profile.to_string(),
            library: "pybag_rs".to_string(),
        };
        Self::write_header_record(&mut writer, &header)?;

        Ok(Self {
            writer,
            profile: profile.to_string(),
            library: "pybag_rs".to_string(),
            chunk_size,
            chunk_compression,
            schemas: HashMap::new(),
            channels: HashMap::new(),
            chunk_indices: Vec::new(),
            attachment_indices: Vec::new(),
            metadata_indices: Vec::new(),
            chunk_buffer: Vec::new(),
            chunk_message_start_time: None,
            chunk_message_end_time: None,
            chunk_message_counts: HashMap::new(),
            message_count: 0,
            message_start_time: None,
            message_end_time: None,
            channel_message_counts: HashMap::new(),
        })
    }

    /// Write a schema record.
    pub fn write_schema(&mut self, schema: &SchemaRecord) -> Result<()> {
        self.schemas.insert(schema.id, schema.clone());

        if self.chunk_size.is_some() {
            // Buffer to chunk
            let mut buf = Vec::new();
            Self::encode_schema_record(&mut buf, schema)?;
            self.chunk_buffer.extend(buf);
        } else {
            // Write directly
            Self::write_schema_record(&mut self.writer, schema)?;
        }

        Ok(())
    }

    /// Write a channel record.
    pub fn write_channel(&mut self, channel: &ChannelRecord) -> Result<()> {
        self.channels.insert(channel.id, channel.clone());

        if self.chunk_size.is_some() {
            // Buffer to chunk
            let mut buf = Vec::new();
            Self::encode_channel_record(&mut buf, channel)?;
            self.chunk_buffer.extend(buf);
        } else {
            // Write directly
            Self::write_channel_record(&mut self.writer, channel)?;
        }

        Ok(())
    }

    /// Write a message record.
    pub fn write_message(&mut self, message: &MessageRecord) -> Result<()> {
        // Update statistics
        self.message_count += 1;
        *self
            .channel_message_counts
            .entry(message.channel_id)
            .or_insert(0) += 1;

        if self.message_start_time.is_none() || message.log_time < self.message_start_time.unwrap()
        {
            self.message_start_time = Some(message.log_time);
        }
        if self.message_end_time.is_none() || message.log_time > self.message_end_time.unwrap() {
            self.message_end_time = Some(message.log_time);
        }

        if self.chunk_size.is_some() {
            // Update chunk time range
            if self.chunk_message_start_time.is_none()
                || message.log_time < self.chunk_message_start_time.unwrap()
            {
                self.chunk_message_start_time = Some(message.log_time);
            }
            if self.chunk_message_end_time.is_none()
                || message.log_time > self.chunk_message_end_time.unwrap()
            {
                self.chunk_message_end_time = Some(message.log_time);
            }

            *self
                .chunk_message_counts
                .entry(message.channel_id)
                .or_insert(0) += 1;

            // Buffer to chunk
            let mut buf = Vec::new();
            Self::encode_message_record(&mut buf, message)?;
            self.chunk_buffer.extend(buf);

            // Check if we should flush the chunk
            if self.chunk_buffer.len() >= self.chunk_size.unwrap() {
                self.flush_chunk()?;
            }
        } else {
            // Write directly
            Self::write_message_record(&mut self.writer, message)?;
        }

        Ok(())
    }

    /// Write an attachment record.
    pub fn write_attachment(&mut self, attachment: &AttachmentRecord) -> Result<()> {
        // Flush any pending chunk first
        if self.chunk_size.is_some() && !self.chunk_buffer.is_empty() {
            self.flush_chunk()?;
        }

        let offset = self.writer.position();
        Self::write_attachment_record(&mut self.writer, attachment)?;
        let length = self.writer.position() - offset;

        self.attachment_indices.push(AttachmentIndexRecord {
            offset,
            length,
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            data_size: attachment.data.len() as u64,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        });

        Ok(())
    }

    /// Write a metadata record.
    pub fn write_metadata(&mut self, metadata: &MetadataRecord) -> Result<()> {
        // Flush any pending chunk first
        if self.chunk_size.is_some() && !self.chunk_buffer.is_empty() {
            self.flush_chunk()?;
        }

        let offset = self.writer.position();
        Self::write_metadata_record(&mut self.writer, metadata)?;
        let length = self.writer.position() - offset;

        self.metadata_indices.push(MetadataIndexRecord {
            offset,
            length,
            name: metadata.name.clone(),
        });

        Ok(())
    }

    /// Close the writer and finalize the MCAP file.
    pub fn close(mut self) -> Result<()> {
        // Flush any pending chunk
        if self.chunk_size.is_some() && !self.chunk_buffer.is_empty() {
            self.flush_chunk()?;
        }

        // Write DataEnd record
        let data_end = DataEndRecord {
            data_section_crc: 0,
        };
        Self::write_data_end_record(&mut self.writer, &data_end)?;

        // Remember summary start position
        let summary_start = self.writer.position();

        // Write summary section
        // Schemas
        for schema in self.schemas.values() {
            Self::write_schema_record(&mut self.writer, schema)?;
        }

        // Channels
        for channel in self.channels.values() {
            Self::write_channel_record(&mut self.writer, channel)?;
        }

        // Chunk indices
        for chunk_index in &self.chunk_indices {
            Self::write_chunk_index_record(&mut self.writer, chunk_index)?;
        }

        // Attachment indices
        for attachment_index in &self.attachment_indices {
            Self::write_attachment_index_record(&mut self.writer, attachment_index)?;
        }

        // Metadata indices
        for metadata_index in &self.metadata_indices {
            Self::write_metadata_index_record(&mut self.writer, metadata_index)?;
        }

        // Statistics
        let statistics = StatisticsRecord {
            message_count: self.message_count,
            schema_count: self.schemas.len() as u16,
            channel_count: self.channels.len() as u32,
            attachment_count: self.attachment_indices.len() as u32,
            metadata_count: self.metadata_indices.len() as u32,
            chunk_count: self.chunk_indices.len() as u32,
            message_start_time: self.message_start_time.unwrap_or(0),
            message_end_time: self.message_end_time.unwrap_or(0),
            channel_message_counts: self.channel_message_counts.clone(),
        };
        Self::write_statistics_record(&mut self.writer, &statistics)?;

        // Footer
        let footer = FooterRecord {
            summary_start,
            summary_offset_start: 0,
            summary_crc: 0,
        };
        Self::write_footer_record(&mut self.writer, &footer)?;

        // Magic bytes at end
        self.writer.write(MAGIC_BYTES)?;

        self.writer.flush()?;

        Ok(())
    }

    // Private methods

    fn flush_chunk(&mut self) -> Result<()> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        let uncompressed_data = std::mem::take(&mut self.chunk_buffer);
        let uncompressed_size = uncompressed_data.len() as u64;
        let uncompressed_crc = compute_crc(&uncompressed_data);

        let compression = self.chunk_compression.clone().unwrap_or_default();
        let compressed_data = compress_chunk(&compression, &uncompressed_data)?;
        let compressed_size = compressed_data.len() as u64;

        let chunk_start_offset = self.writer.position();

        let chunk = ChunkRecord {
            message_start_time: self.chunk_message_start_time.unwrap_or(0),
            message_end_time: self.chunk_message_end_time.unwrap_or(0),
            uncompressed_size,
            uncompressed_crc,
            compression: compression.clone(),
            records: compressed_data,
        };
        Self::write_chunk_record(&mut self.writer, &chunk)?;

        let chunk_length = self.writer.position() - chunk_start_offset;

        // Create chunk index
        let chunk_index = ChunkIndexRecord {
            message_start_time: self.chunk_message_start_time.unwrap_or(0),
            message_end_time: self.chunk_message_end_time.unwrap_or(0),
            chunk_start_offset,
            chunk_length,
            message_index_offsets: HashMap::new(), // Simplified - no per-message indices
            message_index_length: 0,
            compression,
            compressed_size,
            uncompressed_size,
        };
        self.chunk_indices.push(chunk_index);

        // Reset chunk state
        self.chunk_message_start_time = None;
        self.chunk_message_end_time = None;
        self.chunk_message_counts.clear();

        Ok(())
    }

    // Record encoding helpers

    fn write_header_record<W2: Writer>(writer: &mut W2, header: &HeaderRecord) -> Result<()> {
        let mut buf = Vec::new();
        Self::write_string(&mut buf, &header.profile)?;
        Self::write_string(&mut buf, &header.library)?;

        writer.write(&[RecordType::Header as u8])?;
        Self::write_u64_to_writer(writer, buf.len() as u64)?;
        writer.write(&buf)?;

        Ok(())
    }

    fn write_footer_record<W2: Writer>(writer: &mut W2, footer: &FooterRecord) -> Result<()> {
        writer.write(&[RecordType::Footer as u8])?;
        Self::write_u64_to_writer(writer, 20)?;
        Self::write_u64_to_writer(writer, footer.summary_start)?;
        Self::write_u64_to_writer(writer, footer.summary_offset_start)?;
        Self::write_u32_to_writer(writer, footer.summary_crc)?;

        Ok(())
    }

    fn encode_schema_record(buf: &mut Vec<u8>, schema: &SchemaRecord) -> Result<()> {
        let mut content = Vec::new();
        content.write_u16::<LittleEndian>(schema.id)?;
        Self::write_string(&mut content, &schema.name)?;
        Self::write_string(&mut content, &schema.encoding)?;
        content.write_u32::<LittleEndian>(schema.data.len() as u32)?;
        content.extend(&schema.data);

        buf.push(RecordType::Schema as u8);
        buf.write_u64::<LittleEndian>(content.len() as u64)?;
        buf.extend(content);

        Ok(())
    }

    fn write_schema_record<W2: Writer>(writer: &mut W2, schema: &SchemaRecord) -> Result<()> {
        let mut buf = Vec::new();
        Self::encode_schema_record(&mut buf, schema)?;
        writer.write(&buf)?;
        Ok(())
    }

    fn encode_channel_record(buf: &mut Vec<u8>, channel: &ChannelRecord) -> Result<()> {
        let mut content = Vec::new();
        content.write_u16::<LittleEndian>(channel.id)?;
        content.write_u16::<LittleEndian>(channel.schema_id)?;
        Self::write_string(&mut content, &channel.topic)?;
        Self::write_string(&mut content, &channel.message_encoding)?;
        Self::write_map_string_string(&mut content, &channel.metadata)?;

        buf.push(RecordType::Channel as u8);
        buf.write_u64::<LittleEndian>(content.len() as u64)?;
        buf.extend(content);

        Ok(())
    }

    fn write_channel_record<W2: Writer>(writer: &mut W2, channel: &ChannelRecord) -> Result<()> {
        let mut buf = Vec::new();
        Self::encode_channel_record(&mut buf, channel)?;
        writer.write(&buf)?;
        Ok(())
    }

    fn encode_message_record(buf: &mut Vec<u8>, message: &MessageRecord) -> Result<()> {
        let content_len = 2 + 4 + 8 + 8 + message.data.len();

        buf.push(RecordType::Message as u8);
        buf.write_u64::<LittleEndian>(content_len as u64)?;
        buf.write_u16::<LittleEndian>(message.channel_id)?;
        buf.write_u32::<LittleEndian>(message.sequence)?;
        buf.write_u64::<LittleEndian>(message.log_time)?;
        buf.write_u64::<LittleEndian>(message.publish_time)?;
        buf.extend(&message.data);

        Ok(())
    }

    fn write_message_record<W2: Writer>(writer: &mut W2, message: &MessageRecord) -> Result<()> {
        let mut buf = Vec::new();
        Self::encode_message_record(&mut buf, message)?;
        writer.write(&buf)?;
        Ok(())
    }

    fn write_chunk_record<W2: Writer>(writer: &mut W2, chunk: &ChunkRecord) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(chunk.message_start_time)?;
        content.write_u64::<LittleEndian>(chunk.message_end_time)?;
        content.write_u64::<LittleEndian>(chunk.uncompressed_size)?;
        content.write_u32::<LittleEndian>(chunk.uncompressed_crc)?;
        Self::write_string(&mut content, &chunk.compression)?;
        content.write_u64::<LittleEndian>(chunk.records.len() as u64)?;
        content.extend(&chunk.records);

        writer.write(&[RecordType::Chunk as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_chunk_index_record<W2: Writer>(
        writer: &mut W2,
        index: &ChunkIndexRecord,
    ) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(index.message_start_time)?;
        content.write_u64::<LittleEndian>(index.message_end_time)?;
        content.write_u64::<LittleEndian>(index.chunk_start_offset)?;
        content.write_u64::<LittleEndian>(index.chunk_length)?;
        Self::write_map_u16_u64(&mut content, &index.message_index_offsets)?;
        content.write_u64::<LittleEndian>(index.message_index_length)?;
        Self::write_string(&mut content, &index.compression)?;
        content.write_u64::<LittleEndian>(index.compressed_size)?;
        content.write_u64::<LittleEndian>(index.uncompressed_size)?;

        writer.write(&[RecordType::ChunkIndex as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_attachment_record<W2: Writer>(
        writer: &mut W2,
        attachment: &AttachmentRecord,
    ) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(attachment.log_time)?;
        content.write_u64::<LittleEndian>(attachment.create_time)?;
        Self::write_string(&mut content, &attachment.name)?;
        Self::write_string(&mut content, &attachment.media_type)?;
        content.write_u64::<LittleEndian>(attachment.data.len() as u64)?;
        content.extend(&attachment.data);
        content.write_u32::<LittleEndian>(attachment.crc)?;

        writer.write(&[RecordType::Attachment as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_attachment_index_record<W2: Writer>(
        writer: &mut W2,
        index: &AttachmentIndexRecord,
    ) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(index.offset)?;
        content.write_u64::<LittleEndian>(index.length)?;
        content.write_u64::<LittleEndian>(index.log_time)?;
        content.write_u64::<LittleEndian>(index.create_time)?;
        content.write_u64::<LittleEndian>(index.data_size)?;
        Self::write_string(&mut content, &index.name)?;
        Self::write_string(&mut content, &index.media_type)?;

        writer.write(&[RecordType::AttachmentIndex as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_metadata_record<W2: Writer>(writer: &mut W2, metadata: &MetadataRecord) -> Result<()> {
        let mut content = Vec::new();
        Self::write_string(&mut content, &metadata.name)?;
        Self::write_map_string_string(&mut content, &metadata.metadata)?;

        writer.write(&[RecordType::Metadata as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_metadata_index_record<W2: Writer>(
        writer: &mut W2,
        index: &MetadataIndexRecord,
    ) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(index.offset)?;
        content.write_u64::<LittleEndian>(index.length)?;
        Self::write_string(&mut content, &index.name)?;

        writer.write(&[RecordType::MetadataIndex as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_statistics_record<W2: Writer>(
        writer: &mut W2,
        stats: &StatisticsRecord,
    ) -> Result<()> {
        let mut content = Vec::new();
        content.write_u64::<LittleEndian>(stats.message_count)?;
        content.write_u16::<LittleEndian>(stats.schema_count)?;
        content.write_u32::<LittleEndian>(stats.channel_count)?;
        content.write_u32::<LittleEndian>(stats.attachment_count)?;
        content.write_u32::<LittleEndian>(stats.metadata_count)?;
        content.write_u32::<LittleEndian>(stats.chunk_count)?;
        content.write_u64::<LittleEndian>(stats.message_start_time)?;
        content.write_u64::<LittleEndian>(stats.message_end_time)?;
        Self::write_map_u16_u64(&mut content, &stats.channel_message_counts)?;

        writer.write(&[RecordType::Statistics as u8])?;
        Self::write_u64_to_writer(writer, content.len() as u64)?;
        writer.write(&content)?;

        Ok(())
    }

    fn write_data_end_record<W2: Writer>(writer: &mut W2, data_end: &DataEndRecord) -> Result<()> {
        writer.write(&[RecordType::DataEnd as u8])?;
        Self::write_u64_to_writer(writer, 4)?;
        Self::write_u32_to_writer(writer, data_end.data_section_crc)?;

        Ok(())
    }

    // Utility methods

    fn write_u32_to_writer<W2: Writer>(writer: &mut W2, value: u32) -> Result<()> {
        writer.write(&value.to_le_bytes())?;
        Ok(())
    }

    fn write_u64_to_writer<W2: Writer>(writer: &mut W2, value: u64) -> Result<()> {
        writer.write(&value.to_le_bytes())?;
        Ok(())
    }

    fn write_string(buf: &mut Vec<u8>, s: &str) -> Result<()> {
        buf.write_u32::<LittleEndian>(s.len() as u32)?;
        buf.extend(s.as_bytes());
        Ok(())
    }

    fn write_map_string_string(buf: &mut Vec<u8>, map: &HashMap<String, String>) -> Result<()> {
        let mut content = Vec::new();
        for (k, v) in map {
            content.write_u32::<LittleEndian>(k.len() as u32)?;
            content.extend(k.as_bytes());
            content.write_u32::<LittleEndian>(v.len() as u32)?;
            content.extend(v.as_bytes());
        }
        buf.write_u32::<LittleEndian>(content.len() as u32)?;
        buf.extend(content);
        Ok(())
    }

    fn write_map_u16_u64(buf: &mut Vec<u8>, map: &HashMap<u16, u64>) -> Result<()> {
        let content_len = map.len() * 10; // 2 + 8 bytes per entry
        buf.write_u32::<LittleEndian>(content_len as u32)?;
        for (k, v) in map {
            buf.write_u16::<LittleEndian>(*k)?;
            buf.write_u64::<LittleEndian>(*v)?;
        }
        Ok(())
    }
}
