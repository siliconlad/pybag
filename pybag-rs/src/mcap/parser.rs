//! MCAP record parser.

use crate::error::{PybagError, Result};
use crate::io::{BytesReader, Reader};
use crate::mcap::records::*;
use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::Cursor;

/// MCAP magic bytes.
pub const MAGIC_BYTES: &[u8] = b"\x89MCAP0\r\n";

/// Parser for MCAP records.
pub struct McapRecordParser;

impl McapRecordParser {
    /// Peek at the next record type without consuming it.
    pub fn peek_record<R: Reader>(reader: &mut R) -> Result<Option<u8>> {
        let peeked = reader.peek(1)?;
        if peeked.is_empty() {
            Ok(None)
        } else {
            Ok(Some(peeked[0]))
        }
    }

    /// Skip the next record.
    pub fn skip_record<R: Reader>(reader: &mut R) -> Result<()> {
        reader.read(1)?; // Skip record type
        let len = Self::read_u64(reader)?;
        reader.seek_from_current(len as i64)?;
        Ok(())
    }

    /// Parse magic bytes.
    pub fn parse_magic_bytes<R: Reader>(reader: &mut R) -> Result<char> {
        let magic = reader.read(8)?;
        if magic.len() < 8 || &magic[..5] != b"\x89MCAP" || &magic[6..] != b"\r\n" {
            return Err(PybagError::InvalidMagicBytes);
        }
        Ok(magic[5] as char)
    }

    /// Parse a header record.
    pub fn parse_header<R: Reader>(reader: &mut R) -> Result<HeaderRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Header as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Header as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let profile = Self::read_string_cursor(&mut cursor)?;
        let library = Self::read_string_cursor(&mut cursor)?;

        Ok(HeaderRecord { profile, library })
    }

    /// Parse a footer record.
    pub fn parse_footer<R: Reader>(reader: &mut R) -> Result<FooterRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Footer as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Footer as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        if record_len != 20 {
            return Err(PybagError::InvalidMcap(format!(
                "Unexpected footer record length: {} bytes",
                record_len
            )));
        }

        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let summary_start = cursor.read_u64::<LittleEndian>()?;
        let summary_offset_start = cursor.read_u64::<LittleEndian>()?;
        let summary_crc = cursor.read_u32::<LittleEndian>()?;

        Ok(FooterRecord {
            summary_start,
            summary_offset_start,
            summary_crc,
        })
    }

    /// Parse a schema record.
    pub fn parse_schema<R: Reader>(reader: &mut R) -> Result<Option<SchemaRecord>> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Schema as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Schema as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let id = cursor.read_u16::<LittleEndian>()?;
        if id == 0 {
            return Ok(None); // Invalid schema, should be ignored
        }

        let name = Self::read_string_cursor(&mut cursor)?;
        let encoding = Self::read_string_cursor(&mut cursor)?;
        let data_len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut data = vec![0u8; data_len];
        std::io::Read::read_exact(&mut cursor, &mut data)?;

        Ok(Some(SchemaRecord {
            id,
            name,
            encoding,
            data,
        }))
    }

    /// Parse a channel record.
    pub fn parse_channel<R: Reader>(reader: &mut R) -> Result<ChannelRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Channel as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Channel as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let id = cursor.read_u16::<LittleEndian>()?;
        let schema_id = cursor.read_u16::<LittleEndian>()?;
        let topic = Self::read_string_cursor(&mut cursor)?;
        let message_encoding = Self::read_string_cursor(&mut cursor)?;
        let metadata = Self::read_map_string_string_cursor(&mut cursor)?;

        Ok(ChannelRecord {
            id,
            schema_id,
            topic,
            message_encoding,
            metadata,
        })
    }

    /// Parse a message record.
    pub fn parse_message<R: Reader>(reader: &mut R) -> Result<MessageRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Message as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Message as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let channel_id = cursor.read_u16::<LittleEndian>()?;
        let sequence = cursor.read_u32::<LittleEndian>()?;
        let log_time = cursor.read_u64::<LittleEndian>()?;
        let publish_time = cursor.read_u64::<LittleEndian>()?;
        // Remaining bytes are the data: 2 + 4 + 8 + 8 = 22 bytes header
        let data_len = record_len as usize - 22;
        let mut data = vec![0u8; data_len];
        std::io::Read::read_exact(&mut cursor, &mut data)?;

        Ok(MessageRecord {
            channel_id,
            sequence,
            log_time,
            publish_time,
            data,
        })
    }

    /// Parse a chunk record.
    pub fn parse_chunk<R: Reader>(reader: &mut R) -> Result<ChunkRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Chunk as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Chunk as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_crc = cursor.read_u32::<LittleEndian>()?;
        let compression = Self::read_string_cursor(&mut cursor)?;
        let records_len = cursor.read_u64::<LittleEndian>()? as usize;
        let mut records = vec![0u8; records_len];
        std::io::Read::read_exact(&mut cursor, &mut records)?;

        Ok(ChunkRecord {
            message_start_time,
            message_end_time,
            uncompressed_size,
            uncompressed_crc,
            compression,
            records,
        })
    }

    /// Parse a message index record.
    pub fn parse_message_index<R: Reader>(reader: &mut R) -> Result<MessageIndexRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::MessageIndex as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::MessageIndex as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let channel_id = cursor.read_u16::<LittleEndian>()?;
        let records = Self::read_message_index_entries_cursor(&mut cursor)?;

        Ok(MessageIndexRecord { channel_id, records })
    }

    /// Parse a chunk index record.
    pub fn parse_chunk_index<R: Reader>(reader: &mut R) -> Result<ChunkIndexRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::ChunkIndex as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::ChunkIndex as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let chunk_start_offset = cursor.read_u64::<LittleEndian>()?;
        let chunk_length = cursor.read_u64::<LittleEndian>()?;
        let message_index_offsets = Self::read_map_u16_u64_cursor(&mut cursor)?;
        let message_index_length = cursor.read_u64::<LittleEndian>()?;
        let compression = Self::read_string_cursor(&mut cursor)?;
        let compressed_size = cursor.read_u64::<LittleEndian>()?;
        let uncompressed_size = cursor.read_u64::<LittleEndian>()?;

        Ok(ChunkIndexRecord {
            message_start_time,
            message_end_time,
            chunk_start_offset,
            chunk_length,
            message_index_offsets,
            message_index_length,
            compression,
            compressed_size,
            uncompressed_size,
        })
    }

    /// Parse an attachment record.
    pub fn parse_attachment<R: Reader>(reader: &mut R) -> Result<AttachmentRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Attachment as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Attachment as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let log_time = cursor.read_u64::<LittleEndian>()?;
        let create_time = cursor.read_u64::<LittleEndian>()?;
        let name = Self::read_string_cursor(&mut cursor)?;
        let media_type = Self::read_string_cursor(&mut cursor)?;
        let data_len = cursor.read_u64::<LittleEndian>()? as usize;
        let mut data = vec![0u8; data_len];
        std::io::Read::read_exact(&mut cursor, &mut data)?;
        let crc = cursor.read_u32::<LittleEndian>()?;

        Ok(AttachmentRecord {
            log_time,
            create_time,
            name,
            media_type,
            data,
            crc,
        })
    }

    /// Parse an attachment index record.
    pub fn parse_attachment_index<R: Reader>(reader: &mut R) -> Result<AttachmentIndexRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::AttachmentIndex as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::AttachmentIndex as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let offset = cursor.read_u64::<LittleEndian>()?;
        let length = cursor.read_u64::<LittleEndian>()?;
        let log_time = cursor.read_u64::<LittleEndian>()?;
        let create_time = cursor.read_u64::<LittleEndian>()?;
        let data_size = cursor.read_u64::<LittleEndian>()?;
        let name = Self::read_string_cursor(&mut cursor)?;
        let media_type = Self::read_string_cursor(&mut cursor)?;

        Ok(AttachmentIndexRecord {
            offset,
            length,
            log_time,
            create_time,
            data_size,
            name,
            media_type,
        })
    }

    /// Parse a metadata record.
    pub fn parse_metadata<R: Reader>(reader: &mut R) -> Result<MetadataRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Metadata as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Metadata as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let name = Self::read_string_cursor(&mut cursor)?;
        let metadata = Self::read_map_string_string_cursor(&mut cursor)?;

        Ok(MetadataRecord { name, metadata })
    }

    /// Parse a metadata index record.
    pub fn parse_metadata_index<R: Reader>(reader: &mut R) -> Result<MetadataIndexRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::MetadataIndex as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::MetadataIndex as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let offset = cursor.read_u64::<LittleEndian>()?;
        let length = cursor.read_u64::<LittleEndian>()?;
        let name = Self::read_string_cursor(&mut cursor)?;

        Ok(MetadataIndexRecord {
            offset,
            length,
            name,
        })
    }

    /// Parse a statistics record.
    pub fn parse_statistics<R: Reader>(reader: &mut R) -> Result<StatisticsRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::Statistics as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::Statistics as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let message_count = cursor.read_u64::<LittleEndian>()?;
        let schema_count = cursor.read_u16::<LittleEndian>()?;
        let channel_count = cursor.read_u32::<LittleEndian>()?;
        let attachment_count = cursor.read_u32::<LittleEndian>()?;
        let metadata_count = cursor.read_u32::<LittleEndian>()?;
        let chunk_count = cursor.read_u32::<LittleEndian>()?;
        let message_start_time = cursor.read_u64::<LittleEndian>()?;
        let message_end_time = cursor.read_u64::<LittleEndian>()?;
        let channel_message_counts = Self::read_map_u16_u64_cursor(&mut cursor)?;

        Ok(StatisticsRecord {
            message_count,
            schema_count,
            channel_count,
            attachment_count,
            metadata_count,
            chunk_count,
            message_start_time,
            message_end_time,
            channel_message_counts,
        })
    }

    /// Parse a summary offset record.
    pub fn parse_summary_offset<R: Reader>(reader: &mut R) -> Result<SummaryOffsetRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::SummaryOffset as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::SummaryOffset as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let group_opcode = cursor.read_u8()?;
        let group_start = cursor.read_u64::<LittleEndian>()?;
        let group_length = cursor.read_u64::<LittleEndian>()?;

        Ok(SummaryOffsetRecord {
            group_opcode,
            group_start,
            group_length,
        })
    }

    /// Parse a data end record.
    pub fn parse_data_end<R: Reader>(reader: &mut R) -> Result<DataEndRecord> {
        let record_type = Self::read_u8(reader)?;
        if record_type != RecordType::DataEnd as u8 {
            return Err(PybagError::UnexpectedRecordType {
                expected: RecordType::DataEnd as u8,
                got: record_type,
            });
        }

        let record_len = Self::read_u64(reader)?;
        let record_data = reader.read(record_len as usize)?;
        let mut cursor = Cursor::new(&record_data);

        let data_section_crc = cursor.read_u32::<LittleEndian>()?;

        Ok(DataEndRecord { data_section_crc })
    }

    // Helper methods

    fn read_u8<R: Reader>(reader: &mut R) -> Result<u8> {
        let data = reader.read(1)?;
        Ok(data[0])
    }

    fn read_u64<R: Reader>(reader: &mut R) -> Result<u64> {
        let data = reader.read(8)?;
        Ok(u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]))
    }

    fn read_string_cursor(cursor: &mut Cursor<&Vec<u8>>) -> Result<String> {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut buf = vec![0u8; len];
        std::io::Read::read_exact(cursor, &mut buf)?;
        String::from_utf8(buf)
            .map_err(|e| PybagError::InvalidMcap(format!("Invalid UTF-8 string: {}", e)))
    }

    fn read_map_string_string_cursor(
        cursor: &mut Cursor<&Vec<u8>>,
    ) -> Result<HashMap<String, String>> {
        let map_len = cursor.read_u32::<LittleEndian>()? as i64;
        let mut remaining = map_len;
        let mut map = HashMap::new();

        while remaining > 0 {
            let key_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut key_buf = vec![0u8; key_len];
            std::io::Read::read_exact(cursor, &mut key_buf)?;
            let key = String::from_utf8(key_buf)
                .map_err(|e| PybagError::InvalidMcap(format!("Invalid UTF-8 key: {}", e)))?;

            let val_len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut val_buf = vec![0u8; val_len];
            std::io::Read::read_exact(cursor, &mut val_buf)?;
            let val = String::from_utf8(val_buf)
                .map_err(|e| PybagError::InvalidMcap(format!("Invalid UTF-8 value: {}", e)))?;

            remaining -= 4 + key_len as i64 + 4 + val_len as i64;
            map.insert(key, val);
        }

        Ok(map)
    }

    fn read_map_u16_u64_cursor(cursor: &mut Cursor<&Vec<u8>>) -> Result<HashMap<u16, u64>> {
        let map_len = cursor.read_u32::<LittleEndian>()? as i64;
        let mut remaining = map_len;
        let mut map = HashMap::new();

        while remaining > 0 {
            let key = cursor.read_u16::<LittleEndian>()?;
            let val = cursor.read_u64::<LittleEndian>()?;
            remaining -= 2 + 8;
            map.insert(key, val);
        }

        Ok(map)
    }

    fn read_message_index_entries_cursor(
        cursor: &mut Cursor<&Vec<u8>>,
    ) -> Result<Vec<MessageIndexEntry>> {
        let array_len = cursor.read_u32::<LittleEndian>()? as i64;
        let mut remaining = array_len;
        let mut entries = Vec::new();

        while remaining > 0 {
            let log_time = cursor.read_u64::<LittleEndian>()?;
            let offset = cursor.read_u64::<LittleEndian>()?;
            remaining -= 16;
            entries.push(MessageIndexEntry { log_time, offset });
        }

        Ok(entries)
    }
}
