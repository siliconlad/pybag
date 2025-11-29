//! Zero-copy MCAP parsing for maximum performance.
//!
//! This module provides zero-copy message parsing that borrows directly from
//! memory-mapped file data instead of copying into owned buffers.

use crate::error::{PybagError, Result};
use crate::io::{SliceReader, SliceView};
use crate::mcap::chunk::decompress_chunk;
use crate::mcap::records::RecordType;
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::str;

/// Zero-copy message reference.
#[derive(Debug, Clone, Copy)]
pub struct MessageRef<'a> {
    pub channel_id: u16,
    pub sequence: u32,
    pub log_time: u64,
    pub publish_time: u64,
    pub data: &'a [u8],
}

/// Chunk metadata for fast seeking.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub offset: u64,
    pub message_start_time: u64,
    pub message_end_time: u64,
    pub compression: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

/// Zero-copy MCAP reader for maximum performance.
pub struct FastMcapReader {
    mmap: Mmap,
    chunks: Vec<ChunkMeta>,
    data_start: u64,
    data_end: u64,
}

impl FastMcapReader {
    /// Open an MCAP file for fast zero-copy reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let mut reader = Self {
            mmap,
            chunks: Vec::new(),
            data_start: 0,
            data_end: 0,
        };

        reader.parse_structure()?;
        Ok(reader)
    }

    /// Parse the MCAP file structure (header, footer, summary).
    fn parse_structure(&mut self) -> Result<()> {
        let data = &self.mmap[..];
        let len = data.len();

        // Verify magic bytes at start
        if len < 8 || &data[..5] != b"\x89MCAP" || &data[6..8] != b"\r\n" {
            return Err(PybagError::InvalidMagicBytes);
        }

        // Parse footer (at end - 8 magic - 29 footer)
        if len < 45 {
            return Err(PybagError::InvalidMcap("File too small".into()));
        }

        let footer_start = len - 37;
        let mut view = SliceView::new(&data[footer_start..]);

        let opcode = view.read_u8()?;
        if opcode != RecordType::Footer as u8 {
            return Err(PybagError::InvalidMcap("Invalid footer".into()));
        }

        let footer_len = view.read_u64_le()?;
        if footer_len != 20 {
            return Err(PybagError::InvalidMcap("Invalid footer length".into()));
        }

        let summary_start = view.read_u64_le()?;
        let _summary_offset_start = view.read_u64_le()?;
        let _summary_crc = view.read_u32_le()?;

        // Parse header
        let mut view = SliceView::new(&data[8..]);
        let opcode = view.read_u8()?;
        if opcode != RecordType::Header as u8 {
            return Err(PybagError::InvalidMcap("Invalid header".into()));
        }
        let header_len = view.read_u64_le()?;
        view.skip(header_len as usize)?;

        self.data_start = 8 + 1 + 8 + header_len;
        self.data_end = if summary_start > 0 { summary_start } else { footer_start as u64 };

        // Parse summary if exists
        if summary_start > 0 && summary_start < len as u64 {
            self.parse_summary(summary_start as usize)?;
        }

        Ok(())
    }

    /// Parse the summary section for chunk indices.
    fn parse_summary(&mut self, start: usize) -> Result<()> {
        let data = &self.mmap[..];
        let mut view = SliceView::new(&data[start..]);

        while !view.is_empty() && view.remaining() > 9 {
            let opcode = view.read_u8()?;
            let record_len = view.read_u64_le()? as usize;

            if view.remaining() < record_len {
                break;
            }

            if opcode == RecordType::ChunkIndex as u8 {
                let message_start_time = view.read_u64_le()?;
                let message_end_time = view.read_u64_le()?;
                let chunk_start_offset = view.read_u64_le()?;
                let _chunk_length = view.read_u64_le()?;

                // Skip message_index_offsets map
                let map_len = view.read_u32_le()? as usize;
                view.skip(map_len)?;

                let _message_index_length = view.read_u64_le()?;

                let compression_len = view.read_u32_le()? as usize;
                let compression_slice = view.slice(compression_len)?;
                let compression = str::from_utf8(compression_slice)
                    .unwrap_or("")
                    .to_string();

                let compressed_size = view.read_u64_le()?;
                let uncompressed_size = view.read_u64_le()?;

                self.chunks.push(ChunkMeta {
                    offset: chunk_start_offset,
                    message_start_time,
                    message_end_time,
                    compression,
                    compressed_size,
                    uncompressed_size,
                });
            } else if opcode == RecordType::Footer as u8 {
                break;
            } else {
                view.skip(record_len)?;
            }
        }

        Ok(())
    }

    /// Get the underlying mmap data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.mmap
    }

    /// Get number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Iterate over all messages, calling a function for each.
    /// This avoids the borrow checker issues with returning references.
    pub fn for_each_message<F>(&self, mut f: F) -> Result<usize>
    where
        F: FnMut(MessageRef<'_>),
    {
        let mut count = 0;
        let data = self.data();

        if self.chunks.is_empty() {
            // No chunks - iterate directly over data section
            let mut view = SliceView::new(&data[self.data_start as usize..self.data_end as usize]);

            while !view.is_empty() && view.remaining() > 9 {
                let opcode = view.read_u8()?;
                let record_len = view.read_u64_le()? as usize;

                if view.remaining() < record_len {
                    break;
                }

                if opcode == RecordType::Message as u8 && record_len >= 22 {
                    let channel_id = view.read_u16_le()?;
                    let sequence = view.read_u32_le()?;
                    let log_time = view.read_u64_le()?;
                    let publish_time = view.read_u64_le()?;
                    let data_len = record_len - 22;
                    let msg_data = view.slice(data_len)?;

                    f(MessageRef {
                        channel_id,
                        sequence,
                        log_time,
                        publish_time,
                        data: msg_data,
                    });
                    count += 1;
                } else if opcode == RecordType::DataEnd as u8 || opcode == RecordType::Footer as u8 {
                    break;
                } else {
                    view.skip(record_len)?;
                }
            }
        } else {
            // Chunk-based iteration
            for chunk_meta in &self.chunks {
                let offset = chunk_meta.offset as usize;
                if offset + 9 > data.len() {
                    continue;
                }

                let mut view = SliceView::new(&data[offset..]);
                let opcode = view.read_u8()?;
                if opcode != RecordType::Chunk as u8 {
                    continue;
                }

                let record_len = view.read_u64_le()? as usize;
                if view.remaining() < record_len {
                    continue;
                }

                // Skip chunk header: message_start_time(8) + message_end_time(8) + uncompressed_size(8) + uncompressed_crc(4) = 28
                view.skip(28)?;

                // Read compression string
                let compression_len = view.read_u32_le()? as usize;
                let compression = str::from_utf8(view.slice(compression_len)?)
                    .unwrap_or("")
                    .to_string();

                // Read records data
                let records_len = view.read_u64_le()? as usize;
                let records_data = view.slice(records_len)?;

                // Decompress
                let decompressed = decompress_chunk(&compression, records_data, chunk_meta.uncompressed_size as usize)?;

                // Parse messages from decompressed data
                let mut chunk_view = SliceView::new(&decompressed);
                while !chunk_view.is_empty() && chunk_view.remaining() > 9 {
                    let opcode = chunk_view.read_u8()?;
                    let record_len = chunk_view.read_u64_le()? as usize;

                    if chunk_view.remaining() < record_len {
                        break;
                    }

                    if opcode == RecordType::Message as u8 && record_len >= 22 {
                        let channel_id = chunk_view.read_u16_le()?;
                        let sequence = chunk_view.read_u32_le()?;
                        let log_time = chunk_view.read_u64_le()?;
                        let publish_time = chunk_view.read_u64_le()?;
                        let data_len = record_len - 22;
                        let msg_data = chunk_view.slice(data_len)?;

                        f(MessageRef {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time,
                            data: msg_data,
                        });
                        count += 1;
                    } else {
                        chunk_view.skip(record_len)?;
                    }
                }
            }
        }

        Ok(count)
    }

    /// Iterate over messages without chunks (for non-chunked files).
    pub fn iter_messages(&self) -> DirectMessageIterator<'_> {
        DirectMessageIterator::new(self)
    }
}

/// Iterator over messages in files without chunks.
pub struct DirectMessageIterator<'a> {
    data: &'a [u8],
    position: usize,
    end: usize,
}

impl<'a> DirectMessageIterator<'a> {
    fn new(reader: &'a FastMcapReader) -> Self {
        Self {
            data: reader.data(),
            position: reader.data_start as usize,
            end: reader.data_end as usize,
        }
    }
}

impl<'a> Iterator for DirectMessageIterator<'a> {
    type Item = MessageRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.position + 9 < self.end {
            let opcode = self.data[self.position];
            self.position += 1;

            let record_len = u64::from_le_bytes([
                self.data[self.position],
                self.data[self.position + 1],
                self.data[self.position + 2],
                self.data[self.position + 3],
                self.data[self.position + 4],
                self.data[self.position + 5],
                self.data[self.position + 6],
                self.data[self.position + 7],
            ]) as usize;
            self.position += 8;

            if self.position + record_len > self.end {
                return None;
            }

            if opcode == RecordType::Message as u8 && record_len >= 22 {
                let channel_id = u16::from_le_bytes([
                    self.data[self.position],
                    self.data[self.position + 1],
                ]);
                let sequence = u32::from_le_bytes([
                    self.data[self.position + 2],
                    self.data[self.position + 3],
                    self.data[self.position + 4],
                    self.data[self.position + 5],
                ]);
                let log_time = u64::from_le_bytes([
                    self.data[self.position + 6],
                    self.data[self.position + 7],
                    self.data[self.position + 8],
                    self.data[self.position + 9],
                    self.data[self.position + 10],
                    self.data[self.position + 11],
                    self.data[self.position + 12],
                    self.data[self.position + 13],
                ]);
                let publish_time = u64::from_le_bytes([
                    self.data[self.position + 14],
                    self.data[self.position + 15],
                    self.data[self.position + 16],
                    self.data[self.position + 17],
                    self.data[self.position + 18],
                    self.data[self.position + 19],
                    self.data[self.position + 20],
                    self.data[self.position + 21],
                ]);

                let data_start = self.position + 22;
                let data_end = self.position + record_len;
                self.position = data_end;

                return Some(MessageRef {
                    channel_id,
                    sequence,
                    log_time,
                    publish_time,
                    data: &self.data[data_start..data_end],
                });
            } else if opcode == RecordType::DataEnd as u8 || opcode == RecordType::Footer as u8 {
                return None;
            } else {
                self.position += record_len;
            }
        }
        None
    }
}

/// Count messages in an MCAP file efficiently (for benchmarking).
pub fn count_messages_fast(path: &Path) -> Result<usize> {
    let reader = FastMcapReader::open(path)?;
    reader.for_each_message(|_| {})
}
