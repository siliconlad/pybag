//! Writer implementations for binary data.

use crate::error::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Trait for writing binary data.
pub trait Writer {
    /// Write bytes.
    fn write(&mut self, data: &[u8]) -> Result<()>;

    /// Get the current position/length.
    fn position(&self) -> u64;

    /// Align to boundary.
    fn align(&mut self, alignment: usize) {
        let pos = self.position() as usize;
        let remainder = pos % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            let zeros = vec![0u8; padding];
            let _ = self.write(&zeros);
        }
    }

    /// Flush any buffered data.
    fn flush(&mut self) -> Result<()>;
}

/// File-backed writer.
pub struct FileWriter {
    writer: BufWriter<File>,
    position: u64,
}

impl FileWriter {
    /// Create a new file writer.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            position: 0,
        })
    }
}

impl Writer for FileWriter {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write_all(data)?;
        self.position += data.len() as u64;
        Ok(())
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// In-memory bytes writer.
pub struct BytesWriter {
    data: Vec<u8>,
}

impl BytesWriter {
    /// Create a new bytes writer.
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Create with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Get the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consume and return the underlying bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }
}

impl Default for BytesWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer for BytesWriter {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.data.extend_from_slice(data);
        Ok(())
    }

    fn position(&self) -> u64 {
        self.data.len() as u64
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
