//! Reader implementations for binary data.

use crate::error::{PybagError, Result};
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

/// Trait for reading binary data with seeking support.
pub trait Reader {
    /// Read exactly `n` bytes into a new Vec.
    fn read(&mut self, n: usize) -> Result<Vec<u8>>;

    /// Read exactly `n` bytes into the provided buffer.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()>;

    /// Peek at the next `n` bytes without advancing the position.
    fn peek(&mut self, n: usize) -> Result<Vec<u8>>;

    /// Seek to an absolute position.
    fn seek(&mut self, pos: u64) -> Result<u64>;

    /// Seek relative to the current position.
    fn seek_from_current(&mut self, offset: i64) -> Result<u64>;

    /// Seek relative to the end of the data.
    fn seek_from_end(&mut self, offset: i64) -> Result<u64>;

    /// Get the current position.
    fn position(&self) -> u64;

    /// Get the total length.
    fn len(&self) -> u64;

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Align to boundary and return self for chaining
    fn align(&mut self, alignment: usize) -> &mut Self {
        let pos = self.position() as usize;
        let remainder = pos % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            let _ = self.seek_from_current(padding as i64);
        }
        self
    }

    /// Read remaining bytes
    fn read_to_end(&mut self) -> Result<Vec<u8>> {
        let remaining = (self.len() - self.position()) as usize;
        self.read(remaining)
    }
}

/// Trait for zero-copy reading from memory-mapped data.
pub trait SliceReader {
    /// Get a slice of `n` bytes at current position and advance.
    fn slice(&mut self, n: usize) -> Result<&[u8]>;

    /// Peek at a slice without advancing position.
    fn peek_slice(&self, n: usize) -> Result<&[u8]>;

    /// Get the underlying data slice.
    fn data(&self) -> &[u8];

    /// Skip `n` bytes.
    fn skip(&mut self, n: usize) -> Result<()>;

    /// Read a u8.
    #[inline]
    fn read_u8(&mut self) -> Result<u8> {
        let slice = self.slice(1)?;
        Ok(slice[0])
    }

    /// Read a u16 little-endian.
    #[inline]
    fn read_u16_le(&mut self) -> Result<u16> {
        let slice = self.slice(2)?;
        Ok(u16::from_le_bytes([slice[0], slice[1]]))
    }

    /// Read a u32 little-endian.
    #[inline]
    fn read_u32_le(&mut self) -> Result<u32> {
        let slice = self.slice(4)?;
        Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    /// Read a u64 little-endian.
    #[inline]
    fn read_u64_le(&mut self) -> Result<u64> {
        let slice = self.slice(8)?;
        Ok(u64::from_le_bytes([
            slice[0], slice[1], slice[2], slice[3],
            slice[4], slice[5], slice[6], slice[7],
        ]))
    }
}

/// Memory-mapped file reader for maximum performance.
pub struct FileReader {
    mmap: Mmap,
    position: u64,
}

impl FileReader {
    /// Open a file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap, position: 0 })
    }
}

impl FileReader {
    /// Get the underlying mmap slice at a specific position.
    #[inline]
    pub fn get_slice(&self, start: usize, len: usize) -> Option<&[u8]> {
        let end = start + len;
        if end <= self.mmap.len() {
            Some(&self.mmap[start..end])
        } else {
            None
        }
    }

    /// Get the underlying mmap.
    #[inline]
    pub fn mmap(&self) -> &[u8] {
        &self.mmap
    }
}

impl Reader for FileReader {
    fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        let start = self.position as usize;
        let end = start + n;
        if end > self.mmap.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.mmap.len() - start,
            });
        }
        let data = self.mmap[start..end].to_vec();
        self.position = end as u64;
        Ok(data)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let start = self.position as usize;
        let end = start + buf.len();
        if end > self.mmap.len() {
            return Err(PybagError::BufferTooSmall {
                needed: buf.len(),
                available: self.mmap.len() - start,
            });
        }
        buf.copy_from_slice(&self.mmap[start..end]);
        self.position = end as u64;
        Ok(())
    }

    fn peek(&mut self, n: usize) -> Result<Vec<u8>> {
        let start = self.position as usize;
        let end = (start + n).min(self.mmap.len());
        Ok(self.mmap[start..end].to_vec())
    }

    fn seek(&mut self, pos: u64) -> Result<u64> {
        self.position = pos.min(self.mmap.len() as u64);
        Ok(self.position)
    }

    fn seek_from_current(&mut self, offset: i64) -> Result<u64> {
        let new_pos = if offset >= 0 {
            self.position.saturating_add(offset as u64)
        } else {
            self.position.saturating_sub((-offset) as u64)
        };
        self.position = new_pos.min(self.mmap.len() as u64);
        Ok(self.position)
    }

    fn seek_from_end(&mut self, offset: i64) -> Result<u64> {
        let len = self.mmap.len() as i64;
        let new_pos = (len + offset).max(0) as u64;
        self.position = new_pos.min(self.mmap.len() as u64);
        Ok(self.position)
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn len(&self) -> u64 {
        self.mmap.len() as u64
    }
}

impl SliceReader for FileReader {
    #[inline]
    fn slice(&mut self, n: usize) -> Result<&[u8]> {
        let start = self.position as usize;
        let end = start + n;
        if end > self.mmap.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.mmap.len() - start,
            });
        }
        self.position = end as u64;
        Ok(&self.mmap[start..end])
    }

    #[inline]
    fn peek_slice(&self, n: usize) -> Result<&[u8]> {
        let start = self.position as usize;
        let end = start + n;
        if end > self.mmap.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.mmap.len() - start,
            });
        }
        Ok(&self.mmap[start..end])
    }

    #[inline]
    fn data(&self) -> &[u8] {
        &self.mmap
    }

    #[inline]
    fn skip(&mut self, n: usize) -> Result<()> {
        self.position += n as u64;
        Ok(())
    }
}

/// In-memory bytes reader.
pub struct BytesReader {
    data: Vec<u8>,
    position: u64,
}

impl BytesReader {
    /// Create a new bytes reader.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, position: 0 }
    }

    /// Create from a byte slice.
    pub fn from_slice(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
            position: 0,
        }
    }

    /// Get a reference to the underlying data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get a slice of remaining data.
    pub fn remaining(&self) -> &[u8] {
        &self.data[self.position as usize..]
    }
}

impl Reader for BytesReader {
    fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        let start = self.position as usize;
        let end = start + n;
        if end > self.data.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.data.len() - start,
            });
        }
        let data = self.data[start..end].to_vec();
        self.position = end as u64;
        Ok(data)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let start = self.position as usize;
        let end = start + buf.len();
        if end > self.data.len() {
            return Err(PybagError::BufferTooSmall {
                needed: buf.len(),
                available: self.data.len() - start,
            });
        }
        buf.copy_from_slice(&self.data[start..end]);
        self.position = end as u64;
        Ok(())
    }

    fn peek(&mut self, n: usize) -> Result<Vec<u8>> {
        let start = self.position as usize;
        let end = (start + n).min(self.data.len());
        Ok(self.data[start..end].to_vec())
    }

    fn seek(&mut self, pos: u64) -> Result<u64> {
        self.position = pos.min(self.data.len() as u64);
        Ok(self.position)
    }

    fn seek_from_current(&mut self, offset: i64) -> Result<u64> {
        let new_pos = if offset >= 0 {
            self.position.saturating_add(offset as u64)
        } else {
            self.position.saturating_sub((-offset) as u64)
        };
        self.position = new_pos.min(self.data.len() as u64);
        Ok(self.position)
    }

    fn seek_from_end(&mut self, offset: i64) -> Result<u64> {
        let len = self.data.len() as i64;
        let new_pos = (len + offset).max(0) as u64;
        self.position = new_pos.min(self.data.len() as u64);
        Ok(self.position)
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

/// Zero-copy view into a slice of bytes.
#[derive(Clone)]
pub struct SliceView<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> SliceView<'a> {
    /// Create a new slice view.
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Get remaining bytes.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    /// Check if at end.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.position >= self.data.len()
    }

    /// Get current position.
    #[inline]
    pub fn position(&self) -> usize {
        self.position
    }

    /// Set position.
    #[inline]
    pub fn set_position(&mut self, pos: usize) {
        self.position = pos.min(self.data.len());
    }
}

impl<'a> SliceReader for SliceView<'a> {
    #[inline]
    fn slice(&mut self, n: usize) -> Result<&[u8]> {
        let start = self.position;
        let end = start + n;
        if end > self.data.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.data.len() - start,
            });
        }
        self.position = end;
        Ok(&self.data[start..end])
    }

    #[inline]
    fn peek_slice(&self, n: usize) -> Result<&[u8]> {
        let start = self.position;
        let end = start + n;
        if end > self.data.len() {
            return Err(PybagError::BufferTooSmall {
                needed: n,
                available: self.data.len() - start,
            });
        }
        Ok(&self.data[start..end])
    }

    #[inline]
    fn data(&self) -> &[u8] {
        self.data
    }

    #[inline]
    fn skip(&mut self, n: usize) -> Result<()> {
        self.position += n;
        Ok(())
    }
}
