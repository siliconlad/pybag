//! CDR (Common Data Representation) encoding and decoding.

use crate::error::{PybagError, Result};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

/// CDR decoder for deserializing ROS2 messages.
pub struct CdrDecoder<'a> {
    data: &'a [u8],
    position: usize,
    is_little_endian: bool,
}

impl<'a> CdrDecoder<'a> {
    /// Create a new CDR decoder.
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(PybagError::CdrDecodeError(
                "Data must be at least 4 bytes (CDR header)".to_string(),
            ));
        }

        // Get endianness from second byte
        let is_little_endian = data[1] != 0;

        Ok(Self {
            data,
            position: 4, // Skip CDR header
            is_little_endian,
        })
    }

    /// Get current position.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Align to boundary.
    pub fn align(&mut self, alignment: usize) {
        let remainder = self.position % alignment;
        if remainder != 0 {
            self.position += alignment - remainder;
        }
    }

    /// Read a bool.
    pub fn read_bool(&mut self) -> Result<bool> {
        self.align(1);
        self.check_remaining(1)?;
        let value = self.data[self.position] != 0;
        self.position += 1;
        Ok(value)
    }

    /// Read an i8.
    pub fn read_i8(&mut self) -> Result<i8> {
        self.align(1);
        self.check_remaining(1)?;
        let value = self.data[self.position] as i8;
        self.position += 1;
        Ok(value)
    }

    /// Read a u8.
    pub fn read_u8(&mut self) -> Result<u8> {
        self.align(1);
        self.check_remaining(1)?;
        let value = self.data[self.position];
        self.position += 1;
        Ok(value)
    }

    /// Read a byte.
    pub fn read_byte(&mut self) -> Result<u8> {
        self.read_u8()
    }

    /// Read a char.
    pub fn read_char(&mut self) -> Result<char> {
        self.align(1);
        self.check_remaining(1)?;
        let value = self.data[self.position] as char;
        self.position += 1;
        Ok(value)
    }

    /// Read an i16.
    pub fn read_i16(&mut self) -> Result<i16> {
        self.align(2);
        self.check_remaining(2)?;
        let bytes = &self.data[self.position..self.position + 2];
        self.position += 2;
        let value = if self.is_little_endian {
            i16::from_le_bytes([bytes[0], bytes[1]])
        } else {
            i16::from_be_bytes([bytes[0], bytes[1]])
        };
        Ok(value)
    }

    /// Read a u16.
    pub fn read_u16(&mut self) -> Result<u16> {
        self.align(2);
        self.check_remaining(2)?;
        let bytes = &self.data[self.position..self.position + 2];
        self.position += 2;
        let value = if self.is_little_endian {
            u16::from_le_bytes([bytes[0], bytes[1]])
        } else {
            u16::from_be_bytes([bytes[0], bytes[1]])
        };
        Ok(value)
    }

    /// Read an i32.
    pub fn read_i32(&mut self) -> Result<i32> {
        self.align(4);
        self.check_remaining(4)?;
        let bytes = &self.data[self.position..self.position + 4];
        self.position += 4;
        let value = if self.is_little_endian {
            i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        };
        Ok(value)
    }

    /// Read a u32.
    pub fn read_u32(&mut self) -> Result<u32> {
        self.align(4);
        self.check_remaining(4)?;
        let bytes = &self.data[self.position..self.position + 4];
        self.position += 4;
        let value = if self.is_little_endian {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        };
        Ok(value)
    }

    /// Read an i64.
    pub fn read_i64(&mut self) -> Result<i64> {
        self.align(8);
        self.check_remaining(8)?;
        let bytes = &self.data[self.position..self.position + 8];
        self.position += 8;
        let value = if self.is_little_endian {
            i64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        } else {
            i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        };
        Ok(value)
    }

    /// Read a u64.
    pub fn read_u64(&mut self) -> Result<u64> {
        self.align(8);
        self.check_remaining(8)?;
        let bytes = &self.data[self.position..self.position + 8];
        self.position += 8;
        let value = if self.is_little_endian {
            u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        } else {
            u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        };
        Ok(value)
    }

    /// Read an f32.
    pub fn read_f32(&mut self) -> Result<f32> {
        self.align(4);
        self.check_remaining(4)?;
        let bytes = &self.data[self.position..self.position + 4];
        self.position += 4;
        let value = if self.is_little_endian {
            f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        };
        Ok(value)
    }

    /// Read an f64.
    pub fn read_f64(&mut self) -> Result<f64> {
        self.align(8);
        self.check_remaining(8)?;
        let bytes = &self.data[self.position..self.position + 8];
        self.position += 8;
        let value = if self.is_little_endian {
            f64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        } else {
            f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        };
        Ok(value)
    }

    /// Read a string.
    pub fn read_string(&mut self) -> Result<String> {
        let length = self.read_u32()? as usize;
        if length == 0 {
            return Ok(String::new());
        }
        self.check_remaining(length)?;
        // String is null-terminated, so actual content length is length - 1
        let content_len = if length > 0 { length - 1 } else { 0 };
        let bytes = &self.data[self.position..self.position + content_len];
        self.position += length; // Skip including null terminator
        String::from_utf8(bytes.to_vec())
            .map_err(|e| PybagError::CdrDecodeError(format!("Invalid UTF-8 string: {}", e)))
    }

    /// Read a wstring (wide string).
    pub fn read_wstring(&mut self) -> Result<String> {
        let length = self.read_u32()? as usize;
        if length <= 1 {
            if length == 1 {
                // Read and discard null terminator
                self.align(4);
                self.position += 4;
            }
            return Ok(String::new());
        }
        let mut chars = Vec::with_capacity(length - 1);
        for _ in 0..length - 1 {
            self.align(4);
            let char_code = self.read_u32()?;
            if let Some(c) = char::from_u32(char_code) {
                chars.push(c);
            }
        }
        // Read and discard null terminator
        self.align(4);
        self.position += 4;
        Ok(chars.into_iter().collect())
    }

    /// Read a fixed-size array.
    pub fn read_array<T, F>(&mut self, length: usize, read_fn: F) -> Result<Vec<T>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let mut result = Vec::with_capacity(length);
        for _ in 0..length {
            result.push(read_fn(self)?);
        }
        Ok(result)
    }

    /// Read a sequence (variable-length array).
    pub fn read_sequence<T, F>(&mut self, read_fn: F) -> Result<Vec<T>>
    where
        F: Fn(&mut Self) -> Result<T>,
    {
        let length = self.read_u32()? as usize;
        self.read_array(length, read_fn)
    }

    /// Read raw bytes.
    pub fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        self.check_remaining(length)?;
        let bytes = self.data[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(bytes)
    }

    fn check_remaining(&self, needed: usize) -> Result<()> {
        if self.position + needed > self.data.len() {
            return Err(PybagError::BufferTooSmall {
                needed,
                available: self.data.len() - self.position,
            });
        }
        Ok(())
    }
}

/// CDR encoder for serializing ROS2 messages.
pub struct CdrEncoder {
    data: Vec<u8>,
    is_little_endian: bool,
}

impl CdrEncoder {
    /// Create a new CDR encoder.
    pub fn new(little_endian: bool) -> Self {
        let endian_flag = if little_endian { 1 } else { 0 };
        Self {
            data: vec![0x00, endian_flag, 0x00, 0x00], // CDR header
            is_little_endian: little_endian,
        }
    }

    /// Get the encoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consume and return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    /// Align to boundary.
    pub fn align(&mut self, alignment: usize) {
        let remainder = self.data.len() % alignment;
        if remainder != 0 {
            let padding = alignment - remainder;
            self.data.extend(std::iter::repeat(0u8).take(padding));
        }
    }

    /// Write a bool.
    pub fn write_bool(&mut self, value: bool) {
        self.align(1);
        self.data.push(if value { 1 } else { 0 });
    }

    /// Write an i8.
    pub fn write_i8(&mut self, value: i8) {
        self.align(1);
        self.data.push(value as u8);
    }

    /// Write a u8.
    pub fn write_u8(&mut self, value: u8) {
        self.align(1);
        self.data.push(value);
    }

    /// Write a byte.
    pub fn write_byte(&mut self, value: u8) {
        self.write_u8(value);
    }

    /// Write a char.
    pub fn write_char(&mut self, value: char) {
        self.align(1);
        self.data.push(value as u8);
    }

    /// Write an i16.
    pub fn write_i16(&mut self, value: i16) {
        self.align(2);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write a u16.
    pub fn write_u16(&mut self, value: u16) {
        self.align(2);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write an i32.
    pub fn write_i32(&mut self, value: i32) {
        self.align(4);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write a u32.
    pub fn write_u32(&mut self, value: u32) {
        self.align(4);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write an i64.
    pub fn write_i64(&mut self, value: i64) {
        self.align(8);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write a u64.
    pub fn write_u64(&mut self, value: u64) {
        self.align(8);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write an f32.
    pub fn write_f32(&mut self, value: f32) {
        self.align(4);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write an f64.
    pub fn write_f64(&mut self, value: f64) {
        self.align(8);
        if self.is_little_endian {
            self.data.extend(&value.to_le_bytes());
        } else {
            self.data.extend(&value.to_be_bytes());
        }
    }

    /// Write a string.
    pub fn write_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.write_u32((bytes.len() + 1) as u32); // Include null terminator
        self.data.extend(bytes);
        self.data.push(0); // Null terminator
    }

    /// Write a wstring (wide string).
    pub fn write_wstring(&mut self, value: &str) {
        self.write_u32((value.chars().count() + 1) as u32); // Include null terminator
        for c in value.chars() {
            self.align(4);
            if self.is_little_endian {
                self.data.extend(&(c as u32).to_le_bytes());
            } else {
                self.data.extend(&(c as u32).to_be_bytes());
            }
        }
        // Write null terminator
        self.align(4);
        self.data.extend(&[0u8; 4]);
    }

    /// Write raw bytes.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend(bytes);
    }

    /// Write a sequence length prefix.
    pub fn write_sequence_length(&mut self, length: usize) {
        self.write_u32(length as u32);
    }
}

impl Default for CdrEncoder {
    fn default() -> Self {
        Self::new(true) // Little endian by default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_primitives() {
        let mut encoder = CdrEncoder::new(true);
        encoder.write_bool(true);
        encoder.write_i8(-42);
        encoder.write_u8(42);
        encoder.write_i16(-1000);
        encoder.write_u16(1000);
        encoder.write_i32(-100000);
        encoder.write_u32(100000);
        encoder.write_i64(-10000000000);
        encoder.write_u64(10000000000);
        encoder.write_f32(3.14);
        encoder.write_f64(3.14159265359);

        let data = encoder.into_bytes();
        let mut decoder = CdrDecoder::new(&data).unwrap();

        assert_eq!(decoder.read_bool().unwrap(), true);
        assert_eq!(decoder.read_i8().unwrap(), -42);
        assert_eq!(decoder.read_u8().unwrap(), 42);
        assert_eq!(decoder.read_i16().unwrap(), -1000);
        assert_eq!(decoder.read_u16().unwrap(), 1000);
        assert_eq!(decoder.read_i32().unwrap(), -100000);
        assert_eq!(decoder.read_u32().unwrap(), 100000);
        assert_eq!(decoder.read_i64().unwrap(), -10000000000);
        assert_eq!(decoder.read_u64().unwrap(), 10000000000);
        assert!((decoder.read_f32().unwrap() - 3.14).abs() < 0.001);
        assert!((decoder.read_f64().unwrap() - 3.14159265359).abs() < 0.000001);
    }

    #[test]
    fn test_encode_decode_string() {
        let mut encoder = CdrEncoder::new(true);
        encoder.write_string("hello world");

        let data = encoder.into_bytes();
        let mut decoder = CdrDecoder::new(&data).unwrap();

        assert_eq!(decoder.read_string().unwrap(), "hello world");
    }
}
