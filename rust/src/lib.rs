use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use std::io::{Cursor, Write};
use byteorder::{ByteOrder, LittleEndian, BigEndian, ReadBytesExt};

/// BytesWriter for aligned byte writing
#[pyclass]
struct RustBytesWriter {
    buffer: Vec<u8>,
}

#[pymethods]
impl RustBytesWriter {
    #[new]
    fn new() -> Self {
        RustBytesWriter {
            buffer: Vec::new(),
        }
    }

    fn write(&mut self, data: &[u8]) -> usize {
        self.buffer.extend_from_slice(data);
        data.len()
    }

    fn tell(&self) -> usize {
        self.buffer.len()
    }

    fn align(&mut self, size: usize) {
        let current_length = self.buffer.len();
        if current_length % size > 0 {
            let padding = size - (current_length % size);
            self.buffer.resize(current_length + padding, 0);
        }
    }

    fn as_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.buffer)
    }

    fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// BytesReader for aligned byte reading
#[pyclass]
struct RustBytesReader {
    data: Vec<u8>,
    position: usize,
}

#[pymethods]
impl RustBytesReader {
    #[new]
    fn new(data: Vec<u8>) -> Self {
        RustBytesReader {
            data,
            position: 0,
        }
    }

    fn read(&mut self, size: usize) -> PyResult<Vec<u8>> {
        if self.position + size > self.data.len() {
            return Err(PyValueError::new_err("Not enough data to read"));
        }
        let result = self.data[self.position..self.position + size].to_vec();
        self.position += size;
        Ok(result)
    }

    fn tell(&self) -> usize {
        self.position
    }

    fn align(&mut self, size: usize) {
        if self.position % size > 0 {
            self.position += size - (self.position % size);
        }
    }
}

/// CDR Encoder
#[pyclass]
struct RustCdrEncoder {
    is_little_endian: bool,
    payload: RustBytesWriter,
    header: Vec<u8>,
}

#[pymethods]
impl RustCdrEncoder {
    #[new]
    #[pyo3(signature = (*, little_endian=true))]
    fn new(little_endian: bool) -> Self {
        let endian_flag = if little_endian { 1u8 } else { 0u8 };
        let header = vec![0x00, endian_flag, 0x00, 0x00];

        RustCdrEncoder {
            is_little_endian: little_endian,
            payload: RustBytesWriter::new(),
            header,
        }
    }

    #[staticmethod]
    fn encoding() -> &'static str {
        "cdr"
    }

    fn save<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let mut result = self.header.clone();
        result.extend_from_slice(&self.payload.buffer);
        PyBytes::new(py, &result)
    }

    // Primitive encoders
    fn bool(&mut self, value: bool) {
        self.payload.align(1);
        self.payload.write(&[value as u8]);
    }

    fn int8(&mut self, value: i8) {
        self.payload.align(1);
        self.payload.write(&value.to_ne_bytes());
    }

    fn uint8(&mut self, value: u8) {
        self.payload.align(1);
        self.payload.write(&[value]);
    }

    fn byte(&mut self, value: &[u8]) {
        self.payload.align(1);
        if !value.is_empty() {
            self.payload.write(&value[0..1]);
        }
    }

    fn char(&mut self, value: &str) {
        self.payload.align(1);
        if let Some(first_char) = value.chars().next() {
            let mut buf = [0u8; 1];
            let _ = first_char.encode_utf8(&mut buf);
            self.payload.write(&buf);
        }
    }

    fn int16(&mut self, value: i16) {
        self.payload.align(2);
        let mut buf = [0u8; 2];
        if self.is_little_endian {
            LittleEndian::write_i16(&mut buf, value);
        } else {
            BigEndian::write_i16(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn uint16(&mut self, value: u16) {
        self.payload.align(2);
        let mut buf = [0u8; 2];
        if self.is_little_endian {
            LittleEndian::write_u16(&mut buf, value);
        } else {
            BigEndian::write_u16(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn int32(&mut self, value: i32) {
        self.payload.align(4);
        let mut buf = [0u8; 4];
        if self.is_little_endian {
            LittleEndian::write_i32(&mut buf, value);
        } else {
            BigEndian::write_i32(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn uint32(&mut self, value: u32) {
        self.payload.align(4);
        let mut buf = [0u8; 4];
        if self.is_little_endian {
            LittleEndian::write_u32(&mut buf, value);
        } else {
            BigEndian::write_u32(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn int64(&mut self, value: i64) {
        self.payload.align(8);
        let mut buf = [0u8; 8];
        if self.is_little_endian {
            LittleEndian::write_i64(&mut buf, value);
        } else {
            BigEndian::write_i64(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn uint64(&mut self, value: u64) {
        self.payload.align(8);
        let mut buf = [0u8; 8];
        if self.is_little_endian {
            LittleEndian::write_u64(&mut buf, value);
        } else {
            BigEndian::write_u64(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn float32(&mut self, value: f32) {
        self.payload.align(4);
        let mut buf = [0u8; 4];
        if self.is_little_endian {
            LittleEndian::write_f32(&mut buf, value);
        } else {
            BigEndian::write_f32(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn float64(&mut self, value: f64) {
        self.payload.align(8);
        let mut buf = [0u8; 8];
        if self.is_little_endian {
            LittleEndian::write_f64(&mut buf, value);
        } else {
            BigEndian::write_f64(&mut buf, value);
        }
        self.payload.write(&buf);
    }

    fn string(&mut self, value: &str) {
        let encoded = value.as_bytes();
        // Write length (including null terminator)
        self.uint32((encoded.len() + 1) as u32);
        self.payload.write(encoded);
        self.payload.write(&[0u8]);
    }

    // Expose internal payload for direct access (needed for compatibility)
    #[getter]
    fn _payload(&self) -> PyResult<Vec<u8>> {
        Ok(self.payload.buffer.clone())
    }

    #[getter]
    fn _is_little_endian(&self) -> bool {
        self.is_little_endian
    }
}

/// CDR Decoder
#[pyclass]
struct RustCdrDecoder {
    is_little_endian: bool,
    data: Vec<u8>,
    position: usize,
}

#[pymethods]
impl RustCdrDecoder {
    #[new]
    fn new(data: Vec<u8>) -> PyResult<Self> {
        if data.len() < 4 {
            return Err(PyValueError::new_err("Data must be at least 4 bytes long (CDR header)."));
        }

        // Get endianness from second byte
        let is_little_endian = data[1] != 0;

        // Skip first 4 bytes (header)
        let data = data[4..].to_vec();

        Ok(RustCdrDecoder {
            is_little_endian,
            data,
            position: 0,
        })
    }

    fn align(&mut self, size: usize) {
        if self.position % size > 0 {
            self.position += size - (self.position % size);
        }
    }

    fn read(&mut self, size: usize) -> PyResult<Vec<u8>> {
        if self.position + size > self.data.len() {
            return Err(PyValueError::new_err("Not enough data to read"));
        }
        let result = self.data[self.position..self.position + size].to_vec();
        self.position += size;
        Ok(result)
    }

    // Primitive decoders
    fn bool(&mut self) -> PyResult<bool> {
        self.align(1);
        let bytes = self.read(1)?;
        Ok(bytes[0] != 0)
    }

    fn int8(&mut self) -> PyResult<i8> {
        self.align(1);
        let bytes = self.read(1)?;
        Ok(bytes[0] as i8)
    }

    fn uint8(&mut self) -> PyResult<u8> {
        self.align(1);
        let bytes = self.read(1)?;
        Ok(bytes[0])
    }

    fn byte(&mut self) -> PyResult<Vec<u8>> {
        self.align(1);
        self.read(1)
    }

    fn char(&mut self) -> PyResult<String> {
        self.align(1);
        let bytes = self.read(1)?;
        Ok(String::from_utf8_lossy(&bytes).chars().next().unwrap_or('\0').to_string())
    }

    fn int16(&mut self) -> PyResult<i16> {
        self.align(2);
        let bytes = self.read(2)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_i16(&bytes))
        } else {
            Ok(BigEndian::read_i16(&bytes))
        }
    }

    fn uint16(&mut self) -> PyResult<u16> {
        self.align(2);
        let bytes = self.read(2)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_u16(&bytes))
        } else {
            Ok(BigEndian::read_u16(&bytes))
        }
    }

    fn int32(&mut self) -> PyResult<i32> {
        self.align(4);
        let bytes = self.read(4)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_i32(&bytes))
        } else {
            Ok(BigEndian::read_i32(&bytes))
        }
    }

    fn uint32(&mut self) -> PyResult<u32> {
        self.align(4);
        let bytes = self.read(4)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_u32(&bytes))
        } else {
            Ok(BigEndian::read_u32(&bytes))
        }
    }

    fn int64(&mut self) -> PyResult<i64> {
        self.align(8);
        let bytes = self.read(8)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_i64(&bytes))
        } else {
            Ok(BigEndian::read_i64(&bytes))
        }
    }

    fn uint64(&mut self) -> PyResult<u64> {
        self.align(8);
        let bytes = self.read(8)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_u64(&bytes))
        } else {
            Ok(BigEndian::read_u64(&bytes))
        }
    }

    fn float32(&mut self) -> PyResult<f32> {
        self.align(4);
        let bytes = self.read(4)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_f32(&bytes))
        } else {
            Ok(BigEndian::read_f32(&bytes))
        }
    }

    fn float64(&mut self) -> PyResult<f64> {
        self.align(8);
        let bytes = self.read(8)?;
        if self.is_little_endian {
            Ok(LittleEndian::read_f64(&bytes))
        } else {
            Ok(BigEndian::read_f64(&bytes))
        }
    }

    fn string(&mut self) -> PyResult<String> {
        // Strings are null-terminated
        let length = self.uint32()? as usize;
        if length <= 1 {
            self.read(length)?; // discard
            return Ok(String::new());
        }
        let bytes = self.read(length)?;
        // Remove null terminator
        Ok(String::from_utf8_lossy(&bytes[..length - 1]).to_string())
    }

    // Expose internal data for direct access (needed for compatibility)
    #[getter]
    fn _is_little_endian(&self) -> bool {
        self.is_little_endian
    }
}

/// Python module
#[pymodule]
fn pybag_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustBytesWriter>()?;
    m.add_class::<RustBytesReader>()?;
    m.add_class::<RustCdrEncoder>()?;
    m.add_class::<RustCdrDecoder>()?;
    Ok(())
}
