//! pybag-rs: High-performance Rust implementation of pybag MCAP library.
//!
//! This library provides Python bindings for reading and writing MCAP files
//! with ROS2 message support.

pub mod encoding;
pub mod error;
pub mod io;
pub mod mcap;
pub mod schema;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::encoding::cdr::CdrDecoder;
use crate::io::FileReader;
use crate::mcap::reader::McapReader;
use crate::schema::ros2msg::Ros2MsgParser;
use crate::schema::types::{FieldType, PrimitiveType, Schema};

/// Python wrapper for decoded messages.
#[pyclass]
pub struct PyDecodedMessage {
    #[pyo3(get)]
    pub channel_id: u16,
    #[pyo3(get)]
    pub sequence: u32,
    #[pyo3(get)]
    pub log_time: u64,
    #[pyo3(get)]
    pub publish_time: u64,
    data: PyObject,
}

#[pymethods]
impl PyDecodedMessage {
    #[getter]
    fn data(&self, py: Python<'_>) -> PyObject {
        self.data.clone_ref(py)
    }
}

/// Python wrapper for MCAP file reader.
#[pyclass]
pub struct PyMcapFileReader {
    reader: Arc<Mutex<McapReader<FileReader>>>,
    schema_parser: Ros2MsgParser,
    parsed_schemas: HashMap<u16, (Schema, HashMap<String, Schema>)>,
}

#[pymethods]
impl PyMcapFileReader {
    /// Open an MCAP file for reading.
    #[staticmethod]
    #[pyo3(signature = (file_path, enable_crc_check=false))]
    fn from_file(file_path: &str, enable_crc_check: bool) -> PyResult<Self> {
        let reader = McapReader::open(file_path, enable_crc_check)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("{}", e)))?;

        Ok(Self {
            reader: Arc::new(Mutex::new(reader)),
            schema_parser: Ros2MsgParser::new(),
            parsed_schemas: HashMap::new(),
        })
    }

    /// Get the MCAP profile.
    #[getter]
    fn profile(&self) -> PyResult<String> {
        let reader = self.reader.lock().unwrap();
        Ok(reader.profile().to_string())
    }

    /// Get all topic names.
    fn get_topics(&self) -> PyResult<Vec<String>> {
        let reader = self.reader.lock().unwrap();
        Ok(reader.topics().into_iter().map(|s| s.to_string()).collect())
    }

    /// Get message count for a topic.
    fn get_message_count(&self, topic: &str) -> PyResult<Option<u64>> {
        let reader = self.reader.lock().unwrap();
        Ok(reader.message_count(topic))
    }

    /// Get start time in nanoseconds.
    #[getter]
    fn start_time(&self) -> PyResult<Option<u64>> {
        let reader = self.reader.lock().unwrap();
        Ok(reader.start_time())
    }

    /// Get end time in nanoseconds.
    #[getter]
    fn end_time(&self) -> PyResult<Option<u64>> {
        let reader = self.reader.lock().unwrap();
        Ok(reader.end_time())
    }

    /// Iterate over messages for the given topics.
    #[pyo3(signature = (topic, start_time=None, end_time=None, in_log_time_order=true, in_reverse=false))]
    fn messages(
        &mut self,
        py: Python<'_>,
        topic: &Bound<'_, PyAny>,
        start_time: Option<u64>,
        end_time: Option<u64>,
        in_log_time_order: bool,
        in_reverse: bool,
    ) -> PyResult<Vec<PyDecodedMessage>> {
        // Get topic list
        let topics: Vec<String> = if let Ok(s) = topic.extract::<String>() {
            vec![s]
        } else if let Ok(list) = topic.extract::<Vec<String>>() {
            list
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "topic must be a string or list of strings",
            ));
        };

        // Get channel IDs for topics
        let reader = self.reader.lock().unwrap();
        let channel_ids: Vec<u16> = topics
            .iter()
            .filter_map(|t| reader.channel_id_by_topic(t))
            .collect();

        if channel_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Build schema cache for channels we need
        let mut schemas_to_parse: Vec<(u16, u16)> = Vec::new(); // (channel_id, schema_id)
        for &channel_id in &channel_ids {
            if !self.parsed_schemas.contains_key(&channel_id) {
                if let Some(channel) = reader.channel(channel_id) {
                    schemas_to_parse.push((channel_id, channel.schema_id));
                }
            }
        }

        // Parse schemas
        for (channel_id, schema_id) in schemas_to_parse {
            if let Some(schema_record) = reader.schema(schema_id) {
                if let Ok(parsed) = self.schema_parser.parse(&schema_record.name, &schema_record.data) {
                    self.parsed_schemas.insert(channel_id, parsed);
                }
            }
        }
        drop(reader);

        // Get messages
        let mut reader = self.reader.lock().unwrap();
        let messages = reader
            .messages(
                Some(&channel_ids),
                start_time,
                end_time,
                in_log_time_order,
                in_reverse,
            )
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("{}", e)))?;

        // Decode messages
        let mut decoded = Vec::with_capacity(messages.len());
        for msg in messages {
            let data = if let Some((schema, sub_schemas)) = self.parsed_schemas.get(&msg.channel_id) {
                // Try to decode, fall back to raw bytes on error
                match self.decode_message(py, &msg.data, schema, sub_schemas) {
                    Ok(decoded_data) => decoded_data,
                    Err(_) => PyBytes::new_bound(py, &msg.data).into_any().unbind(),
                }
            } else {
                // Return raw bytes if we can't decode
                PyBytes::new_bound(py, &msg.data).into_any().unbind()
            };

            decoded.push(PyDecodedMessage {
                channel_id: msg.channel_id,
                sequence: msg.sequence,
                log_time: msg.log_time,
                publish_time: msg.publish_time,
                data,
            });
        }

        Ok(decoded)
    }

    fn close(&self) -> PyResult<()> {
        // Nothing to do, resources are dropped when the object is garbage collected
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

impl PyMcapFileReader {
    fn decode_message(
        &self,
        py: Python<'_>,
        data: &[u8],
        schema: &Schema,
        sub_schemas: &HashMap<String, Schema>,
    ) -> PyResult<PyObject> {
        let mut decoder = CdrDecoder::new(data)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;

        self.decode_schema_fields(py, &mut decoder, schema, sub_schemas)
    }

    fn decode_schema_fields(
        &self,
        py: Python<'_>,
        decoder: &mut CdrDecoder,
        schema: &Schema,
        sub_schemas: &HashMap<String, Schema>,
    ) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);

        for field in &schema.fields {
            let value = self.decode_field(py, decoder, &field.field_type, sub_schemas)?;
            dict.set_item(&field.name, value)?;
        }

        Ok(dict.into_any().unbind())
    }

    fn decode_field(
        &self,
        py: Python<'_>,
        decoder: &mut CdrDecoder,
        field_type: &FieldType,
        sub_schemas: &HashMap<String, Schema>,
    ) -> PyResult<PyObject> {
        match field_type {
            FieldType::Primitive(prim) => self.decode_primitive(py, decoder, prim),
            FieldType::String(string_type) => {
                let s = if string_type.is_wide {
                    decoder.read_wstring()
                } else {
                    decoder.read_string()
                }
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;
                Ok(s.to_object(py))
            }
            FieldType::Array { element_type, length, .. } => {
                let list = PyList::empty_bound(py);
                for _ in 0..*length {
                    let item = self.decode_field(py, decoder, element_type, sub_schemas)?;
                    list.append(item)?;
                }
                Ok(list.into_any().unbind())
            }
            FieldType::Sequence { element_type, .. } => {
                let length = decoder.read_u32()
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                    as usize;
                let list = PyList::empty_bound(py);
                for _ in 0..length {
                    let item = self.decode_field(py, decoder, element_type, sub_schemas)?;
                    list.append(item)?;
                }
                Ok(list.into_any().unbind())
            }
            FieldType::Complex { type_name } => {
                // Look up the schema
                if let Some(complex_schema) = sub_schemas.get(type_name) {
                    self.decode_schema_fields(py, decoder, complex_schema, sub_schemas)
                } else {
                    // Try to find it without "/msg/" in the name
                    let alt_name = type_name.replace("/msg/", "/");
                    if let Some(complex_schema) = sub_schemas.get(&alt_name) {
                        self.decode_schema_fields(py, decoder, complex_schema, sub_schemas)
                    } else {
                        Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "Unknown complex type: {}",
                            type_name
                        )))
                    }
                }
            }
        }
    }

    fn decode_primitive(
        &self,
        py: Python<'_>,
        decoder: &mut CdrDecoder,
        prim: &PrimitiveType,
    ) -> PyResult<PyObject> {
        let value: PyObject = match prim {
            PrimitiveType::Bool => decoder.read_bool()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Int8 => decoder.read_i8()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Uint8 => decoder.read_u8()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Int16 => decoder.read_i16()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Uint16 => decoder.read_u16()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Int32 => decoder.read_i32()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Uint32 => decoder.read_u32()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Int64 => decoder.read_i64()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Uint64 => decoder.read_u64()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Float32 => decoder.read_f32()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Float64 => decoder.read_f64()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Byte => decoder.read_byte()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?
                .to_object(py),
            PrimitiveType::Char => {
                let c = decoder.read_char()
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{}", e)))?;
                c.to_string().to_object(py)
            }
        };
        Ok(value)
    }
}

/// Python module for pybag_rs.
#[pymodule]
fn pybag_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMcapFileReader>()?;
    m.add_class::<PyDecodedMessage>()?;
    Ok(())
}
