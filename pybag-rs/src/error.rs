//! Error types for the pybag Rust library.

use thiserror::Error;

/// Main error type for pybag operations.
#[derive(Error, Debug)]
pub enum PybagError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid MCAP file: {0}")]
    InvalidMcap(String),

    #[error("Invalid magic bytes")]
    InvalidMagicBytes,

    #[error("Unexpected record type: expected {expected}, got {got}")]
    UnexpectedRecordType { expected: u8, got: u8 },

    #[error("CRC mismatch: expected {expected}, got {computed}")]
    CrcMismatch { expected: u32, computed: u32 },

    #[error("Unknown compression: {0}")]
    UnknownCompression(String),

    #[error("Decompression error: {0}")]
    DecompressionError(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("Unknown topic: {0}")]
    UnknownTopic(String),

    #[error("Unknown channel ID: {0}")]
    UnknownChannel(u16),

    #[error("Unknown schema ID: {0}")]
    UnknownSchema(u16),

    #[error("Unknown encoding: {0}")]
    UnknownEncoding(String),

    #[error("Schema parsing error: {0}")]
    SchemaParseError(String),

    #[error("CDR decoding error: {0}")]
    CdrDecodeError(String),

    #[error("CDR encoding error: {0}")]
    CdrEncodeError(String),

    #[error("Buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("End of file reached unexpectedly")]
    UnexpectedEof,

    #[error("Python error: {0}")]
    PythonError(String),
}

/// Result type alias for pybag operations.
pub type Result<T> = std::result::Result<T, PybagError>;
