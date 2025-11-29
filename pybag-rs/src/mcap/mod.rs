//! MCAP file format handling.

pub mod chunk;
pub mod crc;
pub mod parser;
pub mod reader;
pub mod records;
pub mod writer;

pub use chunk::{compress_chunk, decompress_chunk};
pub use crc::compute_crc;
pub use parser::McapRecordParser;
pub use reader::McapReader;
pub use records::*;
pub use writer::McapWriter;
