//! I/O abstractions for reading and writing binary data.

mod reader;
mod writer;

pub use reader::{BytesReader, FileReader, Reader, SliceReader, SliceView};
pub use writer::{BytesWriter, FileWriter, Writer};
