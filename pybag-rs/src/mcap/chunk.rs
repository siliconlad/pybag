//! Chunk compression and decompression.

use crate::error::{PybagError, Result};

/// Decompress chunk data based on compression type.
pub fn decompress_chunk(compression: &str, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    match compression {
        "" | "none" => Ok(data.to_vec()),
        "lz4" => decompress_lz4(data, uncompressed_size),
        "zstd" => decompress_zstd(data),
        _ => Err(PybagError::UnknownCompression(compression.to_string())),
    }
}

/// Compress chunk data using the specified compression type.
pub fn compress_chunk(compression: &str, data: &[u8]) -> Result<Vec<u8>> {
    match compression {
        "" | "none" => Ok(data.to_vec()),
        "lz4" => compress_lz4(data),
        "zstd" => compress_zstd(data),
        _ => Err(PybagError::UnknownCompression(compression.to_string())),
    }
}

fn decompress_lz4(data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
    let mut output = vec![0u8; uncompressed_size];
    lz4::block::decompress_to_buffer(data, None, &mut output)
        .map_err(|e| PybagError::DecompressionError(format!("LZ4 decompression failed: {}", e)))?;
    Ok(output)
}

fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    lz4::block::compress(data, None, false)
        .map_err(|e| PybagError::CompressionError(format!("LZ4 compression failed: {}", e)))
}

fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data)
        .map_err(|e| PybagError::DecompressionError(format!("Zstd decompression failed: {}", e)))
}

fn compress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::encode_all(data, 3)
        .map_err(|e| PybagError::CompressionError(format!("Zstd compression failed: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world hello world hello world";
        let compressed = compress_lz4(data).unwrap();
        let decompressed = decompress_lz4(&compressed, data.len()).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"hello world hello world hello world";
        let compressed = compress_zstd(data).unwrap();
        let decompressed = decompress_zstd(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_no_compression() {
        let data = b"hello world";
        let result = decompress_chunk("", data, data.len()).unwrap();
        assert_eq!(data.as_slice(), result.as_slice());
    }
}
