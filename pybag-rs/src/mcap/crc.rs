//! CRC32 computation for MCAP files.

use crc32fast::Hasher;

/// Compute CRC32 checksum of data.
pub fn compute_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// CRC32 hasher for incremental computation.
pub struct Crc32Hasher {
    hasher: Hasher,
}

impl Crc32Hasher {
    /// Create a new CRC32 hasher.
    pub fn new() -> Self {
        Self {
            hasher: Hasher::new(),
        }
    }

    /// Update the hash with more data.
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Finalize and return the CRC32 value.
    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }

    /// Reset the hasher for reuse.
    pub fn reset(&mut self) {
        self.hasher = Hasher::new();
    }
}

impl Default for Crc32Hasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_crc() {
        let data = b"hello world";
        let crc = compute_crc(data);
        assert_eq!(crc, 0x0d4a1185);
    }

    #[test]
    fn test_incremental_crc() {
        let data = b"hello world";
        let mut hasher = Crc32Hasher::new();
        hasher.update(b"hello ");
        hasher.update(b"world");
        assert_eq!(hasher.finalize(), compute_crc(data));
    }
}
