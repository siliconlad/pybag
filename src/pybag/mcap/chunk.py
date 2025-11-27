from __future__ import annotations

from typing import TYPE_CHECKING

import lz4.frame
import zstandard as zstd

from pybag.mcap.crc import assert_crc
from pybag.mcap.error import McapUnknownCompressionError, McapUnknownEncryptionError
from pybag.mcap.records import ChunkRecord

if TYPE_CHECKING:
    from pybag.mcap.encryption import EncryptionProvider


def parse_chunk_compression(compression: str) -> tuple[str, str]:
    """Parse the compression field to extract encryption and compression algorithms.

    The compression field can contain:
    - Just compression: "lz4", "zstd", ""
    - Just encryption: "aes-256-gcm"
    - Both: "aes-256-gcm+lz4", "aes-256-gcm+zstd"

    Args:
        compression: The compression field value from a ChunkRecord.

    Returns:
        A tuple of (encryption_algorithm, compression_algorithm).
        Either value may be an empty string if not specified.
    """
    if '+' in compression:
        # Combined format: encryption+compression
        parts = compression.split('+', 1)
        return parts[0], parts[1]
    elif compression in ('lz4', 'zstd', ''):
        # Compression only (or no compression)
        return '', compression
    else:
        # Assume it's encryption only
        return compression, ''


def decompress_chunk(
    chunk: ChunkRecord,
    *,
    check_crc: bool = False,
    encryption_provider: EncryptionProvider | None = None,
) -> bytes:
    """Decompress (and optionally decrypt) the records field of a chunk.

    When a chunk is encrypted, decryption happens first, then decompression.

    Args:
        chunk: The chunk record to decompress.
        check_crc: Whether to validate the CRC of the decompressed data.
        encryption_provider: Optional encryption provider for decrypting
                           encrypted chunks. Required if the chunk is encrypted.

    Returns:
        The decompressed (and decrypted if applicable) chunk data.

    Raises:
        McapUnknownCompressionError: If the compression type is not supported.
        McapUnknownEncryptionError: If the chunk is encrypted but no provider given,
                                   or if the encryption algorithm doesn't match.
        McapDecryptionError: If decryption fails.
    """
    encryption_algo, compression_algo = parse_chunk_compression(chunk.compression)
    data = chunk.records

    # Step 1: Decrypt if encrypted
    if encryption_algo:
        if encryption_provider is None:
            raise McapUnknownEncryptionError(
                f"Chunk is encrypted with '{encryption_algo}' but no encryption "
                "provider was given"
            )
        if encryption_provider.algorithm != encryption_algo:
            raise McapUnknownEncryptionError(
                f"Chunk is encrypted with '{encryption_algo}' but provider uses "
                f"'{encryption_provider.algorithm}'"
            )
        data = encryption_provider.decrypt(data)

    # Step 2: Decompress
    if compression_algo == 'zstd':
        chunk_data = zstd.ZstdDecompressor().decompress(data)
    elif compression_algo == 'lz4':
        chunk_data = lz4.frame.decompress(data)
    elif compression_algo == '':
        chunk_data = data
    else:
        error_msg = f'Unknown compression type: {compression_algo}'
        raise McapUnknownCompressionError(error_msg)

    # Validate the CRC if requested
    if check_crc and chunk.uncompressed_crc != 0:
        assert_crc(chunk_data, chunk.uncompressed_crc)
    return chunk_data
