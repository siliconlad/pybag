import lz4.frame
import zstandard as zstd

from pybag.mcap.crc import assert_crc
from pybag.mcap.error import McapUnknownCompressionError
from pybag.mcap.records import ChunkRecord


def decompress_chunk(chunk: ChunkRecord, *, check_crc: bool = False) -> bytes:
    """Decompress the records field of a chunk."""
    if chunk.compression == 'zstd':
        chunk_data = zstd.ZstdDecompressor().decompress(chunk.records)
    elif chunk.compression == 'lz4':
        chunk_data = lz4.frame.decompress(chunk.records)
    elif chunk.compression == '':
        chunk_data = chunk.records
    else:
        error_msg = f'Unknown compression type: {chunk.compression}'
        raise McapUnknownCompressionError(error_msg)

    # Validate the CRC if requested
    if check_crc and chunk.uncompressed_crc != 0:
        assert_crc(chunk_data, chunk.uncompressed_crc)
    return chunk_data
