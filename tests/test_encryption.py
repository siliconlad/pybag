"""Tests for MCAP encryption functionality."""

import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.mcap.chunk import decompress_chunk, parse_chunk_compression
from pybag.mcap.encryption import (
    AesGcmEncryptionProvider,
    EncryptionProvider,
    decrypt_message_data,
    encrypt_message_data,
)
from pybag.mcap.error import (
    McapDecryptionError,
    McapEncryptionError,
    McapUnknownEncryptionError,
)
from pybag.mcap.record_reader import McapChunkedReader
from pybag.mcap.records import ChunkRecord
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


class TestAesGcmEncryptionProvider:
    """Tests for AES-GCM encryption provider."""

    def test_generate_key(self):
        """Test that generate_key produces valid 32-byte keys."""
        key = AesGcmEncryptionProvider.generate_key()
        assert len(key) == 32
        # Keys should be random
        key2 = AesGcmEncryptionProvider.generate_key()
        assert key != key2

    def test_invalid_key_size(self):
        """Test that invalid key sizes raise ValueError."""
        with pytest.raises(ValueError, match="32-byte key"):
            AesGcmEncryptionProvider(b"short")
        with pytest.raises(ValueError, match="32-byte key"):
            AesGcmEncryptionProvider(b"x" * 16)  # AES-128 key size
        with pytest.raises(ValueError, match="32-byte key"):
            AesGcmEncryptionProvider(b"x" * 64)  # Too long

    def test_algorithm_property(self):
        """Test the algorithm property returns correct identifier."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)
        assert provider.algorithm == "aes-256-gcm"

    def test_encrypt_decrypt_roundtrip(self):
        """Test that data can be encrypted and decrypted successfully."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"Hello, World! This is a test message."
        ciphertext = provider.encrypt(plaintext)

        # Ciphertext should be different from plaintext
        assert ciphertext != plaintext

        # Ciphertext should be larger (nonce + tag overhead)
        assert len(ciphertext) > len(plaintext)

        # Decrypt should return original plaintext
        decrypted = provider.decrypt(ciphertext)
        assert decrypted == plaintext

    def test_encrypt_empty_data(self):
        """Test encrypting empty data."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b""
        ciphertext = provider.encrypt(plaintext)
        decrypted = provider.decrypt(ciphertext)
        assert decrypted == plaintext

    def test_encrypt_large_data(self):
        """Test encrypting large data."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"x" * 1024 * 1024  # 1 MB
        ciphertext = provider.encrypt(plaintext)
        decrypted = provider.decrypt(ciphertext)
        assert decrypted == plaintext

    def test_each_encryption_unique(self):
        """Test that encrypting same data twice produces different ciphertext."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"Same message"
        ciphertext1 = provider.encrypt(plaintext)
        ciphertext2 = provider.encrypt(plaintext)

        # Ciphertexts should be different due to random nonce
        assert ciphertext1 != ciphertext2

        # But both should decrypt to the same plaintext
        assert provider.decrypt(ciphertext1) == plaintext
        assert provider.decrypt(ciphertext2) == plaintext

    def test_wrong_key_fails(self):
        """Test that decryption with wrong key fails."""
        key1 = AesGcmEncryptionProvider.generate_key()
        key2 = AesGcmEncryptionProvider.generate_key()

        provider1 = AesGcmEncryptionProvider(key1)
        provider2 = AesGcmEncryptionProvider(key2)

        plaintext = b"Secret message"
        ciphertext = provider1.encrypt(plaintext)

        with pytest.raises(McapDecryptionError):
            provider2.decrypt(ciphertext)

    def test_tampered_ciphertext_fails(self):
        """Test that tampered ciphertext fails authentication."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"Important data"
        ciphertext = provider.encrypt(plaintext)

        # Tamper with the ciphertext
        tampered = bytearray(ciphertext)
        tampered[20] ^= 0xFF  # Flip some bits
        tampered = bytes(tampered)

        with pytest.raises(McapDecryptionError):
            provider.decrypt(tampered)

    def test_truncated_ciphertext_fails(self):
        """Test that truncated ciphertext fails."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        # Minimum size is nonce (12) + tag (16) = 28 bytes
        with pytest.raises(McapDecryptionError, match="too short"):
            provider.decrypt(b"short")

        with pytest.raises(McapDecryptionError, match="too short"):
            provider.decrypt(b"x" * 27)


class TestParseChunkCompression:
    """Tests for parsing the compression field."""

    def test_compression_only(self):
        """Test parsing compression-only values."""
        assert parse_chunk_compression("lz4") == ("", "lz4")
        assert parse_chunk_compression("zstd") == ("", "zstd")
        assert parse_chunk_compression("") == ("", "")

    def test_encryption_only(self):
        """Test parsing encryption-only values."""
        assert parse_chunk_compression("aes-256-gcm") == ("aes-256-gcm", "")

    def test_encryption_plus_compression(self):
        """Test parsing combined encryption+compression values."""
        assert parse_chunk_compression("aes-256-gcm+lz4") == ("aes-256-gcm", "lz4")
        assert parse_chunk_compression("aes-256-gcm+zstd") == ("aes-256-gcm", "zstd")


class TestDecompressChunkWithEncryption:
    """Tests for chunk decompression with encryption."""

    def test_encrypted_chunk_without_provider_fails(self):
        """Test that reading encrypted chunk without provider fails."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"chunk data"
        encrypted = provider.encrypt(plaintext)

        chunk = ChunkRecord(
            message_start_time=0,
            message_end_time=1000,
            uncompressed_size=len(plaintext),
            uncompressed_crc=0,
            compression="aes-256-gcm",
            records=encrypted,
        )

        with pytest.raises(McapUnknownEncryptionError, match="no encryption provider"):
            decompress_chunk(chunk)

    def test_encrypted_chunk_with_wrong_algorithm_fails(self):
        """Test that mismatched algorithm fails."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"chunk data"
        encrypted = provider.encrypt(plaintext)

        # Chunk claims to use different encryption
        chunk = ChunkRecord(
            message_start_time=0,
            message_end_time=1000,
            uncompressed_size=len(plaintext),
            uncompressed_crc=0,
            compression="aes-128-gcm",  # Different algorithm
            records=encrypted,
        )

        with pytest.raises(McapUnknownEncryptionError, match="aes-128-gcm"):
            decompress_chunk(chunk, encryption_provider=provider)

    def test_encrypted_chunk_decryption(self):
        """Test successful decryption of encrypted chunk."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        plaintext = b"chunk data content"
        encrypted = provider.encrypt(plaintext)

        chunk = ChunkRecord(
            message_start_time=0,
            message_end_time=1000,
            uncompressed_size=len(plaintext),
            uncompressed_crc=0,
            compression="aes-256-gcm",
            records=encrypted,
        )

        decrypted = decompress_chunk(chunk, encryption_provider=provider)
        assert decrypted == plaintext


class TestMessageLevelEncryption:
    """Tests for message-level encryption helpers."""

    def test_encrypt_decrypt_message_data(self):
        """Test message data encryption/decryption."""
        key = AesGcmEncryptionProvider.generate_key()
        provider = AesGcmEncryptionProvider(key)

        message_data = b"\x00\x01\x02\x03" * 100
        encrypted = encrypt_message_data(message_data, provider)
        decrypted = decrypt_message_data(encrypted, provider)
        assert decrypted == message_data


class TestChunkEncryptionIntegration:
    """Integration tests for chunk-level encryption."""

    @pytest.mark.parametrize(
        "compression",
        [
            pytest.param(None, id="no_compression"),
            pytest.param("lz4", id="lz4"),
            pytest.param("zstd", id="zstd"),
        ],
    )
    def test_write_read_encrypted_mcap(self, compression):
        """Test writing and reading encrypted MCAP files."""
        key = AesGcmEncryptionProvider.generate_key()
        encryption = AesGcmEncryptionProvider(key)

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write encrypted MCAP
            with McapFileWriter.open(
                temp_path,
                chunk_size=1024,
                chunk_compression=compression,
                chunk_encryption=encryption,
            ) as writer:
                writer.write_message("/topic1", 1000, std_msgs.String(data="hello"))
                writer.write_message("/topic1", 2000, std_msgs.String(data="world"))
                writer.write_message("/topic2", 1500, std_msgs.Int32(data=42))

            # Read encrypted MCAP with correct key
            with McapFileReader.from_file(
                temp_path, encryption_provider=encryption
            ) as reader:
                topics = reader.get_topics()
                assert "/topic1" in topics
                assert "/topic2" in topics

                messages = list(reader.messages("/topic1"))
                assert len(messages) == 2
                assert messages[0].data.data == "hello"
                assert messages[1].data.data == "world"

                messages = list(reader.messages("/topic2"))
                assert len(messages) == 1
                assert messages[0].data.data == 42

        finally:
            temp_path.unlink()

    def test_read_without_key_fails(self):
        """Test that reading encrypted MCAP without key fails."""
        key = AesGcmEncryptionProvider.generate_key()
        encryption = AesGcmEncryptionProvider(key)

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write encrypted MCAP
            with McapFileWriter.open(
                temp_path,
                chunk_size=1024,
                chunk_compression="lz4",
                chunk_encryption=encryption,
            ) as writer:
                writer.write_message("/topic", 1000, std_msgs.String(data="secret"))

            # Try to read without encryption provider
            with McapFileReader.from_file(temp_path) as reader:
                with pytest.raises(McapUnknownEncryptionError):
                    list(reader.messages("/topic"))

        finally:
            temp_path.unlink()

    def test_read_with_wrong_key_fails(self):
        """Test that reading encrypted MCAP with wrong key fails."""
        key1 = AesGcmEncryptionProvider.generate_key()
        key2 = AesGcmEncryptionProvider.generate_key()
        encryption1 = AesGcmEncryptionProvider(key1)
        encryption2 = AesGcmEncryptionProvider(key2)

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write with key1
            with McapFileWriter.open(
                temp_path,
                chunk_size=1024,
                chunk_encryption=encryption1,
            ) as writer:
                writer.write_message("/topic", 1000, std_msgs.String(data="secret"))

            # Try to read with key2
            with McapFileReader.from_file(
                temp_path, encryption_provider=encryption2
            ) as reader:
                with pytest.raises(McapDecryptionError):
                    list(reader.messages("/topic"))

        finally:
            temp_path.unlink()

    def test_metadata_and_attachments_not_encrypted(self):
        """Test that metadata and attachments are not affected by chunk encryption."""
        key = AesGcmEncryptionProvider.generate_key()
        encryption = AesGcmEncryptionProvider(key)

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write with encryption
            with McapFileWriter.open(
                temp_path,
                chunk_size=1024,
                chunk_encryption=encryption,
            ) as writer:
                writer.write_message("/topic", 1000, std_msgs.String(data="msg"))
                writer.write_attachment("test.txt", b"attachment data")
                writer.write_metadata("info", {"key": "value"})

            # Read with encryption - should be able to read metadata/attachments
            with McapFileReader.from_file(
                temp_path, encryption_provider=encryption
            ) as reader:
                attachments = reader.get_attachments()
                assert len(attachments) == 1
                assert attachments[0].data == b"attachment data"

                metadata = reader.get_metadata()
                assert len(metadata) == 1
                assert metadata[0].metadata == {"key": "value"}

        finally:
            temp_path.unlink()

    def test_multiple_chunks_encrypted(self):
        """Test encryption with multiple chunks."""
        key = AesGcmEncryptionProvider.generate_key()
        encryption = AesGcmEncryptionProvider(key)

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write many messages to create multiple chunks
            with McapFileWriter.open(
                temp_path,
                chunk_size=100,  # Small chunk size to force multiple chunks
                chunk_compression="lz4",
                chunk_encryption=encryption,
            ) as writer:
                for i in range(100):
                    writer.write_message(
                        "/topic", i * 1000, std_msgs.String(data=f"message_{i}")
                    )

            # Verify we have multiple chunks
            with McapChunkedReader.from_file(
                temp_path, encryption_provider=encryption
            ) as reader:
                chunk_indexes = reader.get_chunk_indexes()
                assert len(chunk_indexes) > 1

            # Read all messages
            with McapFileReader.from_file(
                temp_path, encryption_provider=encryption
            ) as reader:
                messages = list(reader.messages("/topic"))
                assert len(messages) == 100
                for i, msg in enumerate(messages):
                    assert msg.data.data == f"message_{i}"

        finally:
            temp_path.unlink()


class TestNonChunkedEncryption:
    """Tests for non-chunked (encryption not applicable) files."""

    def test_non_chunked_file_no_encryption_option(self):
        """Test that non-chunked files work normally without encryption."""
        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as f:
            temp_path = Path(f.name)

        try:
            # Write non-chunked MCAP (no chunk_size)
            with McapFileWriter.open(
                temp_path,
                chunk_size=None,  # Non-chunked
            ) as writer:
                writer.write_message("/topic", 1000, std_msgs.String(data="hello"))

            # Read normally
            with McapFileReader.from_file(temp_path) as reader:
                messages = list(reader.messages("/topic"))
                assert len(messages) == 1
                assert messages[0].data.data == "hello"

        finally:
            temp_path.unlink()
