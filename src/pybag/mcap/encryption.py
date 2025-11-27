"""Encryption providers for MCAP chunk and message encryption.

This module provides an abstract interface for encryption and a concrete
implementation using AES-256-GCM, which is a widely-used authenticated
encryption algorithm.
"""

import os
import struct
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from pybag.mcap.error import McapDecryptionError, McapEncryptionError


class EncryptionProvider(ABC):
    """Abstract base class for encryption providers.

    Encryption providers handle both encryption and decryption of data,
    typically used for chunk-level or message-level encryption in MCAP files.
    """

    @property
    @abstractmethod
    def algorithm(self) -> str:
        """Return the encryption algorithm identifier string.

        This string is stored in the MCAP file to identify which
        encryption algorithm was used.

        Returns:
            The algorithm identifier (e.g., "aes-256-gcm").
        """
        ...  # pragma: no cover

    @abstractmethod
    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt plaintext data.

        Args:
            plaintext: The data to encrypt.

        Returns:
            The encrypted data, including any necessary metadata
            (e.g., nonce, authentication tag) for decryption.

        Raises:
            McapEncryptionError: If encryption fails.
        """
        ...  # pragma: no cover

    @abstractmethod
    def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt ciphertext data.

        Args:
            ciphertext: The encrypted data, including any metadata
                       that was prepended during encryption.

        Returns:
            The decrypted plaintext data.

        Raises:
            McapDecryptionError: If decryption fails (e.g., invalid key,
                                corrupted data, authentication failure).
        """
        ...  # pragma: no cover


class AesGcmEncryptionProvider(EncryptionProvider):
    """AES-256-GCM encryption provider.

    This provider uses AES-256 in GCM mode, which provides both
    confidentiality and authenticity. The encrypted output format is:

        [12-byte nonce][ciphertext][16-byte authentication tag]

    The nonce is randomly generated for each encryption operation.
    """

    # AES-256 requires a 32-byte key
    KEY_SIZE = 32
    # GCM uses a 12-byte nonce (96 bits) as recommended by NIST
    NONCE_SIZE = 12
    # GCM authentication tag is 16 bytes (128 bits)
    TAG_SIZE = 16

    def __init__(self, key: bytes) -> None:
        """Initialize the AES-GCM encryption provider.

        Args:
            key: A 32-byte (256-bit) encryption key.

        Raises:
            ValueError: If the key is not exactly 32 bytes.
        """
        if len(key) != self.KEY_SIZE:
            raise ValueError(
                f"AES-256 requires a {self.KEY_SIZE}-byte key, "
                f"got {len(key)} bytes"
            )
        self._key = key

    @classmethod
    def generate_key(cls) -> bytes:
        """Generate a random 256-bit encryption key.

        Returns:
            A 32-byte random key suitable for AES-256.
        """
        return os.urandom(cls.KEY_SIZE)

    @property
    def algorithm(self) -> str:
        """Return the encryption algorithm identifier.

        Returns:
            "aes-256-gcm"
        """
        return "aes-256-gcm"

    def encrypt(self, plaintext: bytes) -> bytes:
        """Encrypt data using AES-256-GCM.

        The output format is: [nonce (12 bytes)][ciphertext][tag (16 bytes)]

        Args:
            plaintext: The data to encrypt.

        Returns:
            The encrypted data with nonce and authentication tag.

        Raises:
            McapEncryptionError: If encryption fails.
        """
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as e:
            raise McapEncryptionError(
                "The 'cryptography' package is required for encryption. "
                "Install it with: pip install cryptography"
            ) from e

        try:
            nonce = os.urandom(self.NONCE_SIZE)
            aesgcm = AESGCM(self._key)
            # AESGCM.encrypt returns ciphertext + tag concatenated
            ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, None)
            # Prepend nonce to the output
            return nonce + ciphertext_with_tag
        except Exception as e:
            raise McapEncryptionError(f"Encryption failed: {e}") from e

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Decrypt data encrypted with AES-256-GCM.

        Expects input format: [nonce (12 bytes)][ciphertext][tag (16 bytes)]

        Args:
            ciphertext: The encrypted data with nonce and tag.

        Returns:
            The decrypted plaintext.

        Raises:
            McapDecryptionError: If decryption fails.
        """
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as e:
            raise McapDecryptionError(
                "The 'cryptography' package is required for decryption. "
                "Install it with: pip install cryptography"
            ) from e

        min_size = self.NONCE_SIZE + self.TAG_SIZE
        if len(ciphertext) < min_size:
            raise McapDecryptionError(
                f"Ciphertext too short: expected at least {min_size} bytes, "
                f"got {len(ciphertext)}"
            )

        try:
            nonce = ciphertext[:self.NONCE_SIZE]
            ciphertext_with_tag = ciphertext[self.NONCE_SIZE:]
            aesgcm = AESGCM(self._key)
            return aesgcm.decrypt(nonce, ciphertext_with_tag, None)
        except Exception as e:
            raise McapDecryptionError(f"Decryption failed: {e}") from e


def encrypt_message_data(data: bytes, provider: EncryptionProvider) -> bytes:
    """Encrypt message data using the given encryption provider.

    This function is intended for message-level encryption, where individual
    message payloads are encrypted before being written to the MCAP file.

    Args:
        data: The serialized message data to encrypt.
        provider: The encryption provider to use.

    Returns:
        The encrypted message data.

    Raises:
        McapEncryptionError: If encryption fails.
    """
    return provider.encrypt(data)


def decrypt_message_data(data: bytes, provider: EncryptionProvider) -> bytes:
    """Decrypt message data using the given encryption provider.

    This function is intended for message-level decryption, where individual
    message payloads were encrypted before being written to the MCAP file.

    Args:
        data: The encrypted message data.
        provider: The encryption provider to use.

    Returns:
        The decrypted message data.

    Raises:
        McapDecryptionError: If decryption fails.
    """
    return provider.decrypt(data)
