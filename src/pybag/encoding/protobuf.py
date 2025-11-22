"""Protobuf message encoding/decoding for MCAP files.

This module provides encoder/decoder for protobuf messages. Unlike CDR,
protobuf messages are self-contained and handle their own serialization.
"""

import logging
from typing import Any

from google.protobuf.message import Message as ProtobufMessage

from pybag.encoding import MessageDecoder, MessageEncoder

logger = logging.getLogger(__name__)


class ProtobufDecoder(MessageDecoder):
    """Decoder for protobuf messages.

    This decoder wraps a protobuf message and provides a unified interface
    for deserialization. Unlike CDR, protobuf handles its own deserialization,
    so this class primarily serves as an adapter.
    """

    def __init__(self, data: bytes):
        """Initialize the decoder with raw protobuf data.

        Args:
            data: Raw protobuf-encoded bytes.
        """
        self._data = data
        self._message: ProtobufMessage | None = None

    def set_message_type(self, message_class: type[ProtobufMessage]) -> None:
        """Set the protobuf message type and parse the data.

        Args:
            message_class: The protobuf message class to deserialize into.
        """
        self._message = message_class()
        self._message.ParseFromString(self._data)

    def get_message(self) -> ProtobufMessage:
        """Get the deserialized protobuf message.

        Returns:
            The deserialized protobuf message.

        Raises:
            ValueError: If message type has not been set.
        """
        if self._message is None:
            raise ValueError("Message type not set. Call set_message_type() first.")
        return self._message

    # The following methods are required by the MessageDecoder interface
    # but are not used for protobuf messages, which handle their own deserialization.

    def parse(self, type_str: str) -> Any:
        raise NotImplementedError("Protobuf uses native deserialization")

    def bool(self) -> bool:
        raise NotImplementedError("Protobuf uses native deserialization")

    def int8(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def uint8(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def byte(self) -> bytes:
        raise NotImplementedError("Protobuf uses native deserialization")

    def char(self) -> str:
        raise NotImplementedError("Protobuf uses native deserialization")

    def int16(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def uint16(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def int32(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def uint32(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def int64(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def uint64(self) -> int:
        raise NotImplementedError("Protobuf uses native deserialization")

    def float32(self) -> float:
        raise NotImplementedError("Protobuf uses native deserialization")

    def float64(self) -> float:
        raise NotImplementedError("Protobuf uses native deserialization")

    def string(self) -> str:
        raise NotImplementedError("Protobuf uses native deserialization")

    def array(self, type: str, length: int) -> list:
        raise NotImplementedError("Protobuf uses native deserialization")

    def sequence(self, type: str) -> list:
        raise NotImplementedError("Protobuf uses native deserialization")


class ProtobufEncoder(MessageEncoder):
    """Encoder for protobuf messages.

    This encoder wraps a protobuf message and provides a unified interface
    for serialization. Unlike CDR, protobuf handles its own serialization,
    so this class primarily serves as an adapter.
    """

    def __init__(self, *, little_endian: bool = True) -> None:
        """Initialize the encoder.

        Args:
            little_endian: Ignored for protobuf (kept for interface compatibility).
        """
        self._message: ProtobufMessage | None = None

    @classmethod
    def encoding(cls) -> str:
        """Return the encoding type.

        Returns:
            The string "protobuf".
        """
        return "protobuf"

    def set_message(self, message: ProtobufMessage) -> None:
        """Set the protobuf message to encode.

        Args:
            message: The protobuf message to encode.
        """
        if not isinstance(message, ProtobufMessage):
            raise TypeError(f"Expected protobuf Message, got {type(message)}")
        self._message = message

    def save(self) -> bytes:
        """Serialize the protobuf message to bytes.

        Returns:
            The serialized protobuf message.

        Raises:
            ValueError: If no message has been set.
        """
        if self._message is None:
            raise ValueError("No message set. Call set_message() first.")
        return self._message.SerializeToString()

    # The following methods are required by the MessageEncoder interface
    # but are not used for protobuf messages, which handle their own serialization.

    def encode(self, type_str: str, value: Any) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def bool(self, value: bool) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def int8(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def uint8(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def byte(self, value: bytes) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def char(self, value: str) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def int16(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def uint16(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def int32(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def uint32(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def int64(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def uint64(self, value: int) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def float32(self, value: float) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def float64(self, value: float) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def string(self, value: str) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def array(self, type: str, values: list[Any]) -> None:
        raise NotImplementedError("Protobuf uses native serialization")

    def sequence(self, type: str, values: list[Any]) -> None:
        raise NotImplementedError("Protobuf uses native serialization")
