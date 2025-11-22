"""Protobuf schema encoding/decoding for MCAP files.

This module handles protobuf schemas stored as FileDescriptorSets in MCAP files.
"""

import logging
from typing import TYPE_CHECKING

from google.protobuf import message_factory
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message import Message as ProtobufMessage

from pybag.schema import Schema, SchemaDecoder, SchemaEncoder

if TYPE_CHECKING:
    from pybag.mcap.records import SchemaRecord
    from pybag.types import Message

logger = logging.getLogger(__name__)


class ProtobufSchemaEncoder(SchemaEncoder):
    """Encoder for protobuf schemas stored as FileDescriptorSets."""

    @classmethod
    def encoding(cls) -> str:
        """Return the schema encoding type.

        Returns:
            The string "protobuf".
        """
        return "protobuf"

    def encode(self, schema: "Message | type[Message]") -> bytes:
        """Encode a protobuf message class as a FileDescriptorSet.

        Args:
            schema: The protobuf message class or instance.

        Returns:
            The serialized FileDescriptorSet containing the message descriptor.

        Raises:
            TypeError: If the schema is not a protobuf message.
        """
        # Get the class if an instance was passed
        schema_class = schema if isinstance(schema, type) else type(schema)

        # Verify it's a protobuf message
        if not issubclass(schema_class, ProtobufMessage):
            raise TypeError(f"Expected protobuf Message class, got {schema_class}")

        # Get the descriptor
        descriptor = schema_class.DESCRIPTOR

        # Create a FileDescriptorSet containing this message's file descriptor
        file_descriptor_set = FileDescriptorSet()

        # Add the file descriptor and all its dependencies
        self._add_file_descriptor(descriptor.file, file_descriptor_set, set())

        return file_descriptor_set.SerializeToString()

    def _add_file_descriptor(
        self,
        file_desc: FileDescriptor,
        fds: FileDescriptorSet,
        visited: set[str]
    ) -> None:
        """Recursively add a file descriptor and its dependencies to a FileDescriptorSet.

        Args:
            file_desc: The file descriptor to add.
            fds: The FileDescriptorSet to add to.
            visited: Set of already-visited file names to avoid duplicates.
        """
        if file_desc.name in visited:
            return
        visited.add(file_desc.name)

        # Add dependencies first
        for dep in file_desc.dependencies:
            self._add_file_descriptor(dep, fds, visited)

        # Add this file descriptor
        file_desc_proto = FileDescriptorProto()
        file_desc.CopyToProto(file_desc_proto)
        fds.file.append(file_desc_proto)

    def parse_schema(self, schema: "Message | type[Message]") -> tuple[Schema, dict[str, Schema]]:
        """Parse a protobuf message into the internal Schema representation.

        Note: This is not typically used for protobuf messages since they use
        native protobuf descriptors, but is provided for interface compatibility.

        Args:
            schema: The protobuf message class or instance.

        Returns:
            A tuple of (main schema, sub-schemas dict).

        Raises:
            NotImplementedError: Protobuf uses native descriptors.
        """
        raise NotImplementedError(
            "Protobuf schemas use native descriptors, not the internal Schema representation"
        )


class ProtobufSchemaDecoder(SchemaDecoder):
    """Decoder for protobuf schemas stored as FileDescriptorSets."""

    def __init__(self) -> None:
        """Initialize the decoder with a descriptor pool."""
        self._descriptor_pool = DescriptorPool()
        # Cache of schema_id -> message class
        self._message_classes: dict[int, type[ProtobufMessage]] = {}

    def parse_schema(self, schema: "SchemaRecord") -> tuple[Schema, dict[str, Schema]]:
        """Parse a FileDescriptorSet and create a protobuf message class.

        Note: This doesn't return the internal Schema representation. Instead,
        it registers the descriptors with the descriptor pool and creates a
        message class that can be retrieved via get_message_class().

        Args:
            schema: The schema record containing a serialized FileDescriptorSet.

        Returns:
            A tuple of (empty Schema, empty dict) for interface compatibility.

        Raises:
            ValueError: If the schema data is not a valid FileDescriptorSet.
        """
        # Parse the FileDescriptorSet
        file_descriptor_set = FileDescriptorSet()
        try:
            file_descriptor_set.ParseFromString(schema.data)
        except Exception as e:
            raise ValueError(f"Failed to parse FileDescriptorSet: {e}") from e

        # Add all file descriptors to the pool
        for file_descriptor_proto in file_descriptor_set.file:
            try:
                self._descriptor_pool.Add(file_descriptor_proto)
            except Exception:
                # Descriptor might already exist, which is fine
                pass

        # Find the message descriptor by name
        # The schema name should be the fully qualified protobuf message name
        try:
            message_descriptor = self._descriptor_pool.FindMessageTypeByName(schema.name)
        except KeyError as e:
            raise ValueError(
                f"Message type '{schema.name}' not found in FileDescriptorSet"
            ) from e

        # Create and cache the message class
        # Use message_factory.GetMessageClass for protobuf 6.x+
        message_class = message_factory.GetMessageClass(message_descriptor)
        self._message_classes[schema.id] = message_class

        # Return empty Schema for interface compatibility
        # The actual message class is cached and retrieved via get_message_class()
        return Schema(name=schema.name, fields={}), {}

    def get_message_class(self, schema_id: int) -> type[ProtobufMessage]:
        """Get the protobuf message class for a schema ID.

        Args:
            schema_id: The schema ID.

        Returns:
            The protobuf message class.

        Raises:
            KeyError: If the schema ID is not found.
        """
        return self._message_classes[schema_id]
