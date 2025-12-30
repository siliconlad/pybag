"""ROS 1 message schema parsing and encoding.

ROS 1 message definitions are similar to ROS 2 but with some key differences:
- time and duration are primitive types (not messages)
- Header is std_msgs/Header (same package as ROS 2)
- Package names don't include '/msg/' in the middle
"""

import dataclasses
import hashlib
import logging
import re
from dataclasses import fields, is_dataclass
from typing import (
    Annotated,
    Any,
    Literal,
    get_args,
    get_origin,
    get_type_hints
)

from pybag.bag.records import ConnectionRecord
from pybag.io.raw_writer import BytesWriter
from pybag.mcap.records import SchemaRecord
from pybag.schema import (
    PRIMITIVE_TYPE_MAP,
    STRING_TYPE_MAP,
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaDecoder,
    SchemaEncoder,
    SchemaEntry,
    SchemaField,
    SchemaFieldType,
    Sequence,
    String
)
from pybag.types import Message

logger = logging.getLogger(__name__)


class Ros1MsgError(Exception):
    """Exception raised for errors in ROS 1 message parsing."""
    def __init__(self, message: str):
        super().__init__(message)


class _Ros1SchemaParserMixin:
    """Shared parsing logic for ROS 1 schema decoders.

    This mixin provides common functionality for parsing ROS 1 message
    definitions, used by both Ros1MsgSchemaDecoder (for bag files) and
    Ros1McapSchemaDecoder (for MCAP files).
    """

    _builtin_schemas: dict[str, Schema]

    def _create_builtin_schemas(self) -> dict[str, Schema]:
        """Create schemas for built-in ROS 1 message types."""
        builtin_schemas = {}

        # std_msgs/Header
        builtin_schemas['std_msgs/Header'] = Schema(
            'std_msgs/Header',
            {
                'seq': SchemaField(Primitive('uint32'), None),
                'stamp': SchemaField(Primitive('time'), None),
                'frame_id': SchemaField(String('string'), None),
            }
        )

        return builtin_schemas

    def _remove_inline_comment(self, line: str) -> str:
        """Remove inline comments from a line."""
        in_single = False
        in_double = False
        for i, ch in enumerate(line):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == '#' and not in_single and not in_double:
                return line[:i].rstrip()
        return line.strip()

    def _parse_value(self, field_type: SchemaFieldType, raw_value: str) -> Any:
        """Parse a default value for a field."""
        if isinstance(field_type, Primitive):
            return PRIMITIVE_TYPE_MAP.get(field_type.type, int)(raw_value)

        if isinstance(field_type, String):
            return raw_value.strip('"') if raw_value.startswith('"') else raw_value.strip("'")

        raise Ros1MsgError('Default values not supported for this field type')

    def _parse_field_type(self, field_raw_type: str, package_name: str) -> SchemaFieldType:
        """Parse a field type string into a SchemaFieldType."""
        # Handle array types: type[N] or type[]
        if array_match := re.match(r'(.*)\[(.*)\]$', field_raw_type):
            element_raw, length_spec = array_match.groups()
            element_field = self._parse_field_type(element_raw, package_name)

            if length_spec == '':
                return Sequence(element_field)
            return Array(element_field, int(length_spec), is_bounded=False)

        # Handle string types
        if field_raw_type == 'string':
            return String('string')

        # Handle primitive types (including ROS 1 specific time/duration)
        if field_raw_type in PRIMITIVE_TYPE_MAP:
            return Primitive(field_raw_type)
        if field_raw_type in ('time', 'duration'):
            return Primitive(field_raw_type)

        # Handle complex types
        if field_raw_type == 'Header':
            return Complex('std_msgs/Header')
        elif '/' not in field_raw_type:
            return Complex(f'{package_name}/{field_raw_type}')
        return Complex(field_raw_type)

    def _parse_field(self, field: str, package_name: str) -> tuple[str, SchemaEntry]:
        """Parse a single field definition."""
        # Extract the field type, name and optional constant value
        if not (match := re.match(r'(\S+)\s+(\S+)(?:\s+(.+))?$', field)):
            raise Ros1MsgError(f'Invalid field definition: {field}')
        field_raw_type, field_raw_name, raw_default = match.groups()

        if not field_raw_type:
            raise Ros1MsgError('Field type cannot be empty')
        if not field_raw_name:
            raise Ros1MsgError('Field name cannot be empty')

        # Check for constants: TYPE NAME=VALUE
        is_constant = '=' in field_raw_name or (raw_default and raw_default.startswith('='))
        if is_constant:
            if '=' in field_raw_name:
                field_raw_name, raw_default = field_raw_name.split('=', 1)
            else:
                raw_default = raw_default[1:].strip()
            if not raw_default:
                raise Ros1MsgError('Constant value cannot be empty')

        # Parse the field type
        schema_type = self._parse_field_type(field_raw_type, package_name)

        # Parse the default value if it exists
        default_value = None
        if raw_default is not None:
            default_value = self._parse_value(schema_type, raw_default)

        if is_constant:
            # default_value is guaranteed to be not None by the control flow above
            if default_value is None:
                raise Ros1MsgError('Constant value cannot be empty')
            return field_raw_name, SchemaConstant(schema_type, default_value)
        return field_raw_name, SchemaField(schema_type, default_value)

    def _add_missing_builtin_schemas(
        self,
        schema_data: bytes,
        sub_schemas: dict[str, Schema]
    ) -> None:
        """Add any missing built-in schemas that are referenced but not defined."""
        schema_text = schema_data.decode('utf-8')
        for builtin_name, builtin_schema in self._builtin_schemas.items():
            if builtin_name not in sub_schemas and builtin_name in schema_text:
                sub_schemas[builtin_name] = builtin_schema

    def _parse_message_definition(
        self,
        msg_name: str,
        msg_def: str,
        schema_data: bytes,
    ) -> tuple[Schema, dict[str, Schema]]:
        """Parse a ROS 1 message definition text into Schema objects.

        Args:
            msg_name: The full message type name (e.g., 'std_msgs/Header').
            msg_def: The message definition text.
            schema_data: The raw schema data bytes (for builtin schema detection).

        Returns:
            Tuple of (main_schema, sub_schemas).
        """
        package_name = msg_name.split('/')[0]

        # Remove comments and empty lines
        lines = [self._remove_inline_comment(line) for line in msg_def.split('\n')]
        lines = [line for line in lines if line]
        msg = '\n'.join(lines)

        # Split along '=' delimiter (80 '=' characters separates message defs)
        msg_parts = [m.strip() for m in msg.split('=' * 80)]

        msg_schema: dict[str, SchemaEntry] = {}
        # The first message does not have the 'MSG: ' prefix line
        main_fields = [m.strip() for m in msg_parts[0].split('\n') if m.strip()]
        for raw_field in main_fields:
            field_name, field = self._parse_field(raw_field, package_name)
            msg_schema[field_name] = field

        sub_msg_schemas: dict[str, Schema] = {}
        for sub_msg in msg_parts[1:]:
            if not sub_msg:
                continue
            # Format: MSG: package/MessageType
            first_line = sub_msg.split('\n')[0].strip()
            if not first_line.startswith('MSG: '):
                continue
            sub_msg_name = first_line[5:]  # Remove 'MSG: ' prefix
            sub_msg_fields = [m.strip() for m in sub_msg.split('\n')[1:] if m.strip()]
            sub_msg_schema: dict[str, SchemaEntry] = {}
            sub_msg_package_name = sub_msg_name.split('/')[0]
            for raw_field in sub_msg_fields:
                field_name, field = self._parse_field(raw_field, sub_msg_package_name)
                sub_msg_schema[field_name] = field
            sub_msg_schemas[sub_msg_name] = Schema(sub_msg_name, sub_msg_schema)

        # Add any required built-in schemas
        main_schema = Schema(msg_name, msg_schema)
        self._add_missing_builtin_schemas(schema_data, sub_msg_schemas)

        return main_schema, sub_msg_schemas


class Ros1MsgSchemaDecoder(_Ros1SchemaParserMixin, SchemaDecoder):
    """Decoder for ROS 1 message definitions from ConnectionRecord (bag files)."""

    def __init__(self):
        self._cache: dict[int, tuple[Schema, dict[str, Schema]]] = {}
        self._builtin_schemas = self._create_builtin_schemas()

    def parse_schema(self, schema: ConnectionRecord) -> tuple[Schema, dict[str, Schema]]:  # type: ignore[override]
        """Parse a ROS 1 message definition into a Schema.

        Args:
            schema: A ConnectionRecord containing the message definition.

        Returns:
            Tuple of (main_schema, sub_schemas).
        """
        if schema.conn in self._cache:
            return self._cache[schema.conn]

        conn_header = schema.connection_header
        result = self._parse_message_definition(
            conn_header.type,
            conn_header.message_definition,
            conn_header.message_definition.encode('utf-8'),
        )

        self._cache[schema.conn] = result
        return result


class Ros1McapSchemaDecoder(_Ros1SchemaParserMixin, SchemaDecoder):
    """Decoder for ROS 1 message definitions from MCAP SchemaRecord.

    This decoder is specifically designed for parsing ROS 1 message schemas
    stored in MCAP files, where the schema is provided as a SchemaRecord
    rather than a ConnectionRecord.
    """

    def __init__(self):
        self._cache: dict[int, tuple[Schema, dict[str, Schema]]] = {}
        self._builtin_schemas = self._create_builtin_schemas()

    def parse_schema(self, schema: SchemaRecord) -> tuple[Schema, dict[str, Schema]]:  # type: ignore[override]
        """Parse a ROS 1 message definition from MCAP SchemaRecord into a Schema.

        Args:
            schema: A SchemaRecord containing the message definition.

        Returns:
            Tuple of (main_schema, sub_schemas).
        """
        if schema.id in self._cache:
            return self._cache[schema.id]

        if schema.encoding != "ros1msg":
            raise Ros1MsgError(f"Expected ros1msg encoding, got: {schema.encoding}")

        result = self._parse_message_definition(
            schema.name,
            schema.data.decode('utf-8'),
            schema.data,
        )

        self._cache[schema.id] = result
        return result


class Ros1MsgSchemaEncoder(SchemaEncoder):
    """Encoder for ROS 1 message definitions."""

    def __init__(self):
        self._cache = None

    @classmethod
    def encoding(cls) -> str:
        """Return the encoding name."""
        return "ros1msg"

    def _extract_literal_int(self, literal_type: Any) -> int:
        """Extract the value from a Literal type annotation."""
        if hasattr(literal_type, '__origin__') and literal_type.__origin__ is Literal:
            if literal_args := get_args(literal_type):
                return int(literal_args[0])
            else:
                raise Ros1MsgError(f"Unknown literal type: {literal_type}")
        if isinstance(literal_type, str) and literal_type.isdigit():
            return int(literal_type)
        if isinstance(literal_type, int):
            return literal_type
        raise Ros1MsgError(f"Unknown literal type: {literal_type}")

    def _parse_annotation(self, annotation_type: Any) -> SchemaFieldType:
        """Parse a type annotation into a SchemaFieldType."""
        annotation_args = get_args(annotation_type)
        if len(annotation_args) < 2:
            raise Ros1MsgError("Field is not correctly annotated.")

        field_type = annotation_args[-1]
        if field_type[0] in PRIMITIVE_TYPE_MAP:
            return Primitive(field_type[0])
        if field_type[0] in ('time', 'duration'):
            return Primitive(field_type[0])

        if field_type[0] in STRING_TYPE_MAP:
            return String(field_type[0])

        if field_type[0] == 'array':
            sub_type = self._parse_annotation(field_type[1])
            if (length := field_type[2]) is None:
                return Sequence(sub_type)
            actual_length = self._extract_literal_int(length)
            return Array(sub_type, length=actual_length, is_bounded=False)

        if field_type[0] == 'complex':
            return Complex(field_type[1])

        if field_type[0] == 'constant':
            return self._parse_annotation(field_type[1])

        raise Ros1MsgError(f"Unknown field type: {field_type}")

    def _parse_default_value(self, annotation: dataclasses.Field) -> Any:
        """Parse the default value from a dataclass field."""
        if annotation.default is not dataclasses.MISSING:
            return annotation.default
        if annotation.default_factory is not dataclasses.MISSING:
            return annotation.default_factory()
        return None

    def _parse_message(self, message: Message | type[Message]) -> tuple[Schema, dict[str, Schema]]:
        """Parse a message dataclass into a Schema."""
        if not is_dataclass(message):
            raise TypeError("Expected a dataclass instance")

        cls = message if isinstance(message, type) else type(message)
        class_name = cls.__msg_name__

        schema = Schema(class_name, {})
        sub_schemas: dict[str, Schema] = {}

        # Use get_type_hints to resolve string annotations from
        # `from __future__ import annotations`
        type_hints = get_type_hints(cls, include_extras=True)

        for field in fields(cls):
            # Get the resolved type from type_hints instead of field.type
            # This handles `from __future__ import annotations` which causes
            # field.type to be a string instead of the actual type
            field_type_hint = type_hints.get(field.name, field.type)

            if get_origin(field_type_hint) is not Annotated:
                if hasattr(field_type_hint, '__msg_name__'):
                    resolved_field_type = Complex(field_type_hint.__msg_name__)
                    field_default = self._parse_default_value(field)
                    schema.fields[field.name] = SchemaField(resolved_field_type, field_default)
                    sub_schema, sub_sub_schemas = self._parse_message(field_type_hint)
                    sub_schemas[sub_schema.name] = sub_schema
                    sub_schemas.update(sub_sub_schemas)
                    continue
                else:
                    raise Ros1MsgError(f"Field '{field.name}' is not correctly annotated.")

            resolved_field_type = self._parse_annotation(field_type_hint)
            field_default = self._parse_default_value(field)

            if get_args(field_type_hint)[-1][0] == 'constant':
                schema.fields[field.name] = SchemaConstant(resolved_field_type, field_default)
                continue
            schema.fields[field.name] = SchemaField(resolved_field_type, field_default)

            # Handle nested complex types
            if isinstance(resolved_field_type, (Sequence, Array)):
                if isinstance(resolved_field_type.type, Complex):
                    list_type = get_args(field_type_hint)[0]
                    if get_origin(list_type) is list and get_args(list_type):
                        complex_annotation = get_args(list_type)[0]
                        if get_origin(complex_annotation) is Annotated:
                            complex_type = get_args(complex_annotation)[0]
                            sub_schema, sub_sub_schemas = self._parse_message(complex_type)
                            sub_schemas[sub_schema.name] = sub_schema
                            sub_schemas.update(sub_sub_schemas)

            if isinstance(resolved_field_type, Complex):
                complex_type = get_args(field_type_hint)[0]
                sub_schema, sub_sub_schemas = self._parse_message(complex_type)
                sub_schemas[sub_schema.name] = sub_schema
                sub_schemas.update(sub_sub_schemas)

        return schema, sub_schemas

    def _type_str(self, field_type: SchemaFieldType) -> str:
        """Convert a SchemaFieldType to a ROS 1 type string."""
        if isinstance(field_type, Primitive):
            return field_type.type
        if isinstance(field_type, String):
            return 'string'
        if isinstance(field_type, Array):
            sub_type = self._type_str(field_type.type)
            return f'{sub_type}[{field_type.length}]'
        if isinstance(field_type, Sequence):
            return f'{self._type_str(field_type.type)}[]'
        if isinstance(field_type, Complex):
            # Remove '/msg/' for ROS 1 style
            return field_type.type.replace('/msg/', '/')
        raise Ros1MsgError(f'Unknown field type: {field_type}')

    def _value_str(self, value: Any) -> str:
        """Convert a value to its string representation."""
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, (int, float, str, bytes)):
            return str(value)
        if isinstance(value, list):
            return f'[{", ".join(self._value_str(v) for v in value)}]'
        raise Ros1MsgError(f'Unknown value type: {type(value)}')

    def _encode_constant(self, writer: BytesWriter, field_name: str, field: SchemaConstant) -> None:
        """Encode a constant field."""
        encoded_type = self._type_str(field.type)
        encoded_name = field_name.upper()
        encoded_value = self._value_str(field.value)
        writer.write(f'{encoded_type} {encoded_name}={encoded_value}\n'.encode('utf-8'))

    def _encode_field(self, writer: BytesWriter, field_name: str, field: SchemaField) -> None:
        """Encode a regular field."""
        encoded_type = self._type_str(field.type)
        writer.write(f'{encoded_type} {field_name}\n'.encode('utf-8'))

    def encode(self, schema: Message | type[Message]) -> bytes:
        """Encode a message type into ROS 1 message definition format."""
        parsed_schema, sub_schemas = self._parse_message(schema)

        writer = BytesWriter()
        for field_name, field in parsed_schema.fields.items():
            if isinstance(field, SchemaConstant):
                self._encode_constant(writer, field_name, field)
            elif isinstance(field, SchemaField):
                self._encode_field(writer, field_name, field)

        for sub_schema in sub_schemas.values():
            writer.write(('=' * 80 + '\n').encode('utf-8'))
            writer.write(f'MSG: {sub_schema.name.replace("/msg/", "/")}\n'.encode('utf-8'))
            for field_name, field in sub_schema.fields.items():
                if isinstance(field, SchemaConstant):
                    self._encode_constant(writer, field_name, field)
                elif isinstance(field, SchemaField):
                    self._encode_field(writer, field_name, field)

        return writer.as_bytes()

    def parse_schema(self, schema: Message | type[Message]) -> tuple[Schema, dict[str, Schema]]:
        """Parse a message type into a Schema."""
        return self._parse_message(schema)


def compute_md5sum(message_definition: str, msg_type: str) -> str:
    """Compute the MD5 hash for a ROS 1 message definition.

    The MD5 sum is computed following the ROS 1 algorithm:
    1. Remove comments and normalize whitespace
    2. Constants appear first in original order as "type name=value"
    3. For builtin types: "type name"
    4. For complex types: the MD5 of the nested message replaces the type name

    Args:
        message_definition: The full message definition text (may include
            embedded sub-message definitions separated by 80 '=' characters).
        msg_type: The message type name (e.g., 'std_msgs/Header').

    Returns:
        The 32-character hexadecimal MD5 hash.
    """
    # Parse sub-message definitions from the full message definition
    sub_msg_defs = _parse_sub_message_definitions(message_definition)

    # Get the main message definition (first part before any separator)
    main_def = message_definition.split('=' * 80)[0].strip()

    # Compute MD5 text for the main message
    md5_text = _compute_md5_text(main_def, msg_type, sub_msg_defs)

    return hashlib.md5(md5_text.encode('utf-8')).hexdigest()


def _parse_sub_message_definitions(message_definition: str) -> dict[str, str]:
    """Parse embedded sub-message definitions from a full message definition.

    Sub-messages are separated by 80 '=' characters and start with 'MSG: type'.

    Args:
        message_definition: The full message definition text.

    Returns:
        Dictionary mapping message type to its definition text.
    """
    sub_msgs: dict[str, str] = {}

    # Split on the 80 '=' separator
    parts = message_definition.split('=' * 80)

    for part in parts[1:]:  # Skip the first part (main message)
        part = part.strip()
        if not part:
            continue

        lines = part.split('\n')
        first_line = lines[0].strip()

        if first_line.startswith('MSG: '):
            msg_type = first_line[5:].strip()
            # The rest is the message definition
            msg_def = '\n'.join(lines[1:]).strip()
            sub_msgs[msg_type] = msg_def

    return sub_msgs


# ROS 1 builtin types (including time and duration which are special in ROS 1)
_ROS1_BUILTIN_TYPES = {
    'bool', 'byte', 'char',
    'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64',
    'float32', 'float64',
    'string',
    'time', 'duration',
}


def _is_builtin_type(type_name: str) -> bool:
    """Check if a type is a ROS 1 builtin type."""
    # Strip array notation
    bare_type = re.sub(r'\[.*\]$', '', type_name)
    return bare_type in _ROS1_BUILTIN_TYPES


def _compute_md5_text(
    msg_def: str,
    msg_type: str,
    sub_msg_defs: dict[str, str]
) -> str:
    """Compute the canonical MD5 text for a message definition.

    Args:
        msg_def: The message definition (just fields, no embedded types).
        msg_type: The message type name.
        sub_msg_defs: Dictionary of sub-message type -> definition.

    Returns:
        The canonical text to hash for MD5 computation.
    """
    package = msg_type.split('/')[0] if '/' in msg_type else ''

    constants: list[str] = []
    fields: list[str] = []

    for line in msg_def.split('\n'):
        # Remove comments
        if '#' in line:
            line = line[:line.index('#')]
        line = line.strip()
        if not line:
            continue

        # Parse the line to determine if it's a constant or field
        # Constants have the form: TYPE NAME=VALUE
        if '=' in line:
            # It's a constant
            constants.append(line)
        else:
            # It's a field: TYPE NAME
            parts = line.split()
            if len(parts) >= 2:
                field_type = parts[0]
                field_name = parts[1]

                # Get the bare type (without array notation) for type checking
                bare_type = re.sub(r'\[.*\]$', '', field_type)

                if _is_builtin_type(field_type):
                    # Builtin type: use as-is
                    fields.append(f"{field_type} {field_name}")
                else:
                    # Complex type: compute its MD5 and use that instead
                    # Resolve the type name (add package if not specified)
                    if '/' not in bare_type:
                        if bare_type == 'Header':
                            full_type = 'std_msgs/Header'
                        else:
                            full_type = f"{package}/{bare_type}"
                    else:
                        full_type = bare_type

                    # Get the sub-message definition
                    sub_def = sub_msg_defs.get(full_type, '')
                    if not sub_def and full_type == 'std_msgs/Header':
                        # Built-in Header definition
                        sub_def = "uint32 seq\ntime stamp\nstring frame_id"

                    # Recursively compute MD5 for the sub-message
                    sub_md5 = _compute_md5_text(sub_def, full_type, sub_msg_defs)
                    sub_md5_hash = hashlib.md5(sub_md5.encode('utf-8')).hexdigest()

                    fields.append(f"{sub_md5_hash} {field_name}")

    # Combine: constants first, then fields
    result_lines = constants + fields
    return '\n'.join(result_lines)
