import logging
import re
from typing import Any

from pybag.mcap.records import SchemaRecord
from pybag.schema import (
    PRIMITIVE_TYPE_MAP,
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaDecoder,
    SchemaEntry,
    SchemaField,
    SchemaFieldType,
    Sequence,
    String,
)

logger = logging.getLogger(__name__)


class Ros1MsgError(Exception):
    """Exception raised for errors in the ROS1 message parsing."""


class Ros1MsgSchemaDecoder(SchemaDecoder):
    def __init__(self) -> None:
        self._cache: dict[int, tuple[Schema, dict[str, Schema]]] = {}

    def _remove_inline_comment(self, line: str) -> str:
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
        if isinstance(field_type, Primitive):
            return PRIMITIVE_TYPE_MAP[field_type.type](raw_value)
        if isinstance(field_type, String):
            return raw_value.strip('"') if raw_value.startswith('"') else raw_value.strip("'")
        raise Ros1MsgError('Constants must be primitive or string types')

    def _parse_field_type(self, field_raw_type: str, package_name: str) -> SchemaFieldType:
        if array_match := re.match(r'(.*)\[(.*)\]$', field_raw_type):
            element_raw, length_spec = array_match.groups()
            element_field = self._parse_field_type(element_raw, package_name)
            if length_spec == '':
                return Sequence(element_field)
            length = int(length_spec)
            return Array(element_field, length)

        if field_raw_type == 'string':
            return String('string')

        if field_raw_type in PRIMITIVE_TYPE_MAP:
            return Primitive(field_raw_type)

        if field_raw_type == 'Header':
            field_raw_type = 'std_msgs/Header'
        elif '/' not in field_raw_type:
            field_raw_type = f'{package_name}/{field_raw_type}'
        return Complex(field_raw_type)

    def _parse_field(self, field: str, package_name: str) -> tuple[str, SchemaEntry]:
        if '=' in field:
            if not (match := re.match(r'(\S+)\s+([A-Z][A-Z0-9_]*)=(.+)$', field)):
                raise Ros1MsgError(f'Invalid constant definition: {field}')
            field_raw_type, field_raw_name, raw_value = match.groups()
            schema_type = self._parse_field_type(field_raw_type, package_name)
            value = self._parse_value(schema_type, raw_value.strip())
            return field_raw_name, SchemaConstant(schema_type, value)

        if not (match := re.match(r'(\S+)\s+([a-z][a-z0-9_]*)$', field)):
            raise Ros1MsgError(f'Invalid field definition: {field}')
        field_raw_type, field_raw_name = match.groups()

        if '__' in field_raw_name:
            raise Ros1MsgError('Field name cannot contain double underscore "__"')
        if field_raw_name.endswith('_'):
            raise Ros1MsgError('Field name cannot end with "_"')

        schema_type = self._parse_field_type(field_raw_type, package_name)
        return field_raw_name, SchemaField(schema_type)

    def parse_schema(self, schema: SchemaRecord) -> tuple[Schema, dict[str, Schema]]:
        if schema.id in self._cache:
            return self._cache[schema.id]

        assert schema.encoding == 'ros1msg'
        logger.debug(f'Parsing schema: {schema.name}')
        package_name = schema.name.split('/')[0]
        msg = schema.data.decode('utf-8')

        lines = [self._remove_inline_comment(line) for line in msg.split('\n')]
        lines = [line for line in lines if line]
        msg = '\n'.join(lines)

        msg_parts = [m.strip() for m in msg.split('=' * 80)]

        msg_schema: dict[str, SchemaEntry] = {}
        main_fields = [m.strip() for m in msg_parts[0].split('\n') if m.strip()]
        for raw_field in main_fields:
            field_name, field = self._parse_field(raw_field, package_name)
            msg_schema[field_name] = field

        sub_msg_schemas: dict[str, Schema] = {}
        for sub_msg in msg_parts[1:]:
            sub_msg_name = sub_msg.split('\n')[0].strip()[5:]
            sub_msg_fields = [m.strip() for m in sub_msg.split('\n')[1:] if m]
            sub_msg_schema: dict[str, SchemaEntry] = {}
            for raw_field in sub_msg_fields:
                field_name, field = self._parse_field(raw_field, package_name)
                sub_msg_schema[field_name] = field
            sub_msg_schemas[sub_msg_name] = Schema(sub_msg_name, sub_msg_schema)

        result = Schema(schema.name, msg_schema), sub_msg_schemas
        self._cache[schema.id] = result
        return result
