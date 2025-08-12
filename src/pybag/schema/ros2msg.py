import ast
import dataclasses
import logging
import re
from abc import ABC
from dataclasses import dataclass, fields, is_dataclass
from typing import Annotated, Any, Tuple, get_args, get_origin

from pybag.mcap.records import SchemaRecord

logger = logging.getLogger(__name__)

# Map ROS2 types to Python types
# https://docs.ros.org/en/foxy/Concepts/About-ROS-Interfaces.html#field-types
PRIMITIVE_TYPE_MAP = {
    'bool': bool,
    'byte': bytes,
    'char': str,
    'float32': float,
    'float64': float,
    'int8': int,
    'uint8': int,
    'int16': int,
    'uint16': int,
    'int32': int,
    'uint32': int,
    'int64': int,
    'uint64': int,
}
STRING_TYPE_MAP = {
    'string': str,
    'wstring': str,
}


class Ros2MsgError(Exception):
    """Exception raised for errors in the ROS2 message parsing."""
    def __init__(self, message: str):
        super().__init__(message)

# Schema Field Types

@dataclass
class SchemaFieldType(ABC):
    ...


@dataclass
class Primitive(SchemaFieldType):
    type: str

    @classmethod
    def is_primitive(cls, type: str) -> bool:
        return type in PRIMITIVE_TYPE_MAP


@dataclass
class String(SchemaFieldType):
    type: str = 'string'
    max_length: int | None = None


@dataclass
class Array(SchemaFieldType):
    type: 'SchemaFieldType'
    length: int
    is_bounded: bool = False  # False == fixed length


@dataclass
class Sequence(SchemaFieldType):
    type: 'SchemaFieldType'


@dataclass
class Complex(SchemaFieldType):
    type: str

# Schema Entry (one line of a message)

@dataclass
class SchemaEntry(ABC):
    ...


@dataclass
class SchemaConstant(SchemaEntry):
    type: SchemaFieldType
    value: int | float | bool | str | bytes


@dataclass
class SchemaField(SchemaEntry):
    type: SchemaFieldType
    default: Any = None

# Schema (one entire message)

@dataclass
class Schema:
    name: str
    fields: dict[str, SchemaEntry]


class Ros2MsgSchemaDecoder:
    def __init__(self):
        self._cache = None  # TODO: Cache messages we come across

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
            if field_type.max_length is not None and len(raw_value) > field_type.max_length:
                raise Ros2MsgError('String default exceeds length')
            return raw_value.strip('"') if raw_value.startswith('"') else raw_value.strip("'")

        if isinstance(field_type, (Array, Sequence)):
            values = ast.literal_eval(raw_value.strip())
            if not isinstance(values, list):
                raise Ros2MsgError('Array default must be a list')
            element_type = field_type.type
            return [PRIMITIVE_TYPE_MAP[element_type.type](v) for v in values]

        raise Ros2MsgError('Default values not supported for this field type')

    def _parse_field_type(self, field_raw_type: str, package_name: str) -> SchemaFieldType:
        # Handle array and sequence types
        if array_match := re.match(r'(.*)\[(.*)\]$', field_raw_type):
            element_raw, length_spec = array_match.groups()
            element_field = self._parse_field_type(element_raw, package_name)

            if length_spec == '':
                return Sequence(element_field)

            is_bounded = length_spec.startswith('<=')
            length = int(length_spec[2:]) if is_bounded else int(length_spec)
            return Array(element_field, length, is_bounded)

        # Handle string types
        if field_raw_type.startswith('string'):
            if not (match := re.match(r'string(?:<=(\d+))?$', field_raw_type)):
                raise Ros2MsgError(f'Invalid string field: {field_raw_type}')
            length = int(match.group(1)) if match.group(1) else None
            return String('string', max_length=length)
        if field_raw_type.startswith('wstring'):
            if not (match := re.match(r'wstring(?:<=(\d+))?$', field_raw_type)):
                raise Ros2MsgError(f'Invalid wstring field: {field_raw_type}')
            length = int(match.group(1)) if match.group(1) else None
            return String('wstring', max_length=length)

        # Handle primitive types
        if field_raw_type in PRIMITIVE_TYPE_MAP:
            return Primitive(field_raw_type)

        # Handle complex types
        if field_raw_type == 'Header':
            field_raw_type = 'std_msgs/Header'
        elif '/' not in field_raw_type:
            field_raw_type = f'{package_name}/{field_raw_type}'
        return Complex(field_raw_type)

    def _parse_field(self, field: str, package_name: str) -> tuple[str, SchemaEntry]:
        # Extract the field type, name and optional default value
        if not (match := re.match(r'(\S+)\s+(\S+)(?:\s+(.+))?$', field)):
            raise Ros2MsgError(f'Invalid field definition: {field}')
        field_raw_type, field_raw_name, raw_default = match.groups()

        if not field_raw_type:
            raise Ros2MsgError('Field type cannot be empty')
        if not field_raw_name:
            raise Ros2MsgError('Field name cannot be empty')

        if is_constant := '=' in field_raw_name:
            field_raw_name, raw_default = field_raw_name.split('=', 1)
            if not field_raw_name.isupper():
                raise Ros2MsgError('Constant name must be uppercase')
            if not raw_default:
                raise Ros2MsgError('Constant value cannot be empty')
        else:
            if not re.match(r'[a-z][a-z0-9_]*$', field_raw_name):
                raise Ros2MsgError('Field name must be lowercase, alphanumeric or _')

        # Check the field name is valid
        if '__' in field_raw_name:
            raise Ros2MsgError('Field name cannot contain double underscore "__"')
        if field_raw_name.endswith('_'):
            raise Ros2MsgError('Field name cannot end with "_"')

        # Parse the field type
        schema_type = self._parse_field_type(field_raw_type, package_name)

        # Parse the default value if it exists
        default_value = None
        if raw_default is not None:
            # Certain field types cannot have default values
            if isinstance(schema_type, Complex):
                raise Ros2MsgError('Complex fields cannot have default values')
            if isinstance(schema_type, Array) and isinstance(schema_type.type, String):
                raise Ros2MsgError('Array of strings cannot have default values')
            if isinstance(schema_type, Sequence) and isinstance(schema_type.type, String):
                raise Ros2MsgError('Sequence of strings cannot have default values')
            default_value = self._parse_value(schema_type, raw_default)

        if is_constant:
            return field_raw_name, SchemaConstant(schema_type, default_value)
        return field_raw_name, SchemaField(schema_type, default_value)


    def parse(self, schema: SchemaRecord) -> tuple[Schema, dict[str, SchemaEntry]]:
        assert schema.encoding == 'ros2msg'
        logger.debug(f'Parsing schema: {schema.name}')

        package_name = schema.name.split('/')[0]
        msg = schema.data.decode('utf-8')

        # Remove comments and empty lines
        lines = [self._remove_inline_comment(line) for line in msg.split('\n')]
        lines = [line for line in lines if line]
        msg = '\n'.join(lines)

        # Split along '=' delimiter
        msg = [m.strip() for m in msg.split('=' * 80)]

        msg_schema = {}
        # The first message does not have the 'MSG: ' prefix line
        main_fields = [m.strip() for m in msg[0].split('\n') if m.strip()]
        for raw_field in main_fields:
            field_name, field = self._parse_field(raw_field, package_name)
            msg_schema[field_name] = field

        sub_msg_schemas = {}
        for sub_msg in msg[1:]:
            sub_msg_name = sub_msg.split('\n')[0].strip()[5:]  # Remove 'MSG: ' prefix
            sub_msg_fields = [m.strip() for m in sub_msg.split('\n')[1:] if m]
            # TODO: Do some caching here
            sub_msg_schema = {}
            for raw_field in sub_msg_fields:
                field_name, field = self._parse_field(raw_field, package_name)
                sub_msg_schema[field_name] = field
            sub_msg_schemas[sub_msg_name] = Schema(sub_msg_name, sub_msg_schema)

        return Schema(schema.name, msg_schema), sub_msg_schemas


class Ros2MsgSchemaEncoder:
    def __init__(self):
        self._cache = None  # TODO: Cache messages we come across

    def _parse_annotation(self, annotation_type: Any) -> SchemaFieldType:
        annotation_args = get_args(annotation_type)
        if len(annotation_args) < 2:
            raise Ros2MsgError(f"Field is not correctly annotated.")

        field_type = annotation_args[1]
        if field_type[0] in PRIMITIVE_TYPE_MAP:
            return Primitive(field_type[0])

        if field_type[0] in STRING_TYPE_MAP:
            return String(field_type[0])

        if field_type[0] == 'array':
            sub_type = self._parse_annotation(field_type[1])
            if (length := field_type[2]) is None:
                return Sequence(sub_type)
            return Array(sub_type, length=length, is_bounded=False)

        if field_type[0] == 'complex':
            return Complex(field_type[1])

        raise Ros2MsgError(f"Unknown field type: {field_type}")

    def _parse_default_value(self, annotation: dataclasses.Field) -> Any:
        if annotation.default is not dataclasses.MISSING:
            return annotation.default
        if annotation.default_factory is not dataclasses.MISSING:
            return annotation.default_factory()
        return None

    def encode(self, message: Any) -> Schema:
        if not is_dataclass(message):
            raise TypeError("Expected a dataclass instance")

        class_name = message.__name__ if isinstance(message, type) else type(message).__name__

        schema = Schema(class_name, {})
        sub_schemas: dict[str, Schema] = {}
        for field in fields(message):
            if get_origin(field.type) is not Annotated:
                raise Ros2MsgError(f"Field '{field.name}' is not correctly annotated.")
            field_type = self._parse_annotation(field.type)
            field_default = self._parse_default_value(field)
            schema.fields[field.name] = SchemaField(field_type, field_default)
            if isinstance(field_type, Complex):
                sub_schema, sub_sub_schemas = self.encode(getattr(message, field.name))
                sub_schemas[sub_schema.name] = sub_schema
                sub_schemas.update(sub_sub_schemas)

        return schema, sub_schemas


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    pose_schema = SchemaRecord(
        id=1,
        name="geometry_msgs/msg/Pose",
        encoding='ros2msg',
        data=b'# A representation of pose in free space, composed of position and orientation.\n\nPoint position\nQuaternion orientation\n\n================================================================================\nMSG: geometry_msgs/Point\n# This contains the position of a point in free space\nfloat64 x\nfloat64 y\nfloat64 z\n\n================================================================================\nMSG: geometry_msgs/Quaternion\n# This represents an orientation in free space in quaternion form.\n\nfloat64 x 0\nfloat64 y 0\nfloat64 z 0\nfloat64 w 1\n'
    )
    point_schema = SchemaRecord(
        id=1,
        name='geometry_msgs/msg/Point',
        encoding='ros2msg',
        data=b'# This contains the position of a point in free space\nfloat64 x\nfloat64 y\nfloat64 z\n'
    )
    header_schema = SchemaRecord(
        id=1,
        name='std_msgs/msg/Header',
        encoding='ros2msg',
        data=b'builtin_interfaces/Time stamp\nstring frame_id\n================================================================================\nMSG: builtin_interfaces/Time\nint32 sec\nuint32 nanosec\n'
    )
    pose_with_covariance_schema = SchemaRecord(
        id=1,
        name='geometry_msgs/msg/PoseWithCovariance',
        encoding='ros2msg',
        data=b'geometry_msgs/Pose pose\nfloat64[36] covariance\n================================================================================\nMSG: geometry_msgs/Pose\ngeometry_msgs/Point position\ngeometry_msgs/Quaternion orientation\n================================================================================\nMSG: geometry_msgs/Point\nfloat64 x\nfloat64 y\nfloat64 z\n================================================================================\nMSG: geometry_msgs/Quaternion\nfloat64 x\nfloat64 y\nfloat64 z\nfloat64 w\n'
    )
    diagnostic_status_schema = SchemaRecord(
        id=1,
        name='diagnostic_msgs/msg/DiagnosticStatus',
        encoding='ros2msg',
        data=b'byte OK=0\nbyte WARN=1\nbyte ERROR=2\nbyte STALE=3\nbyte level\nstring name\nstring message\nstring hardware_id\ndiagnostic_msgs/KeyValue[] values\n================================================================================\nMSG: diagnostic_msgs/KeyValue\nstring key\nstring value\n'
    )
