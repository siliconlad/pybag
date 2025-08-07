import ast
import logging
import re
from abc import ABC
from dataclasses import dataclass
from typing import Any

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
    'string': str,
    'wstring': str,
}


class Ros2MsgError(Exception):
    """Exception raised for errors in the ROS2 message parsing."""
    def __init__(self, message: str):
        super().__init__(message)


class SchemaField(ABC):
    ...


@dataclass
class Primitive(SchemaField):
    type: str
    default: int | float | bool | str | bytes | None = None

    @classmethod
    def is_primitive(cls, type: str) -> bool:
        return type in PRIMITIVE_TYPE_MAP


@dataclass
class String(SchemaField):
    type: str
    default: str | None = None
    length: int | None = None


@dataclass
class Array(SchemaField):
    type: str
    length: int
    default: list[int | float | bool | str | bytes] | None = None


@dataclass
class Sequence(SchemaField):
    type: str
    default: list[int | float | bool | str | bytes] | None = None


@dataclass
class Complex(SchemaField):
    type: str


@dataclass
class Constant(SchemaField):
    type: str
    value: int | float | bool | str | bytes


@dataclass
class Schema:
    name: str
    fields: dict[str, SchemaField]


class Ros2MsgSchema:
    def __init__(self):
        self._cache = None  # TODO: Cache messages we come across

    def _remove_inline_comment(self, line: str) -> str:
        in_single = in_double = False
        for i, ch in enumerate(line):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == '#' and not in_single and not in_double:
                return line[:i].rstrip()
        return line.strip()

    def _resolve_base_type(self, type_str: str, package_name: str) -> str:
        if type_str.startswith('wstring'):
            return 'wstring'
        if type_str in PRIMITIVE_TYPE_MAP:
            return type_str
        data_type = type_str
        if type_str == 'Header':
            data_type = 'std_msgs/Header'
        elif '/' not in type_str:
            data_type = f'{package_name}/{type_str}'
        return data_type

    def _parse_field_type(self, field_raw_type: str, package_name: str) -> SchemaField:
        array_match = re.match(r'(.*)\[(.*)\]$', field_raw_type)
        if array_match:
            element_raw, length_spec = array_match.groups()
            element_field = self._parse_field_type(element_raw, package_name)
            element_type = element_field.type
            if length_spec == '':
                return Sequence(element_type)
            if length_spec.startswith('<='):
                length = int(length_spec[2:])
            else:
                length = int(length_spec)
            return Array(element_type, length)

        if field_raw_type.startswith('string'):
            match = re.match(r'string(?:<=(\d+))?$', field_raw_type)
            if not match:
                msg = f'Invalid string field: {field_raw_type}'
                raise Ros2MsgError(msg)
            length = int(match.group(1)) if match.group(1) else None
            return String('string', length=length)

        base_type = self._resolve_base_type(field_raw_type, package_name)
        if base_type in PRIMITIVE_TYPE_MAP:
            return Primitive(base_type)
        return Complex(base_type)

    def _parse_constant_value(self, field_type: str, raw_value: str) -> int | float | bool | str | bytes | list[Any]:
        logger.debug(f'Parsing constant value: {field_type} = {raw_value}')
        raw_value = raw_value.strip()
        if field_type not in PRIMITIVE_TYPE_MAP:
            msg = f'Unknown constant type: {field_type}'
            raise Ros2MsgError(msg)
        value = ast.literal_eval(raw_value)
        py_type = PRIMITIVE_TYPE_MAP[field_type]
        if isinstance(value, list):
            return [py_type(v) for v in value]
        if not isinstance(value, py_type):
            value = py_type(value)
        return value

    def _parse_default_value(self, field_type: SchemaField, raw_value: str) -> Any:
        if isinstance(field_type, String):
            value = self._parse_constant_value(field_type.type, raw_value)
            if field_type.length is not None and len(value) > field_type.length:
                msg = 'String default exceeds length'
                raise Ros2MsgError(msg)
            return value
        if isinstance(field_type, Primitive):
            return self._parse_constant_value(field_type.type, raw_value)
        if isinstance(field_type, (Array, Sequence)):
            values = ast.literal_eval(raw_value.strip())
            if not isinstance(values, list):
                raise Ros2MsgError('Array default must be a list')
            element_type = field_type.type
            return [self._parse_constant_value(element_type, repr(v)) for v in values]
        msg = 'Default values not supported for this field type'
        raise Ros2MsgError(msg)

    def _parse_field(self, field: str, package_name: str) -> tuple[str, SchemaField]:
        field = self._remove_inline_comment(field)
        match = re.match(r'(\S+)\s+(\S+)(?:\s+(.+))?$', field)
        if not match:
            msg = f'Invalid field definition: {field}'
            raise Ros2MsgError(msg)
        field_raw_type, field_raw_name, raw_default = match.groups()

        if '=' in field_raw_name:
            field_name, raw_value = field_raw_name.split('=', 1)
            if not field_name.isupper():
                msg = 'Constant name must be uppercase'
                raise Ros2MsgError(msg)
            value = self._parse_constant_value(field_raw_type, raw_value)
            return field_name, Constant(field_raw_type, value)

        field_name = field_raw_name
        if '__' in field_name or not re.match(r'[a-z][a-z0-9_]*$', field_name):
            msg = f'Invalid field name: {field_name}'
            raise Ros2MsgError(msg)

        schema_field = self._parse_field_type(field_raw_type, package_name)
        if raw_default is not None:
            default_value = self._parse_default_value(schema_field, raw_default)
            schema_field.default = default_value  # type: ignore[assignment]
        return field_name, schema_field

    def parse(self, schema: SchemaRecord) -> tuple[Schema, dict[str, Schema]]:
        assert schema.encoding == 'ros2msg'
        logger.debug(f'Parsing schema: {schema.name}')
        package_name = schema.name.split('/')[0]
        msg = schema.data.decode('utf-8')

        # Tidy up the message schema
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
    print(parse_ros2msg(pose_schema))
    # parse_ros2msg(point_schema)
    # parse_ros2msg(header_schema)
    # parse_ros2msg(pose_with_covariance_schema)
    # parse_ros2msg(diagnostic_status_schema)
