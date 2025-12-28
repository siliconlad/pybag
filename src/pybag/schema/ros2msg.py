import ast
import dataclasses
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


class Ros2MsgError(Exception):
    """Exception raised for errors in the ROS2 message parsing."""
    def __init__(self, message: str):
        super().__init__(message)


class Ros2MsgSchemaDecoder(SchemaDecoder):
    def __init__(self):
        self._cache: dict[int, tuple[Schema, dict[str, Schema]]] = {}
        self._builtin_schemas = self._create_builtin_schemas()

    def _create_builtin_schemas(self) -> dict[str, Schema]:
        """Create schemas for built-in ROS2 message types."""
        builtin_schemas = {}

        # builtin_interfaces/Time
        builtin_schemas['builtin_interfaces/Time'] = Schema(
            'builtin_interfaces/Time',
            {
                'sec': SchemaField(Primitive('int32'), None),
                'nanosec': SchemaField(Primitive('uint32'), None)
            }
        )

        # builtin_interfaces/Duration
        builtin_schemas['builtin_interfaces/Duration'] = Schema(
            'builtin_interfaces/Duration',
            {
                'sec': SchemaField(Primitive('int32'), None),
                'nanosec': SchemaField(Primitive('uint32'), None)
            }
        )

        return builtin_schemas

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
            if isinstance(element_type, Primitive):
                return [PRIMITIVE_TYPE_MAP[element_type.type](v) for v in values]
            else:
                raise Ros2MsgError('Default values not supported for this field type')

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

        if is_constant := ('=' in field_raw_name or (raw_default and raw_default.startswith('='))):
            if '=' in field_raw_name:
                field_raw_name, raw_default = field_raw_name.split('=', 1)
            else:
                # TODO: Hack should be made more robust
                # Handle case where = is separated by spaces: "CONST = value"
                raw_default = raw_default[1:].strip()  # Remove the '=' and strip spaces
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
            if default_value is None:
                raise Ros2MsgError('Constant must have a default value')
            return field_raw_name, SchemaConstant(schema_type, default_value)
        return field_raw_name, SchemaField(schema_type, default_value)

    def _add_missing_builtin_schemas(
        self,
        main_schema: SchemaRecord,
        sub_schemas: dict[str, Schema]
    ) -> None:
        """Add any missing built-in schemas that are referenced but not defined."""
        schema_text = main_schema.data.decode('utf-8')
        for builtin_name, builtin_schema in self._builtin_schemas.items():
            if builtin_name not in sub_schemas and builtin_name in schema_text:
                sub_schemas[builtin_name] = builtin_schema

    def parse_schema(self, schema: SchemaRecord) -> tuple[Schema, dict[str, Schema]]:
        if schema.id in self._cache:
            return self._cache[schema.id]

        assert schema.encoding == "ros2msg"

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
            # Use the package name from the sub-message, not the main message
            sub_msg_package_name = sub_msg_name.split('/')[0]
            for raw_field in sub_msg_fields:
                field_name, field = self._parse_field(raw_field, sub_msg_package_name)
                sub_msg_schema[field_name] = field
            sub_msg_schemas[sub_msg_name] = Schema(sub_msg_name, sub_msg_schema)

        # Add any required built-in schemas
        main_schema = Schema(schema.name, msg_schema)
        self._add_missing_builtin_schemas(schema, sub_msg_schemas)
        result = main_schema, sub_msg_schemas

        self._cache[schema.id] = result
        return result


class Ros2MsgSchemaEncoder(SchemaEncoder):
    def __init__(self):
        self._cache = None  # TODO: Cache messages we come across

    @classmethod
    def encoding(cls) -> str:
        return "ros2msg"

    def _extract_literal_int(self, literal_type: Any) -> int:
        """Extract the value from a Literal type annotation."""
        if hasattr(literal_type, '__origin__') and literal_type.__origin__ is Literal:
            if literal_args := get_args(literal_type):
                return int(literal_args[0])
            else:
                raise Ros2MsgError(f"Unknown literal type: {literal_type}")
        if isinstance(literal_type, str) and literal_type.isdigit():
            return int(literal_type)
        if isinstance(literal_type, int):
            return literal_type
        raise Ros2MsgError(f"Unknown literal type: {literal_type}")

    def _parse_annotation(self, annotation_type: Any) -> SchemaFieldType:
        annotation_args = get_args(annotation_type)
        if len(annotation_args) < 2:
            raise Ros2MsgError(f"Field is not correctly annotated.")

        field_type = annotation_args[-1]
        if field_type[0] in PRIMITIVE_TYPE_MAP:
            return Primitive(field_type[0])

        if field_type[0] in STRING_TYPE_MAP:
            return String(field_type[0])

        if field_type[0] == 'array':
            sub_type = self._parse_annotation(field_type[1])
            if (length := field_type[2]) is None:
                return Sequence(sub_type)
            # Extract the actual integer value from Literal types
            actual_length = self._extract_literal_int(length)
            return Array(sub_type, length=actual_length, is_bounded=False)

        if field_type[0] == 'complex':
            return Complex(field_type[1])

        if field_type[0] == 'constant':
            return self._parse_annotation(field_type[1])

        # Provide helpful error messages for ROS 1 specific types
        if field_type[0] == 'time':
            raise Ros2MsgError(
                "ROS 1 'time' type cannot be used with ROS 2/MCAP. "
                "Use 'builtin_interfaces/Time' (with sec and nanosec fields) instead. "
                "The pybag.ros1.Time class is only for ROS 1 bag files."
            )
        if field_type[0] == 'duration':
            raise Ros2MsgError(
                "ROS 1 'duration' type cannot be used with ROS 2/MCAP. "
                "Use 'builtin_interfaces/Duration' (with sec and nanosec fields) instead. "
                "The pybag.ros1.Duration class is only for ROS 1 bag files."
            )

        raise Ros2MsgError(f"Unknown field type: {field_type}")

    def _parse_default_value(self, annotation: dataclasses.Field) -> Any:
        if annotation.default is not dataclasses.MISSING:
            return annotation.default
        if annotation.default_factory is not dataclasses.MISSING:
            return annotation.default_factory()
        return None

    def _parse_message(self, message: Message | type[Message]) -> tuple[Schema, dict[str, Schema]]:
        if not is_dataclass(message):
            raise TypeError("Expected a dataclass instance")

        cls = message if isinstance(message, type) else type(message)
        class_name = cls.__msg_name__

        schema = Schema(class_name, {})
        sub_schemas: dict[str, Schema] = {}

        # Use get_type_hints to resolve string annotations from PEP 563
        # (from __future__ import annotations)
        type_hints = get_type_hints(cls, include_extras=True)

        for field in fields(cls):
            # Get the resolved type hint instead of the raw field.type
            field_type_hint = type_hints.get(field.name, field.type)

            # Allow direct message types (with __msg_name__) without Annotated wrapper
            if get_origin(field_type_hint) is not Annotated:
                if isinstance(field_type_hint, type) and hasattr(field_type_hint, '__msg_name__'):
                    # This is a direct message type - treat it as Complex
                    field_type = Complex(field_type_hint.__msg_name__)
                    field_default = self._parse_default_value(field)
                    schema.fields[field.name] = SchemaField(field_type, field_default)
                    # Recursively parse the sub-message
                    sub_schema, sub_sub_schemas = self._parse_message(field_type_hint)
                    sub_schemas[sub_schema.name] = sub_schema
                    sub_schemas.update(sub_sub_schemas)
                    continue
                else:
                    raise Ros2MsgError(f"Field '{field.name}' is not correctly annotated.")

            field_type = self._parse_annotation(field_type_hint)
            field_default = self._parse_default_value(field)

            if get_args(field_type_hint)[-1][0] == 'constant':
                schema.fields[field.name] = SchemaConstant(field_type, field_default)
                continue
            schema.fields[field.name] = SchemaField(field_type, field_default)

            if isinstance(field_type, Sequence):
                if isinstance(field_type.type, Complex):
                    # For Sequence[Complex[Class]], extract the Class from the annotation
                    list_type = get_args(field_type_hint)[0]  # list[Annotated[Class, ...]]
                    if get_origin(list_type) is list and get_args(list_type):
                        complex_annotation = get_args(list_type)[0]  # Annotated[Class, ...]
                        if get_origin(complex_annotation) is Annotated:
                            complex_type = get_args(complex_annotation)[0]  # Class
                            sub_schema, sub_sub_schemas = self._parse_message(complex_type)
                            sub_schemas[sub_schema.name] = sub_schema
                            sub_schemas.update(sub_sub_schemas)

            if isinstance(field_type, Array):
                if isinstance(field_type.type, Complex):
                    # For Array[Complex[Class]], extract the Class from the annotation
                    list_type = get_args(field_type_hint)[0]  # list[Annotated[Class, ...]]
                    if get_origin(list_type) is list and get_args(list_type):
                        complex_annotation = get_args(list_type)[0]  # Annotated[Class, ...]
                        if get_origin(complex_annotation) is Annotated:
                            complex_type = get_args(complex_annotation)[0]  # Class
                            sub_schema, sub_sub_schemas = self._parse_message(complex_type)
                            sub_schemas[sub_schema.name] = sub_schema
                            sub_schemas.update(sub_sub_schemas)

            if isinstance(field_type, Complex):
                complex_type = get_args(field_type_hint)[0]
                sub_schema, sub_sub_schemas = self._parse_message(complex_type)
                sub_schemas[sub_schema.name] = sub_schema
                sub_schemas.update(sub_sub_schemas)

        return schema, sub_schemas

    def _type_str(self, field_type: SchemaFieldType) -> str:
        if isinstance(field_type, Primitive):
            return field_type.type
        if isinstance(field_type, String):
            if field_type.max_length is None:
                return field_type.type
            return f'{field_type.type}<={field_type.max_length}'
        if isinstance(field_type, Array):
            sub_type = self._type_str(field_type.type)
            if field_type.is_bounded:
                return f'{sub_type}[<={field_type.length}]'
            return f'{sub_type}[{field_type.length}]'
        if isinstance(field_type, Sequence):
            return f'{self._type_str(field_type.type)}[]'
        if isinstance(field_type, Complex):
            return field_type.type.replace('/msg/', '/')
        raise Ros2MsgError(f'Unknown field type: {field_type}')

    def _value_str(self, value: Any) -> str:
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, (int, float, str, bytes)):
            return str(value)
        if isinstance(value, list):
            return f'[{", ".join(self._value_str(v) for v in value)}]'
        raise Ros2MsgError(f'Unknown value type: {type(value)}')

    def _encode_constant(self, writer: BytesWriter, field_name: str, field: SchemaConstant) -> None:
        encoded_type = self._type_str(field.type)
        encoded_name = field_name.upper()
        encoded_value = self._value_str(field.value)
        writer.write(f'{encoded_type} {encoded_name}={encoded_value}\n'.encode('utf-8'))

    def _encode_field(self, writer: BytesWriter, field_name: str, field: SchemaField) -> None:
        encoded_type = self._type_str(field.type)
        if field.default is not None:
            encoded_value = self._value_str(field.default)
            writer.write(f'{encoded_type} {field_name} {encoded_value}\n'.encode('utf-8'))
        else:
            writer.write(f'{encoded_type} {field_name}\n'.encode('utf-8'))

    def encode(self, schema: Message | type[Message]) -> bytes:
        parsed_schema, sub_schemas = self._parse_message(schema)

        writer = BytesWriter()
        # Output constants first, then regular fields (ROS2 convention)
        for field_name, field in parsed_schema.fields.items():
            if isinstance(field, SchemaConstant):
                self._encode_constant(writer, field_name, field)
        for field_name, field in parsed_schema.fields.items():
            if isinstance(field, SchemaField):
                self._encode_field(writer, field_name, field)

        for sub_schema in sub_schemas.values():
            writer.write(('=' * 80 + '\n').encode('utf-8'))
            writer.write(f'MSG: {sub_schema.name.replace("/msg/", "/")}\n'.encode('utf-8'))
            # Output constants first, then regular fields (ROS2 convention)
            for field_name, field in sub_schema.fields.items():
                if isinstance(field, SchemaConstant):
                    self._encode_constant(writer, field_name, field)
            for field_name, field in sub_schema.fields.items():
                if isinstance(field, SchemaField):
                    self._encode_field(writer, field_name, field)

        return writer.as_bytes()

    def parse_schema(self, schema: Message | type[Message]) -> tuple[Schema, dict[str, Schema]]:
        return self._parse_message(schema)


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
