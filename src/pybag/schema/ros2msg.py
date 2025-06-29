import re
import logging
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

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


class Ros2MsgFieldType(str, Enum):
    PRIMITIVE = 'primitive'
    ARRAY = 'array'
    SEQUENCE = 'sequence'
    COMPLEX = 'complex'


class Ros2MsgError(Exception):
    """Exception raised for errors in the ROS2 message parsing."""
    def __init__(self, message: str):
        super().__init__(message)


def parse_ros2msg_type(field_raw_type: str, package_name: str) -> dict:
    # Check if the field is a string with a length limit
    string_length_match = re.match(r'string(.*)\[', field_raw_type)

    # Handle arrays
    if re.match(r'.*\[.*\]$', field_raw_type):
        if match := re.match(r'.*\[(\d*)\]$', field_raw_type):
            field_type = Ros2MsgFieldType.ARRAY
            length = int(match.group(1))
        else:
            field_type = Ros2MsgFieldType.SEQUENCE
            length = None

        # TODO: Do something with a string length limit
        if string_length_match:
            logger.debug(f'String is limited: {string_length_match.group(1)}')

        # Get the type of the array elements
        element_type = re.match(r'^(.*)\[', field_raw_type).group(1)
        return {
            'field_type': field_type,
            'data_type': element_type,
            'length': length
        }

    # Handle strings with a length limit
    if 'string' in field_raw_type and string_length_match:
        # TODO: Do something with the limit
        logger.debug(f'String is limited: {string_length_match.group(1)}')
        return {
            'field_type': Ros2MsgFieldType.PRIMITIVE,
            'data_type': 'string',
        }

    # Handle primitive types
    if field_raw_type in PRIMITIVE_TYPE_MAP:
        return {
            'field_type': Ros2MsgFieldType.PRIMITIVE,
            'data_type': field_raw_type,
        }

    # Handle complex types
    data_type = field_raw_type
    if field_raw_type == 'Header':
        data_type = 'std_msgs/Header'
    elif '/' not in field_raw_type:
        data_type = f'{package_name}/{field_raw_type}'
    return {
        'field_type': Ros2MsgFieldType.COMPLEX,
        'data_type': data_type,
    }


def parse_ros2msg_field(package_name: str, field: str) -> tuple[str, dict]:
    # Remove inline comments
    field = re.sub(r'#.*\n', '', field)

    # TODO: Handle default values + constant values
    # TODO: split() does not work for strings and arrays
    field_raw_type, field_raw_name = field.split()[:2]
    if '=' in field_raw_name:
        error_msg = 'Constant values are not supported yet'
        raise Ros2MsgError(error_msg)

    field_name = field_raw_name
    field_type = parse_ros2msg_type(field_raw_type, package_name)

    return field_name, {
        'field_type': field_type['field_type'],
        'data_type': field_type['data_type'],
        'length': field_type.get('length'),
    }


def parse_ros2msg(schema: SchemaRecord) -> tuple[dict, dict]:
    """Parse a ros2msg schema record."""
    assert schema.encoding == 'ros2msg'
    logger.debug(f'Parsing schema: {schema.name}')
    package_name = schema.name.split('/')[0]
    msg = schema.data.decode('utf-8')

    # Tidy up the message schema
    msg = '\n'.join([line.strip() for line in msg.split('\n')])  # Remove blank space
    msg = re.sub(r'#.*\n', '', msg, flags=re.MULTILINE)  # Remove line comments
    msg = re.sub(r'\n\n', '\n', msg, flags=re.MULTILINE)  # Remove empty lines

    # Split along '=' delimiter
    msg = [m.strip() for m in msg.split('=' * 80)]

    msg_schema = defaultdict(dict)
    # The first message does not have the 'MSG: ' prefix line
    main_fields = [m.strip() for m in msg[0].split('\n')]
    for field in main_fields:
        logger.debug(f'Field: {field}')
        field_name, field_dict = parse_ros2msg_field(package_name, field)
        msg_schema[field_name] = field_dict
        logger.debug(f'{field_name}: {field_dict}')

    schema_msgs = defaultdict(dict)
    for sub_msg in msg[1:]:
        sub_msg_name = sub_msg.split('\n')[0].strip()[5:]  # Remove 'MSG: ' prefix
        sub_msg_fields = [m.strip() for m in sub_msg.split('\n')[1:] if m]
        for field in sub_msg_fields:
            logger.debug(f'{sub_msg_name}: {field}')
            field_name, field_dict = parse_ros2msg_field(package_name, field)
            schema_msgs[sub_msg_name][field_name] = field_dict
            logger.debug(f'{field_name}: {field_dict}')

    return msg_schema, schema_msgs


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
