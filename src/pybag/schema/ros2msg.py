import re
from collections import defaultdict

from pybag.mcap.records import SchemaRecord

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


def parse_ros2msg_type(field_type: str, package_name: str) -> dict:
    # Check if the field is an array
    if re.match(r'.*\[.*\]$', field_type):
        length = re.match(r'.*\[(\d*)\]$', field_type).group(1)
        print(f'Array length: {length}')

        # Get the type of the array elements
        element_type = re.match(r'^(.*)\[', field_type).group(1)
        return {
            'type': element_type,
            'length': int(length) if length else None,
        }
    if field_type in PRIMITIVE_TYPE_MAP:
        return {
            'type': field_type,
            'length': None,
        }
    # Message type is in the same package
    if len(field_type.split('/')) == 1:
        # Special case for Header
        if field_type == 'Header':
            return {
                'type': 'std_msgs/Header',
                'length': None,
            }
        return {
            'type': f'{package_name}/{field_type}',
            'length': None,
        }

    return {
        'type': field_type,
        'length': None,
    }


def parse_ros2msg_name(field: str) -> dict:
    if '=' in field:
        field_name = field.split('=')[0]
        field_constant_value = field.split('=')[1]
        is_constant = True
    else:
        field_name = field
        field_constant_value = None
        is_constant = False

    return field_name, {
        'constant_value': field_constant_value,
        'is_constant': is_constant,
    }


def parse_ros2msg_field(package_name: str, field: str) -> tuple[str, dict]:
    # Remove comments (search stops at first newline)
    # For constant strings fields, the comment is part of the constant value
    if not (field.split()[0] == 'string' and '=' in field.split()[1]):
        field = re.sub(r'#.*\n', '', field)

    print(f'Field: {field}')
    if len(field.split()) == 2:
        field_default = None
        field_raw_type, field_raw_name = field.split()
    elif len(field.split()) == 3:
        field_default = field.split()[2]
        field_raw_type, field_raw_name = field.split()[:2]
    else:
        error_msg = f'Invalid field: {field}'
        raise Ros2MsgError(error_msg)

    field_type = parse_ros2msg_type(field_raw_type, package_name)
    name, field_name = parse_ros2msg_name(field_raw_name)

    if field_type['type'] in PRIMITIVE_TYPE_MAP:
        primitive_type = PRIMITIVE_TYPE_MAP[field_type['type']]
        if field_default is not None:
            field_default = primitive_type(field_default)
        if field_name['constant_value'] is not None:
            field_name['constant_value'] = primitive_type(
                field_name['constant_value'][0],
                encoding='utf-8',
            )

    return name, {
        **field_type,
        **field_name,
        'default': field_default,
    }


def parse_ros2msg(schema: SchemaRecord) -> tuple[dict, dict]:
    """Parse a ros2msg schema record."""
    assert schema.encoding == 'ros2msg'
    package_name = schema.name.split('/')[0]
    msg = schema.data.decode('utf-8')

    # Remove blank space at beginning and end of each line
    msg = '\n'.join([line.strip() for line in msg.split('\n')])

    # Remove comments spanning entire line
    msg = re.sub(r'^#.*\n', '', msg)

    # Split along '=' delimiter
    msg = [m.strip() for m in msg.split('=' * 80)]

    msg_schema = defaultdict(dict)
    # The first message does not have the 'MSG: ' prefix line
    main_fields = [m.strip() for m in msg[0].split('\n')]
    for field in main_fields:
        field_name, field_dict = parse_ros2msg_field(package_name, field)
        msg_schema[field_name] = field_dict

    schema_msgs = defaultdict(dict)
    for sub_msg in msg[1:]:
        sub_msg_name = sub_msg.split('\n')[0].strip()[5:]  # Remove 'MSG: ' prefix
        sub_msg_fields = [m.strip() for m in sub_msg.strip().split('\n')[1:] if m]
        for field in sub_msg_fields:
            field_name, field_dict = parse_ros2msg_field(package_name, field)
            schema_msgs[sub_msg_name][field_name] = field_dict

    return msg_schema, schema_msgs
