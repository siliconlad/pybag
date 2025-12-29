"""Translation layer for converting messages between ROS1 and ROS2 formats.

This module provides functions to translate message objects between ROS1 and ROS2
representations, handling the differences in:
- time: ROS1 primitive (secs, nsecs) vs ROS2 builtin_interfaces/Time (sec, nanosec)
- duration: ROS1 primitive (secs, nsecs) vs ROS2 builtin_interfaces/Duration (sec, nanosec)
"""

import re
from dataclasses import fields, is_dataclass, replace
from typing import Any, TypeVar

from pybag.ros2.humble.builtin_interfaces import Duration as Ros2Duration
from pybag.ros2.humble.builtin_interfaces import Time as Ros2Time
from pybag.types import Duration as Ros1Duration
from pybag.types import SchemaText
from pybag.types import Time as Ros1Time

T = TypeVar('T')


def _is_ros1_time(obj: Any) -> bool:
    """Check if an object is a ROS1 Time instance."""
    return isinstance(obj, Ros1Time)


def _is_ros1_duration(obj: Any) -> bool:
    """Check if an object is a ROS1 Duration instance."""
    return isinstance(obj, Ros1Duration)


def _is_ros2_time(obj: Any) -> bool:
    """Check if an object is a ROS2 Time instance."""
    # Check for the ROS2 Time structure: has sec and nanosec attributes
    # and has __msg_name__ that matches builtin_interfaces Time
    # (may be 'builtin_interfaces/msg/Time' or 'builtin_interfaces/Time')
    if not hasattr(obj, '__msg_name__'):
        return False
    msg_name = obj.__msg_name__
    return (
        msg_name in ('builtin_interfaces/msg/Time', 'builtin_interfaces/Time') and
        hasattr(obj, 'sec') and
        hasattr(obj, 'nanosec')
    )


def _is_ros2_duration(obj: Any) -> bool:
    """Check if an object is a ROS2 Duration instance."""
    # (may be 'builtin_interfaces/msg/Duration' or 'builtin_interfaces/Duration')
    if not hasattr(obj, '__msg_name__'):
        return False
    msg_name = obj.__msg_name__
    return (
        msg_name in ('builtin_interfaces/msg/Duration', 'builtin_interfaces/Duration') and
        hasattr(obj, 'sec') and
        hasattr(obj, 'nanosec')
    )


def ros1_time_to_ros2(time: Ros1Time) -> Ros2Time:
    """Convert ROS1 Time to ROS2 Time.

    Args:
        time: ROS1 Time with secs and nsecs attributes.

    Returns:
        ROS2 Time with sec and nanosec attributes.
    """
    return Ros2Time(sec=time.secs, nanosec=time.nsecs)


def ros2_time_to_ros1(time: Any) -> Ros1Time:
    """Convert ROS2 Time to ROS1 Time.

    Args:
        time: ROS2 Time with sec and nanosec attributes.

    Returns:
        ROS1 Time with secs and nsecs attributes.
    """
    return Ros1Time(secs=time.sec, nsecs=time.nanosec)


def ros1_duration_to_ros2(duration: Ros1Duration) -> Ros2Duration:
    """Convert ROS1 Duration to ROS2 Duration.

    Args:
        duration: ROS1 Duration with secs and nsecs attributes.

    Returns:
        ROS2 Duration with sec and nanosec attributes.
    """
    return Ros2Duration(sec=duration.secs, nanosec=duration.nsecs)


def ros2_duration_to_ros1(duration: Any) -> Ros1Duration:
    """Convert ROS2 Duration to ROS1 Duration.

    Args:
        duration: ROS2 Duration with sec and nanosec attributes.

    Returns:
        ROS1 Duration with secs and nsecs attributes.
    """
    return Ros1Duration(secs=duration.sec, nsecs=duration.nanosec)


def translate_ros1_to_ros2(message: T) -> T:
    """Recursively translate a ROS1 message to ROS2 format.

    This function walks through the message object and converts:
    - ROS1 Time(secs, nsecs) -> ROS2 Time(sec, nanosec)
    - ROS1 Duration(secs, nsecs) -> ROS2 Duration(sec, nanosec)

    Args:
        message: A ROS1 message object (dataclass).

    Returns:
        A new message object with time/duration fields converted to ROS2 format.
    """
    return _translate_value_ros1_to_ros2(message)


def _translate_value_ros1_to_ros2(value: Any) -> Any:
    """Recursively translate a value from ROS1 to ROS2 format."""
    # Handle ROS1 Time
    if _is_ros1_time(value):
        return ros1_time_to_ros2(value)

    # Handle ROS1 Duration
    if _is_ros1_duration(value):
        return ros1_duration_to_ros2(value)

    # Handle lists/arrays
    if isinstance(value, list):
        return [_translate_value_ros1_to_ros2(item) for item in value]

    # Handle dataclass (nested message)
    if is_dataclass(value) and not isinstance(value, type):
        # Get all field values and translate them
        new_values = {}
        for field in fields(value):
            field_value = getattr(value, field.name)
            new_values[field.name] = _translate_value_ros1_to_ros2(field_value)

        # Create new instance with translated values
        return replace(value, **new_values)

    # Return other values unchanged
    return value


def translate_ros2_to_ros1(message: T) -> T:
    """Recursively translate a ROS2 message to ROS1 format.

    This function walks through the message object and converts:
    - ROS2 Time(sec, nanosec) -> ROS1 Time(secs, nsecs)
    - ROS2 Duration(sec, nanosec) -> ROS1 Duration(secs, nsecs)

    Args:
        message: A ROS2 message object (dataclass).

    Returns:
        A new message object with time/duration fields converted to ROS1 format.
    """
    return _translate_value_ros2_to_ros1(message)


def _translate_value_ros2_to_ros1(value: Any) -> Any:
    """Recursively translate a value from ROS2 to ROS1 format."""
    # Handle ROS2 Time
    if _is_ros2_time(value):
        return ros2_time_to_ros1(value)

    # Handle ROS2 Duration
    if _is_ros2_duration(value):
        return ros2_duration_to_ros1(value)

    # Handle lists/arrays
    if isinstance(value, list):
        return [_translate_value_ros2_to_ros1(item) for item in value]

    # Handle dataclass (nested message)
    if is_dataclass(value) and not isinstance(value, type):
        # Get all field values and translate them
        new_values = {}
        for field in fields(value):
            field_value = getattr(value, field.name)
            new_values[field.name] = _translate_value_ros2_to_ros1(field_value)

        # Create new instance with translated values
        return replace(value, **new_values)

    # Return other values unchanged
    return value


def translate_schema_ros1_to_ros2(msg_name: str, schema_text: str) -> SchemaText:
    """Translate a ROS1 message schema to ROS2 format.

    This function transforms ROS1 message definition text to ROS2 format:
    - time -> builtin_interfaces/Time (with sec/nanosec fields)
    - duration -> builtin_interfaces/Duration (with sec/nanosec fields)
    - char remains as-is (handled by the serializer as uint8 vs string)
    - Message name: package/Message -> package/msg/Message

    Args:
        msg_name: ROS1 message type name (e.g., "std_msgs/Header").
        schema_text: ROS1 message definition text.

    Returns:
        SchemaText with ROS2-compatible name and definition text.
    """
    # Convert msg_name from ROS1 format (pkg/Msg) to ROS2 format (pkg/msg/Msg)
    parts = msg_name.split('/')
    if len(parts) == 2:
        ros2_msg_name = f"{parts[0]}/msg/{parts[1]}"
    else:
        ros2_msg_name = msg_name
    lines = schema_text.split('\n')
    result_lines = []
    has_time = False
    has_duration = False

    for line in lines:
        stripped = line.strip()

        # Skip empty lines and comments
        if not stripped or stripped.startswith('#'):
            result_lines.append(line)
            continue

        # Check for MSG: delimiter (sub-message definition)
        if stripped.startswith('MSG:'):
            result_lines.append(line)
            continue

        # Check for separator
        if stripped == '=' * 80:
            result_lines.append(line)
            continue

        # Parse field: TYPE NAME [=VALUE]
        # Handle constants: TYPE NAME=VALUE
        if '=' in stripped and not stripped.startswith('='):
            # This is a constant definition, keep as-is
            result_lines.append(line)
            continue

        # Match field definition
        match = re.match(r'^(\S+)\s+(\S+)(.*)$', stripped)
        if not match:
            result_lines.append(line)
            continue

        field_type, field_name, rest = match.groups()

        # Handle array notation
        array_suffix = ''
        base_type = field_type
        if '[' in field_type:
            bracket_pos = field_type.index('[')
            base_type = field_type[:bracket_pos]
            array_suffix = field_type[bracket_pos:]

        # Translate time and duration
        if base_type == 'time':
            has_time = True
            new_type = f'builtin_interfaces/Time{array_suffix}'
            result_lines.append(f'{new_type} {field_name}{rest}')
        elif base_type == 'duration':
            has_duration = True
            new_type = f'builtin_interfaces/Duration{array_suffix}'
            result_lines.append(f'{new_type} {field_name}{rest}')
        else:
            result_lines.append(line)

    result = '\n'.join(result_lines)

    # Add builtin_interfaces sub-schemas if needed
    time_schema = """================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec"""

    duration_schema = """================================================================================
MSG: builtin_interfaces/Duration
int32 sec
uint32 nanosec"""

    # Check if the schema already contains these definitions
    if has_time and 'MSG: builtin_interfaces/Time' not in result:
        result += '\n' + time_schema

    if has_duration and 'MSG: builtin_interfaces/Duration' not in result:
        result += '\n' + duration_schema

    return SchemaText(name=ros2_msg_name, text=result)


def _is_separator_line(line: str) -> bool:
    """Check if a line is a schema separator.

    The standard separator is 80 '=' characters. However, some tools
    (e.g., certain MCAP writers) incorrectly use 40 '=' characters.
    We accept both for compatibility with real-world files.
    """
    stripped = line.strip()
    return stripped == '=' * 80


def translate_schema_ros2_to_ros1(msg_name: str, schema_text: str) -> SchemaText:
    """Translate a ROS2 message schema to ROS1 format.

    This function transforms ROS2 message definition text to ROS1 format:
    - builtin_interfaces/Time -> time (primitive)
    - builtin_interfaces/Duration -> duration (primitive)
    - builtin_interfaces/msg/Time -> time (primitive)
    - builtin_interfaces/msg/Duration -> duration (primitive)
    - Message name: package/msg/Message -> package/Message
    - Separator lines: non-standard 40 '=' chars -> standard 80 '=' chars
    - char remains as-is (handled by the serializer)

    Args:
        msg_name: ROS2 message type name (e.g., "std_msgs/msg/Header").
        schema_text: ROS2 message definition text.

    Returns:
        SchemaText with ROS1-compatible name and definition text.
    """
    # Convert msg_name from ROS2 format (pkg/msg/Msg) to ROS1 format (pkg/Msg)
    ros1_msg_name = msg_name.replace('/msg/', '/')
    lines = schema_text.split('\n')
    result_lines = []
    skip_sub_schema = False

    for line in lines:
        stripped = line.strip()

        # Check for MSG: delimiter (sub-message definition)
        if stripped.startswith('MSG:'):
            sub_msg_name = stripped[4:].strip()
            # Skip builtin_interfaces Time and Duration sub-schemas
            if sub_msg_name in ('builtin_interfaces/Time', 'builtin_interfaces/msg/Time',
                               'builtin_interfaces/Duration', 'builtin_interfaces/msg/Duration'):
                skip_sub_schema = True
                # Also remove the preceding separator line if it exists
                if result_lines and _is_separator_line(result_lines[-1]):
                    result_lines.pop()
                continue
            else:
                skip_sub_schema = False
                # Convert MSG: line to ROS1 format (remove /msg/ from type name)
                ros1_sub_msg_name = sub_msg_name.replace('/msg/', '/')
                result_lines.append(f'MSG: {ros1_sub_msg_name}')
                continue

        # Check for separator - might start a new sub-schema
        # Convert separator to standard 80 '=' format
        # Some tools incorrectly use 40 '=' chars, so we normalize to 80
        if _is_separator_line(line):
            if skip_sub_schema:
                # This separator ends the skipped sub-schema
                skip_sub_schema = False
            else:
                # Use ROS1 format separator (80 chars)
                result_lines.append('=' * 80)
            continue

        # Skip lines that are part of a builtin_interfaces sub-schema
        if skip_sub_schema:
            continue

        # Skip empty lines and comments
        if not stripped or stripped.startswith('#'):
            result_lines.append(line)
            continue

        # Handle constants: TYPE NAME=VALUE
        if '=' in stripped and not stripped.startswith('='):
            result_lines.append(line)
            continue

        # Match field definition
        match = re.match(r'^(\S+)\s+(\S+)(.*)$', stripped)
        if not match:
            result_lines.append(line)
            continue

        field_type, field_name, rest = match.groups()

        # Handle array notation
        array_suffix = ''
        base_type = field_type
        if '[' in field_type:
            bracket_pos = field_type.index('[')
            base_type = field_type[:bracket_pos]
            array_suffix = field_type[bracket_pos:]

        # Translate builtin_interfaces/Time and Duration to primitives
        if base_type in ('builtin_interfaces/Time', 'builtin_interfaces/msg/Time'):
            new_type = f'time{array_suffix}'
            result_lines.append(f'{new_type} {field_name}{rest}')
        elif base_type in ('builtin_interfaces/Duration', 'builtin_interfaces/msg/Duration'):
            new_type = f'duration{array_suffix}'
            result_lines.append(f'{new_type} {field_name}{rest}')
        else:
            # Remove /msg/ from package names for ROS1 compatibility
            if '/msg/' in base_type:
                base_type = base_type.replace('/msg/', '/')
                new_type = f'{base_type}{array_suffix}'
                result_lines.append(f'{new_type} {field_name}{rest}')
            else:
                result_lines.append(line)

    # Clean up any trailing empty separators
    while result_lines and _is_separator_line(result_lines[-1]):
        result_lines.pop()

    return SchemaText(name=ros1_msg_name, text='\n'.join(result_lines))


__all__ = [
    'translate_ros1_to_ros2',
    'translate_ros2_to_ros1',
    'ros1_time_to_ros2',
    'ros2_time_to_ros1',
    'ros1_duration_to_ros2',
    'ros2_duration_to_ros1',
    'translate_schema_ros1_to_ros2',
    'translate_schema_ros2_to_ros1',
]
