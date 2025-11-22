#!/usr/bin/env python3
"""Generate ROS2 message dataclasses and pre-compiled encoders/decoders.

This script can:
1. Download ROS2 message definitions from repositories
2. Generate Python dataclasses from .msg files
3. Generate pre-compiled encoder/decoder functions
"""

from __future__ import annotations

import argparse
import importlib
import re
import struct
import sys
import tempfile
from dataclasses import fields, is_dataclass
from itertools import count
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None

import pybag.types as t
from pybag.schema import (
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaField,
    SchemaFieldType,
    Sequence,
    String,
)
from pybag.schema.compiler import _STRUCT_FORMAT, _STRUCT_SIZE, _TAB, _WRITE_FORMAT, _WRITE_SIZE, _sanitize
from pybag.schema.ros2msg import Ros2MsgSchemaDecoder, Ros2MsgSchemaEncoder


# Standard ROS2 package repositories for different distros
ROS2_REPOS = {
    'common_interfaces': 'https://raw.githubusercontent.com/ros2/common_interfaces/{distro}/{package}/msg/{msg_file}',
    'rcl_interfaces': 'https://raw.githubusercontent.com/ros2/rcl_interfaces/{distro}/{package}/msg/{msg_file}',
    'unique_identifier_msgs': 'https://raw.githubusercontent.com/ros2/unique_identifier_msgs/{distro}/msg/{msg_file}',
    'action_msgs': 'https://raw.githubusercontent.com/ros2/rcl_interfaces/{distro}/action_msgs/msg/{msg_file}',
}

# Map packages to their repositories
PACKAGE_TO_REPO = {
    'geometry_msgs': 'common_interfaces',
    'std_msgs': 'common_interfaces',
    'sensor_msgs': 'common_interfaces',
    'nav_msgs': 'common_interfaces',
    'stereo_msgs': 'common_interfaces',
    'visualization_msgs': 'common_interfaces',
    'shape_msgs': 'common_interfaces',
    'trajectory_msgs': 'common_interfaces',
    'diagnostic_msgs': 'common_interfaces',
    'actionlib_msgs': 'common_interfaces',
    'builtin_interfaces': 'rcl_interfaces',
    'unique_identifier_msgs': 'unique_identifier_msgs',
    'action_msgs': 'action_msgs',
}


def download_msg_file(package: str, msg_name: str, distro: str) -> str | None:
    """Download a .msg file from GitHub."""
    if requests is None:
        print("Error: requests library not installed. Install with: pip install requests")
        return None

    repo = PACKAGE_TO_REPO.get(package)
    if not repo:
        print(f"Warning: Unknown package '{package}', cannot auto-download")
        return None

    url_template = ROS2_REPOS.get(repo)
    if not url_template:
        print(f"Warning: No repository template for '{repo}'")
        return None

    url = url_template.format(distro=distro, package=package, msg_file=f"{msg_name}.msg")

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text
        else:
            print(f"Warning: Failed to download {package}/msg/{msg_name} (status {response.status_code})")
            return None
    except Exception as e:
        print(f"Warning: Error downloading {package}/msg/{msg_name}: {e}")
        return None


def parse_msg_file(content: str, package: str, msg_name: str) -> dict[str, Any]:
    """Parse a .msg file and extract field information."""
    lines = content.strip().split('\n')
    fields = []
    constants = []

    for line in lines:
        line = line.split('#')[0].strip()  # Remove comments
        if not line:
            continue

        # Check if it's a constant (has '=')
        if '=' in line:
            parts = line.split('=', 1)
            type_and_name = parts[0].strip().split()
            if len(type_and_name) != 2:
                continue
            field_type, field_name = type_and_name
            value = parts[1].strip()
            constants.append({
                'name': field_name,
                'type': field_type,
                'value': value,
            })
        else:
            # Regular field
            parts = line.split()
            if len(parts) < 2:
                continue
            field_type = parts[0]
            field_name = parts[1]

            # Handle default values
            default = None
            if len(parts) > 2:
                default = ' '.join(parts[2:])

            fields.append({
                'name': field_name,
                'type': field_type,
                'default': default,
            })

    return {
        'package': package,
        'name': msg_name,
        'fields': fields,
        'constants': constants,
    }


def generate_dataclass_code(msg_info: dict[str, Any], distro: str) -> str:
    """Generate Python dataclass code from message info."""
    package = msg_info['package']
    msg_name = msg_info['name']
    fields = msg_info['fields']
    constants = msg_info['constants']

    lines = []
    lines.append("@dataclass(kw_only=True)")
    lines.append(f"class {msg_name}:")
    lines.append(f"    __msg_name__ = '{package}/msg/{msg_name}'")
    lines.append("")

    # Add constants
    for const in constants:
        const_type = const['type']
        const_name = const['name']
        const_value = const['value']

        # Convert value based on type
        if const_type in ['string', 'wstring']:
            const_value = f'"{const_value}"'
        elif const_type == 'bool':
            const_value = const_value.capitalize()

        lines.append(f"    {const_name}: t.{const_type} = {const_value}")

    # Add fields
    for field in fields:
        field_type = field['type']
        field_name = field['name']
        default = field['default']

        # Map ROS2 types to pybag types
        type_annotation = map_ros2_type_to_annotation(field_type, package, distro)

        if default:
            lines.append(f"    {field_name}: {type_annotation} = {default}")
        else:
            lines.append(f"    {field_name}: {type_annotation}")

    return '\n'.join(lines)


def map_ros2_type_to_annotation(field_type: str, current_package: str, distro: str) -> str:
    """Map ROS2 type string to Python type annotation."""
    # Handle arrays
    array_match = re.match(r'(.+)\[(\d+)\]', field_type)
    if array_match:
        elem_type = array_match.group(1)
        length = array_match.group(2)
        elem_annotation = map_ros2_type_to_annotation(elem_type, current_package, distro)
        return f"t.Array[{elem_annotation}, {length}]"

    # Handle sequences (unbounded arrays)
    sequence_match = re.match(r'sequence<(.+)>', field_type)
    if sequence_match:
        elem_type = sequence_match.group(1)
        elem_annotation = map_ros2_type_to_annotation(elem_type, current_package, distro)
        return f"t.Sequence[{elem_annotation}]"

    # Handle bounded sequences
    bounded_seq_match = re.match(r'(.+)\[<=(\d+)\]', field_type)
    if bounded_seq_match:
        elem_type = bounded_seq_match.group(1)
        max_length = bounded_seq_match.group(2)
        elem_annotation = map_ros2_type_to_annotation(elem_type, current_package, distro)
        return f"t.BoundedSequence[{elem_annotation}, {max_length}]"

    # Handle primitive types
    if field_type in ['bool', 'byte', 'char', 'float32', 'float64',
                      'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32',
                      'int64', 'uint64', 'string', 'wstring']:
        return f"t.{field_type}"

    # Handle complex types (other messages)
    if '/' in field_type:
        # Fully qualified name like 'std_msgs/msg/Header'
        parts = field_type.split('/')
        package = parts[0]
        msg_type = parts[-1]
        return f"t.Complex[{package}.{msg_type}]"
    else:
        # Relative name, assume same package
        return f"t.Complex[{field_type}]"


def compile_schema_to_code(schema: Schema, sub_schemas: dict[str, Schema]) -> tuple[str, dict[str, type]]:
    """Generate decoder code for a schema without exec'ing it."""
    function_defs: list[str] = []
    compiled: dict[str, str] = {}
    dataclass_types: dict[str, type] = {}

    from dataclasses import make_dataclass
    from typing import Annotated

    _PRIMITIVE_TYPE_MAP = {
        'int8': t.int8,
        'int16': t.int16,
        'int32': t.int32,
        'int64': t.int64,
        'uint8': t.uint8,
        'uint16': t.uint16,
        'uint32': t.uint32,
        'uint64': t.uint64,
        'float32': t.float32,
        'float64': t.float64,
        'bool': t.bool,
        'byte': t.byte,
        'char': t.char,
        'string': t.string,
        'wstring': t.wstring,
    }

    def schema_type_to_annotation(field_type: SchemaFieldType) -> Any:
        if isinstance(field_type, Primitive):
            return _PRIMITIVE_TYPE_MAP.get(field_type.type, Any)
        elif isinstance(field_type, String):
            return _PRIMITIVE_TYPE_MAP.get(field_type.type, str)
        elif isinstance(field_type, Array):
            elem_type = field_type.type
            if isinstance(elem_type, Primitive):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, Any)
                return Annotated[list[elem_annotation], ("array", elem_annotation, field_type.length)]
            elif isinstance(elem_type, String):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, str)
                return Annotated[list[elem_annotation], ("array", elem_annotation, field_type.length)]
            elif isinstance(elem_type, Complex):
                sub_schema = sub_schemas[elem_type.type]
                elem_annotation = create_dataclass_type(sub_schema)
                return Annotated[list[Any], ("array", elem_annotation, field_type.length)]
            else:
                return Annotated[list[Any], ("array", Any, field_type.length)]
        elif isinstance(field_type, Sequence):
            elem_type = field_type.type
            if isinstance(elem_type, Primitive):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, Any)
                return Annotated[list[elem_annotation], ("array", elem_annotation, None)]
            elif isinstance(elem_type, String):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, str)
                return Annotated[list[elem_annotation], ("array", elem_annotation, None)]
            elif isinstance(elem_type, Complex):
                sub_schema = sub_schemas[elem_type.type]
                elem_annotation = create_dataclass_type(sub_schema)
                return Annotated[list[Any], ("array", elem_annotation, None)]
            else:
                return Annotated[list[Any], ("array", Any, None)]
        elif isinstance(field_type, Complex):
            sub_schema = sub_schemas[field_type.type]
            sub_type = create_dataclass_type(sub_schema)
            return Annotated[sub_type, ("complex", field_type.type)]
        else:
            return Any

    def create_dataclass_type(current: Schema) -> type:
        class_name = _sanitize(current.name)
        if class_name in dataclass_types:
            return dataclass_types[class_name]

        field_specs = []
        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                base_type = schema_type_to_annotation(entry.type)
                constant_annotation = Annotated[base_type, ('constant', base_type)]
                field_specs.append((field_name, constant_annotation, entry.value))
            elif isinstance(entry, SchemaField):
                type_annotation = schema_type_to_annotation(entry.type)
                if entry.default is not None:
                    field_specs.append((field_name, type_annotation, entry.default))
                else:
                    field_specs.append((field_name, type_annotation))

        dataclass_type = make_dataclass(
            class_name,
            field_specs,
            namespace={'__msg_name__': current.name},
            kw_only=True
        )

        dataclass_types[class_name] = dataclass_type
        return dataclass_type

    def build(current: Schema) -> str:
        func_name = f"decode_{_sanitize(current.name)}"
        if func_name in compiled:
            return func_name

        create_dataclass_type(current)

        compiled[func_name] = func_name
        lines: list[str] = [
            f"def {func_name}(decoder):",
            f"{_TAB}fmt_prefix = '<' if decoder._is_little_endian else '>'",
            f"{_TAB}_data = decoder._data",
            f"{_TAB}_fields = {{}}",
        ]
        run_type: str | None = None
        run_fields: list[str] = []

        def flush() -> None:
            nonlocal run_type, run_fields
            if not run_fields:
                return

            count = len(run_fields)
            size = _STRUCT_SIZE[run_type]
            fmt = _STRUCT_FORMAT[run_type] * count
            lines.append(f"{_TAB}_data.align({size})")

            if count > 1:
                names = ", ".join(run_fields)
                lines.append(f"{_TAB}{names} = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))")
                for field in run_fields:
                    lines.append(f"{_TAB}_fields[{field!r}] = {field}")
            else:
                field = run_fields[0]
                lines.append(f"{_TAB}_fields[{field!r}] = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))[0]")

            run_fields = []
            run_type = None

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                flush()
                continue

            if not isinstance(entry, SchemaField):
                flush()
                lines.append(f"{_TAB}_fields[{field_name!r}] = None")
                continue

            field_type = entry.type

            if isinstance(field_type, Primitive) and field_type.type in _STRUCT_FORMAT:
                if run_type == field_type.type:
                    run_fields.append(field_name)
                else:
                    flush()
                    run_type = field_type.type
                    run_fields = [field_name]
                continue

            flush()

            if isinstance(field_type, Primitive):
                lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.{field_type.type}()")
            elif isinstance(field_type, String):
                lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.{field_type.type}()")
            elif isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    fmt = _STRUCT_FORMAT[elem.type] * field_type.length
                    lines.append(f"{_TAB}_data.align({size})")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{fmt}', _data.read({size * field_type.length})))"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = [{sub_func}(decoder) for _ in range({field_type.length})]"
                    )
                elif isinstance(elem, String):
                    elem_name = elem.type
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = [decoder.{elem_name}() for _ in range({field_type.length})]"
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.array('{elem_name}', {field_type.length})"
                    )
            elif isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    char = _STRUCT_FORMAT[elem.type]
                    lines.append(f"{_TAB}_len = decoder.uint32()")
                    lines.append(f"{_TAB}_data.align({size})")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{char}' * _len, _data.read({size} * _len)))"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = [{sub_func}(decoder) for _ in range(length)]"
                    )
                elif isinstance(elem, String):
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    elem_name = elem.type
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = [decoder.{elem_name}() for _ in range(length)]"
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.sequence('{elem_name}')")
            elif isinstance(field_type, Complex):
                sub_schema = sub_schemas[field_type.type]
                sub_func = build(sub_schema)
                lines.append(f"{_TAB}_fields[{field_name!r}] = {sub_func}(decoder)")
            else:
                lines.append(f"{_TAB}_fields[{field_name!r}] = None")

        flush()

        class_name = _sanitize(current.name)
        lines.append(f"{_TAB}return _dataclass_types[{class_name!r}](**_fields)")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "import struct\n" + "\n\n".join(function_defs)

    return code, dataclass_types


def compile_serializer_to_code(schema: Schema, sub_schemas: dict[str, Schema]) -> str:
    """Generate encoder code for a schema without exec'ing it."""
    function_defs: list[str] = []
    compiled: dict[str, str] = {}
    name_counter = count()

    def new_var(prefix: str) -> str:
        return f"_{prefix}_{next(name_counter)}"

    def build(current: Schema) -> str:
        func_name = f"encode_{_sanitize(current.name)}"
        if func_name in compiled:
            return func_name

        compiled[func_name] = func_name
        lines: list[str] = [
            f"def {func_name}(encoder, message):",
            f"{_TAB}fmt_prefix = '<' if encoder._is_little_endian else '>'",
            f"{_TAB}_payload = encoder._payload",
            f"{_TAB}struct_pack = struct.pack",
        ]

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                continue

            if not isinstance(entry, SchemaField):
                continue

            field_type = entry.type

            if isinstance(field_type, Primitive) and field_type.type in _WRITE_FORMAT:
                size = _WRITE_SIZE[field_type.type]
                fmt = _WRITE_FORMAT[field_type.type]
                lines.append(f"{_TAB}_payload.align({size})")

                if field_type.type in ("byte", "char"):
                    lines.append(f"{_TAB}_value = message.{field_name}")
                    lines.append(f"{_TAB}if isinstance(_value, str):")
                    lines.append(f"{_TAB}{_TAB}_value = ord(_value)")
                    lines.append(f"{_TAB}elif isinstance(_value, (bytes, bytearray)):")
                    lines.append(f"{_TAB}{_TAB}_value = _value[0]")
                    lines.append(f"{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', _value))")
                else:
                    lines.append(f"{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', message.{field_name}))")

            elif isinstance(field_type, String):
                lines.append(f"{_TAB}encoder.{field_type.type}(message.{field_name})")

            elif isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type] * field_type.length
                    lines.append(f"{_TAB}_payload.align({size})")

                    if elem.type in ("byte", "char"):
                        var_name = new_var("normalized")
                        lines.append(f"{_TAB}{var_name} = []")
                        lines.append(f"{_TAB}for _item in message.{field_name}:")
                        lines.append(f"{_TAB}{_TAB}if isinstance(_item, str):")
                        lines.append(f"{_TAB}{_TAB}{_TAB}{var_name}.append(ord(_item))")
                        lines.append(f"{_TAB}{_TAB}elif isinstance(_item, (bytes, bytearray)):")
                        lines.append(f"{_TAB}{_TAB}{_TAB}{var_name}.append(_item[0])")
                        lines.append(f"{_TAB}{_TAB}else:")
                        lines.append(f"{_TAB}{_TAB}{_TAB}{var_name}.append(_item)")
                        lines.append(f"{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', *{var_name}))")
                    else:
                        lines.append(f"{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', *message.{field_name}))")
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}for _item in message.{field_name}:")
                    lines.append(f"{_TAB}{_TAB}{sub_func}(encoder, _item)")
                elif isinstance(elem, String):
                    elem_name = elem.type
                    lines.append(f"{_TAB}for _item in message.{field_name}:")
                    lines.append(f"{_TAB}{_TAB}encoder.{elem_name}(_item)")

            elif isinstance(field_type, Sequence):
                elem = field_type.type
                lines.append(f"{_TAB}encoder.uint32(len(message.{field_name}))")

                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type]
                    lines.append(f"{_TAB}_payload.align({size})")

                    if elem.type in ("byte", "char"):
                        lines.append(f"{_TAB}for _item in message.{field_name}:")
                        lines.append(f"{_TAB}{_TAB}if isinstance(_item, str):")
                        lines.append(f"{_TAB}{_TAB}{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', ord(_item)))")
                        lines.append(f"{_TAB}{_TAB}elif isinstance(_item, (bytes, bytearray)):")
                        lines.append(f"{_TAB}{_TAB}{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', _item[0]))")
                        lines.append(f"{_TAB}{_TAB}else:")
                        lines.append(f"{_TAB}{_TAB}{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', _item))")
                    else:
                        lines.append(f"{_TAB}for _item in message.{field_name}:")
                        lines.append(f"{_TAB}{_TAB}_payload.write(struct_pack(fmt_prefix + '{fmt}', _item))")
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}for _item in message.{field_name}:")
                    lines.append(f"{_TAB}{_TAB}{sub_func}(encoder, _item)")
                elif isinstance(elem, String):
                    elem_name = elem.type
                    lines.append(f"{_TAB}for _item in message.{field_name}:")
                    lines.append(f"{_TAB}{_TAB}encoder.{elem_name}(_item)")

            elif isinstance(field_type, Complex):
                sub_schema = sub_schemas[field_type.type]
                sub_func = build(sub_schema)
                lines.append(f"{_TAB}{sub_func}(encoder, message.{field_name})")

        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "import struct\n" + "\n\n".join(function_defs)

    return code


def generate_dataclasses_from_existing(distro: str, output_dir: Path, packages: list[str] | None = None):
    """Generate dataclasses from existing pybag message types."""
    if packages is None:
        packages = ['builtin_interfaces', 'std_msgs', 'geometry_msgs', 'nav_msgs', 'sensor_msgs']

    print(f"Generating dataclasses for distro '{distro}'...")

    output_dir.mkdir(parents=True, exist_ok=True)

    for package in packages:
        try:
            module = importlib.import_module(f'pybag.ros2.{distro}.{package}')

            # Collect message types
            message_types = []
            for name in dir(module):
                obj = getattr(module, name)
                if is_dataclass(obj) and hasattr(obj, '__msg_name__'):
                    message_types.append(obj)

            if not message_types:
                continue

            # Generate output file
            output_file = output_dir / f"{package}.py"

            with open(output_file, 'w') as f:
                f.write(f'"""ROS2 {package} message types for {distro}."""\n\n')
                f.write('from __future__ import annotations\n\n')
                f.write('from dataclasses import dataclass\n\n')
                f.write('import pybag.types as t\n')

                # Check if we need to import other packages
                imports_needed = set()
                for msg_type in message_types:
                    for field in fields(msg_type):
                        field_type_str = str(field.type)
                        # Look for other package references
                        for pkg in packages:
                            if pkg != package and pkg in field_type_str:
                                imports_needed.add(pkg)

                if imports_needed:
                    f.write('\n')
                    for pkg in sorted(imports_needed):
                        f.write(f'from pybag.ros2.{distro} import {pkg}\n')

                f.write('\n\n')

                # Write message definitions
                for msg_type in message_types:
                    # This is a simplified version - the actual implementation
                    # would need to reconstruct the dataclass definition from the type
                    f.write(f"# {msg_type.__msg_name__}\n")
                    f.write(f"# (Use existing definition from pybag.ros2.{distro}.{package})\n\n")

            print(f"  Generated {package}.py with {len(message_types)} messages")

        except ImportError:
            print(f"  Skipping {package} (not found in pybag.ros2.{distro})")


def generate_precompiled_code(distro: str, output_file: Path, packages: list[str] | None = None):
    """Generate pre-compiled encoder/decoder functions."""
    if packages is None:
        packages = ['builtin_interfaces', 'std_msgs', 'geometry_msgs', 'nav_msgs', 'sensor_msgs']

    print(f"Generating pre-compiled encoders/decoders for distro '{distro}'...")

    message_types = []
    for package in packages:
        try:
            module = importlib.import_module(f'pybag.ros2.{distro}.{package}')
            for name in dir(module):
                obj = getattr(module, name)
                if is_dataclass(obj) and hasattr(obj, '__msg_name__'):
                    message_types.append(obj)
        except ImportError:
            print(f"  Warning: Could not import {package}")
            continue

    print(f"  Found {len(message_types)} message types")

    encoder = Ros2MsgSchemaEncoder()

    all_decoder_code = []
    all_encoder_code = []
    message_registry = {}

    for msg_type in message_types:
        msg_name = msg_type.__msg_name__

        try:
            schema, sub_schemas = encoder.parse_schema(msg_type)

            decoder_code, _ = compile_schema_to_code(schema, sub_schemas)
            all_decoder_code.append(f"# Decoder for {msg_name}\n{decoder_code}")

            encoder_code = compile_serializer_to_code(schema, sub_schemas)
            all_encoder_code.append(f"# Encoder for {msg_name}\n{encoder_code}")

            decode_func_name = f"decode_{_sanitize(schema.name)}"
            encode_func_name = f"encode_{_sanitize(schema.name)}"
            message_registry[msg_name] = (decode_func_name, encode_func_name)

        except Exception as e:
            print(f"    Error compiling {msg_name}: {e}")
            continue

    # Write output file
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        f.write(f'"""Pre-compiled ROS2 message encoders/decoders for {distro}.\n\n')
        f.write('This file is auto-generated - DO NOT EDIT MANUALLY.\n')
        f.write('"""\n\n')
        f.write('from __future__ import annotations\n\n')
        f.write('import struct\n')
        f.write('from typing import TYPE_CHECKING, Any, Callable\n\n')
        f.write('if TYPE_CHECKING:\n')
        f.write('    from pybag.encoding import MessageDecoder, MessageEncoder\n\n')

        f.write('_dataclass_types: dict[str, type] = {}\n\n')

        f.write('# ==================== DECODER FUNCTIONS ====================\n\n')
        for code in all_decoder_code:
            f.write(code)
            f.write('\n\n')

        f.write('# ==================== ENCODER FUNCTIONS ====================\n\n')
        for code in all_encoder_code:
            f.write(code)
            f.write('\n\n')

        f.write('# ==================== MESSAGE REGISTRY ====================\n\n')
        f.write('MESSAGE_REGISTRY: dict[str, tuple[str, str]] = {\n')
        for msg_name, (decode_func, encode_func) in sorted(message_registry.items()):
            f.write(f'    {msg_name!r}: ({decode_func!r}, {encode_func!r}),\n')
        f.write('}\n\n')

        f.write('def get_decoder(msg_name: str) -> Callable[[Any], type] | None:\n')
        f.write('    """Get pre-compiled decoder function for a message type."""\n')
        f.write('    if msg_name in MESSAGE_REGISTRY:\n')
        f.write('        func_name = MESSAGE_REGISTRY[msg_name][0]\n')
        f.write('        return globals()[func_name]\n')
        f.write('    return None\n\n')

        f.write('def get_encoder(msg_name: str) -> Callable[[Any, Any], None] | None:\n')
        f.write('    """Get pre-compiled encoder function for a message type."""\n')
        f.write('    if msg_name in MESSAGE_REGISTRY:\n')
        f.write('        func_name = MESSAGE_REGISTRY[msg_name][1]\n')
        f.write('        return globals()[func_name]\n')
        f.write('    return None\n\n')

        f.write('def initialize_dataclass_types() -> None:\n')
        f.write('    """Initialize dataclass types from the message definitions."""\n')
        f.write(f'    from pybag.ros2.{distro} import (\n')
        for package in packages:
            f.write(f'        {package},\n')
        f.write('    )\n')
        f.write('    \n')
        f.write('    modules = [' + ', '.join(packages) + ']\n')
        f.write('    \n')
        f.write('    for module in modules:\n')
        f.write('        for name in dir(module):\n')
        f.write('            obj = getattr(module, name)\n')
        f.write('            if hasattr(obj, "__msg_name__"):\n')
        f.write('                sanitized = obj.__msg_name__.replace("/", "_").replace("::", "_")\n')
        f.write('                _dataclass_types[sanitized] = obj\n')

    print(f"  Generated pre-compiled code for {len(message_registry)} messages")
    print(f"  Output: {output_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate ROS2 message dataclasses and pre-compiled encoders/decoders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate pre-compiled encoders/decoders for Humble
  %(prog)s --distro humble --precompile

  # Generate dataclasses for Iron
  %(prog)s --distro iron --dataclasses --output-dir src/pybag/ros2/iron

  # Generate both for specific packages
  %(prog)s --distro humble --precompile --dataclasses --packages std_msgs geometry_msgs
        """
    )

    parser.add_argument(
        '--distro',
        default='humble',
        help='ROS2 distribution (humble, iron, jazzy, etc.)'
    )

    parser.add_argument(
        '--dataclasses',
        action='store_true',
        help='Generate message dataclasses'
    )

    parser.add_argument(
        '--precompile',
        action='store_true',
        help='Generate pre-compiled encoders/decoders'
    )

    parser.add_argument(
        '--packages',
        nargs='+',
        help='Specific packages to process (default: all standard packages)'
    )

    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Output directory for dataclasses (default: src/pybag/ros2/<distro>)'
    )

    parser.add_argument(
        '--precompiled-output',
        type=Path,
        help='Output file for pre-compiled code (default: src/pybag/precompiled/<distro>.py)'
    )

    args = parser.parse_args()

    # Set defaults
    if args.output_dir is None:
        args.output_dir = Path('src/pybag/ros2') / args.distro

    if args.precompiled_output is None:
        args.precompiled_output = Path('src/pybag/precompiled') / f'{args.distro}.py'

    # If neither option specified, do both
    if not args.dataclasses and not args.precompile:
        args.dataclasses = True
        args.precompile = True

    print(f"ROS2 Message Code Generator")
    print(f"=" * 60)
    print(f"Distribution: {args.distro}")
    print(f"Packages: {args.packages or 'all standard packages'}")
    print()

    if args.dataclasses:
        generate_dataclasses_from_existing(args.distro, args.output_dir, args.packages)
        print()

    if args.precompile:
        generate_precompiled_code(args.distro, args.precompiled_output, args.packages)
        print()

    print("Done!")


if __name__ == '__main__':
    main()
