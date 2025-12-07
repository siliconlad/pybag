#!/usr/bin/env python3
"""Generate ROS2 message dataclasses with embedded encode/decode methods.

This script generates Python dataclasses for ROS2 messages with pre-compiled
encode() and decode() methods embedded directly in the class definitions.
"""

from __future__ import annotations

import argparse
import importlib
import re
from dataclasses import fields, is_dataclass
from itertools import count
from pathlib import Path
from typing import Any

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
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder


def generate_dataclass_with_methods(
    msg_type: type,
    encoder: Ros2MsgSchemaEncoder,
    all_schemas: dict[str, tuple[Schema, dict[str, Schema]]]
) -> str:
    """Generate a complete dataclass with encode/decode methods."""

    msg_name = msg_type.__msg_name__
    class_name = msg_name.split('/')[-1]

    # Parse schema
    schema, sub_schemas = encoder.parse_schema(msg_type)
    all_schemas[schema.name] = (schema, sub_schemas)

    lines = []

    # Generate dataclass definition
    lines.append("@dataclass(kw_only=True)")
    lines.append(f"class {class_name}:")
    lines.append(f"    __msg_name__ = '{msg_name}'")
    lines.append("")

    # Add fields
    field_definitions = []
    for field_name, entry in schema.fields.items():
        if isinstance(entry, SchemaConstant):
            # Constant field
            field_type = entry.type
            value = entry.value

            # Format the value appropriately
            if isinstance(field_type, String):
                formatted_value = repr(value)
            elif isinstance(field_type, Primitive):
                if field_type.type == 'bool':
                    formatted_value = str(value).capitalize()
                else:
                    formatted_value = str(value)
            else:
                formatted_value = repr(value)

            type_annotation = get_type_annotation(field_type)
            field_definitions.append(f"    {field_name}: {type_annotation} = {formatted_value}")

        elif isinstance(entry, SchemaField):
            # Regular field
            type_annotation = get_type_annotation(entry.type, sub_schemas)

            if entry.default is not None:
                field_definitions.append(f"    {field_name}: {type_annotation} = {entry.default}")
            else:
                field_definitions.append(f"    {field_name}: {type_annotation}")

    lines.extend(field_definitions)
    lines.append("")

    # Generate decode method
    decode_code = generate_decode_method(schema, sub_schemas)
    lines.extend(decode_code)
    lines.append("")

    # Generate encode method
    encode_code = generate_encode_method(schema, sub_schemas)
    lines.extend(encode_code)

    return '\n'.join(lines)


def get_type_annotation(field_type: SchemaFieldType, sub_schemas: dict[str, Schema] | None = None) -> str:
    """Get the type annotation string for a field type."""

    if isinstance(field_type, Primitive):
        return f"t.{field_type.type}"

    elif isinstance(field_type, String):
        return f"t.{field_type.type}"

    elif isinstance(field_type, Array):
        elem_type = field_type.type
        elem_annotation = get_type_annotation(elem_type, sub_schemas)

        # For complex types in arrays, we need to reference the class
        if isinstance(elem_type, Complex):
            # Extract just the class name from the full message name
            class_name = elem_type.type.split('/')[-1]
            return f"t.Array[t.Complex[{class_name}], {field_type.length}]"
        else:
            return f"t.Array[{elem_annotation}, {field_type.length}]"

    elif isinstance(field_type, Sequence):
        elem_type = field_type.type
        elem_annotation = get_type_annotation(elem_type, sub_schemas)

        # For complex types in sequences, we need to reference the class
        if isinstance(elem_type, Complex):
            class_name = elem_type.type.split('/')[-1]
            return f"t.Sequence[t.Complex[{class_name}]]"
        else:
            return f"t.Sequence[{elem_annotation}]"

    elif isinstance(field_type, Complex):
        # Extract just the class name from the full message name
        class_name = field_type.type.split('/')[-1]
        return f"t.Complex[{class_name}]"

    else:
        return "t.Any"


def generate_decode_method(schema: Schema, sub_schemas: dict[str, Schema]) -> list[str]:
    """Generate the decode() static method."""

    lines = []
    lines.append("    @staticmethod")
    lines.append("    def decode(decoder):")
    lines.append("        \"\"\"Decode message from decoder.\"\"\"")
    lines.append("        fmt_prefix = '<' if decoder._is_little_endian else '>'")
    lines.append("        _data = decoder._data")
    lines.append("        _fields = {}")
    lines.append("")

    # Generate decoding logic
    run_type: str | None = None
    run_fields: list[str] = []

    def flush_batch():
        nonlocal run_type, run_fields
        if not run_fields:
            return

        count_val = len(run_fields)
        size = _STRUCT_SIZE[run_type]
        fmt = _STRUCT_FORMAT[run_type] * count_val
        lines.append(f"        _data.align({size})")

        if count_val > 1:
            names = ", ".join(run_fields)
            lines.append(f"        {names} = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count_val}))")
            for field in run_fields:
                lines.append(f"        _fields['{field}'] = {field}")
        else:
            field = run_fields[0]
            lines.append(f"        _fields['{field}'] = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count_val}))[0]")

        run_fields = []
        run_type = None

    for field_name, entry in schema.fields.items():
        if isinstance(entry, SchemaConstant):
            flush_batch()
            continue

        if not isinstance(entry, SchemaField):
            flush_batch()
            lines.append(f"        _fields['{field_name}'] = None")
            continue

        field_type = entry.type

        # Batch consecutive primitive fields of the same type
        if isinstance(field_type, Primitive) and field_type.type in _STRUCT_FORMAT:
            if run_type == field_type.type:
                run_fields.append(field_name)
            else:
                flush_batch()
                run_type = field_type.type
                run_fields = [field_name]
            continue

        flush_batch()

        if isinstance(field_type, Primitive):
            lines.append(f"        _fields['{field_name}'] = decoder.{field_type.type}()")

        elif isinstance(field_type, String):
            lines.append(f"        _fields['{field_name}'] = decoder.{field_type.type}()")

        elif isinstance(field_type, Array):
            elem = field_type.type
            if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                size = _STRUCT_SIZE[elem.type]
                fmt = _STRUCT_FORMAT[elem.type] * field_type.length
                lines.append(f"        _data.align({size})")
                lines.append(f"        _fields['{field_name}'] = list(struct.unpack(fmt_prefix + '{fmt}', _data.read({size * field_type.length})))")
            elif isinstance(elem, Complex):
                # Use the class name directly
                class_name = elem.type.split('/')[-1]
                lines.append(f"        _fields['{field_name}'] = [{class_name}.decode(decoder) for _ in range({field_type.length})]")
            elif isinstance(elem, String):
                lines.append(f"        _fields['{field_name}'] = [decoder.{elem.type}() for _ in range({field_type.length})]")
            else:
                elem_name = getattr(elem, "type", "unknown")
                lines.append(f"        _fields['{field_name}'] = decoder.array('{elem_name}', {field_type.length})")

        elif isinstance(field_type, Sequence):
            elem = field_type.type
            if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                size = _STRUCT_SIZE[elem.type]
                char = _STRUCT_FORMAT[elem.type]
                lines.append(f"        _len = decoder.uint32()")
                lines.append(f"        _data.align({size})")
                lines.append(f"        _fields['{field_name}'] = list(struct.unpack(fmt_prefix + '{char}' * _len, _data.read({size} * _len)))")
            elif isinstance(elem, Complex):
                class_name = elem.type.split('/')[-1]
                lines.append(f"        _len = decoder.uint32()")
                lines.append(f"        _fields['{field_name}'] = [{class_name}.decode(decoder) for _ in range(_len)]")
            elif isinstance(elem, String):
                lines.append(f"        _len = decoder.uint32()")
                lines.append(f"        _fields['{field_name}'] = [decoder.{elem.type}() for _ in range(_len)]")
            else:
                elem_name = getattr(elem, "type", "unknown")
                lines.append(f"        _fields['{field_name}'] = decoder.sequence('{elem_name}')")

        elif isinstance(field_type, Complex):
            class_name = field_type.type.split('/')[-1]
            lines.append(f"        _fields['{field_name}'] = {class_name}.decode(decoder)")

        else:
            lines.append(f"        _fields['{field_name}'] = None")

    flush_batch()

    class_name = schema.name.split('/')[-1]
    lines.append(f"        return {class_name}(**_fields)")

    return lines


def generate_encode_method(schema: Schema, sub_schemas: dict[str, Schema]) -> list[str]:
    """Generate the encode() instance method."""

    lines = []
    lines.append("    def encode(self, encoder):")
    lines.append("        \"\"\"Encode message to encoder.\"\"\"")
    lines.append("        fmt_prefix = '<' if encoder._is_little_endian else '>'")
    lines.append("        _payload = encoder._payload")
    lines.append("")

    for field_name, entry in schema.fields.items():
        if isinstance(entry, SchemaConstant):
            continue

        if not isinstance(entry, SchemaField):
            continue

        field_type = entry.type

        if isinstance(field_type, Primitive) and field_type.type in _WRITE_FORMAT:
            size = _WRITE_SIZE[field_type.type]
            fmt = _WRITE_FORMAT[field_type.type]
            lines.append(f"        _payload.align({size})")

            if field_type.type in ("byte", "char"):
                lines.append(f"        _value = self.{field_name}")
                lines.append(f"        if isinstance(_value, str):")
                lines.append(f"            _value = ord(_value)")
                lines.append(f"        elif isinstance(_value, (bytes, bytearray)):")
                lines.append(f"            _value = _value[0]")
                lines.append(f"        _payload.write(struct.pack(fmt_prefix + '{fmt}', _value))")
            else:
                lines.append(f"        _payload.write(struct.pack(fmt_prefix + '{fmt}', self.{field_name}))")

        elif isinstance(field_type, String):
            lines.append(f"        encoder.{field_type.type}(self.{field_name})")

        elif isinstance(field_type, Array):
            elem = field_type.type
            if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                size = _WRITE_SIZE[elem.type]
                fmt = _WRITE_FORMAT[elem.type] * field_type.length
                lines.append(f"        _payload.align({size})")

                if elem.type in ("byte", "char"):
                    lines.append(f"        _normalized = []")
                    lines.append(f"        for _item in self.{field_name}:")
                    lines.append(f"            if isinstance(_item, str):")
                    lines.append(f"                _normalized.append(ord(_item))")
                    lines.append(f"            elif isinstance(_item, (bytes, bytearray)):")
                    lines.append(f"                _normalized.append(_item[0])")
                    lines.append(f"            else:")
                    lines.append(f"                _normalized.append(_item)")
                    lines.append(f"        _payload.write(struct.pack(fmt_prefix + '{fmt}', *_normalized))")
                else:
                    lines.append(f"        _payload.write(struct.pack(fmt_prefix + '{fmt}', *self.{field_name}))")
            elif isinstance(elem, Complex):
                lines.append(f"        for _item in self.{field_name}:")
                lines.append(f"            _item.encode(encoder)")
            elif isinstance(elem, String):
                lines.append(f"        for _item in self.{field_name}:")
                lines.append(f"            encoder.{elem.type}(_item)")

        elif isinstance(field_type, Sequence):
            elem = field_type.type
            lines.append(f"        encoder.uint32(len(self.{field_name}))")

            if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                size = _WRITE_SIZE[elem.type]
                fmt = _WRITE_FORMAT[elem.type]
                lines.append(f"        _payload.align({size})")

                if elem.type in ("byte", "char"):
                    lines.append(f"        for _item in self.{field_name}:")
                    lines.append(f"            if isinstance(_item, str):")
                    lines.append(f"                _payload.write(struct.pack(fmt_prefix + '{fmt}', ord(_item)))")
                    lines.append(f"            elif isinstance(_item, (bytes, bytearray)):")
                    lines.append(f"                _payload.write(struct.pack(fmt_prefix + '{fmt}', _item[0]))")
                    lines.append(f"            else:")
                    lines.append(f"                _payload.write(struct.pack(fmt_prefix + '{fmt}', _item))")
                else:
                    lines.append(f"        for _item in self.{field_name}:")
                    lines.append(f"            _payload.write(struct.pack(fmt_prefix + '{fmt}', _item))")
            elif isinstance(elem, Complex):
                lines.append(f"        for _item in self.{field_name}:")
                lines.append(f"            _item.encode(encoder)")
            elif isinstance(elem, String):
                lines.append(f"        for _item in self.{field_name}:")
                lines.append(f"            encoder.{elem.type}(_item)")

        elif isinstance(field_type, Complex):
            lines.append(f"        self.{field_name}.encode(encoder)")

    return lines


def generate_package_file(package: str, distro: str, output_dir: Path) -> None:
    """Generate a complete package file with all message types."""

    print(f"Generating {package}...")

    # Import the existing package to get message types
    try:
        module = importlib.import_module(f'pybag.ros2.{distro}.{package}')
    except ImportError:
        print(f"  Warning: Could not import pybag.ros2.{distro}.{package}")
        return

    # Collect all message types
    message_types = []
    for name in dir(module):
        obj = getattr(module, name)
        if is_dataclass(obj) and hasattr(obj, '__msg_name__'):
            message_types.append(obj)

    if not message_types:
        print(f"  No message types found in {package}")
        return

    print(f"  Found {len(message_types)} message types")

    # Sort by dependencies (simple messages first)
    # This ensures dependencies are defined before they're used
    sorted_types = sort_by_dependencies(message_types)

    # Generate code for all messages
    encoder = Ros2MsgSchemaEncoder()
    all_schemas = {}

    message_code = []
    for msg_type in sorted_types:
        try:
            code = generate_dataclass_with_methods(msg_type, encoder, all_schemas)
            message_code.append(code)
        except Exception as e:
            print(f"  Error generating {msg_type.__msg_name__}: {e}")
            continue

    # Determine dependencies on other packages
    dependencies = set()
    for msg_type in message_types:
        for field in fields(msg_type):
            field_type_str = str(field.type)
            # Look for references to other packages
            for other_pkg in ['builtin_interfaces', 'std_msgs', 'geometry_msgs', 'nav_msgs', 'sensor_msgs']:
                if other_pkg != package and other_pkg in field_type_str:
                    dependencies.add(other_pkg)

    # Write output file
    output_file = output_dir / f"{package}.py"

    with open(output_file, 'w') as f:
        f.write(f'"""ROS2 {package} message types for {distro}.\n\n')
        f.write('Auto-generated with embedded encode/decode methods.\n')
        f.write('"""\n\n')
        f.write('from __future__ import annotations\n\n')
        f.write('import struct\n')
        f.write('from dataclasses import dataclass\n')
        f.write('from typing import TYPE_CHECKING\n\n')
        f.write('import pybag.types as t\n')

        # Add imports for dependencies
        if dependencies:
            f.write('\n')
            # Import dependencies at runtime (needed for encode/decode methods)
            for dep in sorted(dependencies):
                f.write(f'from pybag.ros2.{distro} import {dep}\n')

        f.write('\n\n')

        # Write all message definitions
        for code in message_code:
            f.write(code)
            f.write('\n\n\n')

    print(f"  Generated {output_file}")


def sort_by_dependencies(message_types: list[type]) -> list[type]:
    """Sort message types so dependencies come first."""

    # Build dependency graph
    deps = {}
    for msg_type in message_types:
        msg_name = msg_type.__msg_name__
        class_name = msg_name.split('/')[-1]

        dependencies = set()
        for field in fields(msg_type):
            field_type_str = str(field.type)
            # Look for references to other messages in the same package
            for other_msg in message_types:
                other_name = other_msg.__msg_name__.split('/')[-1]
                if other_name != class_name and other_name in field_type_str:
                    dependencies.add(other_name)

        deps[class_name] = dependencies

    # Topological sort
    sorted_names = []
    visited = set()

    def visit(name: str):
        if name in visited:
            return
        visited.add(name)
        for dep in deps.get(name, set()):
            visit(dep)
        sorted_names.append(name)

    for msg_type in message_types:
        class_name = msg_type.__msg_name__.split('/')[-1]
        visit(class_name)

    # Return message types in sorted order
    name_to_type = {msg.__msg_name__.split('/')[-1]: msg for msg in message_types}
    return [name_to_type[name] for name in sorted_names if name in name_to_type]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate ROS2 message dataclasses with embedded encode/decode methods',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all standard packages for Humble
  %(prog)s --distro humble --all

  # Generate specific packages
  %(prog)s --distro humble --packages std_msgs geometry_msgs

  # Custom output directory
  %(prog)s --distro humble --packages nav_msgs --output-dir src/pybag/ros2/humble
        """
    )

    parser.add_argument(
        '--distro',
        default='humble',
        help='ROS2 distribution (default: humble)'
    )

    parser.add_argument(
        '--packages',
        nargs='+',
        help='Specific packages to generate'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Generate all standard packages'
    )

    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Output directory (default: src/pybag/ros2/<distro>)'
    )

    args = parser.parse_args()

    # Set defaults
    if args.output_dir is None:
        args.output_dir = Path('src/pybag/ros2') / args.distro

    # Determine which packages to generate
    if args.all or not args.packages:
        packages = ['builtin_interfaces', 'std_msgs', 'geometry_msgs', 'nav_msgs', 'sensor_msgs']
    else:
        packages = args.packages

    print(f"ROS2 Message Dataclass Generator")
    print(f"=" * 60)
    print(f"Distribution: {args.distro}")
    print(f"Output: {args.output_dir}")
    print(f"Packages: {', '.join(packages)}")
    print()

    # Ensure output directory exists
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Generate each package
    for package in packages:
        generate_package_file(package, args.distro, args.output_dir)

    print()
    print("Done!")


if __name__ == '__main__':
    main()
