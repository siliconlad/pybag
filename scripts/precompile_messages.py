#!/usr/bin/env python3
"""Pre-compile standard ROS2 message types at build time.

This script generates optimized encoder/decoder functions for common ROS2
message types and saves them to a Python module for fast loading.
"""

from __future__ import annotations

import importlib
import inspect
import re
import struct
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


def get_standard_message_types() -> list[type]:
    """Get all standard ROS2 message types from the humble distribution."""
    message_types = []

    # List of standard ROS2 packages to pre-compile
    packages = [
        'builtin_interfaces',
        'std_msgs',
        'geometry_msgs',
        'nav_msgs',
        'sensor_msgs',
    ]

    for package in packages:
        try:
            module = importlib.import_module(f'pybag.ros2.humble.{package}')
            for name in dir(module):
                obj = getattr(module, name)
                if is_dataclass(obj) and hasattr(obj, '__msg_name__'):
                    message_types.append(obj)
        except ImportError:
            print(f"Warning: Could not import {package}")

    return message_types


def compile_schema_to_code(schema: Schema, sub_schemas: dict[str, Schema]) -> tuple[str, dict[str, type]]:
    """Generate decoder code for a schema without exec'ing it.

    Returns the generated code as a string and the dataclass types dictionary.
    This is a modified version of compile_schema that returns the code instead of exec'ing it.
    """
    function_defs: list[str] = []
    compiled: dict[str, str] = {}
    dataclass_types: dict[str, type] = {}

    # Map primitive types to their annotated equivalents (copied from compile_schema)
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
        """Convert a schema field type to a proper type annotation."""
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
        field_names: list[str] = []
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

            field_names.append(field_name)

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


def main():
    """Main pre-compilation script."""
    print("Pre-compiling standard ROS2 message types...")

    # Get all standard message types
    message_types = get_standard_message_types()
    print(f"Found {len(message_types)} message types to pre-compile")

    # Create schema encoder
    encoder = Ros2MsgSchemaEncoder()

    # Store all compiled code
    all_decoder_code = []
    all_encoder_code = []
    all_dataclass_types = {}
    message_registry = {}

    for msg_type in message_types:
        msg_name = msg_type.__msg_name__
        print(f"  Compiling {msg_name}...")

        try:
            # Parse schema
            schema, sub_schemas = encoder.parse_schema(msg_type)

            # Generate decoder code
            decoder_code, dataclass_types = compile_schema_to_code(schema, sub_schemas)
            all_decoder_code.append(f"# Decoder for {msg_name}\n{decoder_code}")

            # Generate encoder code
            encoder_code = compile_serializer_to_code(schema, sub_schemas)
            all_encoder_code.append(f"# Encoder for {msg_name}\n{encoder_code}")

            # Store dataclass types
            all_dataclass_types.update(dataclass_types)

            # Register message
            decode_func_name = f"decode_{_sanitize(schema.name)}"
            encode_func_name = f"encode_{_sanitize(schema.name)}"
            message_registry[msg_name] = (decode_func_name, encode_func_name)

        except Exception as e:
            print(f"    Error compiling {msg_name}: {e}")
            continue

    # Generate the pre-compiled module
    output_dir = Path(__file__).parent.parent / 'src' / 'pybag' / 'precompiled'
    output_dir.mkdir(exist_ok=True)

    # Write the pre-compiled module
    output_file = output_dir / 'humble.py'

    with open(output_file, 'w') as f:
        f.write('"""Pre-compiled ROS2 message encoders/decoders for Humble.\n\n')
        f.write('This file is auto-generated by scripts/precompile_messages.py\n')
        f.write('DO NOT EDIT MANUALLY.\n')
        f.write('"""\n\n')
        f.write('from __future__ import annotations\n\n')
        f.write('import struct\n')
        f.write('from typing import TYPE_CHECKING, Any, Callable\n\n')
        f.write('if TYPE_CHECKING:\n')
        f.write('    from pybag.encoding import MessageDecoder, MessageEncoder\n\n')

        # Write dataclass types initialization
        f.write('# Dataclass types dictionary will be populated at runtime\n')
        f.write('_dataclass_types: dict[str, type] = {}\n\n')

        # Write all decoder functions
        f.write('# ==================== DECODER FUNCTIONS ====================\n\n')
        for code in all_decoder_code:
            f.write(code)
            f.write('\n\n')

        # Write all encoder functions
        f.write('# ==================== ENCODER FUNCTIONS ====================\n\n')
        for code in all_encoder_code:
            f.write(code)
            f.write('\n\n')

        # Write message registry
        f.write('# ==================== MESSAGE REGISTRY ====================\n\n')
        f.write('# Maps message names to (decoder_func, encoder_func) tuples\n')
        f.write('MESSAGE_REGISTRY: dict[str, tuple[str, str]] = {\n')
        for msg_name, (decode_func, encode_func) in sorted(message_registry.items()):
            f.write(f'    {msg_name!r}: ({decode_func!r}, {encode_func!r}),\n')
        f.write('}\n\n')

        # Write helper functions to get compiled functions
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

        # Write initialization function
        f.write('def initialize_dataclass_types() -> None:\n')
        f.write('    """Initialize dataclass types from the message definitions."""\n')
        f.write('    from pybag.ros2.humble import (\n')
        f.write('        builtin_interfaces,\n')
        f.write('        geometry_msgs,\n')
        f.write('        nav_msgs,\n')
        f.write('        sensor_msgs,\n')
        f.write('        std_msgs,\n')
        f.write('    )\n')
        f.write('    \n')
        f.write('    # Map sanitized names to actual dataclass types\n')
        f.write('    modules = [builtin_interfaces, geometry_msgs, nav_msgs, sensor_msgs, std_msgs]\n')
        f.write('    \n')
        f.write('    for module in modules:\n')
        f.write('        for name in dir(module):\n')
        f.write('            obj = getattr(module, name)\n')
        f.write('            if hasattr(obj, "__msg_name__"):\n')
        f.write('                # Sanitize the message name to match the dataclass type name\n')
        f.write('                sanitized = obj.__msg_name__.replace("/", "_").replace("::", "_")\n')
        f.write('                _dataclass_types[sanitized] = obj\n')

    print(f"\nPre-compilation complete!")
    print(f"Generated {len(message_registry)} message encoders/decoders")
    print(f"Output: {output_file}")


if __name__ == '__main__':
    main()
