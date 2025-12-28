"""Compile ROS 2 schemas into efficient message encoders/decoders."""
from __future__ import annotations

import re
import struct
from dataclasses import make_dataclass
from itertools import count
from types import SimpleNamespace
from typing import Annotated, Any, Callable

import pybag.types as t
from pybag.encoding import MessageDecoder
from pybag.schema import (
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaField,
    SchemaFieldType,
    Sequence,
    String
)

# Map primitive ROS2 types to struct format characters
_STRUCT_FORMAT = {
    "bool": "?",
    "int8": "b",
    "uint8": "B",
    "int16": "h",
    "uint16": "H",
    "int32": "i",
    "uint32": "I",
    "int64": "q",
    "uint64": "Q",
    "float32": "f",
    "float64": "d",
}
_STRUCT_SIZE = {k: struct.calcsize(v) for k, v in _STRUCT_FORMAT.items()}
_WRITE_FORMAT = dict(_STRUCT_FORMAT, byte="B", char="B")
_WRITE_SIZE = {k: struct.calcsize(v) for k, v in _WRITE_FORMAT.items()}
_TAB = '    '


def _sanitize(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


def _to_uint8(value: Any) -> int:
    """Normalize ``value`` to an unsigned 8-bit integer."""

    if isinstance(value, int):
        return value
    if isinstance(value, (bytes, bytearray)):
        if len(value) != 1:
            error_msg = "Byte values must contain exactly one byte"
            raise ValueError(error_msg)
        return value[0]
    if isinstance(value, str):
        if len(value) != 1:
            error_msg = "Char values must contain exactly one character"
            raise ValueError(error_msg)
        return ord(value)
    error_msg = f"Cannot convert value of type {type(value)!r} to uint8"
    raise TypeError(error_msg)


def compile_schema(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[MessageDecoder], type]:
    """Compile ``schema`` into a decoder function.

    The returned function accepts a :class:`MessageDecoder` instance and returns a
    dynamically constructed dataclass instance with the decoded fields.
    """

    function_defs: list[str] = []
    compiled: dict[str, str] = {}
    dataclass_types: dict[str, type] = {}

    # Map primitive types to their annotated equivalents
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
        'char': t.ros2.char,
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
                return Annotated[list[elem_annotation], ("array", elem_annotation, field_type.length)]  # type: ignore[valid-type]
            elif isinstance(elem_type, String):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, str)
                return Annotated[list[elem_annotation], ("array", elem_annotation, field_type.length)]  # type: ignore[valid-type]
            elif isinstance(elem_type, Complex):
                # Create sub-type if needed - use Any for list element since type is dynamic
                sub_schema = sub_schemas[elem_type.type]
                elem_annotation = create_dataclass_type(sub_schema)
                return Annotated[list[Any], ("array", elem_annotation, field_type.length)]
            else:
                return Annotated[list[Any], ("array", Any, field_type.length)]
        elif isinstance(field_type, Sequence):
            elem_type = field_type.type
            if isinstance(elem_type, Primitive):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, Any)
                return Annotated[list[elem_annotation], ("array", elem_annotation, None)]  # type: ignore[valid-type]
            elif isinstance(elem_type, String):
                elem_annotation = _PRIMITIVE_TYPE_MAP.get(elem_type.type, str)
                return Annotated[list[elem_annotation], ("array", elem_annotation, None)]  # type: ignore[valid-type]
            elif isinstance(elem_type, Complex):
                # Create sub-type if needed - use Any for list element since type is dynamic
                sub_schema = sub_schemas[elem_type.type]
                elem_annotation = create_dataclass_type(sub_schema)
                return Annotated[list[Any], ("array", elem_annotation, None)]
            else:
                return Annotated[list[Any], ("array", Any, None)]
        elif isinstance(field_type, Complex):
            # Create sub-type if needed - use Any for type annotation since sub_type is runtime
            sub_schema = sub_schemas[field_type.type]
            sub_type = create_dataclass_type(sub_schema)
            return Annotated[Any, ("complex", field_type.type)]
        else:
            return Any

    # Create dataclass types for all schemas
    def create_dataclass_type(current: Schema) -> type:
        class_name = _sanitize(current.name)
        if class_name in dataclass_types:
            return dataclass_types[class_name]

        # Collect field names and types with proper annotations
        field_specs = []
        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                # Constants must be dataclass fields with a special annotation
                # so they can be recovered by the schema encoder
                base_type = schema_type_to_annotation(entry.type)
                # Wrap the type in a ('constant', base_type) annotation
                constant_annotation = Annotated[base_type, ('constant', base_type)]
                # Constants always have a default value
                field_specs.append((field_name, constant_annotation, entry.value))
            elif isinstance(entry, SchemaField):
                type_annotation = schema_type_to_annotation(entry.type)
                if entry.default is not None:
                    field_specs.append((field_name, type_annotation, entry.default))
                else:
                    field_specs.append((field_name, type_annotation))

        # Create dataclass with __msg_name__ attribute
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

        # Create dataclass type for this schema
        dataclass_type = create_dataclass_type(current)

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
            size = _STRUCT_SIZE[run_type]  # type: ignore[index]
            fmt = _STRUCT_FORMAT[run_type] * count  # type: ignore[index]
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
                # Skip constants - they're class attributes, not instance fields
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
                    # Special optimization for uint8 - keep as bytes instead of unpacking
                    if elem.type == 'uint8':
                        lines.append(
                            f"{_TAB}_fields[{field_name!r}] = _data.read({field_type.length})"
                        )
                    else:
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
                    # Special optimization for uint8 - keep as bytes instead of unpacking
                    if elem.type == 'uint8':
                        lines.append(f"{_TAB}_len = decoder.uint32()")
                        lines.append(f"{_TAB}_fields[{field_name!r}] = _data.read(_len)")
                    else:
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

        # Return dataclass instance - always instantiate, even if _fields is empty
        # (e.g., for messages with only constants or no fields at all)
        class_name = _sanitize(current.name)
        lines.append(f"{_TAB}return _dataclass_types[{class_name!r}](**_fields)")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "import struct\n" + "\n\n".join(function_defs)

    namespace: dict[str, object] = {"struct": struct, "_dataclass_types": dataclass_types}
    exec(code, namespace)
    return namespace[f"decode_{_sanitize(schema.name)}"]  # type: ignore[index]


def compile_serializer(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[Any, Any], None]:
    """Compile ``schema`` into a serializer function."""

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

        run_type: str | None = None
        run_values: list[str] = []

        def flush_run() -> None:
            nonlocal run_type, run_values
            if not run_values:
                return

            assert run_type is not None
            fmt = _WRITE_FORMAT[run_type]
            size = _WRITE_SIZE[run_type]
            value_exprs: list[str] = []
            for expr in run_values:
                if run_type in {"byte", "char"}:
                    value_var = new_var("value")
                    lines.append(f"{_TAB}{value_var} = _to_uint8({expr})")
                    value_exprs.append(value_var)
                elif run_type == "bool":
                    value_exprs.append(f"bool({expr})")
                else:
                    value_exprs.append(expr)

            if size > 1:
                lines.append(f"{_TAB}_payload.align({size})")
            fmt_repeat = fmt * len(run_values)
            pack_line = (
                _TAB
                + "_payload.write(struct_pack(fmt_prefix + "
                + repr(fmt_repeat)
                + ", "
                + ", ".join(value_exprs)
                + "))"
            )
            lines.append(pack_line)
            run_type = None
            run_values = []

        def emit(field_type: SchemaFieldType, value_expr: str, indent: int) -> list[str]:
            pad = _TAB * indent

            if isinstance(field_type, Primitive):
                primitive = field_type.type
                if primitive in _WRITE_FORMAT:
                    fmt = _WRITE_FORMAT[primitive]
                    size = _WRITE_SIZE[primitive]
                    value_var = new_var("value")
                    lines_ = [f"{pad}{value_var} = {value_expr}"]
                    if primitive in {"byte", "char"}:
                        lines_.append(f"{pad}{value_var} = _to_uint8({value_var})")
                    elif primitive == "bool":
                        lines_.append(f"{pad}{value_var} = bool({value_var})")
                    lines_.append(f"{pad}_payload.align({size})")
                    pack_line = (
                        f"{pad}_payload.write(struct_pack(fmt_prefix + {repr(fmt)}, {value_var}))"
                    )
                    lines_.append(pack_line)
                    return lines_
                return [f"{pad}encoder.{primitive}({value_expr})"]

            if isinstance(field_type, String):
                if field_type.type == 'wstring':
                    return [f"{pad}encoder.wstring({value_expr})"]
                # Regular string: inline UTF-8 encoding
                value_var = new_var("value")
                encoded_var = new_var("encoded")
                return [
                    f"{pad}{value_var} = {value_expr}",
                    f"{pad}{encoded_var} = {value_var}.encode()",
                    f"{pad}_payload.align(4)",
                    f"{pad}_payload.write(struct_pack(fmt_prefix + 'I', len({encoded_var}) + 1))",
                    f"{pad}_payload.write({encoded_var} + b'\\x00')",
                ]

            if isinstance(field_type, Array):
                elem = field_type.type
                values_var = new_var("values")
                result: list[str] = [f"{pad}{values_var} = {value_expr}"]
                # Add validation for fixed-size arrays (non-bounded)
                if not field_type.is_bounded:
                    expected_length = field_type.length
                    result.append(
                        f"{pad}if len({values_var}) != {expected_length}:"
                    )
                    result.append(
                        f"{pad}    raise ValueError(f'Fixed array size mismatch: expected {expected_length} elements, got {{len({values_var})}}')"
                    )
                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    # Special optimization for uint8 - write bytes directly if input is bytes/bytearray
                    if elem.type == 'uint8':
                        length_var = new_var("length")
                        result.append(f"{pad}{length_var} = len({values_var})")
                        result.append(f"{pad}if {length_var}:")
                        child_pad = _TAB * (indent + 1)
                        result.append(f"{child_pad}if isinstance({values_var}, (bytes, bytearray)):")
                        result.append(f"{child_pad}    _payload.write({values_var})")
                        result.append(f"{child_pad}else:")
                        result.append(
                            f"{child_pad}    _payload.write(struct_pack(fmt_prefix + 'B' * {length_var}, *{values_var}))"
                        )
                        return result
                    base_var = values_var
                    if elem.type in {"byte", "char"}:
                        converted_var = new_var("converted")
                        result.append(
                            f"{pad}{converted_var} = [_to_uint8(v) for v in {values_var}]"
                        )
                        base_var = converted_var
                    length_var = new_var("length")
                    result.append(f"{pad}{length_var} = len({base_var})")
                    result.append(f"{pad}if {length_var}:")
                    child_pad = _TAB * (indent + 1)
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type]
                    result.append(f"{child_pad}_payload.align({size})")
                    pack_line = (
                        child_pad
                        + "_payload.write(struct_pack(fmt_prefix + "
                        + repr(fmt)
                        + " * "
                        + length_var
                        + ", *"
                        + base_var
                        + "))"
                    )
                    result.append(pack_line)
                    return result
                if isinstance(elem, String):
                    item_var = new_var("item")
                    result.append(f"{pad}for {item_var} in {values_var}:")
                    result.extend(emit(elem, item_var, indent + 1))
                    return result
                if isinstance(elem, Complex):
                    if elem.type not in sub_schemas:
                        error_msg = f"Unknown schema for complex type {elem.type}"
                        raise ValueError(error_msg)
                    sub_func = build(sub_schemas[elem.type])
                    item_var = new_var("item")
                    result.append(f"{pad}for {item_var} in {values_var}:")
                    result.append(f"{_TAB * (indent + 1)}{sub_func}(encoder, {item_var})")
                    return result
                elem_name = getattr(elem, "type", "unknown")
                return [f"{pad}encoder.array('{elem_name}', {values_var})"]

            if isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    values_var = new_var("values")
                    result = [f"{pad}{values_var} = {value_expr}"]
                    # Special optimization for uint8 - write bytes directly if input is bytes/bytearray
                    if elem.type == 'uint8':
                        length_var = new_var("length")
                        result.append(f"{pad}{length_var} = len({values_var})")
                        result.append(f"{pad}_payload.align(4)")
                        result.append(
                            f"{pad}_payload.write(struct_pack(fmt_prefix + 'I', {length_var}))"
                        )
                        result.append(f"{pad}if {length_var}:")
                        child_pad = _TAB * (indent + 1)
                        result.append(f"{child_pad}if isinstance({values_var}, (bytes, bytearray)):")
                        result.append(f"{child_pad}    _payload.write({values_var})")
                        result.append(f"{child_pad}else:")
                        result.append(
                            f"{child_pad}    _payload.write(struct_pack(fmt_prefix + 'B' * {length_var}, *{values_var}))"
                        )
                        return result
                    base_var = values_var
                    if elem.type in {"byte", "char"}:
                        converted_var = new_var("converted")
                        result.append(
                            f"{pad}{converted_var} = [_to_uint8(v) for v in {values_var}]"
                        )
                        base_var = converted_var
                    length_var = new_var("length")
                    result.append(f"{pad}{length_var} = len({base_var})")
                    result.append(f"{pad}_payload.align(4)")
                    result.append(
                        f"{pad}_payload.write(struct_pack(fmt_prefix + 'I', {length_var}))"
                    )
                    result.append(f"{pad}if {length_var}:")
                    child_pad = _TAB * (indent + 1)
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type]
                    result.append(f"{child_pad}_payload.align({size})")
                    pack_line = (
                        child_pad
                        + "_payload.write(struct_pack(fmt_prefix + "
                        + repr(fmt)
                        + " * "
                        + length_var
                        + ", *"
                        + base_var
                        + "))"
                    )
                    result.append(pack_line)
                    return result
                if isinstance(elem, Primitive):
                    type_name = elem.type
                    return [f"{pad}encoder.sequence('{type_name}', {value_expr})"]
                if not isinstance(elem, (String, Complex)):
                    elem_name = getattr(elem, "type", "unknown")
                    return [f"{pad}encoder.sequence('{elem_name}', {value_expr})"]

                values_var = new_var("values")
                length_var = new_var("length")
                result = [
                    f"{pad}{values_var} = {value_expr}",
                    f"{pad}{length_var} = len({values_var})",
                    f"{pad}_payload.align(4)",
                    f"{pad}_payload.write(struct_pack(fmt_prefix + 'I', {length_var}))",
                ]
                if isinstance(elem, String):
                    item_var = new_var("item")
                    result.append(f"{pad}for {item_var} in {values_var}:")
                    result.extend(emit(elem, item_var, indent + 1))
                    return result

                if elem.type not in sub_schemas:
                    error_msg = f"Unknown schema for complex type {elem.type}"
                    raise ValueError(error_msg)
                sub_func = build(sub_schemas[elem.type])
                item_var = new_var("item")
                result.append(f"{pad}for {item_var} in {values_var}:")
                result.append(f"{_TAB * (indent + 1)}{sub_func}(encoder, {item_var})")
                return result

            if isinstance(field_type, Complex):
                if field_type.type not in sub_schemas:
                    error_msg = f"Unknown schema for complex type {field_type.type}"
                    raise ValueError(error_msg)
                sub_func = build(sub_schemas[field_type.type])
                return [f"{pad}{sub_func}(encoder, {value_expr})"]

            elem_name = getattr(field_type, "type", "unknown")
            return [f"{pad}encoder.encode('{elem_name}', {value_expr})"]

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                flush_run()
                continue
            if not isinstance(entry, SchemaField):
                flush_run()
                continue
            field_type = entry.type
            if isinstance(field_type, Primitive) and field_type.type in _WRITE_FORMAT:
                field_expr = f"message.{field_name}"
                if run_type == field_type.type:
                    run_values.append(field_expr)
                else:
                    flush_run()
                    run_type = field_type.type
                    run_values = [field_expr]
                continue

            flush_run()
            field_expr = f"message.{field_name}"
            lines.extend(emit(field_type, field_expr, 1))

        flush_run()
        lines.append(f"{_TAB}return")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "\n\n".join(function_defs)
    namespace: dict[str, object] = {"struct": struct, "_to_uint8": _to_uint8}
    exec(code, namespace)
    return namespace[f"encode_{_sanitize(schema.name)}"]  # type: ignore[index]


__all__ = ["compile_schema", "compile_serializer"]
