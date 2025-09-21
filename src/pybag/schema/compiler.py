"""Compile ROS 2 schemas into efficient message encoders/decoders."""
from __future__ import annotations

import re
import struct
from itertools import count
from types import SimpleNamespace
from typing import Any, Callable

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
    dynamically constructed type with the decoded fields as class attributes.
    """

    function_defs: list[str] = []
    compiled: dict[str, str] = {}

    def build(current: Schema) -> str:
        func_name = f"decode_{_sanitize(current.name)}"
        if func_name in compiled:
            return func_name

        compiled[func_name] = func_name
        lines: list[str] = [
            f"def {func_name}(decoder):",
            f"{_TAB}fmt_prefix = '<' if decoder._is_little_endian else '>'",
            f"{_TAB}_data = decoder._data",
            f"{_TAB}obj = SimpleNamespace()",
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
                    lines.append(f"{_TAB}obj.{field} = {field}")
            else:
                field = run_fields[0]
                lines.append(f"{_TAB}obj.{field} = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))[0]")

            run_fields = []
            run_type = None

        for field_name, entry in current.fields.items():
            field_names.append(field_name)
            if isinstance(entry, SchemaConstant):
                flush()
                lines.append(f"{_TAB}obj.{field_name} = {repr(entry.value)}")
                continue

            if not isinstance(entry, SchemaField):
                flush()
                lines.append(f"{_TAB}obj.{field_name} = None")
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
                lines.append(f"{_TAB}obj.{field_name} = decoder.{field_type.type}()")

            elif isinstance(field_type, String):
                lines.append(f"{_TAB}obj.{field_name} = decoder.{field_type.type}()")

            elif isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    fmt = _STRUCT_FORMAT[elem.type] * field_type.length
                    lines.append(f"{_TAB}_data.align({size})")
                    lines.append(
                        f"{_TAB}obj.{field_name} = list(struct.unpack(fmt_prefix + '{fmt}', _data.read({size * field_type.length})))"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(
                        f"{_TAB}obj.{field_name} = [{sub_func}(decoder) for _ in range({field_type.length})]"
                    )
                elif isinstance(elem, String):
                    elem_name = elem.type
                    lines.append(
                        f"{_TAB}obj.{field_name} = [decoder.{elem_name}() for _ in range({field_type.length})]"
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(
                        f"{_TAB}obj.{field_name} = decoder.array('{elem_name}', {field_type.length})"
                    )

            elif isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    char = _STRUCT_FORMAT[elem.type]
                    lines.append(f"{_TAB}_len = decoder.uint32()")
                    lines.append(f"{_TAB}_data.align({size})")
                    lines.append(
                        f"{_TAB}obj.{field_name} = list(struct.unpack(fmt_prefix + '{char}' * _len, _data.read({size} * _len)))"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    lines.append(
                        f"{_TAB}obj.{field_name} = [{sub_func}(decoder) for _ in range(length)]"
                    )
                elif isinstance(elem, String):
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    elem_name = elem.type
                    lines.append(
                        f"{_TAB}obj.{field_name} = [decoder.{elem_name}() for _ in range(length)]"
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(f"{_TAB}obj.{field_name} = decoder.sequence('{elem_name}')")

            elif isinstance(field_type, Complex):
                sub_schema = sub_schemas[field_type.type]
                sub_func = build(sub_schema)
                lines.append(f"{_TAB}obj.{field_name} = {sub_func}(decoder)")

            else:
                lines.append(f"{_TAB}obj.{field_name} = None")

        flush()
        lines.append(f"{_TAB}return {'obj' if field_names else 'None'}")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "import struct\nfrom types import SimpleNamespace\n" + "\n\n".join(function_defs)
    namespace: dict[str, object] = {"struct": struct, "SimpleNamespace": SimpleNamespace}
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
                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
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
