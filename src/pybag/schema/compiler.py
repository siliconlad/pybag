"""Compile a schema into an efficient message decoder."""
from __future__ import annotations

import re
import struct
from types import SimpleNamespace
from typing import Callable

from pybag.encoding import MessageDecoder
from pybag.schema import (
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaField,
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
_TAB = '    '


def _sanitize(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


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

__all__ = ["compile_schema"]
