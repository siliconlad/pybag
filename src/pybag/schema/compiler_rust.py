"""Optimized Rust-aware schema compiler."""
import re
import struct
from dataclasses import make_dataclass
from itertools import count
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

# Import the original compiler functions
from pybag.schema.compiler import _sanitize, _STRUCT_FORMAT, _STRUCT_SIZE, _WRITE_FORMAT, _WRITE_SIZE, _TAB, _to_uint8, compile_serializer


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


def _has_batch_methods(decoder: MessageDecoder) -> bool:
    """Check if decoder supports batched operations."""
    return hasattr(decoder, 'read_int32_batch')


def compile_schema_rust_optimized(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[MessageDecoder], type]:
    """Compile schema with Rust-specific optimizations for batched operations.

    This version generates code that uses batched read methods when available,
    significantly reducing PyO3 boundary crossings.
    """

    function_defs: list[str] = []
    compiled: dict[str, str] = {}
    dataclass_types: dict[str, type] = {}

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

        dataclass_type = create_dataclass_type(current)

        compiled[func_name] = func_name
        lines: list[str] = [
            f"def {func_name}(decoder):",
            f"{_TAB}fmt_prefix = '<' if decoder._is_little_endian else '>'",
            f"{_TAB}_data = decoder._data",
            f"{_TAB}_fields = {{}}",
            f"{_TAB}_use_batch = hasattr(decoder, 'read_int32_batch')",
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

            # Use batched Rust method if available
            batch_method_map = {
                'bool': 'read_bool_batch',
                'int8': 'read_int8_batch',
                'uint8': 'read_uint8_batch',
                'int16': 'read_int16_batch',
                'uint16': 'read_uint16_batch',
                'int32': 'read_int32_batch',
                'uint32': 'read_uint32_batch',
                'int64': 'read_int64_batch',
                'uint64': 'read_uint64_batch',
                'float32': 'read_float32_batch',
                'float64': 'read_float64_batch',
            }

            batch_method = batch_method_map.get(run_type)  # type: ignore[arg-type]

            if batch_method and count > 1:
                # Use Rust batched method
                names = ", ".join(run_fields)
                lines.append(f"{_TAB}if _use_batch:")
                lines.append(f"{_TAB}{_TAB}{names} = decoder.{batch_method}({count})")
                lines.append(f"{_TAB}else:")
                lines.append(f"{_TAB}{_TAB}_data.align({size})")
                lines.append(f"{_TAB}{_TAB}{names} = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))")
                for field in run_fields:
                    lines.append(f"{_TAB}_fields[{field!r}] = {field}")
            else:
                # Use Python struct.unpack
                lines.append(f"{_TAB}_data.align({size})")
                if count > 1:
                    names = ", ".join(run_fields)
                    lines.append(f"{_TAB}{names} = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))")
                    for field in run_fields:
                        lines.append(f"{_TAB}_fields[{field!r}] = {field}")
                else:
                    field = run_fields[0]
                    lines.append(f"{_TAB}_fields[{field!r}] = struct.unpack(fmt_prefix + '{fmt}', _data.read({size * count}))[0]")

            run_fields.clear()
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
                    batch_method_map = {
                        'bool': 'read_bool_batch',
                        'int8': 'read_int8_batch',
                        'uint8': 'read_uint8_batch',
                        'int16': 'read_int16_batch',
                        'uint16': 'read_uint16_batch',
                        'int32': 'read_int32_batch',
                        'uint32': 'read_uint32_batch',
                        'int64': 'read_int64_batch',
                        'uint64': 'read_uint64_batch',
                        'float32': 'read_float32_batch',
                        'float64': 'read_float64_batch',
                    }
                    batch_method = batch_method_map.get(elem.type)
                    if batch_method:
                        lines.append(f"{_TAB}if _use_batch:")
                        lines.append(f"{_TAB}{_TAB}_fields[{field_name!r}] = list(decoder.{batch_method}({field_type.length}))")
                        lines.append(f"{_TAB}else:")
                        lines.append(f"{_TAB}{_TAB}_data.align({size})")
                        lines.append(f"{_TAB}{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{fmt}', _data.read({size * field_type.length})))")
                    else:
                        lines.append(f"{_TAB}_data.align({size})")
                        lines.append(f"{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{fmt}', _data.read({size * field_type.length})))")
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}_fields[{field_name!r}] = [{sub_func}(decoder) for _ in range({field_type.length})]")
                elif isinstance(elem, String):
                    elem_name = elem.type
                    lines.append(f"{_TAB}_fields[{field_name!r}] = [decoder.{elem_name}() for _ in range({field_type.length})]")
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.array('{elem_name}', {field_type.length})")

            elif isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    char = _STRUCT_FORMAT[elem.type]
                    batch_method_map = {
                        'bool': 'read_bool_batch',
                        'int8': 'read_int8_batch',
                        'uint8': 'read_uint8_batch',
                        'int16': 'read_int16_batch',
                        'uint16': 'read_uint16_batch',
                        'int32': 'read_int32_batch',
                        'uint32': 'read_uint32_batch',
                        'int64': 'read_int64_batch',
                        'uint64': 'read_uint64_batch',
                        'float32': 'read_float32_batch',
                        'float64': 'read_float64_batch',
                    }
                    batch_method = batch_method_map.get(elem.type)
                    if batch_method:
                        lines.append(f"{_TAB}_len = decoder.uint32()")
                        lines.append(f"{_TAB}if _use_batch:")
                        lines.append(f"{_TAB}{_TAB}_fields[{field_name!r}] = list(decoder.{batch_method}(_len))")
                        lines.append(f"{_TAB}else:")
                        lines.append(f"{_TAB}{_TAB}_data.align({size})")
                        lines.append(f"{_TAB}{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{char}' * _len, _data.read({size} * _len)))")
                    else:
                        lines.append(f"{_TAB}_len = decoder.uint32()")
                        lines.append(f"{_TAB}_data.align({size})")
                        lines.append(f"{_TAB}_fields[{field_name!r}] = list(struct.unpack(fmt_prefix + '{char}' * _len, _data.read({size} * _len)))")
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    lines.append(f"{_TAB}_fields[{field_name!r}] = [{sub_func}(decoder) for _ in range(length)]")
                elif isinstance(elem, String):
                    lines.append(f"{_TAB}length = decoder.uint32()")
                    elem_name = elem.type
                    lines.append(f"{_TAB}_fields[{field_name!r}] = [decoder.{elem_name}() for _ in range(length)]")
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

    namespace: dict[str, object] = {"struct": struct, "_dataclass_types": dataclass_types}
    exec(code, namespace)
    return namespace[f"decode_{_sanitize(schema.name)}"]  # type: ignore[index]


__all__ = ['compile_schema_rust_optimized', 'compile_serializer']
