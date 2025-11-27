"""Compile schemas into JSON message encoders/decoders.

Unlike the CDR-specific compiler, this generates code that uses encoder/decoder
methods instead of direct byte access, making it compatible with JSON encoding.
"""

from __future__ import annotations

import re
from dataclasses import make_dataclass
from itertools import count
from typing import Annotated, Any, Callable

import pybag.types as t
from pybag.encoding import MessageDecoder, MessageEncoder
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

_TAB = '    '


def _sanitize(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


def compile_json_schema(
    schema: Schema, sub_schemas: dict[str, Schema]
) -> Callable[[MessageDecoder], type]:
    """Compile schema into a JSON decoder function.

    Unlike compile_schema, this generates code that uses decoder methods
    instead of direct byte access, making it compatible with JsonDecoder.
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
            create_dataclass_type(sub_schema)
            return Annotated[Any, ("complex", field_type.type)]
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
            kw_only=True,
        )

        dataclass_types[class_name] = dataclass_type
        return dataclass_type

    def build(current: Schema) -> str:
        func_name = f"decode_{_sanitize(current.name)}"
        if func_name in compiled:
            return func_name

        # Create dataclass type for this schema
        create_dataclass_type(current)

        compiled[func_name] = func_name
        lines: list[str] = [
            f"def {func_name}(decoder):",
            f"{_TAB}_fields = {{}}",
        ]

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                # Skip constants
                continue

            if not isinstance(entry, SchemaField):
                lines.append(f"{_TAB}_fields[{field_name!r}] = None")
                continue

            field_type = entry.type

            if isinstance(field_type, Primitive):
                lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.{field_type.type}()")

            elif isinstance(field_type, String):
                lines.append(f"{_TAB}_fields[{field_name!r}] = decoder.{field_type.type}()")

            elif isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive):
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.array('{elem.type}', {field_type.length})"
                    )
                elif isinstance(elem, String):
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.array('{elem.type}', {field_type.length})"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    # For complex arrays, we need to read the array and decode each element
                    lines.append(f"{_TAB}_arr = decoder.array('complex', {field_type.length})")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = _arr"
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.array('{elem_name}', {field_type.length})"
                    )

            elif isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive):
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.sequence('{elem.type}')"
                    )
                elif isinstance(elem, String):
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = decoder.sequence('{elem.type}')"
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    lines.append(f"{_TAB}_seq = decoder.sequence('complex')")
                    lines.append(
                        f"{_TAB}_fields[{field_name!r}] = _seq"
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

        class_name = _sanitize(current.name)
        lines.append(f"{_TAB}return _dataclass_types[{class_name!r}](**_fields)")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "\n\n".join(function_defs)

    namespace: dict[str, object] = {"_dataclass_types": dataclass_types}
    exec(code, namespace)
    return namespace[f"decode_{_sanitize(schema.name)}"]  # type: ignore[index]


def compile_json_serializer(
    schema: Schema, sub_schemas: dict[str, Schema]
) -> Callable[[MessageEncoder, Any], None]:
    """Compile schema into a JSON serializer function.

    Unlike compile_serializer, this generates code that uses encoder methods
    instead of direct byte access, making it compatible with JsonEncoder.
    """

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
        ]

        def emit(field_type: SchemaFieldType, value_expr: str, indent: int) -> list[str]:
            pad = _TAB * indent

            if isinstance(field_type, Primitive):
                return [f"{pad}encoder.{field_type.type}({value_expr})"]

            if isinstance(field_type, String):
                return [f"{pad}encoder.{field_type.type}({value_expr})"]

            if isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive):
                    return [f"{pad}encoder.array('{elem.type}', {value_expr})"]
                if isinstance(elem, String):
                    return [f"{pad}encoder.array('{elem.type}', {value_expr})"]
                if isinstance(elem, Complex):
                    if elem.type not in sub_schemas:
                        raise ValueError(f"Unknown schema for complex type {elem.type}")
                    sub_func = build(sub_schemas[elem.type])
                    values_var = new_var("values")
                    item_var = new_var("item")
                    return [
                        f"{pad}{values_var} = {value_expr}",
                        f"{pad}for {item_var} in {values_var}:",
                        f"{_TAB * (indent + 1)}{sub_func}(encoder, {item_var})",
                    ]
                elem_name = getattr(elem, "type", "unknown")
                return [f"{pad}encoder.array('{elem_name}', {value_expr})"]

            if isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive):
                    return [f"{pad}encoder.sequence('{elem.type}', {value_expr})"]
                if isinstance(elem, String):
                    return [f"{pad}encoder.sequence('{elem.type}', {value_expr})"]
                if isinstance(elem, Complex):
                    if elem.type not in sub_schemas:
                        raise ValueError(f"Unknown schema for complex type {elem.type}")
                    sub_func = build(sub_schemas[elem.type])
                    values_var = new_var("values")
                    item_var = new_var("item")
                    return [
                        f"{pad}{values_var} = {value_expr}",
                        f"{pad}encoder.uint32(len({values_var}))",
                        f"{pad}for {item_var} in {values_var}:",
                        f"{_TAB * (indent + 1)}{sub_func}(encoder, {item_var})",
                    ]
                elem_name = getattr(elem, "type", "unknown")
                return [f"{pad}encoder.sequence('{elem_name}', {value_expr})"]

            if isinstance(field_type, Complex):
                if field_type.type not in sub_schemas:
                    raise ValueError(f"Unknown schema for complex type {field_type.type}")
                sub_func = build(sub_schemas[field_type.type])
                return [f"{pad}{sub_func}(encoder, {value_expr})"]

            elem_name = getattr(field_type, "type", "unknown")
            return [f"{pad}encoder.encode('{elem_name}', {value_expr})"]

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                continue
            if not isinstance(entry, SchemaField):
                continue

            field_expr = f"message.{field_name}"
            lines.extend(emit(entry.type, field_expr, 1))

        lines.append(f"{_TAB}return")
        function_defs.append("\n".join(lines))
        return func_name

    build(schema)
    code = "\n\n".join(function_defs)
    namespace: dict[str, object] = {}
    exec(code, namespace)
    return namespace[f"encode_{_sanitize(schema.name)}"]  # type: ignore[index]


__all__ = ["compile_json_schema", "compile_json_serializer"]
