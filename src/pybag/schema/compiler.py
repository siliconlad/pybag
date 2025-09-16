from __future__ import annotations

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


class SchemaCompiler:
    """Compile :class:`Schema` objects into efficient decoder functions."""

    def __init__(self, sub_schemas: dict[str, Schema]):
        self._sub_schemas = sub_schemas
        self._namespace: dict[str, object] = {}
        self._compiled: dict[str, Callable[[MessageDecoder], object]] = {}

    def _func_name(self, schema_name: str) -> str:
        return "decode_" + schema_name.replace("/", "_").replace(".", "_")

    def compile(self, schema: Schema) -> Callable[[MessageDecoder], object]:
        self._compile_schema(schema)
        return self._compiled[schema.name]

    # ------------------------------------------------------------------
    def _compile_schema(self, schema: Schema) -> None:
        if schema.name in self._compiled:
            return

        func_name = self._func_name(schema.name)
        class_name = schema.name.replace("/", "_").replace(".", "_")

        slot_fields = [
            name
            for name, field_schema in schema.fields.items()
            if isinstance(field_schema, SchemaField)
        ]

        lines: list[str] = [f"class {class_name}:"]
        if slot_fields:
            lines.append(f"    __slots__ = {tuple(slot_fields)!r}")

        for field_name, field_schema in schema.fields.items():
            if isinstance(field_schema, SchemaConstant):
                lines.append(f"    {field_name} = {field_schema.value!r}")

        init_params = ", ".join(["self", *slot_fields])
        lines.append(f"    def __init__({init_params}):")
        if slot_fields:
            for name in slot_fields:
                lines.append(f"        self.{name} = {name}")
        else:
            lines.append("        pass")

        lines.append("")
        lines.append(f"def {func_name}(decoder):")

        arg_vars: list[str] = []
        grouped: list[tuple[str, str]] = []

        def flush() -> None:
            if not grouped:
                return
            calls = "".join([f".{t}()" for _, t in grouped])
            names = [n for n, _ in grouped]
            lines.append(f"    decoder{calls}")
            assign = ", ".join(names)
            if len(names) == 1:
                lines.append(f"    {assign}, = decoder.load()")
            else:
                lines.append(f"    {assign} = decoder.load()")
            arg_vars.extend(names)
            grouped.clear()

        for field_name, field_schema in schema.fields.items():
            if isinstance(field_schema, SchemaConstant):
                continue

            if not isinstance(field_schema, SchemaField):
                raise ValueError(f"Unknown field type: {field_schema}")

            field_type = field_schema.type

            if isinstance(field_type, (Primitive, String)):
                grouped.append((field_name, field_type.type))
                continue

            flush()

            if isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, (Primitive, String)):
                    lines.append(
                        f"    {field_name} = decoder.array('{elem.type}', {field_type.length}).load()[0]"
                    )
                elif isinstance(elem, Complex):
                    sub = self._sub_schemas[elem.type]
                    self._compile_schema(sub)
                    sub_name = self._func_name(elem.type)
                    lines.append(
                        f"    {field_name} = [{sub_name}(decoder) for _ in range({field_type.length})]"
                    )
                else:
                    raise ValueError(f"Unknown array element type: {elem}")
                arg_vars.append(field_name)
                continue

            if isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, (Primitive, String)):
                    lines.append(
                        f"    {field_name} = decoder.sequence('{elem.type}').load()[0]"
                    )
                elif isinstance(elem, Complex):
                    sub = self._sub_schemas[elem.type]
                    self._compile_schema(sub)
                    sub_name = self._func_name(elem.type)
                    length_var = f"len_{field_name}"
                    lines.append(f"    {length_var} = decoder.uint32().load()[0]")
                    lines.append(
                        f"    {field_name} = [{sub_name}(decoder) for _ in range({length_var})]"
                    )
                else:
                    raise ValueError(f"Unknown sequence element type: {elem}")
                arg_vars.append(field_name)
                continue

            if isinstance(field_type, Complex):
                sub = self._sub_schemas[field_type.type]
                self._compile_schema(sub)
                sub_name = self._func_name(field_type.type)
                lines.append(f"    {field_name} = {sub_name}(decoder)")
                arg_vars.append(field_name)
                continue

            raise ValueError(f"Unknown field type: {field_type}")

        flush()
        args_str = ", ".join(arg_vars)
        if slot_fields:
            lines.append(f"    return {class_name}({args_str})")
        else:
            lines.append(f"    return {class_name}")

        src = "\n".join(lines)
        exec(src, self._namespace)
        self._compiled[schema.name] = self._namespace[func_name]


def compile_decoder(
    schema: Schema, sub_schemas: dict[str, Schema]
) -> Callable[[MessageDecoder], object]:
    """Compile ``schema`` into a decoder function."""
    compiler = SchemaCompiler(sub_schemas)
    return compiler.compile(schema)
