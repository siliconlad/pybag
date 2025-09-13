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
        self._compiled: dict[str, Callable[[MessageDecoder], type]] = {}

    def _func_name(self, schema_name: str) -> str:
        return "decode_" + schema_name.replace("/", "_").replace(".", "_")

    def compile(self, schema: Schema) -> Callable[[MessageDecoder], type]:
        self._compile_schema(schema)
        return self._compiled[schema.name]

    # ------------------------------------------------------------------
    def _compile_schema(self, schema: Schema) -> None:
        if schema.name in self._compiled:
            return

        func_name = self._func_name(schema.name)
        lines: list[str] = [f"def {func_name}(decoder):", "    field = {}"]

        grouped: list[tuple[str, str]] = []

        def flush() -> None:
            if not grouped:
                return
            types = ", ".join([f"'{t}'" for _, t in grouped])
            assigns = ", ".join([f"field['{n}']" for n, _ in grouped])
            if len(grouped) == 1:
                lines.append(f"    {assigns} = decoder.load({types})[0]")
            else:
                lines.append(f"    {assigns} = decoder.load({types})")
            grouped.clear()

        for field_name, field_schema in schema.fields.items():
            if isinstance(field_schema, SchemaConstant):
                flush()
                lines.append(f"    field['{field_name}'] = {field_schema.value!r}")
                continue

            if not isinstance(field_schema, SchemaField):
                raise ValueError(f'Unknown field type: {field_schema}')

            field_type = field_schema.type

            if isinstance(field_type, (Primitive, String)):
                grouped.append((field_name, field_type.type))
                continue

            flush()

            if isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, (Primitive, String)):
                    lines.append(
                        f"    field['{field_name}'] = decoder.array('{elem.type}', {field_type.length}).load()[0]"
                    )
                elif isinstance(elem, Complex):
                    sub = self._sub_schemas[elem.type]
                    self._compile_schema(sub)
                    sub_name = self._func_name(elem.type)
                    lines.append(
                        f"    field['{field_name}'] = [{sub_name}(decoder) for _ in range({field_type.length})]"
                    )
                else:
                    raise ValueError(f'Unknown array element type: {elem}')
                continue

            if isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, (Primitive, String)):
                    lines.append(
                        f"    field['{field_name}'] = decoder.sequence('{elem.type}').load()[0]"
                    )
                elif isinstance(elem, Complex):
                    sub = self._sub_schemas[elem.type]
                    self._compile_schema(sub)
                    sub_name = self._func_name(elem.type)
                    lines.append("    length = decoder.uint32().load()[0]")
                    lines.append(
                        f"    field['{field_name}'] = [{sub_name}(decoder) for _ in range(length)]"
                    )
                else:
                    raise ValueError(f'Unknown sequence element type: {elem}')
                continue

            if isinstance(field_type, Complex):
                sub = self._sub_schemas[field_type.type]
                self._compile_schema(sub)
                sub_name = self._func_name(field_type.type)
                lines.append(f"    field['{field_name}'] = {sub_name}(decoder)")
                continue

            raise ValueError(f'Unknown field type: {field_type}')

        flush()
        lines.append(
            f"    return type('{schema.name.replace('/', '.')}', (), field)"
        )

        src = "\n".join(lines)
        exec(src, self._namespace)
        self._compiled[schema.name] = self._namespace[func_name]


def compile_decoder(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[MessageDecoder], type]:
    """Compile ``schema`` into a decoder function."""
    compiler = SchemaCompiler(sub_schemas)
    return compiler.compile(schema)
