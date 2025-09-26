import struct
from typing import Any, Callable, Dict

from pybag.schema import Array, Complex, Primitive, Schema, SchemaConstant, Sequence, String

# Mapping from primitive type names to struct format characters
_PRIMITIVE_FMTS = {
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

# Mapping from primitive type names to their alignment requirements
_PRIMITIVE_ALIGN = {
    "bool": 1,
    "int8": 1,
    "uint8": 1,
    "int16": 2,
    "uint16": 2,
    "int32": 4,
    "uint32": 4,
    "int64": 8,
    "uint64": 8,
    "float32": 4,
    "float64": 8,
}


def _safe_name(name: str) -> str:
    """Return a name that can be used as a valid Python identifier."""
    return name.replace("/", "_").replace(".", "_")


def compile_schemas(schema: Schema, sub_schemas: Dict[str, Schema]) -> Dict[str, Callable[[Any, Any], None]]:
    """Compile a schema and all of its dependencies into encoder functions.

    Returns a dictionary mapping schema names to callables. Each callable
    accepts ``(encoder, message)`` and writes the message into ``encoder``.
    """

    compiled: Dict[str, Callable[[Any, Any], None]] = {}
    namespace: Dict[str, Any] = {"struct": struct}

    def compile_schema(s: Schema) -> None:
        if s.name in compiled:
            return

        # Ensure all dependent schemas are compiled first
        for field in s.fields.values():
            if isinstance(field, SchemaConstant):
                continue
            ft = field.type
            if isinstance(ft, Complex):
                compile_schema(sub_schemas[ft.type])
            elif isinstance(ft, Array) and isinstance(ft.type, Complex):
                compile_schema(sub_schemas[ft.type.type])
            elif isinstance(ft, Sequence) and isinstance(ft.type, Complex):
                compile_schema(sub_schemas[ft.type.type])

        func_name = f"encode_{_safe_name(s.name)}"
        indent = "    "
        lines = [f"def {func_name}(encoder, msg):"]
        lines.append(f"{indent}payload = encoder._payload")
        lines.append(f"{indent}align = payload.align")
        lines.append(f"{indent}write = payload.write")
        lines.append(f"{indent}pack = struct.pack")
        lines.append(f"{indent}prefix = '<' if encoder._is_little_endian else '>'")

        group: list[tuple[str, str]] = []

        def flush_group() -> None:
            if not group:
                return
            first_type = group[0][1]
            align_size = _PRIMITIVE_ALIGN[first_type]
            fmt = ''.join(_PRIMITIVE_FMTS[t] for _, t in group)
            values = ', '.join(f"msg.{n}" for n, _ in group)
            lines.append(f"{indent}align({align_size})")
            lines.append(f"{indent}write(pack(prefix + '{fmt}', {values}))")
            group.clear()

        for field_name, field in s.fields.items():
            if isinstance(field, SchemaConstant):
                continue
            ft = field.type
            if isinstance(ft, Primitive) and ft.type in _PRIMITIVE_FMTS:
                if group and ft.type != group[0][1]:
                    flush_group()
                group.append((field_name, ft.type))
                continue

            flush_group()

            if isinstance(ft, Primitive):
                lines.append(f"{indent}encoder.{ft.type}(msg.{field_name})")
            elif isinstance(ft, String):
                lines.append(f"{indent}encoder.{ft.type}(msg.{field_name})")
            elif isinstance(ft, Array):
                if isinstance(ft.type, (Primitive, String)):
                    lines.append(
                        f"{indent}encoder.array('{ft.type.type}', msg.{field_name})"
                    )
                elif isinstance(ft.type, Complex):
                    sub_name = f"encode_{_safe_name(ft.type.type)}"
                    lines.append(f"{indent}for _v in msg.{field_name}:")
                    lines.append(f"{indent*2}{sub_name}(encoder, _v)")
                else:
                    raise ValueError(f'Unsupported array type: {ft.type}')
            elif isinstance(ft, Sequence):
                if isinstance(ft.type, (Primitive, String)):
                    lines.append(
                        f"{indent}encoder.sequence('{ft.type.type}', msg.{field_name})"
                    )
                elif isinstance(ft.type, Complex):
                    sub_name = f"encode_{_safe_name(ft.type.type)}"
                    lines.append(f"{indent}encoder.uint32(len(msg.{field_name}))")
                    lines.append(f"{indent}for _v in msg.{field_name}:")
                    lines.append(f"{indent*2}{sub_name}(encoder, _v)")
                else:
                    raise ValueError(f'Unsupported sequence type: {ft.type}')
            elif isinstance(ft, Complex):
                sub_name = f"encode_{_safe_name(ft.type)}"
                lines.append(f"{indent}{sub_name}(encoder, msg.{field_name})")
            else:
                raise ValueError(f'Unsupported field type: {ft}')

        flush_group()

        src = "\n".join(lines)
        exec(src, namespace)
        compiled[s.name] = namespace[func_name]

    compile_schema(schema)
    return {name: namespace[f"encode_{_safe_name(name)}"] for name in compiled}
