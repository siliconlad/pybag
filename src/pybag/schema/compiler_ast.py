"""Compile ROS 2 schemas into efficient message encoders/decoders using AST."""
from __future__ import annotations

import ast
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


# Helper functions to create AST nodes more concisely
def _name(id: str, ctx=None) -> ast.Name:
    """Create a Name node."""
    return ast.Name(id=id, ctx=ctx or ast.Load())


def _attr(value: ast.expr, attr: str, ctx=None) -> ast.Attribute:
    """Create an Attribute node."""
    return ast.Attribute(value=value, attr=attr, ctx=ctx or ast.Load())


def _call(func: ast.expr, args: list[ast.expr] | None = None, keywords: list[ast.keyword] | None = None) -> ast.Call:
    """Create a Call node."""
    return ast.Call(func=func, args=args or [], keywords=keywords or [])


def _assign(targets: list[str], value: ast.expr) -> ast.Assign:
    """Create an assignment statement."""
    return ast.Assign(
        targets=[_name(t, ast.Store()) for t in targets],
        value=value
    )


def _subscript(value: ast.expr, slice: ast.expr, ctx=None) -> ast.Subscript:
    """Create a Subscript node."""
    return ast.Subscript(value=value, slice=slice, ctx=ctx or ast.Load())


def compile_schema(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[MessageDecoder], type]:
    """Compile ``schema`` into a decoder function using AST.

    The returned function accepts a :class:`MessageDecoder` instance and returns a
    dynamically constructed dataclass instance with the decoded fields.
    """

    function_defs: list[ast.FunctionDef] = []
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
            # Create sub-type if needed
            sub_schema = sub_schemas[field_type.type]
            sub_type = create_dataclass_type(sub_schema)
            return Annotated[sub_type, ("complex", field_type.type)]
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

        # Build function body using AST nodes
        body: list[ast.stmt] = []

        # fmt_prefix = '<' if decoder._is_little_endian else '>'
        body.append(_assign(
            ['fmt_prefix'],
            ast.IfExp(
                test=_attr(_name('decoder'), '_is_little_endian'),
                body=ast.Constant(value='<'),
                orelse=ast.Constant(value='>')
            )
        ))

        # _data = decoder._data
        body.append(_assign(['_data'], _attr(_name('decoder'), '_data')))

        # _fields = {}
        body.append(_assign(['_fields'], ast.Dict(keys=[], values=[])))

        run_type: str | None = None
        run_fields: list[str] = []

        def flush() -> None:
            nonlocal run_type, run_fields
            if not run_fields:
                return

            count_val = len(run_fields)
            size = _STRUCT_SIZE[run_type]  # type: ignore[index]
            fmt = _STRUCT_FORMAT[run_type] * count_val  # type: ignore[index]

            # _data.align(size)
            body.append(ast.Expr(value=_call(_attr(_name('_data'), 'align'), [ast.Constant(value=size)])))

            if count_val > 1:
                # names = struct.unpack(fmt_prefix + 'fmt', _data.read(size * count))
                unpack_call = _call(
                    _attr(_name('struct'), 'unpack'),
                    [
                        ast.BinOp(
                            left=_name('fmt_prefix'),
                            op=ast.Add(),
                            right=ast.Constant(value=fmt)
                        ),
                        _call(_attr(_name('_data'), 'read'), [ast.Constant(value=size * count_val)])
                    ]
                )
                # Create tuple target for unpacking: (x, y, z) = unpack(...)
                body.append(
                    ast.Assign(
                        targets=[ast.Tuple(elts=[_name(field, ast.Store()) for field in run_fields], ctx=ast.Store())],
                        value=unpack_call
                    )
                )

                # _fields['field'] = field
                for field in run_fields:
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field), ast.Store())],
                            value=_name(field)
                        )
                    )
            else:
                field = run_fields[0]
                # _fields['field'] = struct.unpack(fmt_prefix + 'fmt', _data.read(size))[0]
                unpack_call = _call(
                    _attr(_name('struct'), 'unpack'),
                    [
                        ast.BinOp(
                            left=_name('fmt_prefix'),
                            op=ast.Add(),
                            right=ast.Constant(value=fmt)
                        ),
                        _call(_attr(_name('_data'), 'read'), [ast.Constant(value=size * count_val)])
                    ]
                )
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field), ast.Store())],
                        value=_subscript(unpack_call, ast.Constant(value=0))
                    )
                )

            run_fields.clear()
            run_type = None

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                # Skip constants - they're class attributes, not instance fields
                flush()
                continue

            if not isinstance(entry, SchemaField):
                flush()
                # _fields['field_name'] = None
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                        value=ast.Constant(value=None)
                    )
                )
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
                # _fields['field_name'] = decoder.type()
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                        value=_call(_attr(_name('decoder'), field_type.type))
                    )
                )

            elif isinstance(field_type, String):
                # _fields['field_name'] = decoder.type()
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                        value=_call(_attr(_name('decoder'), field_type.type))
                    )
                )

            elif isinstance(field_type, Array):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    fmt = _STRUCT_FORMAT[elem.type] * field_type.length

                    # _data.align(size)
                    body.append(ast.Expr(value=_call(_attr(_name('_data'), 'align'), [ast.Constant(value=size)])))

                    # _fields['field_name'] = list(struct.unpack(fmt_prefix + 'fmt', _data.read(size * length)))
                    unpack_call = _call(
                        _attr(_name('struct'), 'unpack'),
                        [
                            ast.BinOp(
                                left=_name('fmt_prefix'),
                                op=ast.Add(),
                                right=ast.Constant(value=fmt)
                            ),
                            _call(_attr(_name('_data'), 'read'), [ast.Constant(value=size * field_type.length)])
                        ]
                    )
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=_call(_name('list'), [unpack_call])
                        )
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    # _fields['field_name'] = [sub_func(decoder) for _ in range(length)]
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=ast.ListComp(
                                elt=_call(_name(sub_func), [_name('decoder')]),
                                generators=[
                                    ast.comprehension(
                                        target=_name('_', ast.Store()),
                                        iter=_call(_name('range'), [ast.Constant(value=field_type.length)]),
                                        ifs=[],
                                        is_async=0
                                    )
                                ]
                            )
                        )
                    )
                elif isinstance(elem, String):
                    elem_name = elem.type
                    # _fields['field_name'] = [decoder.elem_name() for _ in range(length)]
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=ast.ListComp(
                                elt=_call(_attr(_name('decoder'), elem_name)),
                                generators=[
                                    ast.comprehension(
                                        target=_name('_', ast.Store()),
                                        iter=_call(_name('range'), [ast.Constant(value=field_type.length)]),
                                        ifs=[],
                                        is_async=0
                                    )
                                ]
                            )
                        )
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    # _fields['field_name'] = decoder.array('elem_name', length)
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=_call(_attr(_name('decoder'), 'array'), [ast.Constant(value=elem_name), ast.Constant(value=field_type.length)])
                        )
                    )

            elif isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                    size = _STRUCT_SIZE[elem.type]
                    char = _STRUCT_FORMAT[elem.type]

                    # _len = decoder.uint32()
                    body.append(_assign(['_len'], _call(_attr(_name('decoder'), 'uint32'))))

                    # _data.align(size)
                    body.append(ast.Expr(value=_call(_attr(_name('_data'), 'align'), [ast.Constant(value=size)])))

                    # _fields['field_name'] = list(struct.unpack(fmt_prefix + char * _len, _data.read(size * _len)))
                    unpack_call = _call(
                        _attr(_name('struct'), 'unpack'),
                        [
                            ast.BinOp(
                                left=_name('fmt_prefix'),
                                op=ast.Add(),
                                right=ast.BinOp(
                                    left=ast.Constant(value=char),
                                    op=ast.Mult(),
                                    right=_name('_len')
                                )
                            ),
                            _call(
                                _attr(_name('_data'), 'read'),
                                [ast.BinOp(left=ast.Constant(value=size), op=ast.Mult(), right=_name('_len'))]
                            )
                        ]
                    )
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=_call(_name('list'), [unpack_call])
                        )
                    )
                elif isinstance(elem, Complex):
                    sub_schema = sub_schemas[elem.type]
                    sub_func = build(sub_schema)
                    # length = decoder.uint32()
                    body.append(_assign(['length'], _call(_attr(_name('decoder'), 'uint32'))))
                    # _fields['field_name'] = [sub_func(decoder) for _ in range(length)]
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=ast.ListComp(
                                elt=_call(_name(sub_func), [_name('decoder')]),
                                generators=[
                                    ast.comprehension(
                                        target=_name('_', ast.Store()),
                                        iter=_call(_name('range'), [_name('length')]),
                                        ifs=[],
                                        is_async=0
                                    )
                                ]
                            )
                        )
                    )
                elif isinstance(elem, String):
                    # length = decoder.uint32()
                    body.append(_assign(['length'], _call(_attr(_name('decoder'), 'uint32'))))
                    elem_name = elem.type
                    # _fields['field_name'] = [decoder.elem_name() for _ in range(length)]
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=ast.ListComp(
                                elt=_call(_attr(_name('decoder'), elem_name)),
                                generators=[
                                    ast.comprehension(
                                        target=_name('_', ast.Store()),
                                        iter=_call(_name('range'), [_name('length')]),
                                        ifs=[],
                                        is_async=0
                                    )
                                ]
                            )
                        )
                    )
                else:
                    elem_name = getattr(elem, "type", "unknown")
                    # _fields['field_name'] = decoder.sequence('elem_name')
                    body.append(
                        ast.Assign(
                            targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                            value=_call(_attr(_name('decoder'), 'sequence'), [ast.Constant(value=elem_name)])
                        )
                    )

            elif isinstance(field_type, Complex):
                sub_schema = sub_schemas[field_type.type]
                sub_func = build(sub_schema)
                # _fields['field_name'] = sub_func(decoder)
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                        value=_call(_name(sub_func), [_name('decoder')])
                    )
                )

            else:
                # _fields['field_name'] = None
                body.append(
                    ast.Assign(
                        targets=[_subscript(_name('_fields', ast.Load()), ast.Constant(value=field_name), ast.Store())],
                        value=ast.Constant(value=None)
                    )
                )

        flush()

        # Return dataclass instance
        class_name = _sanitize(current.name)
        # return _dataclass_types['class_name'](**_fields)
        body.append(
            ast.Return(
                value=_call(
                    _subscript(_name('_dataclass_types'), ast.Constant(value=class_name)),
                    [],
                    [ast.keyword(arg=None, value=_name('_fields'))]
                )
            )
        )

        # Create function definition
        func_def = ast.FunctionDef(
            name=func_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg='decoder', annotation=None)],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[]
            ),
            body=body,
            decorator_list=[],
            returns=None
        )

        function_defs.append(func_def)
        return func_name

    build(schema)

    # Create module with all function definitions
    module = ast.Module(
        body=function_defs,
        type_ignores=[]
    )

    # Fix missing locations
    ast.fix_missing_locations(module)

    # Compile and execute
    code = compile(module, '<generated>', 'exec')
    namespace: dict[str, object] = {"struct": struct, "_dataclass_types": dataclass_types}
    exec(code, namespace)
    return namespace[f"decode_{_sanitize(schema.name)}"]  # type: ignore[index]


def compile_serializer(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable[[Any, Any], None]:
    """Compile ``schema`` into a serializer function using AST."""

    function_defs: list[ast.FunctionDef] = []
    compiled: dict[str, str] = {}
    name_counter = count()

    def new_var(prefix: str) -> str:
        return f"_{prefix}_{next(name_counter)}"

    def build(current: Schema) -> str:
        func_name = f"encode_{_sanitize(current.name)}"
        if func_name in compiled:
            return func_name

        compiled[func_name] = func_name

        # Build function body using AST nodes
        body: list[ast.stmt] = []

        # fmt_prefix = '<' if encoder._is_little_endian else '>'
        body.append(_assign(
            ['fmt_prefix'],
            ast.IfExp(
                test=_attr(_name('encoder'), '_is_little_endian'),
                body=ast.Constant(value='<'),
                orelse=ast.Constant(value='>')
            )
        ))

        # _payload = encoder._payload
        body.append(_assign(['_payload'], _attr(_name('encoder'), '_payload')))

        # struct_pack = struct.pack
        body.append(_assign(['struct_pack'], _attr(_name('struct'), 'pack')))

        run_type: str | None = None
        run_values: list[str] = []

        def flush_run() -> None:
            nonlocal run_type, run_values
            if not run_values:
                return

            assert run_type is not None
            fmt = _WRITE_FORMAT[run_type]
            size = _WRITE_SIZE[run_type]
            value_exprs: list[ast.expr] = []

            for expr_str in run_values:
                value_var = new_var("value")
                body.append(_assign([value_var], _attr(_name('message'), expr_str)))

                if run_type in {"byte", "char"}:
                    # value_var = _to_uint8(value_var)
                    body.append(_assign([value_var], _call(_name('_to_uint8'), [_name(value_var)])))
                    value_exprs.append(_name(value_var))
                elif run_type == "bool":
                    # bool(value_var)
                    value_exprs.append(_call(_name('bool'), [_name(value_var)]))
                else:
                    value_exprs.append(_name(value_var))

            if size > 1:
                # _payload.align(size)
                body.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=size)])))

            fmt_repeat = fmt * len(run_values)
            # _payload.write(struct_pack(fmt_prefix + 'fmt', *values))
            body.append(
                ast.Expr(
                    value=_call(
                        _attr(_name('_payload'), 'write'),
                        [
                            _call(
                                _name('struct_pack'),
                                [
                                    ast.BinOp(
                                        left=_name('fmt_prefix'),
                                        op=ast.Add(),
                                        right=ast.Constant(value=fmt_repeat)
                                    )
                                ] + value_exprs
                            )
                        ]
                    )
                )
            )
            run_type = None
            run_values.clear()

        def emit(field_type: SchemaFieldType, value_expr: str, indent: int) -> list[ast.stmt]:
            """Generate AST nodes for encoding a field."""
            stmts: list[ast.stmt] = []

            if isinstance(field_type, Primitive):
                primitive = field_type.type
                if primitive in _WRITE_FORMAT:
                    fmt = _WRITE_FORMAT[primitive]
                    size = _WRITE_SIZE[primitive]
                    value_var = new_var("value")

                    # value_var = value_expr
                    stmts.append(_assign([value_var], _attr(_name('message'), value_expr) if '.' not in value_expr else _name(value_expr)))

                    if primitive in {"byte", "char"}:
                        # value_var = _to_uint8(value_var)
                        stmts.append(_assign([value_var], _call(_name('_to_uint8'), [_name(value_var)])))
                    elif primitive == "bool":
                        # value_var = bool(value_var)
                        stmts.append(_assign([value_var], _call(_name('bool'), [_name(value_var)])))

                    # _payload.align(size)
                    stmts.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=size)])))

                    # _payload.write(struct_pack(fmt_prefix + 'fmt', value_var))
                    stmts.append(
                        ast.Expr(
                            value=_call(
                                _attr(_name('_payload'), 'write'),
                                [
                                    _call(
                                        _name('struct_pack'),
                                        [
                                            ast.BinOp(
                                                left=_name('fmt_prefix'),
                                                op=ast.Add(),
                                                right=ast.Constant(value=fmt)
                                            ),
                                            _name(value_var)
                                        ]
                                    )
                                ]
                            )
                        )
                    )
                    return stmts

                # encoder.primitive(value_expr)
                return [ast.Expr(value=_call(_attr(_name('encoder'), primitive), [_attr(_name('message'), value_expr)]))]

            if isinstance(field_type, String):
                value_var = new_var("value")
                encoded_var = new_var("encoded")

                # value_var = value_expr
                stmts.append(_assign([value_var], _attr(_name('message'), value_expr)))

                # encoded_var = value_var.encode()
                stmts.append(_assign([encoded_var], _call(_attr(_name(value_var), 'encode'))))

                # _payload.align(4)
                stmts.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=4)])))

                # _payload.write(struct_pack(fmt_prefix + 'I', len(encoded_var) + 1))
                stmts.append(
                    ast.Expr(
                        value=_call(
                            _attr(_name('_payload'), 'write'),
                            [
                                _call(
                                    _name('struct_pack'),
                                    [
                                        ast.BinOp(
                                            left=_name('fmt_prefix'),
                                            op=ast.Add(),
                                            right=ast.Constant(value='I')
                                        ),
                                        ast.BinOp(
                                            left=_call(_name('len'), [_name(encoded_var)]),
                                            op=ast.Add(),
                                            right=ast.Constant(value=1)
                                        )
                                    ]
                                )
                            ]
                        )
                    )
                )

                # _payload.write(encoded_var + b'\x00')
                stmts.append(
                    ast.Expr(
                        value=_call(
                            _attr(_name('_payload'), 'write'),
                            [
                                ast.BinOp(
                                    left=_name(encoded_var),
                                    op=ast.Add(),
                                    right=ast.Constant(value=b'\x00')
                                )
                            ]
                        )
                    )
                )
                return stmts

            if isinstance(field_type, Array):
                elem = field_type.type
                values_var = new_var("values")
                stmts.append(_assign([values_var], _attr(_name('message'), value_expr)))

                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    base_var = values_var
                    if elem.type in {"byte", "char"}:
                        converted_var = new_var("converted")
                        # converted_var = [_to_uint8(v) for v in values_var]
                        stmts.append(
                            _assign(
                                [converted_var],
                                ast.ListComp(
                                    elt=_call(_name('_to_uint8'), [_name('v')]),
                                    generators=[
                                        ast.comprehension(
                                            target=_name('v', ast.Store()),
                                            iter=_name(values_var),
                                            ifs=[],
                                            is_async=0
                                        )
                                    ]
                                )
                            )
                        )
                        base_var = converted_var

                    length_var = new_var("length")
                    stmts.append(_assign([length_var], _call(_name('len'), [_name(base_var)])))

                    # if length_var:
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type]

                    if_body: list[ast.stmt] = []
                    # _payload.align(size)
                    if_body.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=size)])))

                    # _payload.write(struct_pack(fmt_prefix + fmt * length_var, *base_var))
                    if_body.append(
                        ast.Expr(
                            value=_call(
                                _attr(_name('_payload'), 'write'),
                                [
                                    _call(
                                        _name('struct_pack'),
                                        [
                                            ast.BinOp(
                                                left=_name('fmt_prefix'),
                                                op=ast.Add(),
                                                right=ast.BinOp(
                                                    left=ast.Constant(value=fmt),
                                                    op=ast.Mult(),
                                                    right=_name(length_var)
                                                )
                                            )
                                        ],
                                        [ast.keyword(arg=None, value=_name(base_var))]
                                    )
                                ]
                            )
                        )
                    )

                    stmts.append(ast.If(test=_name(length_var), body=if_body, orelse=[]))
                    return stmts

                if isinstance(elem, String):
                    item_var = new_var("item")
                    # for item_var in values_var:
                    for_body = emit(elem, item_var, indent + 1)
                    stmts.append(
                        ast.For(
                            target=_name(item_var, ast.Store()),
                            iter=_name(values_var),
                            body=for_body,
                            orelse=[]
                        )
                    )
                    return stmts

                if isinstance(elem, Complex):
                    if elem.type not in sub_schemas:
                        error_msg = f"Unknown schema for complex type {elem.type}"
                        raise ValueError(error_msg)
                    sub_func = build(sub_schemas[elem.type])
                    item_var = new_var("item")
                    # for item_var in values_var:
                    #     sub_func(encoder, item_var)
                    stmts.append(
                        ast.For(
                            target=_name(item_var, ast.Store()),
                            iter=_name(values_var),
                            body=[ast.Expr(value=_call(_name(sub_func), [_name('encoder'), _name(item_var)]))],
                            orelse=[]
                        )
                    )
                    return stmts

                elem_name = getattr(elem, "type", "unknown")
                # encoder.array('elem_name', values_var)
                return [ast.Expr(value=_call(_attr(_name('encoder'), 'array'), [ast.Constant(value=elem_name), _name(values_var)]))]

            if isinstance(field_type, Sequence):
                elem = field_type.type
                if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                    values_var = new_var("values")
                    stmts.append(_assign([values_var], _attr(_name('message'), value_expr)))

                    base_var = values_var
                    if elem.type in {"byte", "char"}:
                        converted_var = new_var("converted")
                        # converted_var = [_to_uint8(v) for v in values_var]
                        stmts.append(
                            _assign(
                                [converted_var],
                                ast.ListComp(
                                    elt=_call(_name('_to_uint8'), [_name('v')]),
                                    generators=[
                                        ast.comprehension(
                                            target=_name('v', ast.Store()),
                                            iter=_name(values_var),
                                            ifs=[],
                                            is_async=0
                                        )
                                    ]
                                )
                            )
                        )
                        base_var = converted_var

                    length_var = new_var("length")
                    stmts.append(_assign([length_var], _call(_name('len'), [_name(base_var)])))

                    # _payload.align(4)
                    stmts.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=4)])))

                    # _payload.write(struct_pack(fmt_prefix + 'I', length_var))
                    stmts.append(
                        ast.Expr(
                            value=_call(
                                _attr(_name('_payload'), 'write'),
                                [
                                    _call(
                                        _name('struct_pack'),
                                        [
                                            ast.BinOp(
                                                left=_name('fmt_prefix'),
                                                op=ast.Add(),
                                                right=ast.Constant(value='I')
                                            ),
                                            _name(length_var)
                                        ]
                                    )
                                ]
                            )
                        )
                    )

                    # if length_var:
                    size = _WRITE_SIZE[elem.type]
                    fmt = _WRITE_FORMAT[elem.type]

                    if_body: list[ast.stmt] = []
                    # _payload.align(size)
                    if_body.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=size)])))

                    # _payload.write(struct_pack(fmt_prefix + fmt * length_var, *base_var))
                    if_body.append(
                        ast.Expr(
                            value=_call(
                                _attr(_name('_payload'), 'write'),
                                [
                                    _call(
                                        _name('struct_pack'),
                                        [
                                            ast.BinOp(
                                                left=_name('fmt_prefix'),
                                                op=ast.Add(),
                                                right=ast.BinOp(
                                                    left=ast.Constant(value=fmt),
                                                    op=ast.Mult(),
                                                    right=_name(length_var)
                                                )
                                            )
                                        ],
                                        [ast.keyword(arg=None, value=_name(base_var))]
                                    )
                                ]
                            )
                        )
                    )

                    stmts.append(ast.If(test=_name(length_var), body=if_body, orelse=[]))
                    return stmts

                if isinstance(elem, Primitive):
                    type_name = elem.type
                    # encoder.sequence('type_name', value_expr)
                    return [ast.Expr(value=_call(_attr(_name('encoder'), 'sequence'), [ast.Constant(value=type_name), _attr(_name('message'), value_expr)]))]

                if not isinstance(elem, (String, Complex)):
                    elem_name = getattr(elem, "type", "unknown")
                    # encoder.sequence('elem_name', value_expr)
                    return [ast.Expr(value=_call(_attr(_name('encoder'), 'sequence'), [ast.Constant(value=elem_name), _attr(_name('message'), value_expr)]))]

                values_var = new_var("values")
                length_var = new_var("length")

                # values_var = value_expr
                stmts.append(_assign([values_var], _attr(_name('message'), value_expr)))

                # length_var = len(values_var)
                stmts.append(_assign([length_var], _call(_name('len'), [_name(values_var)])))

                # _payload.align(4)
                stmts.append(ast.Expr(value=_call(_attr(_name('_payload'), 'align'), [ast.Constant(value=4)])))

                # _payload.write(struct_pack(fmt_prefix + 'I', length_var))
                stmts.append(
                    ast.Expr(
                        value=_call(
                            _attr(_name('_payload'), 'write'),
                            [
                                _call(
                                    _name('struct_pack'),
                                    [
                                        ast.BinOp(
                                            left=_name('fmt_prefix'),
                                            op=ast.Add(),
                                            right=ast.Constant(value='I')
                                        ),
                                        _name(length_var)
                                    ]
                                )
                            ]
                        )
                    )
                )

                if isinstance(elem, String):
                    item_var = new_var("item")
                    # for item_var in values_var:
                    for_body = emit(elem, item_var, indent + 1)
                    stmts.append(
                        ast.For(
                            target=_name(item_var, ast.Store()),
                            iter=_name(values_var),
                            body=for_body,
                            orelse=[]
                        )
                    )
                    return stmts

                if elem.type not in sub_schemas:
                    error_msg = f"Unknown schema for complex type {elem.type}"
                    raise ValueError(error_msg)
                sub_func = build(sub_schemas[elem.type])
                item_var = new_var("item")
                # for item_var in values_var:
                #     sub_func(encoder, item_var)
                stmts.append(
                    ast.For(
                        target=_name(item_var, ast.Store()),
                        iter=_name(values_var),
                        body=[ast.Expr(value=_call(_name(sub_func), [_name('encoder'), _name(item_var)]))],
                        orelse=[]
                    )
                )
                return stmts

            if isinstance(field_type, Complex):
                if field_type.type not in sub_schemas:
                    error_msg = f"Unknown schema for complex type {field_type.type}"
                    raise ValueError(error_msg)
                sub_func = build(sub_schemas[field_type.type])
                # sub_func(encoder, value_expr)
                return [ast.Expr(value=_call(_name(sub_func), [_name('encoder'), _attr(_name('message'), value_expr)]))]

            elem_name = getattr(field_type, "type", "unknown")
            # encoder.encode('elem_name', value_expr)
            return [ast.Expr(value=_call(_attr(_name('encoder'), 'encode'), [ast.Constant(value=elem_name), _attr(_name('message'), value_expr)]))]

        for field_name, entry in current.fields.items():
            if isinstance(entry, SchemaConstant):
                flush_run()
                continue
            if not isinstance(entry, SchemaField):
                flush_run()
                continue
            field_type = entry.type
            if isinstance(field_type, Primitive) and field_type.type in _WRITE_FORMAT:
                if run_type == field_type.type:
                    run_values.append(field_name)
                else:
                    flush_run()
                    run_type = field_type.type
                    run_values = [field_name]
                continue

            flush_run()
            body.extend(emit(field_type, field_name, 1))

        flush_run()
        # return
        body.append(ast.Return(value=None))

        # Create function definition
        func_def = ast.FunctionDef(
            name=func_name,
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg='encoder', annotation=None), ast.arg(arg='message', annotation=None)],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[]
            ),
            body=body,
            decorator_list=[],
            returns=None
        )

        function_defs.append(func_def)
        return func_name

    build(schema)

    # Create module with all function definitions
    module = ast.Module(
        body=function_defs,
        type_ignores=[]
    )

    # Fix missing locations
    ast.fix_missing_locations(module)

    # Compile and execute
    code = compile(module, '<generated>', 'exec')
    namespace: dict[str, object] = {"struct": struct, "_to_uint8": _to_uint8}
    exec(code, namespace)
    return namespace[f"encode_{_sanitize(schema.name)}"]  # type: ignore[index]


__all__ = ["compile_schema", "compile_serializer"]
