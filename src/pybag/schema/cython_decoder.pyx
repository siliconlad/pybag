# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
"""Cython-optimized ROS 2 schema decoders and encoders."""

import struct
from dataclasses import make_dataclass
from typing import Any, Callable

from pybag.schema import (
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaField,
    Sequence,
    String,
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


cdef inline int _get_alignment(str type_name):
    """Get alignment requirement for a type."""
    return _STRUCT_SIZE.get(type_name, 1)


def _to_uint8(value):
    """Normalize ``value`` to an unsigned 8-bit integer."""
    if isinstance(value, int):
        return value
    if isinstance(value, (bytes, bytearray)):
        if len(value) != 1:
            raise ValueError("Byte values must contain exactly one byte")
        return value[0]
    if isinstance(value, str):
        if len(value) != 1:
            raise ValueError("Char values must contain exactly one character")
        return ord(value)
    raise TypeError(f"Cannot convert value of type {type(value)!r} to uint8")


class CythonDecoderFactory:
    """Factory for creating Cython-optimized decoder functions."""

    def __init__(self, schema: Schema, sub_schemas: dict[str, Schema]):
        self.schema = schema
        self.sub_schemas = sub_schemas
        self.dataclass_types = {}
        self.decoders = {}
        self._build_dataclasses()
        self._build_decoders()

    def _build_dataclasses(self):
        """Build all dataclass types for the schema hierarchy."""
        from pybag.types import (
            int8, int16, int32, int64,
            uint8, uint16, uint32, uint64,
            float32, float64, bool as t_bool,
            byte, char, string, wstring
        )
        from typing import Annotated

        _PRIMITIVE_TYPE_MAP = {
            'int8': int8, 'int16': int16, 'int32': int32, 'int64': int64,
            'uint8': uint8, 'uint16': uint16, 'uint32': uint32, 'uint64': uint64,
            'float32': float32, 'float64': float64,
            'bool': t_bool, 'byte': byte, 'char': char,
            'string': string, 'wstring': wstring,
        }

        def schema_type_to_annotation(field_type):
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
                    sub_schema = self.sub_schemas[elem_type.type]
                    elem_annotation = self._create_dataclass_type(sub_schema)
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
                    sub_schema = self.sub_schemas[elem_type.type]
                    elem_annotation = self._create_dataclass_type(sub_schema)
                    return Annotated[list[Any], ("array", elem_annotation, None)]
                else:
                    return Annotated[list[Any], ("array", Any, None)]
            elif isinstance(field_type, Complex):
                sub_schema = self.sub_schemas[field_type.type]
                sub_type = self._create_dataclass_type(sub_schema)
                return Annotated[sub_type, ("complex", field_type.type)]
            else:
                return Any

        def _create_dataclass_type(current: Schema):
            import re
            class_name = re.sub(r"[^0-9a-zA-Z_]", "_", current.name)
            if class_name in self.dataclass_types:
                return self.dataclass_types[class_name]

            field_specs = []
            for field_name, entry in current.fields.items():
                if isinstance(entry, SchemaConstant):
                    from typing import Annotated
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

            self.dataclass_types[class_name] = dataclass_type
            return dataclass_type

        self._create_dataclass_type = _create_dataclass_type
        _create_dataclass_type(self.schema)

    def _build_decoders(self):
        """Build decoder functions for the schema hierarchy."""

        def build_decoder(current: Schema):
            import re
            class_name = re.sub(r"[^0-9a-zA-Z_]", "_", current.name)
            if class_name in self.decoders:
                return self.decoders[class_name]

            dataclass_type = self.dataclass_types[class_name]

            def decode(decoder):
                fmt_prefix = '<' if decoder._is_little_endian else '>'
                _data = decoder._data
                fields = {}

                for field_name, entry in current.fields.items():
                    if isinstance(entry, SchemaConstant):
                        continue

                    if not isinstance(entry, SchemaField):
                        fields[field_name] = None
                        continue

                    field_type = entry.type

                    if isinstance(field_type, Primitive) and field_type.type in _STRUCT_FORMAT:
                        size = _STRUCT_SIZE[field_type.type]
                        fmt = _STRUCT_FORMAT[field_type.type]
                        _data.align(size)
                        fields[field_name] = struct.unpack(fmt_prefix + fmt, _data.read(size))[0]

                    elif isinstance(field_type, Primitive):
                        fields[field_name] = getattr(decoder, field_type.type)()

                    elif isinstance(field_type, String):
                        fields[field_name] = getattr(decoder, field_type.type)()

                    elif isinstance(field_type, Array):
                        elem = field_type.type
                        if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                            size = _STRUCT_SIZE[elem.type]
                            fmt = _STRUCT_FORMAT[elem.type] * field_type.length
                            _data.align(size)
                            fields[field_name] = list(struct.unpack(
                                fmt_prefix + fmt,
                                _data.read(size * field_type.length)
                            ))
                        elif isinstance(elem, Complex):
                            sub_decoder = build_decoder(self.sub_schemas[elem.type])
                            fields[field_name] = [sub_decoder(decoder) for _ in range(field_type.length)]
                        elif isinstance(elem, String):
                            elem_name = elem.type
                            fields[field_name] = [getattr(decoder, elem_name)() for _ in range(field_type.length)]
                        else:
                            elem_name = getattr(elem, "type", "unknown")
                            fields[field_name] = decoder.array(elem_name, field_type.length)

                    elif isinstance(field_type, Sequence):
                        elem = field_type.type
                        if isinstance(elem, Primitive) and elem.type in _STRUCT_FORMAT:
                            size = _STRUCT_SIZE[elem.type]
                            char = _STRUCT_FORMAT[elem.type]
                            length = decoder.uint32()
                            _data.align(size)
                            fields[field_name] = list(struct.unpack(
                                fmt_prefix + char * length,
                                _data.read(size * length)
                            ))
                        elif isinstance(elem, Complex):
                            length = decoder.uint32()
                            sub_decoder = build_decoder(self.sub_schemas[elem.type])
                            fields[field_name] = [sub_decoder(decoder) for _ in range(length)]
                        elif isinstance(elem, String):
                            length = decoder.uint32()
                            elem_name = elem.type
                            fields[field_name] = [getattr(decoder, elem_name)() for _ in range(length)]
                        else:
                            elem_name = getattr(elem, "type", "unknown")
                            fields[field_name] = decoder.sequence(elem_name)

                    elif isinstance(field_type, Complex):
                        sub_decoder = build_decoder(self.sub_schemas[field_type.type])
                        fields[field_name] = sub_decoder(decoder)

                    else:
                        fields[field_name] = None

                return dataclass_type(**fields)

            self.decoders[class_name] = decode
            return decode

        import re
        class_name = re.sub(r"[^0-9a-zA-Z_]", "_", self.schema.name)
        build_decoder(self.schema)

    def get_decoder(self):
        """Get the main decoder function."""
        import re
        class_name = re.sub(r"[^0-9a-zA-Z_]", "_", self.schema.name)
        return self.decoders[class_name]


class CythonEncoderFactory:
    """Factory for creating Cython-optimized encoder functions."""

    def __init__(self, schema: Schema, sub_schemas: dict[str, Schema]):
        self.schema = schema
        self.sub_schemas = sub_schemas
        self.encoders = {}
        self._build_encoders()

    def _build_encoders(self):
        """Build encoder functions for the schema hierarchy."""

        def build_encoder(current: Schema):
            import re
            class_name = re.sub(r"[^0-9a-zA-Z_]", "_", current.name)
            if class_name in self.encoders:
                return self.encoders[class_name]

            def encode(encoder, message):
                fmt_prefix = '<' if encoder._is_little_endian else '>'
                _payload = encoder._payload

                for field_name, entry in current.fields.items():
                    if isinstance(entry, SchemaConstant):
                        continue

                    if not isinstance(entry, SchemaField):
                        continue

                    field_type = entry.type
                    value = getattr(message, field_name)

                    if isinstance(field_type, Primitive) and field_type.type in _WRITE_FORMAT:
                        fmt = _WRITE_FORMAT[field_type.type]
                        size = _WRITE_SIZE[field_type.type]
                        if field_type.type in {"byte", "char"}:
                            value = _to_uint8(value)
                        elif field_type.type == "bool":
                            value = bool(value)
                        _payload.align(size)
                        _payload.write(struct.pack(fmt_prefix + fmt, value))

                    elif isinstance(field_type, Primitive):
                        getattr(encoder, field_type.type)(value)

                    elif isinstance(field_type, String):
                        encoded = value.encode()
                        _payload.align(4)
                        _payload.write(struct.pack(fmt_prefix + 'I', len(encoded) + 1))
                        _payload.write(encoded + b'\x00')

                    elif isinstance(field_type, Array):
                        elem = field_type.type
                        if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                            if len(value) > 0:
                                if elem.type in {"byte", "char"}:
                                    value = [_to_uint8(v) for v in value]
                                size = _WRITE_SIZE[elem.type]
                                fmt = _WRITE_FORMAT[elem.type]
                                _payload.align(size)
                                _payload.write(struct.pack(fmt_prefix + fmt * len(value), *value))
                        elif isinstance(elem, String):
                            for item in value:
                                encoded = item.encode()
                                _payload.align(4)
                                _payload.write(struct.pack(fmt_prefix + 'I', len(encoded) + 1))
                                _payload.write(encoded + b'\x00')
                        elif isinstance(elem, Complex):
                            sub_encoder = build_encoder(self.sub_schemas[elem.type])
                            for item in value:
                                sub_encoder(encoder, item)
                        else:
                            elem_name = getattr(elem, "type", "unknown")
                            encoder.array(elem_name, value)

                    elif isinstance(field_type, Sequence):
                        elem = field_type.type
                        if isinstance(elem, Primitive) and elem.type in _WRITE_FORMAT:
                            length = len(value)
                            _payload.align(4)
                            _payload.write(struct.pack(fmt_prefix + 'I', length))
                            if length > 0:
                                if elem.type in {"byte", "char"}:
                                    value = [_to_uint8(v) for v in value]
                                size = _WRITE_SIZE[elem.type]
                                fmt = _WRITE_FORMAT[elem.type]
                                _payload.align(size)
                                _payload.write(struct.pack(fmt_prefix + fmt * length, *value))
                        elif isinstance(elem, (String, Complex)):
                            length = len(value)
                            _payload.align(4)
                            _payload.write(struct.pack(fmt_prefix + 'I', length))
                            if isinstance(elem, String):
                                for item in value:
                                    encoded = item.encode()
                                    _payload.align(4)
                                    _payload.write(struct.pack(fmt_prefix + 'I', len(encoded) + 1))
                                    _payload.write(encoded + b'\x00')
                            else:
                                sub_encoder = build_encoder(self.sub_schemas[elem.type])
                                for item in value:
                                    sub_encoder(encoder, item)
                        else:
                            elem_name = getattr(elem, "type", "unknown")
                            encoder.sequence(elem_name, value)

                    elif isinstance(field_type, Complex):
                        sub_encoder = build_encoder(self.sub_schemas[field_type.type])
                        sub_encoder(encoder, value)

            self.encoders[class_name] = encode
            return encode

        import re
        class_name = re.sub(r"[^0-9a-zA-Z_]", "_", self.schema.name)
        build_encoder(self.schema)

    def get_encoder(self):
        """Get the main encoder function."""
        import re
        class_name = re.sub(r"[^0-9a-zA-Z_]", "_", self.schema.name)
        return self.encoders[class_name]


def compile_schema_cython(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable:
    """Compile schema using Cython-optimized decoder."""
    factory = CythonDecoderFactory(schema, sub_schemas)
    return factory.get_decoder()


def compile_serializer_cython(schema: Schema, sub_schemas: dict[str, Schema]) -> Callable:
    """Compile schema using Cython-optimized encoder."""
    factory = CythonEncoderFactory(schema, sub_schemas)
    return factory.get_encoder()
