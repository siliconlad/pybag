from __future__ import annotations

import builtins
import sys
from collections.abc import Sequence as _Sequence
from dataclasses import MISSING
from dataclasses import dataclass as _dataclass
from dataclasses import fields as _dataclass_fields
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable
)

int8 = Annotated[int, ("int8",)]
int16 = Annotated[int, ("int16",)]
int32 = Annotated[int, ("int32",)]
int64 = Annotated[int, ("int64",)]

uint8 = Annotated[int, ("uint8",)]
uint16 = Annotated[int, ("uint16",)]
uint32 = Annotated[int, ("uint32",)]
uint64 = Annotated[int, ("uint64",)]

float32 = Annotated[float, ("float32",)]
float64 = Annotated[float, ("float64",)]

bool = Annotated[bool, ("bool",)]
byte = Annotated[bytes, ("byte",)]
char = Annotated[str, ("char",)]
string = Annotated[str, ("string",)]
wstring = Annotated[str, ("wstring",)]

T = TypeVar("T")


@runtime_checkable
class Message(Protocol):
    """Message protocol for reading and writing."""

    __msg_name__: str


# Type-checker compatible version using Generic classes
class _ConstantType(Generic[T]):
    """Generic type for constants."""
    def __class_getitem__(cls, type_: type[T]) -> type[T]:
        return Annotated[type_, ("constant", type_)]


class _ArrayType:
    """Generic type for arrays that accepts both single type and type+length."""
    @classmethod
    def __class_getitem__(cls, params: Any) -> type[list]:
        if isinstance(params, tuple):
            # Array[type, length] - fixed size array
            if len(params) == 2:
                type_, length = params
                return Annotated[list[type_], ("array", type_, length)]
            else:
                raise TypeError("Array expects either 1 or 2 parameters")
        else:
            # Array[type] - variable size array
            return Annotated[list[params], ("array", params, None)]


class _ComplexType(Generic[T]):
    """Generic type for complex/nested types."""
    def __class_getitem__(cls, type_: type[T]) -> type[T]:
        return Annotated[type_, ("complex", type_.__msg_name__)]


# Type aliases for use in type annotations
Constant: TypeAlias = _ConstantType
Array: TypeAlias = _ArrayType
Complex: TypeAlias = _ComplexType


_INT_RANGES: dict[str, tuple[int, int]] = {
    "int8": (-(2 ** 7), 2 ** 7 - 1),
    "int16": (-(2 ** 15), 2 ** 15 - 1),
    "int32": (-(2 ** 31), 2 ** 31 - 1),
    "int64": (-(2 ** 63), 2 ** 63 - 1),
    "uint8": (0, 2 ** 8 - 1),
    "uint16": (0, 2 ** 16 - 1),
    "uint32": (0, 2 ** 32 - 1),
    "uint64": (0, 2 ** 64 - 1),
}
_FLOAT_TYPES = {"float32", "float64"}
_STRING_TYPES = {"string", "wstring"}
_SCALAR_TYPES = (
    set(_INT_RANGES)
    | _FLOAT_TYPES
    | _STRING_TYPES
    | {"bool", "byte", "char"}
)


def _format_path(parts: list[str]) -> str:
    if not parts:
        return "<value>"

    formatted = parts[0]
    for part in parts[1:]:
        if part.startswith("["):
            formatted += part
        else:
            formatted += f".{part}"
    return formatted


class _TypeSpec:
    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        raise NotImplementedError


class _AnySpec(_TypeSpec):
    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        return


class _ScalarSpec(_TypeSpec):
    def __init__(self, kind: str):
        self._kind = kind

    def _raise(self, exc_type: type[Exception], path: list[str], message: str) -> None:
        formatted_path = _format_path(path)
        raise exc_type(f"{formatted_path} {message}")

    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        kind = self._kind

        if kind in _INT_RANGES:
            if not isinstance(value, builtins.int) or isinstance(value, builtins.bool):
                self._raise(TypeError, path, f"must be an integer (got {type(value).__name__})")
            low, high = _INT_RANGES[kind]
            if not (low <= value <= high):
                self._raise(
                    ValueError,
                    path,
                    f"must be between {low} and {high} (got {value!r})",
                )
            return

        if kind in _FLOAT_TYPES:
            if not isinstance(value, (builtins.int, builtins.float)) or isinstance(value, builtins.bool):
                self._raise(TypeError, path, f"must be a real number (got {type(value).__name__})")
            return

        if kind in _STRING_TYPES:
            if not isinstance(value, builtins.str):
                self._raise(TypeError, path, f"must be a string (got {type(value).__name__})")
            return

        if kind == "bool":
            if not isinstance(value, builtins.bool):
                self._raise(TypeError, path, f"must be a boolean value (got {type(value).__name__})")
            return

        if kind in {"byte", "char"}:
            if isinstance(value, builtins.int):
                if 0 <= value <= 255:
                    return
                self._raise(ValueError, path, f"must be between 0 and 255 (got {value!r})")

            if isinstance(value, (bytes, bytearray)):
                if len(value) == 1:
                    return
                self._raise(ValueError, path, f"must contain exactly one byte (got length {len(value)})")

            if isinstance(value, builtins.str):
                if len(value) == 1:
                    return
                self._raise(ValueError, path, f"must contain exactly one character (got length {len(value)})")

            self._raise(TypeError, path, f"must be an unsigned byte value (got {type(value).__name__})")
            return

        raise TypeError(f"Unsupported scalar type metadata: {kind}")


class _ArraySpec(_TypeSpec):
    def __init__(self, element_spec: _TypeSpec, length: int | None):
        self._element_spec = element_spec
        self._length = length

    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        if not isinstance(value, _Sequence) or isinstance(value, (str, bytes, bytearray)):
            formatted = _format_path(path)
            raise TypeError(f"{formatted} must be a sequence (got {type(value).__name__})")

        if self._length is not None and len(value) != self._length:
            formatted = _format_path(path)
            raise ValueError(
                f"{formatted} must contain exactly {self._length} elements (got {len(value)})"
            )

        for index, element in enumerate(value):
            self._element_spec.validate(element, path + [f"[{index}]"], seen)


class _ComplexSpec(_TypeSpec):
    def __init__(self, message_type: type):
        self._message_type = message_type

    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        if not isinstance(value, self._message_type):
            formatted = _format_path(path)
            raise TypeError(
                f"{formatted} must be of type {self._message_type.__name__} (got {type(value).__name__})"
            )

        validator = _ensure_validator(self._message_type)
        validator.validate(value, path, seen)


class _ConstantSpec(_TypeSpec):
    def __init__(self, inner: _TypeSpec, expected: Any | None):
        self._inner = inner
        self._expected = expected

    def with_expected(self, expected: Any) -> "_ConstantSpec":
        return _ConstantSpec(self._inner, expected)

    def validate(self, value: Any, path: list[str], seen: set[int]) -> None:
        self._inner.validate(value, path, seen)
        if self._expected is None:
            formatted = _format_path(path)
            raise TypeError(f"{formatted} missing expected constant value for validation")
        if value != self._expected:
            formatted = _format_path(path)
            raise ValueError(f"{formatted} must be the constant value {self._expected!r} (got {value!r})")


def _unwrap_annotation(annotation: Any) -> tuple[Any, list[Any]]:
    base = annotation
    metadata: list[Any] = []

    while get_origin(base) is Annotated:
        args = get_args(base)
        base = args[0]
        metadata.extend(args[1:])

    return base, metadata


def _resolve_length(length: Any) -> int | None:
    if length is None:
        return None

    origin = get_origin(length)
    if origin is Literal:
        literal_args = get_args(length)
        if len(literal_args) != 1 or not isinstance(literal_args[0], int):
            raise TypeError(f"Unsupported literal length annotation: {length!r}")
        return int(literal_args[0])

    if isinstance(length, int):
        return length

    raise TypeError(f"Unsupported array length annotation: {length!r}")


def _build_spec(annotation: Any) -> _TypeSpec:
    base, metadata = _unwrap_annotation(annotation)

    constant_annotation: Any | None = None
    spec: _TypeSpec | None = None

    for entry in metadata:
        if not isinstance(entry, tuple) or not entry:
            continue

        key = entry[0]

        if key == "constant":
            constant_annotation = entry[1]
            continue

        if key == "array":
            element_spec = _build_spec(entry[1])
            length = _resolve_length(entry[2])
            spec = _ArraySpec(element_spec, length)
            continue

        if key == "complex":
            spec = _ComplexSpec(base)
            continue

        if key in _SCALAR_TYPES:
            spec = _ScalarSpec(key)
            continue

    if spec is None:
        spec = _AnySpec()

    if constant_annotation is not None:
        spec = _ConstantSpec(_build_spec(constant_annotation), expected=None)

    return spec


class _FieldValidator:
    def __init__(self, name: str, spec: _TypeSpec):
        self._name = name
        self._spec = spec

    def validate(self, instance: Any, seen: set[int], prefix: list[str]) -> None:
        value = getattr(instance, self._name)
        self._spec.validate(value, prefix + [self._name], seen)


class _MessageValidator:
    def __init__(self, cls: type):
        module = sys.modules.get(cls.__module__)
        globalns = module.__dict__ if module is not None else {}
        localns = {cls.__name__: cls}
        annotations = get_type_hints(cls, include_extras=True, globalns=globalns, localns=localns)

        self._fields: list[_FieldValidator] = []

        for field in _dataclass_fields(cls):
            annotation = annotations.get(field.name)
            if annotation is None:
                continue

            spec = _build_spec(annotation)
            if isinstance(spec, _ConstantSpec):
                if field.default is MISSING:
                    raise TypeError(
                        f"Constant field '{field.name}' on {cls.__name__} requires a default value"
                    )
                spec = spec.with_expected(field.default)

            self._fields.append(_FieldValidator(field.name, spec))

    def validate(self, instance: Any, prefix: list[str] | None = None, seen: set[int] | None = None) -> None:
        if seen is None:
            seen = set()

        instance_id = id(instance)
        if instance_id in seen:
            return
        seen.add(instance_id)

        prefix = prefix or []
        for field in self._fields:
            field.validate(instance, seen, prefix)


_VALIDATOR_CACHE: dict[type, _MessageValidator] = {}


def _ensure_validator(cls: type) -> _MessageValidator:
    validator = _VALIDATOR_CACHE.get(cls)
    if validator is None:
        validator = getattr(cls, "__pybag_validator__", None)
        if validator is None:
            validator = _MessageValidator(cls)
        _VALIDATOR_CACHE[cls] = validator
    return validator


def _attach_validator(cls: type) -> type:
    validator = _MessageValidator(cls)
    _VALIDATOR_CACHE[cls] = validator

    original_post_init = getattr(cls, "__post_init__", None)

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if original_post_init is not None:
            original_post_init(self, *args, **kwargs)
        validator.validate(self)

    cls.__post_init__ = __post_init__  # type: ignore[assignment]
    setattr(cls, "__pybag_validator__", validator)
    return cls


if TYPE_CHECKING:
    from dataclasses import dataclass as dataclass
else:

    def dataclass(_cls: type | None = None, /, **kwargs: Any):
        """Dataclass decorator with runtime validation for message fields."""

        def wrap(cls: type) -> type:
            user_post_init = getattr(cls, "__post_init__", None)

            def _placeholder_post_init(self: Any, *args: Any, **inner_kwargs: Any) -> None:
                if user_post_init is not None:
                    user_post_init(self, *args, **inner_kwargs)

            cls.__post_init__ = _placeholder_post_init  # type: ignore[assignment]

            decorator = cast(Callable[[type], type], _dataclass(**kwargs))
            decorated = decorator(cls)
            return _attach_validator(decorated)

        if _cls is None:
            return wrap

        return wrap(_cls)


__all__ = [
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "bool",
    "string",
    "wstring",
    "Message",
    "Array",
    "Complex",
    "Constant",
    "dataclass",
]
