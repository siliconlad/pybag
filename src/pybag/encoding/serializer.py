"""Utilities for serializing dataclasses into CDR byte streams."""

from __future__ import annotations

from dataclasses import is_dataclass
from typing import Any, Annotated, get_args, get_origin, Tuple

from .cdr import CdrEncoder


def _parse_annotation(annotation: Any, field_name: str) -> Tuple[str, Tuple[Any, ...]]:
    """Extract encoder method and arguments from an annotation.

    The annotation must be ``typing.Annotated`` where the first metadata item
    specifies the :class:`CdrEncoder` method name.  Additional metadata items are
    interpreted as positional arguments to that method (excluding the value to be
    encoded which is appended automatically).
    """

    if get_origin(annotation) is not Annotated:
        raise TypeError(
            f"Field '{field_name}' must be typing.Annotated with CDR metadata."
        )

    args = get_args(annotation)
    if len(args) < 2:
        raise TypeError(
            f"Field '{field_name}' missing CDR metadata in its Annotated alias."
        )

    metadata = args[1]
    if isinstance(metadata, str):
        return metadata, ()
    if isinstance(metadata, tuple) and metadata and isinstance(metadata[0], str):
        return metadata[0], tuple(metadata[1:])

    raise TypeError(
        f"Field '{field_name}' has unsupported CDR annotation metadata: {metadata!r}"
    )


def serialize(obj: Any, *, little_endian: bool = True) -> bytes:
    """Serialize a dataclass instance into a CDR byte stream.

    Args:
        obj: The dataclass instance to serialize.
        little_endian: Whether the resulting CDR stream should be little endian.

    Returns:
        The serialized CDR byte stream.
    """

    if not is_dataclass(obj):
        raise TypeError("serialize() expects a dataclass instance")

    encoder = CdrEncoder(little_endian=little_endian)

    for name, annotation in obj.__annotations__.items():
        method_name, method_args = _parse_annotation(annotation, name)
        try:
            method = getattr(encoder, method_name)
        except AttributeError as exc:
            raise ValueError(
                f"Unsupported CDR type '{method_name}' for field '{name}'"
            ) from exc

        value = getattr(obj, name)
        method(*method_args, value)

    return encoder.save()
