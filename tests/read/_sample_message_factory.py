"""Utilities to build sample ROS messages for MCAP reader tests."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
from rosbags.interfaces.typing import Nodetype
from rosbags.typesys.store import Typestore


def create_message(typestore: Typestore, msgtype: str, *, seed: int = 1) -> Any:
    """Construct a deterministic sample message of the given type."""
    msg_cls = typestore.types[msgtype]
    msgdef = typestore.get_msgdef(msgtype)
    if not msgdef.fields:
        return msg_cls()

    annotations = getattr(msg_cls, "__annotations__", {})
    kwargs: dict[str, Any] = {}
    for index, (field_name, node) in enumerate(msgdef.fields, start=1):
        annotation = annotations.get(field_name)
        kwargs[field_name] = _sample_value(
            typestore,
            node,
            seed * 10 + index,
            annotation,
        )
    return msg_cls(**kwargs)


def to_plain(
    typestore: Typestore,
    value: Any,
    msgtype: str,
    fields: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Convert message objects into plain Python structures based on their schema."""
    msgdef = typestore.get_msgdef(msgtype)
    requested = set(fields) if fields is not None else None
    available = set(vars(value).keys())
    result: dict[str, Any] = {}
    for field_name, node in msgdef.fields:
        if requested is not None and field_name not in requested:
            continue
        if field_name not in available:
            continue
        field_value = getattr(value, field_name)
        result[field_name] = _convert_field(typestore, field_value, node)
    return result


def _sample_value(
    typestore: Typestore,
    node: Any,
    seed: int,
    annotation: str | None,
) -> Any:
    nodetype, typeinfo = node
    if nodetype == Nodetype.BASE:
        basetype, _ = typeinfo
        return _sample_base_value(basetype, seed)
    if nodetype == Nodetype.NAME:
        return create_message(typestore, typeinfo, seed=seed)
    if nodetype == Nodetype.ARRAY:
        basenode, size = typeinfo
        if annotation and annotation.startswith("np.ndarray"):
            dtype = _numpy_dtype(annotation, basenode)
            values = [
                _sample_numpy_element(typestore, basenode, seed * 10 + offset)
                for offset in range(1, size + 1)
            ]
            return np.array(values, dtype=dtype)
        return [
            _sample_value(typestore, basenode, seed * 10 + offset, None)
            for offset in range(1, size + 1)
        ]
    if nodetype == Nodetype.SEQUENCE:
        basenode, maxsize = typeinfo
        if maxsize == 0:
            length = 3
        else:
            length = max(1, min(3, maxsize))
        if annotation and annotation.startswith("np.ndarray"):
            dtype = _numpy_dtype(annotation, basenode)
            values = [
                _sample_numpy_element(typestore, basenode, seed * 10 + offset)
                for offset in range(1, length + 1)
            ]
            return np.array(values, dtype=dtype)
        return [
            _sample_value(typestore, basenode, seed * 10 + offset, None)
            for offset in range(1, length + 1)
        ]
    raise ValueError(f"Unsupported node type: {nodetype!r}")


def _sample_base_value(basetype: str, seed: int) -> Any:
    basetype = basetype.lower()
    if basetype in {"float32", "float64"}:
        return float(seed) + 0.5
    if basetype == "byte":
        return seed % 256
    if basetype == "char":
        return 32 + (seed % 95)
    if basetype == "uint8":
        return seed % 256
    if basetype.startswith("uint"):
        bits = int(basetype[4:])
        return seed % (1 << bits)
    if basetype.startswith("int"):
        bits = int(basetype[3:])
        max_val = (1 << (bits - 1)) - 1
        return seed % (max_val + 1)
    if basetype == "bool":
        return bool(seed % 2)
    if basetype in {"string", "wstring"}:
        return f"string{seed}"
    raise ValueError(f"Unsupported base type: {basetype}")


def _numpy_dtype(annotation: str, node: Any) -> np.dtype[Any] | None:
    if "np.dtype" in annotation:
        start = annotation.find("np.dtype[np.")
        if start != -1:
            start += len("np.dtype[np.")
            end = annotation.find("]", start)
            if end != -1:
                dtype_name = annotation[start:end]
                return getattr(np, dtype_name)
    node_type, info = node
    if node_type == Nodetype.BASE:
        basetype = info[0]
        return getattr(np, basetype)
    return None


def _sample_numpy_element(typestore: Typestore, node: Any, seed: int) -> Any:
    node_type, info = node
    if node_type == Nodetype.BASE:
        basetype = info[0].lower()
        if basetype in {"byte", "char"}:
            return seed % 256
        return _sample_base_value(basetype, seed)
    if node_type == Nodetype.NAME:
        raise ValueError("numpy arrays of nested message types are not supported")
    raise ValueError(f"Unsupported numpy element node: {node_type!r}")


def _convert_field(typestore: Typestore, value: Any, node: Any) -> Any:
    nodetype, info = node
    if nodetype == Nodetype.BASE:
        basetype, _ = info
        return _convert_base_value(basetype, value)
    if nodetype == Nodetype.NAME:
        return to_plain(typestore, value, info)
    if nodetype == Nodetype.ARRAY:
        basenode, size = info
        items = _iterable_values(value)
        return [
            _convert_field(typestore, items[index], basenode)
            for index in range(size)
        ]
    if nodetype == Nodetype.SEQUENCE:
        basenode, _ = info
        items = _iterable_values(value)
        return [
            _convert_field(typestore, item, basenode)
            for item in items
        ]
    raise ValueError(f"Unsupported node type: {nodetype!r}")


def _convert_base_value(basetype: str, value: Any) -> Any:
    basetype = basetype.lower()
    if basetype in {"float32", "float64"}:
        return float(value)
    if basetype == "bool":
        return bool(value)
    if basetype == "byte":
        if isinstance(value, (bytes, bytearray)):
            return int(value[0])
        return int(value)
    if basetype == "char":
        if isinstance(value, str):
            return ord(value)
        return int(value)
    if basetype.startswith("uint") or basetype.startswith("int"):
        return int(value)
    if basetype in {"string", "wstring"}:
        return str(value)
    return value


def _iterable_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if hasattr(value, "tolist"):
        return list(value.tolist())
    return list(value)
