#!/usr/bin/env python3
"""Generate Python message modules from ROS2 .msg files on GitHub.

This script fetches .msg files from a GitHub repository and generates
Python dataclasses with optional integrated __encode__ and __decode__ methods.

Usage:
    # Generate with encode/decode methods (default)
    uv run scripts/generate_messages.py \\
        https://github.com/ros2/common_interfaces/tree/humble/geometry_msgs \\
        --distro humble

    # Generate without encode/decode methods
    uv run scripts/generate_messages.py \\
        https://github.com/ros2/common_interfaces/tree/humble/geometry_msgs \\
        --distro humble --no-codecs
"""
from __future__ import annotations

import argparse
import ast
import re
import struct
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Any

# Struct format mappings for primitives
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

# All primitive types
_PRIMITIVE_TYPES = {
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float32", "float64", "bool", "byte", "char",
}
_STRING_TYPES = {"string", "wstring"}

_TAB = "    "


# -----------------------------------------------------------------------------
# Schema representation
# -----------------------------------------------------------------------------

@dataclass
class Primitive:
    """A primitive type like int32, float64, bool, etc."""
    type: str


@dataclass
class String:
    """A string or wstring type, optionally bounded."""
    type: str  # 'string' or 'wstring'
    max_length: int | None = None


@dataclass
class Array:
    """A fixed-size or bounded array: type[N] or type[<=N]."""
    element: Any  # Primitive, String, or Complex
    length: int
    is_bounded: bool = False


@dataclass
class Sequence:
    """A variable-length sequence: type[]."""
    element: Any  # Primitive, String, or Complex


@dataclass
class Complex:
    """A reference to another message type."""
    package: str  # e.g., 'geometry_msgs' or 'std_msgs'
    name: str  # e.g., 'Point' or 'Header'


@dataclass
class Constant:
    """A constant definition: TYPE NAME=value."""
    type: Any  # Primitive or String
    name: str
    value: Any


@dataclass
class Field:
    """A message field: TYPE name [default]."""
    type: Any  # Primitive, String, Array, Sequence, or Complex
    name: str
    default: Any = None


@dataclass
class MessageDef:
    """A complete message definition."""
    package: str
    name: str
    fields: list[Field] = field(default_factory=list)
    constants: list[Constant] = field(default_factory=list)


# -----------------------------------------------------------------------------
# GitHub URL parsing and fetching
# -----------------------------------------------------------------------------

def parse_github_url(url: str) -> tuple[str, str, str, str]:
    """Parse a GitHub URL to extract org, repo, branch, and package.

    Examples:
        https://github.com/ros2/common_interfaces/tree/humble/geometry_msgs
        -> ('ros2', 'common_interfaces', 'humble', 'geometry_msgs')
    """
    # Pattern for tree URLs
    pattern = r"https?://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.+?)/?$"
    match = re.match(pattern, url)
    if not match:
        raise ValueError(
            f"Invalid GitHub URL format: {url}\n"
            "Expected: https://github.com/org/repo/tree/branch/package"
        )
    return match.groups()  # type: ignore[return-value]


def fetch_msg_files(
    org: str, repo: str, branch: str, package: str
) -> dict[str, str]:
    """Fetch all .msg files for a package using GitHub's tarball API.

    Downloads the entire repository as a tarball in a single request,
    then extracts only the .msg files for the specified package.
    This avoids rate limiting issues from making many individual file requests.

    Returns dict mapping message name (e.g., 'Point') to file content.
    """
    import io
    import tarfile

    # Download the tarball for the branch (single API call)
    tarball_url = f"https://github.com/{org}/{repo}/archive/refs/heads/{branch}.tar.gz"
    print(f"  Downloading tarball: {tarball_url}")

    try:
        req = urllib.request.Request(tarball_url, headers={"User-Agent": "pybag"})
        with urllib.request.urlopen(req, timeout=120) as response:
            tarball_data = response.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Failed to download tarball: {e}") from e

    # Extract .msg files from the tarball
    msg_files = {}
    msg_path_prefix = f"{package}/msg/"

    with tarfile.open(fileobj=io.BytesIO(tarball_data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue

            # Tarball has a root directory like "repo-branch/"
            # We need to strip that and check if the path matches our package
            parts = member.name.split("/", 1)
            if len(parts) < 2:
                continue

            relative_path = parts[1]  # Path without the root directory

            if relative_path.startswith(msg_path_prefix) and relative_path.endswith(".msg"):
                filename = relative_path[len(msg_path_prefix):]
                # Skip files in subdirectories
                if "/" in filename:
                    continue

                msg_name = filename[:-4]  # Remove .msg extension
                print(f"  Found {filename}")

                # Extract and decode the file content
                file_obj = tar.extractfile(member)
                if file_obj is not None:
                    content = file_obj.read().decode("utf-8")
                    msg_files[msg_name] = content

    return msg_files


# -----------------------------------------------------------------------------
# .msg file parsing
# -----------------------------------------------------------------------------

def remove_comment(line: str) -> str:
    """Remove inline comments from a line, respecting string literals."""
    in_single = False
    in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return line[:i].rstrip()
    return line.strip()


def parse_type(type_str: str, current_package: str) -> Any:
    """Parse a type string into a schema type object."""
    # Handle array and sequence types: type[N], type[<=N], type[]
    if array_match := re.match(r"(.+)\[(.*)\]$", type_str):
        element_str, length_spec = array_match.groups()
        element_type = parse_type(element_str, current_package)

        if length_spec == "":
            return Sequence(element_type)

        is_bounded = length_spec.startswith("<=")
        length = int(length_spec[2:]) if is_bounded else int(length_spec)
        return Array(element_type, length, is_bounded)

    # Handle bounded strings: string<=N, wstring<=N
    if type_str.startswith("string"):
        if match := re.match(r"string(?:<=(\d+))?$", type_str):
            length = int(match.group(1)) if match.group(1) else None
            return String("string", length)
        raise ValueError(f"Invalid string type: {type_str}")

    if type_str.startswith("wstring"):
        if match := re.match(r"wstring(?:<=(\d+))?$", type_str):
            length = int(match.group(1)) if match.group(1) else None
            return String("wstring", length)
        raise ValueError(f"Invalid wstring type: {type_str}")

    # Handle primitive types
    if type_str in _PRIMITIVE_TYPES:
        return Primitive(type_str)

    # Handle complex types
    if type_str == "Header":
        # Special case: Header always means std_msgs/Header
        return Complex("std_msgs", "Header")

    if "/" in type_str:
        # Fully qualified: package/Type or package/msg/Type
        parts = type_str.split("/")
        if len(parts) == 2:
            return Complex(parts[0], parts[1])
        elif len(parts) == 3 and parts[1] == "msg":
            return Complex(parts[0], parts[2])
        else:
            raise ValueError(f"Invalid complex type: {type_str}")

    # Same package reference
    return Complex(current_package, type_str)


def parse_default_value(type_obj: Any, raw_value: str) -> Any:
    """Parse a default value string based on the field type."""
    if isinstance(type_obj, Primitive):
        ptype = type_obj.type
        if ptype == "bool":
            return raw_value.lower() in ("true", "1")
        if ptype in ("float32", "float64"):
            return float(raw_value)
        if ptype == "byte":
            return int(raw_value)
        if ptype == "char":
            return raw_value.strip("'\"") if raw_value.startswith(("'", '"')) else raw_value
        return int(raw_value)

    if isinstance(type_obj, String):
        return raw_value.strip('"') if raw_value.startswith('"') else raw_value.strip("'")

    if isinstance(type_obj, (Array, Sequence)):
        values = ast.literal_eval(raw_value.strip())
        if not isinstance(values, list):
            raise ValueError("Array default must be a list")
        elem = type_obj.element
        if isinstance(elem, Primitive):
            return [parse_default_value(elem, str(v)) for v in values]
        return values

    raise ValueError(f"Cannot parse default for type: {type_obj}")


def parse_msg_file(content: str, package: str, msg_name: str) -> MessageDef:
    """Parse a .msg file content into a MessageDef."""
    msg = MessageDef(package=package, name=msg_name)

    for line in content.split("\n"):
        line = remove_comment(line).strip()
        if not line:
            continue

        # Parse: TYPE NAME [= VALUE] or TYPE NAME [DEFAULT]
        # Constants have = in the name or default starts with =
        match = re.match(r"(\S+)\s+(\S+)(?:\s+(.+))?$", line)
        if not match:
            raise ValueError(f"Invalid field definition: {line}")

        type_str, name_str, raw_default = match.groups()

        # Check if this is a constant (NAME is uppercase with = or default starts with =)
        is_constant = False
        if "=" in name_str:
            is_constant = True
            name_str, raw_default = name_str.split("=", 1)
        elif raw_default and raw_default.startswith("="):
            is_constant = True
            raw_default = raw_default[1:].strip()

        if is_constant:
            if not name_str.isupper():
                raise ValueError(f"Constant name must be uppercase: {name_str}")
            type_obj = parse_type(type_str, package)
            value = parse_default_value(type_obj, raw_default.strip())
            msg.constants.append(Constant(type_obj, name_str, value))
        else:
            # Regular field
            type_obj = parse_type(type_str, package)
            default = None
            if raw_default:
                default = parse_default_value(type_obj, raw_default.strip())
            msg.fields.append(Field(type_obj, name_str, default))

    return msg


# -----------------------------------------------------------------------------
# Dependency analysis and topological sorting
# -----------------------------------------------------------------------------

def get_dependencies(msg: MessageDef) -> set[tuple[str, str]]:
    """Get all complex type dependencies for a message.

    Returns set of (package, name) tuples.
    """
    deps = set()

    def collect(type_obj: Any) -> None:
        if isinstance(type_obj, Complex):
            deps.add((type_obj.package, type_obj.name))
        elif isinstance(type_obj, (Array, Sequence)):
            collect(type_obj.element)

    for fld in msg.fields:
        collect(fld.type)

    return deps


def topological_sort(
    messages: dict[str, MessageDef], current_package: str
) -> list[MessageDef]:
    """Sort messages so that dependencies come before dependents."""
    # Build adjacency list
    graph: dict[str, set[str]] = {name: set() for name in messages}

    for name, msg in messages.items():
        for pkg, dep_name in get_dependencies(msg):
            if pkg == current_package and dep_name in messages:
                graph[name].add(dep_name)

    # Kahn's algorithm
    in_degree = {name: 0 for name in messages}
    for name in messages:
        for dep in graph[name]:
            in_degree[dep] += 1  # Note: reversed for proper order

    # Actually we need dependents after dependencies
    # So rebuild with correct direction
    in_degree = {name: 0 for name in messages}
    for name in messages:
        for dep in graph[name]:
            pass  # We count edges going INTO a node

    # Rebuild correctly: if A depends on B, B must come before A
    real_in_degree = {name: 0 for name in messages}
    for name in messages:
        for dep in graph[name]:
            # name depends on dep, so name has an incoming edge from dep
            real_in_degree[name] += 1

    queue = [name for name in messages if real_in_degree[name] == 0]
    result = []

    while queue:
        current = queue.pop(0)
        result.append(messages[current])

        # Find nodes that depend on current
        for name in messages:
            if current in graph[name]:
                real_in_degree[name] -= 1
                if real_in_degree[name] == 0:
                    queue.append(name)

    if len(result) != len(messages):
        # Cycle detected, just return in original order
        print("  Warning: Circular dependency detected, using original order")
        return list(messages.values())

    return result


# -----------------------------------------------------------------------------
# Code generation
# -----------------------------------------------------------------------------

class CodeGenerator:
    """Generate Python code for message definitions."""

    def __init__(
        self,
        package: str,
        distro: str,
        external_packages: set[str],
        *,
        generate_codecs: bool = True,
    ):
        self.package = package
        self.distro = distro
        self.external_packages = external_packages
        self.generate_codecs = generate_codecs
        self._var_counter = count()

    def _new_var(self, prefix: str) -> str:
        return f"_{prefix}_{next(self._var_counter)}"

    def _reset_var_counter(self) -> None:
        self._var_counter = count()

    def type_annotation(self, type_obj: Any) -> str:
        """Generate type annotation string for a field type."""
        if isinstance(type_obj, Primitive):
            # ROS 2 char goes under ros2 namespace
            if type_obj.type == "char":
                return "t.ros2.char"
            return f"t.{type_obj.type}"

        if isinstance(type_obj, String):
            return f"t.{type_obj.type}"

        if isinstance(type_obj, Array):
            elem = self.type_annotation(type_obj.element)
            if type_obj.is_bounded:
                # Bounded arrays are treated like sequences
                return f"t.Array[{elem}]"
            return f"t.Array[{elem}, Literal[{type_obj.length}]]"

        if isinstance(type_obj, Sequence):
            elem = self.type_annotation(type_obj.element)
            return f"t.Array[{elem}]"

        if isinstance(type_obj, Complex):
            if type_obj.package == self.package:
                return type_obj.name
            return f"{type_obj.package}.{type_obj.name}"

        return "Any"

    def default_repr(self, value: Any) -> str:
        """Generate Python repr for a default value."""
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, str):
            return repr(value)
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            # Use field(default_factory=...) for mutable defaults
            return None  # type: ignore[return-value]
        return repr(value)

    def _class_ref(self, cplx: Complex) -> str:
        """Get the class reference string for a complex type."""
        if cplx.package == self.package:
            return cplx.name
        return f"{cplx.package}.{cplx.name}"

    def generate_dataclass(
        self, msg: MessageDef, all_messages: dict[str, MessageDef]
    ) -> list[str]:
        """Generate dataclass definition with __decode__ and __encode__ methods."""
        lines = []
        lines.append("")
        lines.append("")
        # NOTE: We use kw_only=True to allow constants (which have default values)
        # to be defined before fields (which may not have defaults). This is required
        # because ROS2 message definitions have constants at the top, but Python
        # dataclasses normally require fields with defaults to come after fields
        # without defaults. The kw_only=True parameter relaxes this constraint.
        # A future improvement could remove kw_only=True by using ClassVar for
        # constants, but this would require updating the schema encoder/decoder
        # to handle ClassVar-wrapped constants properly.
        lines.append("@dataclass(kw_only=True)")
        lines.append(f"class {msg.name}:")
        lines.append(f"    __msg_name__ = '{msg.package}/msg/{msg.name}'")
        lines.append("")

        # Add constants first (ROS2 convention)
        for const in msg.constants:
            type_ann = self.type_annotation(const.type)
            value_repr = self.default_repr(const.value)
            lines.append(f"    {const.name}: t.Constant[{type_ann}] = {value_repr}")

        # Add fields
        for fld in msg.fields:
            type_ann = self.type_annotation(fld.type)
            if fld.default is not None:
                default_repr = self.default_repr(fld.default)
                if default_repr is None:
                    # Mutable default - use field(default_factory=...)
                    lines.append(
                        f"    {fld.name}: {type_ann} = field(default_factory=lambda: {fld.default!r})"
                    )
                else:
                    lines.append(f"    {fld.name}: {type_ann} = {default_repr}")
            else:
                lines.append(f"    {fld.name}: {type_ann}")

        # Handle empty message (only constants or no fields at all)
        # ROS2 CDR requires at least 1 byte for empty structs
        is_empty_msg = not msg.fields
        if is_empty_msg:
            lines.append("    # ROS2 CDR requires at least 1 byte for empty structs")
            lines.append("    structure_needs_at_least_one_member: t.uint8 = 0")

        # Generate __decode__ and __encode__ methods if codecs are enabled
        if self.generate_codecs:
            lines.append("")
            lines.extend(self._generate_decode_method(msg, all_messages))

            lines.append("")
            lines.extend(self._generate_encode_method(msg, all_messages))

        return lines

    def _generate_decode_method(
        self, msg: MessageDef, all_messages: dict[str, MessageDef]
    ) -> list[str]:
        """Generate __decode__ classmethod for a message."""
        self._reset_var_counter()
        lines = []
        lines.append("    @classmethod")
        lines.append("    def __decode__(cls, decoder: CdrDecoder) -> Self:")
        lines.append(f'        """Decode a {msg.name} message from CDR binary data."""')
        lines.append("        fmt_prefix = '<' if decoder._is_little_endian else '>'")
        lines.append("        _data = decoder._data")

        field_vars: list[str] = []

        # Track runs of same-type primitives for batching
        run_type: str | None = None
        run_fields: list[str] = []

        def flush_run() -> None:
            nonlocal run_type, run_fields
            if not run_fields:
                return

            count_fields = len(run_fields)
            size = _STRUCT_SIZE[run_type]  # type: ignore[index]
            fmt = _STRUCT_FORMAT[run_type] * count_fields  # type: ignore[index]
            lines.append(f"        _data.align({size})")

            if count_fields > 1:
                var_names = ", ".join(run_fields)
                lines.append(
                    f"        {var_names} = struct.unpack(fmt_prefix + '{fmt}', "
                    f"_data.read({size * count_fields}))"
                )
            else:
                fld_name = run_fields[0]
                lines.append(
                    f"        {fld_name} = struct.unpack(fmt_prefix + '{fmt}', "
                    f"_data.read({size * count_fields}))[0]"
                )

            run_fields.clear()
            run_type = None

        for fld in msg.fields:
            field_vars.append(fld.name)
            type_obj = fld.type

            # Check if primitive that can be batched
            if isinstance(type_obj, Primitive) and type_obj.type in _STRUCT_FORMAT:
                if run_type == type_obj.type:
                    run_fields.append(fld.name)
                else:
                    flush_run()
                    run_type = type_obj.type
                    run_fields = [fld.name]
                continue

            flush_run()

            if isinstance(type_obj, Primitive):
                # byte, char handled separately
                lines.append(f"        {fld.name} = decoder.{type_obj.type}()")

            elif isinstance(type_obj, String):
                lines.append(f"        {fld.name} = decoder.{type_obj.type}()")

            elif isinstance(type_obj, Array):
                self._generate_decode_array(lines, fld.name, type_obj, all_messages, indent=2)

            elif isinstance(type_obj, Sequence):
                self._generate_decode_sequence(lines, fld.name, type_obj, all_messages, indent=2)

            elif isinstance(type_obj, Complex):
                self._generate_decode_complex(lines, fld.name, type_obj, indent=2)

        flush_run()

        # Handle empty messages - ROS2 CDR requires reading the dummy byte
        if not msg.fields:
            lines.append("        # Read dummy byte for empty struct")
            lines.append("        _data.read(1)")

        # Build constructor call
        if field_vars:
            args = ", ".join(f"{name}={name}" for name in field_vars)
            lines.append(f"        return cls({args})")
        else:
            lines.append("        return cls()")

        return lines

    def _generate_decode_array(
        self,
        lines: list[str],
        name: str,
        arr: Array,
        all_messages: dict[str, MessageDef],
        indent: int = 1,
    ) -> None:
        """Generate decode code for a fixed-size array."""
        ind = _TAB * indent
        elem = arr.element
        length = arr.length

        if isinstance(elem, Primitive):
            if elem.type == "uint8":
                lines.append(f"{ind}{name} = _data.read({length})")
            elif elem.type in _STRUCT_FORMAT:
                size = _STRUCT_SIZE[elem.type]
                fmt = _STRUCT_FORMAT[elem.type] * length
                lines.append(f"{ind}_data.align({size})")
                lines.append(
                    f"{ind}{name} = list(struct.unpack(fmt_prefix + '{fmt}', "
                    f"_data.read({size * length})))"
                )
            elif elem.type in ("byte", "char"):
                lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range({length})]")
            else:
                lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range({length})]")

        elif isinstance(elem, String):
            lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range({length})]")

        elif isinstance(elem, Complex):
            class_ref = self._class_ref(elem)
            lines.append(f"{ind}{name} = [{class_ref}.__decode__(decoder) for _ in range({length})]")

    def _generate_decode_sequence(
        self,
        lines: list[str],
        name: str,
        seq: Sequence,
        all_messages: dict[str, MessageDef],
        indent: int = 1,
    ) -> None:
        """Generate decode code for a variable-length sequence."""
        ind = _TAB * indent
        elem = seq.element

        if isinstance(elem, Primitive):
            if elem.type == "uint8":
                lines.append(f"{ind}_len = decoder.uint32()")
                lines.append(f"{ind}{name} = _data.read(_len)")
            elif elem.type in _STRUCT_FORMAT:
                size = _STRUCT_SIZE[elem.type]
                fmt_char = _STRUCT_FORMAT[elem.type]
                lines.append(f"{ind}_len = decoder.uint32()")
                lines.append(f"{ind}_data.align({size})")
                lines.append(
                    f"{ind}{name} = list(struct.unpack(fmt_prefix + '{fmt_char}' * _len, "
                    f"_data.read({size} * _len)))"
                )
            elif elem.type in ("byte", "char"):
                lines.append(f"{ind}_len = decoder.uint32()")
                lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range(_len)]")
            else:
                lines.append(f"{ind}_len = decoder.uint32()")
                lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range(_len)]")

        elif isinstance(elem, String):
            lines.append(f"{ind}_len = decoder.uint32()")
            lines.append(f"{ind}{name} = [decoder.{elem.type}() for _ in range(_len)]")

        elif isinstance(elem, Complex):
            class_ref = self._class_ref(elem)
            lines.append(f"{ind}_len = decoder.uint32()")
            lines.append(f"{ind}{name} = [{class_ref}.__decode__(decoder) for _ in range(_len)]")

    def _generate_decode_complex(
        self, lines: list[str], name: str, cplx: Complex, indent: int = 1
    ) -> None:
        """Generate decode code for a complex type field."""
        ind = _TAB * indent
        class_ref = self._class_ref(cplx)
        lines.append(f"{ind}{name} = {class_ref}.__decode__(decoder)")

    def _generate_encode_method(
        self, msg: MessageDef, all_messages: dict[str, MessageDef]
    ) -> list[str]:
        """Generate __encode__ method for a message."""
        self._reset_var_counter()
        lines = []
        lines.append("    def __encode__(self, encoder: CdrEncoder) -> None:")
        lines.append(f'        """Encode this {msg.name} message to CDR binary data."""')
        lines.append("        fmt_prefix = '<' if encoder._is_little_endian else '>'")
        lines.append("        _payload = encoder._payload")
        lines.append("        struct_pack = struct.pack")

        # Track runs of same-type primitives for batching
        run_type: str | None = None
        run_values: list[str] = []

        def flush_run() -> None:
            nonlocal run_type, run_values
            if not run_values:
                return

            assert run_type is not None
            fmt = _WRITE_FORMAT[run_type]
            size = _WRITE_SIZE[run_type]

            value_exprs = []
            for expr in run_values:
                if run_type == "bool":
                    value_exprs.append(f"bool({expr})")
                else:
                    value_exprs.append(expr)

            if size > 1:
                lines.append(f"        _payload.align({size})")

            fmt_str = fmt * len(run_values)
            pack_args = ", ".join(value_exprs)
            lines.append(f"        _payload.write(struct_pack(fmt_prefix + '{fmt_str}', {pack_args}))")

            run_values.clear()
            run_type = None

        for fld in msg.fields:
            type_obj = fld.type
            field_expr = f"self.{fld.name}"

            # Check if primitive that can be batched
            if isinstance(type_obj, Primitive) and type_obj.type in _STRUCT_FORMAT:
                if run_type == type_obj.type:
                    run_values.append(field_expr)
                else:
                    flush_run()
                    run_type = type_obj.type
                    run_values = [field_expr]
                continue

            flush_run()

            if isinstance(type_obj, Primitive):
                # byte, char handled separately
                lines.append(f"        encoder.{type_obj.type}({field_expr})")

            elif isinstance(type_obj, String):
                if type_obj.type == "wstring":
                    lines.append(f"        encoder.wstring({field_expr})")
                else:
                    encoded_var = self._new_var("encoded")
                    lines.append(f"        {encoded_var} = {field_expr}.encode()")
                    lines.append("        _payload.align(4)")
                    lines.append(f"        _payload.write(struct_pack(fmt_prefix + 'I', len({encoded_var}) + 1))")
                    lines.append(f"        _payload.write({encoded_var} + b'\\x00')")

            elif isinstance(type_obj, Array):
                self._generate_encode_array(lines, field_expr, type_obj, all_messages, indent=2)

            elif isinstance(type_obj, Sequence):
                self._generate_encode_sequence(lines, field_expr, type_obj, all_messages, indent=2)

            elif isinstance(type_obj, Complex):
                self._generate_encode_complex(lines, field_expr, type_obj, indent=2)

        flush_run()

        # Handle empty messages - ROS2 CDR requires writing the dummy byte
        if not msg.fields:
            lines.append("        # Write dummy byte for empty struct")
            lines.append("        _payload.write(b'\\x00')")

        return lines

    def _generate_encode_array(
        self,
        lines: list[str],
        expr: str,
        arr: Array,
        all_messages: dict[str, MessageDef],
        indent: int = 1,
    ) -> None:
        """Generate encode code for a fixed-size array."""
        ind = _TAB * indent
        elem = arr.element
        length = arr.length
        values_var = self._new_var("values")

        lines.append(f"{ind}{values_var} = {expr}")
        if not arr.is_bounded:
            lines.append(f"{ind}if len({values_var}) != {length}:")
            lines.append(
                f"{ind}    raise ValueError(f'Fixed array size mismatch: expected {length}, got {{len({values_var})}}')"
            )

        if isinstance(elem, Primitive):
            if elem.type in ("uint8", "byte"):
                # byte and uint8 both write raw bytes
                lines.append(f"{ind}if {values_var}:")
                lines.append(f"{ind}    if isinstance({values_var}, (bytes, bytearray)):")
                lines.append(f"{ind}        _payload.write({values_var})")
                lines.append(f"{ind}    elif {values_var} and isinstance({values_var}[0], int):")
                lines.append(f"{ind}        # List of integers")
                lines.append(f"{ind}        _payload.write(bytes({values_var}))")
                lines.append(f"{ind}    else:")
                lines.append(f"{ind}        # List of bytes objects")
                lines.append(f"{ind}        _payload.write(b''.join({values_var}))")
            elif elem.type in _WRITE_FORMAT:
                size = _WRITE_SIZE[elem.type]
                fmt = _WRITE_FORMAT[elem.type]
                lines.append(f"{ind}if {values_var}:")
                lines.append(f"{ind}    _payload.align({size})")
                lines.append(
                    f"{ind}    _payload.write(struct_pack(fmt_prefix + '{fmt}' * len({values_var}), *{values_var}))"
                )
            elif elem.type == "char":
                item_var = self._new_var("item")
                lines.append(f"{ind}for {item_var} in {values_var}:")
                lines.append(f"{ind}    encoder.char({item_var})")

        elif isinstance(elem, String):
            item_var = self._new_var("item")
            lines.append(f"{ind}for {item_var} in {values_var}:")
            if elem.type == "wstring":
                lines.append(f"{ind}    encoder.wstring({item_var})")
            else:
                encoded_var = self._new_var("encoded")
                lines.append(f"{ind}    {encoded_var} = {item_var}.encode()")
                lines.append(f"{ind}    _payload.align(4)")
                lines.append(f"{ind}    _payload.write(struct_pack(fmt_prefix + 'I', len({encoded_var}) + 1))")
                lines.append(f"{ind}    _payload.write({encoded_var} + b'\\x00')")

        elif isinstance(elem, Complex):
            item_var = self._new_var("item")
            lines.append(f"{ind}for {item_var} in {values_var}:")
            lines.append(f"{ind}    {item_var}.__encode__(encoder)")

    def _generate_encode_sequence(
        self,
        lines: list[str],
        expr: str,
        seq: Sequence,
        all_messages: dict[str, MessageDef],
        indent: int = 1,
    ) -> None:
        """Generate encode code for a variable-length sequence."""
        ind = _TAB * indent
        elem = seq.element
        values_var = self._new_var("values")
        length_var = self._new_var("length")

        lines.append(f"{ind}{values_var} = {expr}")
        lines.append(f"{ind}{length_var} = len({values_var})")
        lines.append(f"{ind}_payload.align(4)")
        lines.append(f"{ind}_payload.write(struct_pack(fmt_prefix + 'I', {length_var}))")

        if isinstance(elem, Primitive):
            if elem.type in ("uint8", "byte"):
                # byte and uint8 both write raw bytes
                lines.append(f"{ind}if {length_var}:")
                lines.append(f"{ind}    if isinstance({values_var}, (bytes, bytearray)):")
                lines.append(f"{ind}        _payload.write({values_var})")
                lines.append(f"{ind}    elif {values_var} and isinstance({values_var}[0], int):")
                lines.append(f"{ind}        # List of integers")
                lines.append(f"{ind}        _payload.write(bytes({values_var}))")
                lines.append(f"{ind}    else:")
                lines.append(f"{ind}        # List of bytes objects")
                lines.append(f"{ind}        _payload.write(b''.join({values_var}))")
            elif elem.type in _WRITE_FORMAT:
                size = _WRITE_SIZE[elem.type]
                fmt = _WRITE_FORMAT[elem.type]
                lines.append(f"{ind}if {length_var}:")
                lines.append(f"{ind}    _payload.align({size})")
                lines.append(
                    f"{ind}    _payload.write(struct_pack(fmt_prefix + '{fmt}' * {length_var}, *{values_var}))"
                )
            elif elem.type == "char":
                item_var = self._new_var("item")
                lines.append(f"{ind}for {item_var} in {values_var}:")
                lines.append(f"{ind}    encoder.char({item_var})")

        elif isinstance(elem, String):
            item_var = self._new_var("item")
            lines.append(f"{ind}for {item_var} in {values_var}:")
            if elem.type == "wstring":
                lines.append(f"{ind}    encoder.wstring({item_var})")
            else:
                encoded_var = self._new_var("encoded")
                lines.append(f"{ind}    {encoded_var} = {item_var}.encode()")
                lines.append(f"{ind}    _payload.align(4)")
                lines.append(f"{ind}    _payload.write(struct_pack(fmt_prefix + 'I', len({encoded_var}) + 1))")
                lines.append(f"{ind}    _payload.write({encoded_var} + b'\\x00')")

        elif isinstance(elem, Complex):
            item_var = self._new_var("item")
            lines.append(f"{ind}for {item_var} in {values_var}:")
            lines.append(f"{ind}    {item_var}.__encode__(encoder)")

    def _generate_encode_complex(
        self, lines: list[str], expr: str, cplx: Complex, indent: int = 1
    ) -> None:
        """Generate encode code for a complex type field."""
        ind = _TAB * indent
        lines.append(f"{ind}{expr}.__encode__(encoder)")

    def generate_module(
        self,
        messages: list[MessageDef],
        source_url: str,
    ) -> str:
        """Generate complete Python module for all messages."""
        all_messages = {m.name: m for m in messages}
        output_lines: list[str] = []

        # Module docstring
        output_lines.append(f'"""ROS2 {self.package} message definitions.')
        output_lines.append("")
        output_lines.append("Auto-generated by scripts/generate_messages.py")
        output_lines.append(f"Source: {source_url}")
        output_lines.append('"""')
        output_lines.append("from __future__ import annotations")
        output_lines.append("")

        # Only import struct if generating codecs
        if self.generate_codecs:
            output_lines.append("import struct")

        output_lines.append("from dataclasses import dataclass, field")

        # Only need TYPE_CHECKING imports if generating codecs
        if self.generate_codecs:
            output_lines.append("from typing import TYPE_CHECKING, Literal")
        else:
            output_lines.append("from typing import Literal")

        output_lines.append("")
        output_lines.append("import pybag.types as t")

        # Add imports for external packages (only message classes, no codecs)
        for ext_pkg in sorted(self.external_packages):
            output_lines.append(f"import pybag.ros2.{self.distro}.{ext_pkg} as {ext_pkg}")

        # Only add TYPE_CHECKING block if generating codecs
        if self.generate_codecs:
            output_lines.append("")
            output_lines.append("if TYPE_CHECKING:")
            output_lines.append("    from typing_extensions import Self")
            output_lines.append("    from pybag.encoding.cdr import CdrDecoder, CdrEncoder")

        # Generate dataclasses (with or without __decode__ and __encode__ methods)
        for msg in messages:
            output_lines.extend(self.generate_dataclass(msg, all_messages))

        output_lines.append("")
        return "\n".join(output_lines)


# -----------------------------------------------------------------------------
# Default package URLs
# -----------------------------------------------------------------------------

# Package URL templates - {distro} will be replaced with the actual distro name
DEFAULT_PACKAGE_TEMPLATES = [
    # Core interfaces (rcl_interfaces repo)
    "https://github.com/ros2/rcl_interfaces/tree/{distro}/builtin_interfaces",
    # Common interfaces (common_interfaces repo)
    "https://github.com/ros2/common_interfaces/tree/{distro}/std_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/geometry_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/sensor_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/nav_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/diagnostic_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/trajectory_msgs",
    "https://github.com/ros2/common_interfaces/tree/{distro}/visualization_msgs",
    # TF2 messages (geometry2 repo)
    "https://github.com/ros2/geometry2/tree/{distro}/tf2_msgs",
]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def generate_package(
    url: str,
    distro: str,
    output_dir: Path | None,
    generate_codecs: bool,
) -> int:
    """Generate Python module for a single package.

    Returns 0 on success, 1 on failure.
    """
    try:
        org, repo, branch, package = parse_github_url(url)
        print(f"Parsing GitHub URL:")
        print(f"  Organization: {org}")
        print(f"  Repository: {repo}")
        print(f"  Branch: {branch}")
        print(f"  Package: {package}")
        print()

        # Fetch .msg files
        print("Fetching .msg files...")
        msg_files = fetch_msg_files(org, repo, branch, package)

        if not msg_files:
            print("No .msg files found!")
            return 1

        print(f"Found {len(msg_files)} .msg files")
        print()

        # Parse all messages
        print("Parsing messages...")
        messages: dict[str, MessageDef] = {}
        for msg_name, content in msg_files.items():
            print(f"  Parsing {msg_name}.msg...")
            try:
                msg = parse_msg_file(content, package, msg_name)
                messages[msg_name] = msg
            except Exception as e:
                print(f"    Error: {e}")
                return 1

        print()

        # Collect external dependencies
        print("Analyzing dependencies...")
        external_packages: set[str] = set()
        for msg in messages.values():
            for pkg, _ in get_dependencies(msg):
                if pkg != package:
                    external_packages.add(pkg)

        if external_packages:
            print(f"  External packages: {', '.join(sorted(external_packages))}")
        else:
            print("  No external dependencies")
        print()

        # Topologically sort messages
        print("Sorting messages by dependencies...")
        sorted_messages = topological_sort(messages, package)
        print(f"  Order: {', '.join(m.name for m in sorted_messages)}")
        print()

        # Generate code
        print(f"Generating Python code (codecs: {'enabled' if generate_codecs else 'disabled'})...")
        generator = CodeGenerator(
            package, distro, external_packages, generate_codecs=generate_codecs
        )
        code = generator.generate_module(sorted_messages, url)

        # Write output
        if output_dir is None:
            script_dir = Path(__file__).parent.parent
            output_dir = script_dir / "src" / "pybag" / "ros2" / distro

        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{package}.py"

        print(f"Writing to {output_file}...")
        output_file.write_text(code)

        print()
        print("Done!")
        return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


def create_init_files(distro: str, output_dir: Path | None) -> None:
    """Create __init__.py files for the ros2 package structure."""
    if output_dir is None:
        script_dir = Path(__file__).parent.parent
        ros2_dir = script_dir / "src" / "pybag" / "ros2"
        distro_dir = ros2_dir / distro
    else:
        ros2_dir = output_dir.parent
        distro_dir = output_dir

    # Create ros2/__init__.py
    ros2_init = ros2_dir / "__init__.py"
    if not ros2_init.exists():
        ros2_init.write_text('"""ROS2 message definitions."""\n')
        print(f"Created {ros2_init}")

    # Create ros2/{distro}/__init__.py
    distro_init = distro_dir / "__init__.py"
    if not distro_init.exists():
        distro_init.write_text(f'"""ROS2 {distro} message definitions."""\n')
        print(f"Created {distro_init}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Python message modules from ROS2 .msg files on GitHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all default packages for humble (default)
  uv run scripts/generate_messages.py

  # Generate all default packages for jazzy
  uv run scripts/generate_messages.py --distro jazzy

  # Generate a specific package
  uv run scripts/generate_messages.py https://github.com/ros2/common_interfaces/tree/humble/geometry_msgs

  # Generate with codecs enabled
  uv run scripts/generate_messages.py --codecs
""",
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=None,
        help="GitHub URL to the package. If not provided, generates all default packages.",
    )
    parser.add_argument(
        "--distro",
        default="humble",
        help="ROS2 distribution name (default: humble)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (default: src/pybag/ros2/{distro}/)",
    )
    parser.add_argument(
        "--no-codecs",
        action="store_true",
        help="Generate dataclasses without __encode__ and __decode__ methods (default)",
    )
    parser.add_argument(
        "--codecs",
        action="store_true",
        help="Generate dataclasses with __encode__ and __decode__ methods",
    )

    args = parser.parse_args()

    # Determine if codecs should be generated
    # --codecs explicitly enables, --no-codecs explicitly disables
    # Default is no codecs (for smaller generated files)
    generate_codecs = args.codecs and not args.no_codecs

    if args.url:
        # Generate a single package from the provided URL
        result = generate_package(args.url, args.distro, args.output_dir, generate_codecs)
        if result == 0:
            # Ensure __init__.py files exist for the package to be importable
            create_init_files(args.distro, args.output_dir)
        return result
    else:
        # Generate all default packages
        print(f"Generating all default ROS2 message packages for distro: {args.distro}")
        print(f"Codecs: {'enabled' if generate_codecs else 'disabled'}")
        print()

        failed = []
        for template in DEFAULT_PACKAGE_TEMPLATES:
            url = template.format(distro=args.distro)
            print("=" * 60)
            print(f"Generating: {url}")
            print("=" * 60)

            result = generate_package(url, args.distro, args.output_dir, generate_codecs)
            if result != 0:
                failed.append(url)

            print()

        # Create __init__.py files
        print("Creating __init__.py files...")
        create_init_files(args.distro, args.output_dir)
        print()

        print("=" * 60)
        if failed:
            print(f"Failed to generate {len(failed)} package(s):")
            for url in failed:
                print(f"  - {url}")
            return 1
        else:
            print("All packages generated successfully!")
            if args.output_dir:
                print(f"Output directory: {args.output_dir}")
            else:
                print(f"Output directory: src/pybag/ros2/{args.distro}/")
        print("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
