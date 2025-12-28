#!/usr/bin/env python3
"""Generate Python message modules from ROS1 .msg files on GitHub.

This script fetches .msg files from ROS1 GitHub repositories and generates
Python dataclasses compatible with ROS1 bag files.

Key differences from ROS2:
- Message names use 'package/MessageName' format (no '/msg/')
- time and duration are primitive types (not messages)
- Header includes a 'seq' field

Usage:
    # Generate all default packages for noetic
    uv run scripts/generate_ros1_messages.py

    # Generate a specific package
    uv run scripts/generate_ros1_messages.py \\
        https://github.com/ros/std_msgs/tree/noetic-devel/msg \\
        --distro noetic
"""
from __future__ import annotations

import argparse
import ast
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# All primitive types (ROS1 includes time and duration as primitives)
_PRIMITIVE_TYPES = {
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "float32", "float64", "bool", "byte", "char",
    "time", "duration",  # ROS1-specific primitives
}
_STRING_TYPES = {"string"}

_TAB = "    "


# -----------------------------------------------------------------------------
# Schema representation
# -----------------------------------------------------------------------------

@dataclass
class Primitive:
    """A primitive type like int32, float64, bool, time, duration, etc."""
    type: str


@dataclass
class String:
    """A string type."""
    type: str = "string"


@dataclass
class Array:
    """A fixed-size array: type[N]."""
    element: Any  # Primitive, String, or Complex
    length: int


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
    """A message field: TYPE name."""
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
    """Parse a GitHub URL to extract org, repo, branch, and msg directory.

    Examples:
        https://github.com/ros/std_msgs/tree/noetic-devel/msg
        -> ('ros', 'std_msgs', 'noetic-devel', 'msg')

        https://github.com/ros/common_msgs/tree/noetic-devel/geometry_msgs/msg
        -> ('ros', 'common_msgs', 'noetic-devel', 'geometry_msgs/msg')
    """
    # Pattern for tree URLs
    pattern = r"https?://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.+?)/?$"
    match = re.match(pattern, url)
    if not match:
        raise ValueError(
            f"Invalid GitHub URL format: {url}\n"
            "Expected: https://github.com/org/repo/tree/branch/path"
        )
    return match.groups()  # type: ignore[return-value]


def fetch_msg_files(
    org: str, repo: str, branch: str, msg_path: str
) -> dict[str, str]:
    """Fetch all .msg files for a package using GitHub's tarball API.

    Downloads the entire repository as a tarball in a single request,
    then extracts only the .msg files for the specified path.

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
    # Normalize the path (ensure it ends with /)
    msg_path_prefix = msg_path.rstrip("/") + "/"

    with tarfile.open(fileobj=io.BytesIO(tarball_data), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue

            # Tarball has a root directory like "repo-branch/"
            # We need to strip that and check if the path matches
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
    # Handle array and sequence types: type[N], type[]
    if array_match := re.match(r"(.+)\[(.*)\]$", type_str):
        element_str, length_spec = array_match.groups()
        element_type = parse_type(element_str, current_package)

        if length_spec == "":
            return Sequence(element_type)
        return Array(element_type, int(length_spec))

    # Handle string type
    if type_str == "string":
        return String("string")

    # Handle primitive types (including ROS1-specific time/duration)
    if type_str in _PRIMITIVE_TYPES:
        return Primitive(type_str)

    # Handle complex types
    if type_str == "Header":
        # Special case: Header always means std_msgs/Header
        return Complex("std_msgs", "Header")

    if "/" in type_str:
        # Fully qualified: package/Type
        parts = type_str.split("/")
        if len(parts) == 2:
            return Complex(parts[0], parts[1])
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
            type_obj = parse_type(type_str, package)
            value = parse_default_value(type_obj, raw_default.strip())
            msg.constants.append(Constant(type_obj, name_str, value))
        else:
            # Regular field
            type_obj = parse_type(type_str, package)
            msg.fields.append(Field(type_obj, name_str))

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

    # Compute in-degrees
    real_in_degree = {name: 0 for name in messages}
    for name in messages:
        for dep in graph[name]:
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
        print("  Warning: Circular dependency detected, using original order")
        return list(messages.values())

    return result


# -----------------------------------------------------------------------------
# Code generation
# -----------------------------------------------------------------------------

class Ros1CodeGenerator:
    """Generate Python code for ROS1 message definitions."""

    def __init__(
        self,
        package: str,
        distro: str,
        external_packages: set[str],
    ):
        self.package = package
        self.distro = distro
        self.external_packages = external_packages

    def type_annotation(self, type_obj: Any) -> str:
        """Generate type annotation string for a field type."""
        if isinstance(type_obj, Primitive):
            # ROS 1 specific types go under ros1 namespace
            if type_obj.type in ("time", "duration", "char"):
                return f"t.ros1.{type_obj.type}"
            return f"t.{type_obj.type}"

        if isinstance(type_obj, String):
            return "t.string"

        if isinstance(type_obj, Array):
            elem = self.type_annotation(type_obj.element)
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
        return repr(value)

    def generate_dataclass(self, msg: MessageDef) -> list[str]:
        """Generate dataclass definition for a message."""
        lines = []
        lines.append("")
        lines.append("")
        lines.append("@dataclass(kw_only=True)")
        lines.append(f"class {msg.name}:")
        # ROS1 message names don't have '/msg/' in them
        lines.append(f"    __msg_name__ = '{msg.package}/{msg.name}'")
        lines.append("")

        # Add constants first
        for const in msg.constants:
            type_ann = self.type_annotation(const.type)
            value_repr = self.default_repr(const.value)
            lines.append(f"    {const.name}: t.Constant[{type_ann}] = {value_repr}")

        # Add fields
        for fld in msg.fields:
            type_ann = self.type_annotation(fld.type)
            lines.append(f"    {fld.name}: {type_ann}")

        # Handle empty message
        if not msg.fields and not msg.constants:
            lines.append("    pass")

        return lines

    def generate_module(
        self,
        messages: list[MessageDef],
        source_url: str,
    ) -> str:
        """Generate complete Python module for all messages."""
        output_lines: list[str] = []

        # Module docstring
        output_lines.append(f'"""ROS1 {self.package} message definitions for {self.distro}.')
        output_lines.append("")
        output_lines.append("Auto-generated by scripts/generate_ros1_messages.py")
        output_lines.append(f"Source: {source_url}")
        output_lines.append('"""')
        output_lines.append("from __future__ import annotations")
        output_lines.append("")
        output_lines.append("from dataclasses import dataclass")
        output_lines.append("from typing import Literal")
        output_lines.append("")
        output_lines.append("import pybag.types as t")

        # Add imports for external packages
        for ext_pkg in sorted(self.external_packages):
            output_lines.append(f"import pybag.ros1.{self.distro}.{ext_pkg} as {ext_pkg}")

        # Generate dataclasses
        for msg in messages:
            output_lines.extend(self.generate_dataclass(msg))

        output_lines.append("")
        return "\n".join(output_lines)


# -----------------------------------------------------------------------------
# Package configurations for ROS1
# -----------------------------------------------------------------------------

@dataclass
class PackageConfig:
    """Configuration for a ROS1 package."""
    url: str
    package_name: str


# ROS1 package URL templates - {distro} will be replaced
# Note: ROS1 repos have different structures than ROS2
DEFAULT_PACKAGES = [
    # std_msgs - standalone repo
    PackageConfig(
        url="https://github.com/ros/std_msgs/tree/{distro}-devel/msg",
        package_name="std_msgs",
    ),
    # common_msgs - contains multiple packages
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/geometry_msgs/msg",
        package_name="geometry_msgs",
    ),
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/sensor_msgs/msg",
        package_name="sensor_msgs",
    ),
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/nav_msgs/msg",
        package_name="nav_msgs",
    ),
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/diagnostic_msgs/msg",
        package_name="diagnostic_msgs",
    ),
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/trajectory_msgs/msg",
        package_name="trajectory_msgs",
    ),
    PackageConfig(
        url="https://github.com/ros/common_msgs/tree/{distro}-devel/visualization_msgs/msg",
        package_name="visualization_msgs",
    ),
    # geometry2 repo for tf2_msgs
    PackageConfig(
        url="https://github.com/ros/geometry2/tree/{distro}-devel/tf2_msgs/msg",
        package_name="tf2_msgs",
    ),
]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def generate_package(
    url: str,
    package_name: str,
    distro: str,
    output_dir: Path | None,
) -> int:
    """Generate Python module for a single package.

    Returns 0 on success, 1 on failure.
    """
    try:
        org, repo, branch, msg_path = parse_github_url(url)
        print(f"Parsing GitHub URL:")
        print(f"  Organization: {org}")
        print(f"  Repository: {repo}")
        print(f"  Branch: {branch}")
        print(f"  Path: {msg_path}")
        print(f"  Package: {package_name}")
        print()

        # Fetch .msg files
        print("Fetching .msg files...")
        msg_files = fetch_msg_files(org, repo, branch, msg_path)

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
                msg = parse_msg_file(content, package_name, msg_name)
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
                if pkg != package_name:
                    external_packages.add(pkg)

        if external_packages:
            print(f"  External packages: {', '.join(sorted(external_packages))}")
        else:
            print("  No external dependencies")
        print()

        # Topologically sort messages
        print("Sorting messages by dependencies...")
        sorted_messages = topological_sort(messages, package_name)
        print(f"  Order: {', '.join(m.name for m in sorted_messages)}")
        print()

        # Generate code
        print("Generating Python code...")
        generator = Ros1CodeGenerator(package_name, distro, external_packages)
        code = generator.generate_module(sorted_messages, url)

        # Write output
        if output_dir is None:
            script_dir = Path(__file__).parent.parent
            output_dir = script_dir / "src" / "pybag" / "ros1" / distro

        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{package_name}.py"

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
    """Create __init__.py files for the ros1 package structure."""
    if output_dir is None:
        script_dir = Path(__file__).parent.parent
        ros1_dir = script_dir / "src" / "pybag" / "ros1"
        distro_dir = ros1_dir / distro
    else:
        ros1_dir = output_dir.parent
        distro_dir = output_dir

    # Create ros1/__init__.py
    ros1_init = ros1_dir / "__init__.py"
    if not ros1_init.exists():
        ros1_init.write_text('"""ROS1 message definitions."""\n')
        print(f"Created {ros1_init}")

    # Create ros1/{distro}/__init__.py
    distro_init = distro_dir / "__init__.py"
    if not distro_init.exists():
        distro_init.write_text(f'"""ROS1 {distro} message definitions."""\n')
        print(f"Created {distro_init}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Python message modules from ROS1 .msg files on GitHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all default packages for noetic (default)
  uv run scripts/generate_ros1_messages.py

  # Generate a specific package
  uv run scripts/generate_ros1_messages.py \\
      https://github.com/ros/std_msgs/tree/noetic-devel/msg \\
      --package std_msgs
""",
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=None,
        help="GitHub URL to the msg directory. If not provided, generates all default packages.",
    )
    parser.add_argument(
        "--package",
        default=None,
        help="Package name (required if URL is provided)",
    )
    parser.add_argument(
        "--distro",
        default="noetic",
        help="ROS1 distribution name (default: noetic)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (default: src/pybag/ros1/{distro}/)",
    )

    args = parser.parse_args()

    if args.url:
        if not args.package:
            print("Error: --package is required when URL is provided")
            return 1
        result = generate_package(args.url, args.package, args.distro, args.output_dir)
        if result == 0:
            # Ensure __init__.py files exist for the package to be importable
            create_init_files(args.distro, args.output_dir)
        return result
    else:
        # Generate all default packages
        print(f"Generating all default ROS1 message packages for distro: {args.distro}")
        print()

        failed = []
        for pkg_config in DEFAULT_PACKAGES:
            url = pkg_config.url.format(distro=args.distro)
            print("=" * 60)
            print(f"Generating: {pkg_config.package_name}")
            print(f"URL: {url}")
            print("=" * 60)

            result = generate_package(url, pkg_config.package_name, args.distro, args.output_dir)
            if result != 0:
                failed.append(pkg_config.package_name)

            print()

        # Create __init__.py files
        print("Creating __init__.py files...")
        create_init_files(args.distro, args.output_dir)
        print()

        print("=" * 60)
        if failed:
            print(f"Failed to generate {len(failed)} package(s):")
            for name in failed:
                print(f"  - {name}")
            return 1
        else:
            print("All packages generated successfully!")
            if args.output_dir:
                print(f"Output directory: {args.output_dir}")
            else:
                print(f"Output directory: src/pybag/ros1/{args.distro}/")
        print("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
