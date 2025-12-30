"""TypeStore for resolving ROS message schemas.

The TypeStore provides a unified interface for finding ROS message schemas,
supporting both user-provided .msg files and pybag's built-in message definitions.
"""
from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pybag.schema.ros1msg import Ros1MsgSchemaEncoder
from pybag.schema.ros2msg import Ros2MsgSchemaEncoder
from pybag.types import SchemaText

if TYPE_CHECKING:
    from pybag.schema import SchemaFieldType

__all__ = ['TypeStore', 'TypeStoreError']


class TypeStoreError(Exception):
    """Exception raised for TypeStore errors."""
    pass


@dataclass(slots=True)
class _MsgDefinition:
    """Represents a parsed .msg file definition (internal use only)."""
    name: str  # Full message name (e.g., 'std_msgs/msg/String')
    text: str  # Raw .msg file content
    path: Path | None  # Path to the .msg file (None for built-in)


class TypeStore:
    """A store for resolving ROS message schemas.

    The TypeStore searches for message definitions in the following order:
    1. User-provided paths (added via add_path())
    2. pybag's built-in message definitions

    Args:
        encoding: The schema encoding format ('ros1msg' or 'ros2msg').
        distro: The ROS distribution to use for built-in messages.
                For ROS1, this is ignored (always uses 'noetic').
                For ROS2, defaults to 'humble'.

    Example:
        type_store = TypeStore(encoding='ros2msg', distro='humble')
        type_store.add_path('/path/to/custom_msgs')
        schema = type_store.find('std_msgs/msg/String')
    """

    # ROS primitive types (used for both ROS1 and ROS2)
    _PRIMITIVE_TYPES = {
        'bool', 'byte', 'char',
        'int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64',
        'float32', 'float64',
        'string', 'wstring',
    }

    # ROS1-specific primitive types
    _ROS1_PRIMITIVE_TYPES = {'time', 'duration'}

    # Valid distributions for each encoding (all historical distros)
    # ROS1: https://wiki.ros.org/Distributions
    _ROS1_DISTROS = {
        'boxturtle', 'cturtle', 'diamondback', 'electric', 'fuerte',
        'groovy', 'hydro', 'indigo', 'jade', 'kinetic', 'lunar',
        'melodic', 'noetic',
    }
    # ROS2: https://docs.ros.org/en/rolling/Releases.html
    _ROS2_DISTROS = {
        'ardent', 'bouncy', 'crystal', 'dashing', 'eloquent',
        'foxy', 'galactic', 'humble', 'iron', 'jazzy', 'kilted', 'rolling',
    }

    def __init__(
        self,
        encoding: Literal['ros1msg', 'ros2msg'] = 'ros2msg',
        distro: str = 'humble',
    ) -> None:
        """Initialize a TypeStore with the specified encoding and distribution."""
        if encoding not in ('ros1msg', 'ros2msg'):
            raise TypeStoreError(f"Unknown encoding: {encoding}")

        # Validate distro is compatible with encoding
        if encoding == 'ros1msg' and distro not in self._ROS1_DISTROS:
            raise TypeStoreError(
                f"Invalid distro '{distro}' for ros1msg encoding. "
                f"Valid options: {sorted(self._ROS1_DISTROS)}"
            )
        if encoding == 'ros2msg' and distro not in self._ROS2_DISTROS:
            raise TypeStoreError(
                f"Invalid distro '{distro}' for ros2msg encoding. "
                f"Valid options: {sorted(self._ROS2_DISTROS)}"
            )

        self._encoding = encoding
        self._distro = distro
        self._user_messages: dict[str, _MsgDefinition] = {}
        self._user_paths: list[Path] = []

    @property
    def encoding(self) -> str:
        """The schema encoding format ('ros1msg' or 'ros2msg')."""
        return self._encoding

    @property
    def distro(self) -> str:
        """The ROS distribution used for built-in messages."""
        return self._distro

    def add_path(self, path: str | Path) -> None:
        """Add a path containing .msg files to the store.

        The path can be:
        1. A folder with .msg files (folder name = package name)
        2. A folder with a 'msg' subfolder containing .msg files
        3. A folder containing subfolders matching patterns 1 or 2

        Args:
            path: Path to a directory containing message definitions.

        Raises:
            TypeStoreError: If the path doesn't exist or is not a directory.
        """
        path = Path(path)

        if not path.exists():
            raise TypeStoreError(f"Path does not exist: {path}")
        if not path.is_dir():
            raise TypeStoreError(f"Path is not a directory: {path}")

        self._scan_path(path)
        self._user_paths.append(path)

    def _scan_path(self, path: Path) -> None:
        """Scan a path for .msg files and add them to the store."""
        # Pattern 1: folder with .msg files, folder name is package name
        if msg_files := list(path.glob('*.msg')):
            package_name = path.name
            for msg_file in msg_files:
                self._add_msg_file(package_name, msg_file)
            return

        # Pattern 2: folder with msg subfolder, folder name is package name
        msg_subfolder = path / 'msg'
        if msg_subfolder.is_dir():
            if msg_files := list(msg_subfolder.glob('*.msg')):
                package_name = path.name
                for msg_file in msg_files:
                    self._add_msg_file(package_name, msg_file)
                return

        # Pattern 3: folder containing subfolders matching patterns 1 or 2
        for subdir in path.iterdir():
            if subdir.is_dir():
                self._scan_path(subdir)

    def _add_msg_file(self, package_name: str, msg_file: Path) -> None:
        """Add a single .msg file to the store.

        If a message with the same name already exists, it will be overwritten.
        This allows later add_path() calls to override earlier ones.
        """
        msg_name = msg_file.stem  # Remove .msg extension
        # Use ROS2 naming convention internally: package/msg/MessageName
        full_name = f"{package_name}/msg/{msg_name}"
        text = msg_file.read_text()
        self._user_messages[full_name] = _MsgDefinition(name=full_name, text=text, path=msg_file)

    def _normalize_name(self, name: str) -> str:
        """Normalize a message name to internal format (ROS2 style).

        Args:
            name: Message name (e.g., 'std_msgs/String' or 'std_msgs/msg/String')

        Returns:
            Normalized name in ROS2 format (e.g., 'std_msgs/msg/String')
        """
        parts = name.split('/')
        if len(parts) == 2:
            # ROS1 style: package/MessageName -> package/msg/MessageName
            return f"{parts[0]}/msg/{parts[1]}"
        elif len(parts) == 3 and parts[1] == 'msg':
            # Already ROS2 style
            return name
        else:
            raise TypeStoreError(f"Invalid message name format: {name}")

    def _to_output_name(self, name: str) -> str:
        """Convert internal name to output format based on encoding.

        Args:
            name: Internal name (ROS2 format: package/msg/MessageName)

        Returns:
            Name in the appropriate format for the encoding.
        """
        if self._encoding == 'ros1msg':
            # ROS1: package/MessageName
            parts = name.split('/')
            return f"{parts[0]}/{parts[2]}"
        else:
            # ROS2: package/msg/MessageName
            return name

    def _to_msg_prefix_name(self, name: str) -> str:
        """Convert internal name to MSG: prefix format (always short format).

        The MSG: prefix in schema text always uses the short format
        (package/MessageName) regardless of encoding, following the
        convention used by the ROS schema encoders.

        Args:
            name: Internal name (ROS2 format: package/msg/MessageName)

        Returns:
            Name in short format (package/MessageName)
        """
        parts = name.split('/')
        if len(parts) == 3 and parts[1] == 'msg':
            return f"{parts[0]}/{parts[2]}"
        return name

    def find(self, name: str) -> SchemaText:
        """Find a message schema by name.

        Searches in user-provided paths first, then falls back to pybag's
        built-in message definitions.

        Args:
            name: The message type name (e.g., 'std_msgs/msg/String' or
                  'std_msgs/String').

        Returns:
            A SchemaText object containing the message schema.

        Raises:
            TypeStoreError: If the message cannot be found.
        """
        normalized_name = self._normalize_name(name)

        # Try user-provided messages first
        if normalized_name in self._user_messages:
            return self._build_schema_text(normalized_name)

        # Fall back to built-in messages
        return self._find_builtin(normalized_name)

    def _build_schema_text(self, name: str) -> SchemaText:
        """Build SchemaText from a user-provided message definition.

        This includes resolving dependencies and building the full schema text
        with embedded sub-message definitions.
        """
        msg_def = self._user_messages[name]
        output_name = self._to_output_name(name)

        # Parse and resolve dependencies
        main_text, dependencies = self._resolve_dependencies(name)

        # Build the full schema text
        schema_parts = [main_text.rstrip()]

        # Track which dependencies we've already emitted to avoid duplicates
        emitted: set[str] = set()

        for dep_name in dependencies:
            if dep_name in emitted:
                continue
            emitted.add(dep_name)

            # MSG: prefix always uses short format (package/MessageName)
            dep_msg_name = self._to_msg_prefix_name(dep_name)
            if dep_name in self._user_messages:
                dep_text = self._clean_msg_text(self._user_messages[dep_name].text)
                schema_parts.append('=' * 80)
                schema_parts.append(f'MSG: {dep_msg_name}')
                schema_parts.append(dep_text.rstrip())
            else:
                # Dependency is a built-in type - get text and sub-dependencies
                dep_text, sub_deps = self._get_builtin_msg_text_with_deps(dep_name)
                schema_parts.append('=' * 80)
                schema_parts.append(f'MSG: {dep_msg_name}')
                schema_parts.append(dep_text.rstrip())

                # Also emit sub-dependencies of the built-in type
                for sub_dep_name, sub_dep_text in sub_deps.items():
                    if sub_dep_name in emitted:
                        continue
                    emitted.add(sub_dep_name)
                    sub_dep_msg_name = self._to_msg_prefix_name(sub_dep_name)
                    schema_parts.append('=' * 80)
                    schema_parts.append(f'MSG: {sub_dep_msg_name}')
                    schema_parts.append(sub_dep_text.rstrip())

        full_text = '\n'.join(schema_parts) + '\n'
        return SchemaText(name=output_name, text=full_text)

    def _clean_msg_text(self, text: str) -> str:
        """Clean a .msg file text by removing comments and normalizing whitespace."""
        lines = []
        for line in text.split('\n'):
            # Remove inline comments (but preserve string literals)
            cleaned = self._remove_inline_comment(line)
            if cleaned:
                lines.append(cleaned)
        return '\n'.join(lines)

    def _remove_inline_comment(self, line: str) -> str:
        """Remove inline comments from a line, preserving string literals."""
        in_single = False
        in_double = False
        for i, ch in enumerate(line):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == '#' and not in_single and not in_double:
                return line[:i].rstrip()
        return line.rstrip()

    def _resolve_dependencies(self, name: str) -> tuple[str, list[str]]:
        """Resolve all dependencies for a message.

        Returns:
            Tuple of (transformed main message text with qualified types,
                     list of dependency names in order)

        Raises:
            TypeStoreError: If circular dependencies are detected.
        """
        msg_def = self._user_messages[name]
        main_text = self._clean_msg_text(msg_def.text)
        package_name = name.split('/')[0]

        # Find all complex types referenced
        dependencies: list[str] = []
        seen: set[str] = {name}
        # Track recursion stack for circular dependency detection
        recursion_stack: set[str] = {name}

        transformed_text = self._collect_dependencies(
            main_text, package_name, dependencies, seen, recursion_stack
        )

        return transformed_text, dependencies

    def _collect_dependencies(
        self,
        msg_text: str,
        package_name: str,
        dependencies: list[str],
        seen: set[str],
        recursion_stack: set[str],
    ) -> str:
        """Recursively collect all dependencies from a message text.

        Returns:
            The transformed message text with fully qualified type names.

        Raises:
            TypeStoreError: If circular dependencies or ROS1 types in ROS2 encoding.
        """
        transformed_lines = []

        for line in msg_text.split('\n'):
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                transformed_lines.append(line)
                continue

            # Parse field: TYPE NAME [=VALUE]
            match = re.match(r'(\S+)(\s+)(\S+)(.*)', stripped)
            if not match:
                transformed_lines.append(line)
                continue

            field_type, space, field_name, rest = match.groups()

            # Strip array notation to get bare type
            array_suffix = ''
            if array_match := re.search(r'(\[.*\])$', field_type):
                array_suffix = array_match.group(1)
                bare_type = field_type[:array_match.start()]
            else:
                bare_type = field_type

            # Strip string length constraints
            bare_type_no_constraint = re.sub(r'<=\d+$', '', bare_type)

            # Check if it's a primitive type
            if bare_type_no_constraint in self._PRIMITIVE_TYPES:
                transformed_lines.append(line)
                continue
            if bare_type_no_constraint in self._ROS1_PRIMITIVE_TYPES:
                if self._encoding == 'ros1msg':
                    transformed_lines.append(line)
                    continue
                else:
                    raise TypeStoreError(
                        f"ROS1 primitive type '{bare_type_no_constraint}' cannot be used with ros2msg encoding. "
                        f"Use 'builtin_interfaces/Time' or 'builtin_interfaces/Duration' instead."
                    )

            # Resolve the full type name
            if bare_type_no_constraint == 'Header':
                full_type = 'std_msgs/msg/Header'
            elif '/' not in bare_type_no_constraint:
                full_type = f'{package_name}/msg/{bare_type_no_constraint}'
            else:
                # Could be ROS1 style (pkg/Type) or ROS2 style (pkg/msg/Type)
                full_type = self._normalize_name(bare_type_no_constraint)

            # Check for circular dependency (type is currently being processed)
            if full_type in recursion_stack:
                raise TypeStoreError(
                    f"Recursion detected: circular dependency involving '{full_type}'"
                )

            # Build qualified type name for output (short format: pkg/Type)
            qualified_type = self._to_msg_prefix_name(full_type) + array_suffix
            transformed_lines.append(f'{qualified_type}{space}{field_name}{rest}')

            if full_type not in seen:
                seen.add(full_type)
                dependencies.append(full_type)

                # Recursively resolve dependencies
                if full_type in self._user_messages:
                    dep_text = self._clean_msg_text(self._user_messages[full_type].text)
                    dep_package = full_type.split('/')[0]
                    recursion_stack.add(full_type)
                    self._collect_dependencies(
                        dep_text, dep_package, dependencies, seen, recursion_stack
                    )
                    recursion_stack.remove(full_type)

        return '\n'.join(transformed_lines)

    def _get_builtin_msg_text_with_deps(self, name: str) -> tuple[str, dict[str, str]]:
        """Get the .msg text for a built-in message type and its dependencies.

        Returns:
            Tuple of (main message text, dict of dependency name -> text)
        """
        from pybag.schema import SchemaConstant, SchemaField
        from pybag.schema.ros1msg import Ros1MsgSchemaEncoder
        from pybag.schema.ros2msg import Ros2MsgSchemaEncoder

        msg_class = self._find_builtin_class(name)
        if msg_class is None:
            raise TypeStoreError(f"Built-in message not found: {name}")

        if self._encoding == 'ros1msg':
            encoder = Ros1MsgSchemaEncoder()
        else:
            encoder = Ros2MsgSchemaEncoder()

        # Get main schema and sub-schemas
        schema, sub_schemas = encoder.parse_schema(msg_class)

        # Convert main schema to text
        main_lines = []
        for field_name, field in schema.fields.items():
            if isinstance(field, SchemaConstant):
                type_str = self._schema_type_to_str(field.type)
                main_lines.append(f'{type_str} {field_name.upper()}={field.value}')
            elif isinstance(field, SchemaField):
                type_str = self._schema_type_to_str(field.type)
                main_lines.append(f'{type_str} {field_name}')

        main_text = '\n'.join(main_lines)

        # Convert sub-schemas to text
        sub_texts: dict[str, str] = {}
        for sub_name, sub_schema in sub_schemas.items():
            sub_lines = []
            for field_name, field in sub_schema.fields.items():
                if isinstance(field, SchemaConstant):
                    type_str = self._schema_type_to_str(field.type)
                    sub_lines.append(f'{type_str} {field_name.upper()}={field.value}')
                elif isinstance(field, SchemaField):
                    type_str = self._schema_type_to_str(field.type)
                    sub_lines.append(f'{type_str} {field_name}')
            sub_texts[sub_name] = '\n'.join(sub_lines)

        return main_text, sub_texts

    def _schema_type_to_str(self, field_type: 'SchemaFieldType') -> str:
        """Convert a SchemaFieldType to a string representation.

        Note: The output always uses short format (package/Type) for complex
        types, matching the MSG: prefix convention used by ROS schema encoders.
        """
        from pybag.schema import Array, Complex, Primitive, Sequence, String

        if isinstance(field_type, Primitive):
            return field_type.type
        if isinstance(field_type, String):
            if field_type.max_length is not None:
                return f'{field_type.type}<={field_type.max_length}'
            return field_type.type
        if isinstance(field_type, Array):
            sub_type = self._schema_type_to_str(field_type.type)
            if field_type.is_bounded:
                return f'{sub_type}[<={field_type.length}]'
            return f'{sub_type}[{field_type.length}]'
        if isinstance(field_type, Sequence):
            sub_type = self._schema_type_to_str(field_type.type)
            return f'{sub_type}[]'
        if isinstance(field_type, Complex):
            # Always use short format (package/Type) for complex types
            return field_type.type.replace('/msg/', '/')
        raise TypeStoreError(f'Unknown field type: {field_type}')

    def _find_builtin_class(self, name: str) -> type | None:
        """Find a built-in message class by name."""
        parts = name.split('/')
        if len(parts) != 3 or parts[1] != 'msg':
            raise TypeStoreError(f"Invalid message name format: {name}")
        package_name, _, msg_name = parts

        if self._encoding == 'ros1msg' and self._distro == 'noetic':
            try:
                module = importlib.import_module(f'pybag.ros1.noetic.{package_name}')
                return getattr(module, msg_name, None)
            except (ImportError, ModuleNotFoundError):
                return None
        elif self._encoding == 'ros2msg':
            try:
                module = importlib.import_module(f'pybag.ros2.{self._distro}.{package_name}')
                return getattr(module, msg_name, None)
            except (ImportError, ModuleNotFoundError):
                return None
        return None

    def _find_builtin(self, name: str) -> SchemaText:
        """Find a built-in message and return its SchemaText."""
        msg_class = self._find_builtin_class(name)
        if msg_class is None:
            raise TypeStoreError(
                f"Message not found: {name}. "
                f"Not found in user paths or pybag built-in messages."
            )

        output_name = self._to_output_name(name)

        if self._encoding == 'ros1msg':
            encoder = Ros1MsgSchemaEncoder()
        else:
            encoder = Ros2MsgSchemaEncoder()

        schema_bytes = encoder.encode(msg_class)
        return SchemaText(name=output_name, text=schema_bytes.decode('utf-8'))

    def list_messages(self) -> list[str]:
        """List all user-provided message names.

        Returns:
            List of message names in ROS2 format (package/msg/MessageName).
        """
        return list(self._user_messages.keys())

    def __contains__(self, name: str) -> bool:
        """Check if a message exists in user-provided messages."""
        try:
            normalized = self._normalize_name(name)
            return normalized in self._user_messages
        except TypeStoreError:
            return False
