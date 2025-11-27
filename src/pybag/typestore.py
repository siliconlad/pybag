"""Type store for managing ROS2 message types dynamically."""
from __future__ import annotations

import re
from dataclasses import dataclass, field, make_dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import pybag.types as t
from pybag.schema import (
    PRIMITIVE_TYPE_MAP,
    Array,
    Complex,
    Primitive,
    Schema,
    SchemaConstant,
    SchemaField,
    SchemaFieldType,
    Sequence,
    String,
)

if TYPE_CHECKING:
    from pybag.types import Message


class Stores(Enum):
    """Available pre-configured type stores."""
    ROS2_HUMBLE = "ros2_humble"


class TypeStoreError(Exception):
    """Exception raised for errors in the type store."""
    pass


class MsgParseError(TypeStoreError):
    """Exception raised for errors parsing .msg files."""
    pass


def _sanitize_class_name(name: str) -> str:
    """Convert a message name to a valid Python class name."""
    # Extract just the message name (after the last /)
    class_name = name.split("/")[-1]
    # Replace invalid characters
    return re.sub(r"[^0-9a-zA-Z_]", "_", class_name)


def _normalize_msg_name(name: str) -> str:
    """Normalize a message name to the full format: package/msg/MessageName."""
    parts = name.split("/")
    if len(parts) == 3 and parts[1] == "msg":
        return name
    if len(parts) == 2:
        # package/MessageName -> package/msg/MessageName
        return f"{parts[0]}/msg/{parts[1]}"
    if len(parts) == 1:
        raise MsgParseError(f"Message name must include package: {name}")
    raise MsgParseError(f"Invalid message name format: {name}")


def _remove_inline_comment(line: str) -> str:
    """Remove inline comments from a line."""
    in_single = False
    in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == '#' and not in_single and not in_double:
            return line[:i].rstrip()
    return line.strip()


# Map primitive ROS2 types to pybag annotated types
_PYBAG_TYPE_MAP = {
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


class TypeStore:
    """
    A type store for managing ROS2 message types.

    The TypeStore provides a unified interface for registering and accessing
    ROS2 message types. It can be pre-loaded with distribution-specific types
    or populated dynamically from .msg definitions.

    Example usage:
        # Create a type store pre-loaded with ROS2 Humble types
        store = TypeStore.ros2_humble()

        # Register a custom type from a .msg definition
        store.register_msg("my_msgs/msg/Point3D", '''
            float64 x
            float64 y
            float64 z
        ''')

        # Access registered types
        Point3D = store.types["my_msgs/msg/Point3D"]
        point = Point3D(x=1.0, y=2.0, z=3.0)
    """

    def __init__(self) -> None:
        """Create an empty type store."""
        self._types: dict[str, type] = {}
        self._schemas: dict[str, Schema] = {}

    @property
    def types(self) -> dict[str, type]:
        """Dictionary of registered message types, keyed by message name."""
        return self._types

    @classmethod
    def ros2_humble(cls) -> "TypeStore":
        """Create a type store pre-loaded with ROS2 Humble message types."""
        store = cls()
        store._load_humble_types()
        return store

    @classmethod
    def from_store(cls, store: Stores) -> "TypeStore":
        """Create a type store from a pre-configured store enum."""
        if store == Stores.ROS2_HUMBLE:
            return cls.ros2_humble()
        raise TypeStoreError(f"Unknown store: {store}")

    def _load_humble_types(self) -> None:
        """Load all ROS2 Humble message types into the store."""
        from pybag.ros2.humble import builtin_interfaces, geometry_msgs, nav_msgs, sensor_msgs, std_msgs

        # Collect all message types from each module
        modules = [builtin_interfaces, geometry_msgs, nav_msgs, sensor_msgs, std_msgs]

        for module in modules:
            for name in dir(module):
                obj = getattr(module, name)
                if (
                    isinstance(obj, type) and
                    hasattr(obj, '__msg_name__') and
                    not name.startswith('_')
                ):
                    msg_name = obj.__msg_name__
                    self._types[msg_name] = obj

    def register(self, types: dict[str, type]) -> None:
        """
        Register multiple types at once.

        Args:
            types: Dictionary mapping message names to their type classes.
        """
        self._types.update(types)

    def register_type(self, msg_type: type) -> None:
        """
        Register a single message type.

        Args:
            msg_type: A dataclass with a __msg_name__ attribute.
        """
        if not hasattr(msg_type, '__msg_name__'):
            raise TypeStoreError("Type must have a __msg_name__ attribute")
        self._types[msg_type.__msg_name__] = msg_type

    def register_msg(
        self,
        name: str,
        msg_text: str,
        *,
        dependencies: dict[str, str] | None = None
    ) -> type:
        """
        Register a message type from a .msg definition string.

        Args:
            name: The message name (e.g., "my_msgs/msg/Point3D" or "my_msgs/Point3D").
            msg_text: The .msg file content as a string.
            dependencies: Optional dict of dependency name -> .msg text for nested types
                         that are not already registered.

        Returns:
            The generated dataclass type.

        Example:
            store.register_msg("my_msgs/msg/Point3D", '''
                float64 x
                float64 y
                float64 z
            ''')
        """
        name = _normalize_msg_name(name)

        # Parse the message definition
        schema, sub_schemas = self._parse_msg_text(name, msg_text)

        # Register dependencies first
        if dependencies:
            for dep_name, dep_text in dependencies.items():
                if dep_name not in self._types:
                    self.register_msg(dep_name, dep_text)

        # Register sub-schemas (nested types from the same .msg file)
        for sub_name, sub_schema in sub_schemas.items():
            if sub_name not in self._types:
                self._schemas[sub_name] = sub_schema
                sub_type = self._schema_to_dataclass(sub_schema)
                self._types[sub_name] = sub_type

        # Register the main schema
        self._schemas[name] = schema
        msg_type = self._schema_to_dataclass(schema)
        self._types[name] = msg_type

        return msg_type

    def register_from_file(
        self,
        file_path: str | Path,
        name: str | None = None,
        *,
        dependencies: dict[str, str] | None = None
    ) -> type:
        """
        Register a message type from a .msg file.

        Args:
            file_path: Path to the .msg file.
            name: Optional message name. If not provided, it will be inferred from
                  the file path (requires standard ROS package structure).
            dependencies: Optional dict of dependency name -> .msg text for nested types.

        Returns:
            The generated dataclass type.

        Example:
            store.register_from_file("/path/to/my_msgs/msg/Point3D.msg")
            # Or with explicit name:
            store.register_from_file("/path/to/Point3D.msg", "my_msgs/msg/Point3D")
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise TypeStoreError(f"File not found: {file_path}")

        if name is None:
            name = self._infer_msg_name_from_path(file_path)

        msg_text = file_path.read_text()
        return self.register_msg(name, msg_text, dependencies=dependencies)

    def register_from_package(
        self,
        package_path: str | Path,
        package_name: str | None = None
    ) -> dict[str, type]:
        """
        Register all message types from a ROS package's msg directory.

        Args:
            package_path: Path to the package's msg directory (containing .msg files)
                         or to the package root (must contain a msg/ subdirectory).
            package_name: Optional package name. If not provided, it will be inferred
                         from the directory structure.

        Returns:
            Dictionary of registered types.

        Example:
            store.register_from_package("/path/to/my_msgs/msg/")
            # Or:
            store.register_from_package("/path/to/my_msgs/", "my_msgs")
        """
        package_path = Path(package_path)

        if not package_path.exists():
            raise TypeStoreError(f"Directory not found: {package_path}")

        # Check if this is the msg directory or package root
        msg_dir = package_path
        if not any(package_path.glob("*.msg")):
            msg_dir = package_path / "msg"
            if not msg_dir.exists():
                raise TypeStoreError(
                    f"No .msg files found in {package_path} or {msg_dir}"
                )

        # Infer package name from directory structure
        if package_name is None:
            if msg_dir.name == "msg":
                package_name = msg_dir.parent.name
            else:
                package_name = msg_dir.name

        # Collect all .msg files
        msg_files = sorted(msg_dir.glob("*.msg"))
        if not msg_files:
            raise TypeStoreError(f"No .msg files found in {msg_dir}")

        # Parse all files first to build dependency graph
        pending: dict[str, tuple[str, str]] = {}  # name -> (msg_text, file_path)
        for msg_file in msg_files:
            msg_name = f"{package_name}/msg/{msg_file.stem}"
            msg_text = msg_file.read_text()
            pending[msg_name] = (msg_text, str(msg_file))

        # Register types, handling dependencies
        registered: dict[str, type] = {}
        max_iterations = len(pending) * 2  # Prevent infinite loops
        iterations = 0

        while pending and iterations < max_iterations:
            iterations += 1
            progress = False

            for name in list(pending.keys()):
                msg_text, file_path = pending[name]
                try:
                    msg_type = self.register_msg(name, msg_text)
                    registered[name] = msg_type
                    del pending[name]
                    progress = True
                except TypeStoreError as e:
                    # Check if it's a missing dependency error
                    if "not found" in str(e).lower() or "unknown" in str(e).lower():
                        continue  # Try again after other types are registered
                    raise

            if not progress and pending:
                # No progress made - likely circular dependency or missing external type
                missing = list(pending.keys())
                raise TypeStoreError(
                    f"Could not resolve dependencies for: {missing}. "
                    "Check for circular dependencies or missing external types."
                )

        return registered

    def _infer_msg_name_from_path(self, file_path: Path) -> str:
        """Infer the message name from a file path."""
        # Expected structure: .../package_name/msg/MessageName.msg
        parts = file_path.parts

        if len(parts) >= 3 and parts[-2] == "msg":
            package_name = parts[-3]
            msg_name = file_path.stem
            return f"{package_name}/msg/{msg_name}"

        raise TypeStoreError(
            f"Cannot infer message name from path: {file_path}. "
            "Please provide the name explicitly or use standard ROS package structure."
        )

    def _parse_msg_text(
        self,
        name: str,
        msg_text: str
    ) -> tuple[Schema, dict[str, Schema]]:
        """
        Parse a .msg definition text into a Schema.

        This is a simplified parser for standalone .msg files.
        For full MCAP schema parsing, use Ros2MsgSchemaDecoder.
        """
        package_name = name.split('/')[0]

        # Remove comments and empty lines
        lines = [_remove_inline_comment(line) for line in msg_text.split('\n')]
        lines = [line for line in lines if line]
        msg_clean = '\n'.join(lines)

        # Split along '=' delimiter (sub-message separator)
        parts = [m.strip() for m in msg_clean.split('=' * 80)]

        # Parse main message fields
        main_fields = [m.strip() for m in parts[0].split('\n') if m.strip()]
        msg_schema: dict[str, SchemaField | SchemaConstant] = {}

        for raw_field in main_fields:
            field_name, field_entry = self._parse_field(raw_field, package_name)
            msg_schema[field_name] = field_entry

        # Parse sub-messages (embedded definitions)
        sub_schemas: dict[str, Schema] = {}
        for sub_msg in parts[1:]:
            if not sub_msg.strip():
                continue
            sub_lines = sub_msg.split('\n')
            # First line should be "MSG: package/MessageName"
            header = sub_lines[0].strip()
            if not header.startswith('MSG:'):
                continue
            sub_msg_name = header[4:].strip()
            # Normalize the name
            if '/msg/' not in sub_msg_name:
                sub_parts = sub_msg_name.split('/')
                if len(sub_parts) == 2:
                    sub_msg_name = f"{sub_parts[0]}/msg/{sub_parts[1]}"

            sub_msg_fields = [m.strip() for m in sub_lines[1:] if m.strip()]
            sub_schema: dict[str, SchemaField | SchemaConstant] = {}
            sub_package = sub_msg_name.split('/')[0]

            for raw_field in sub_msg_fields:
                field_name, field_entry = self._parse_field(raw_field, sub_package)
                sub_schema[field_name] = field_entry

            sub_schemas[sub_msg_name] = Schema(sub_msg_name, sub_schema)

        return Schema(name, msg_schema), sub_schemas

    def _parse_field(
        self,
        field_str: str,
        package_name: str
    ) -> tuple[str, SchemaField | SchemaConstant]:
        """Parse a single field definition."""
        # Match: type name [= default]
        match = re.match(r'(\S+)\s+(\S+)(?:\s+(.+))?$', field_str)
        if not match:
            raise MsgParseError(f"Invalid field definition: {field_str}")

        field_raw_type, field_raw_name, raw_default = match.groups()

        if not field_raw_type:
            raise MsgParseError('Field type cannot be empty')
        if not field_raw_name:
            raise MsgParseError('Field name cannot be empty')

        # Check if this is a constant
        is_constant = '=' in field_raw_name or (raw_default and raw_default.startswith('='))

        if is_constant:
            if '=' in field_raw_name:
                field_raw_name, raw_default = field_raw_name.split('=', 1)
            else:
                raw_default = raw_default[1:].strip() if raw_default else None

            if not raw_default:
                raise MsgParseError('Constant value cannot be empty')

        # Parse the field type
        schema_type = self._parse_field_type(field_raw_type, package_name)

        # Parse default value if present
        default_value = None
        if raw_default is not None:
            default_value = self._parse_default_value(schema_type, raw_default)

        if is_constant:
            if default_value is None:
                raise MsgParseError('Constant must have a default value')
            return field_raw_name, SchemaConstant(schema_type, default_value)

        return field_raw_name, SchemaField(schema_type, default_value)

    def _parse_field_type(self, field_raw_type: str, package_name: str) -> SchemaFieldType:
        """Parse a field type string into a SchemaFieldType."""
        # Handle array and sequence types
        if array_match := re.match(r'(.*)\[(.*)\]$', field_raw_type):
            element_raw, length_spec = array_match.groups()
            element_field = self._parse_field_type(element_raw, package_name)

            if length_spec == '':
                return Sequence(element_field)

            is_bounded = length_spec.startswith('<=')
            length = int(length_spec[2:]) if is_bounded else int(length_spec)
            return Array(element_field, length, is_bounded)

        # Handle string types
        if field_raw_type.startswith('string'):
            if not (match := re.match(r'string(?:<=(\d+))?$', field_raw_type)):
                raise MsgParseError(f'Invalid string field: {field_raw_type}')
            length = int(match.group(1)) if match.group(1) else None
            return String('string', max_length=length)
        if field_raw_type.startswith('wstring'):
            if not (match := re.match(r'wstring(?:<=(\d+))?$', field_raw_type)):
                raise MsgParseError(f'Invalid wstring field: {field_raw_type}')
            length = int(match.group(1)) if match.group(1) else None
            return String('wstring', max_length=length)

        # Handle primitive types
        if field_raw_type in PRIMITIVE_TYPE_MAP:
            return Primitive(field_raw_type)

        # Handle complex types
        if field_raw_type == 'Header':
            field_raw_type = 'std_msgs/msg/Header'
        elif '/' not in field_raw_type:
            field_raw_type = f'{package_name}/msg/{field_raw_type}'
        elif '/msg/' not in field_raw_type:
            # Convert package/Type to package/msg/Type
            parts = field_raw_type.split('/')
            if len(parts) == 2:
                field_raw_type = f'{parts[0]}/msg/{parts[1]}'

        return Complex(field_raw_type)

    def _parse_default_value(self, field_type: SchemaFieldType, raw_value: str) -> Any:
        """Parse a default value string."""
        raw_value = raw_value.strip()

        if isinstance(field_type, Primitive):
            type_converter = PRIMITIVE_TYPE_MAP.get(field_type.type, str)
            if field_type.type == 'bool':
                return raw_value.lower() in ('true', '1', 'yes')
            return type_converter(raw_value)

        if isinstance(field_type, String):
            if raw_value.startswith('"') and raw_value.endswith('"'):
                return raw_value[1:-1]
            if raw_value.startswith("'") and raw_value.endswith("'"):
                return raw_value[1:-1]
            return raw_value

        if isinstance(field_type, (Array, Sequence)):
            import ast
            values = ast.literal_eval(raw_value)
            if not isinstance(values, list):
                raise MsgParseError('Array default must be a list')
            return values

        raise MsgParseError(f'Default values not supported for type: {field_type}')

    def _schema_to_dataclass(self, schema: Schema) -> type:
        """Convert a Schema to a Python dataclass type."""
        class_name = _sanitize_class_name(schema.name)

        # Build field specifications
        field_specs: list[tuple[str, Any] | tuple[str, Any, Any]] = []

        for field_name, entry in schema.fields.items():
            if isinstance(entry, SchemaConstant):
                # Constants get a special annotation
                base_type = self._schema_type_to_annotation(entry.type)
                const_annotation = Annotated[base_type, ('constant', base_type)]
                field_specs.append((field_name, const_annotation, entry.value))
            elif isinstance(entry, SchemaField):
                type_annotation = self._schema_type_to_annotation(entry.type)
                if entry.default is not None:
                    field_specs.append((field_name, type_annotation, entry.default))
                else:
                    field_specs.append((field_name, type_annotation))

        # Create the dataclass
        dataclass_type = make_dataclass(
            class_name,
            field_specs,
            namespace={'__msg_name__': schema.name},
            kw_only=True
        )

        return dataclass_type

    def _schema_type_to_annotation(self, field_type: SchemaFieldType) -> Any:
        """Convert a SchemaFieldType to a Python type annotation."""
        if isinstance(field_type, Primitive):
            return _PYBAG_TYPE_MAP.get(field_type.type, Any)

        if isinstance(field_type, String):
            return _PYBAG_TYPE_MAP.get(field_type.type, str)

        if isinstance(field_type, Array):
            elem_type = field_type.type
            elem_annotation = self._schema_type_to_annotation(elem_type)
            return Annotated[list[elem_annotation], ("array", elem_annotation, field_type.length)]

        if isinstance(field_type, Sequence):
            elem_type = field_type.type
            elem_annotation = self._schema_type_to_annotation(elem_type)
            return Annotated[list[elem_annotation], ("array", elem_annotation, None)]

        if isinstance(field_type, Complex):
            # Look up the type in the store
            if field_type.type in self._types:
                resolved_type = self._types[field_type.type]
                return Annotated[resolved_type, ("complex", field_type.type)]
            # Return a placeholder that will be resolved later
            return Annotated[Any, ("complex", field_type.type)]

        return Any

    def get(self, name: str) -> type | None:
        """
        Get a message type by name.

        Args:
            name: The message name (e.g., "std_msgs/msg/String").

        Returns:
            The message type, or None if not found.
        """
        return self._types.get(name)

    def __getitem__(self, name: str) -> type:
        """
        Get a message type by name.

        Args:
            name: The message name (e.g., "std_msgs/msg/String").

        Returns:
            The message type.

        Raises:
            KeyError: If the type is not found.
        """
        if name not in self._types:
            raise KeyError(f"Type not found: {name}")
        return self._types[name]

    def __contains__(self, name: str) -> bool:
        """Check if a message type is registered."""
        return name in self._types

    def __len__(self) -> int:
        """Return the number of registered types."""
        return len(self._types)

    def __iter__(self):
        """Iterate over registered type names."""
        return iter(self._types)


# Convenience function matching rosbags API
def get_typestore(store: Stores) -> TypeStore:
    """
    Get a pre-configured type store.

    Args:
        store: The store type to get (e.g., Stores.ROS2_HUMBLE).

    Returns:
        A TypeStore pre-loaded with the specified types.

    Example:
        from pybag import get_typestore, Stores

        typestore = get_typestore(Stores.ROS2_HUMBLE)
        String = typestore.types["std_msgs/msg/String"]
    """
    return TypeStore.from_store(store)


# Convenience functions for parsing .msg files
def get_types_from_msg(msg_text: str, name: str) -> dict[str, type]:
    """
    Parse a .msg file into types.

    Args:
        msg_text: The .msg file content as a string.
        name: The message name (e.g., "my_msgs/msg/Point3D").

    Returns:
        Dictionary mapping message names to their generated types.

    Example:
        types = get_types_from_msg(
            msg_text="float64 x\\nfloat64 y\\nfloat64 z",
            name="my_msgs/msg/Point3D"
        )
    """
    store = TypeStore()
    store.register_msg(name, msg_text)
    return dict(store.types)


__all__ = [
    "TypeStore",
    "Stores",
    "TypeStoreError",
    "MsgParseError",
    "get_typestore",
    "get_types_from_msg",
]
