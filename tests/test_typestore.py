"""Tests for the TypeStore module."""
import tempfile
from pathlib import Path

import pytest

from pybag.types import SchemaText
from pybag.typestore import TypeStore, TypeStoreError


class TestTypeStoreInit:
    """Tests for TypeStore initialization."""

    def test_default_encoding(self):
        """Test that default encoding is ros2msg."""
        store = TypeStore()
        assert store.encoding == 'ros2msg'
        assert store.distro == 'humble'

    def test_ros1_encoding(self):
        """Test creating a ROS1 TypeStore."""
        store = TypeStore(encoding='ros1msg', distro='noetic')
        assert store.encoding == 'ros1msg'
        assert store.distro == 'noetic'

    def test_ros2_with_distro(self):
        """Test creating a ROS2 TypeStore with specific distro."""
        store = TypeStore(encoding='ros2msg', distro='foxy')
        assert store.encoding == 'ros2msg'
        assert store.distro == 'foxy'

    def test_invalid_encoding(self):
        """Test that invalid encoding raises an error."""
        with pytest.raises(TypeStoreError, match="Unknown encoding"):
            TypeStore(encoding='invalid')  # type: ignore

    def test_invalid_ros1_distro(self):
        """Test that invalid ROS1 distro raises an error."""
        with pytest.raises(TypeStoreError, match="Invalid distro"):
            TypeStore(encoding='ros1msg', distro='humble')

    def test_invalid_ros2_distro(self):
        """Test that invalid ROS2 distro raises an error."""
        with pytest.raises(TypeStoreError, match="Invalid distro"):
            TypeStore(encoding='ros2msg', distro='noetic')


class TestTypeStoreAddPath:
    """Tests for TypeStore.add_path()."""

    def test_add_path_nonexistent(self):
        """Test that add_path raises an error for non-existent paths."""
        store = TypeStore()
        with pytest.raises(TypeStoreError, match="Path does not exist"):
            store.add_path('/nonexistent/path')

    def test_add_path_file_not_directory(self, tmp_path: Path):
        """Test that add_path raises an error for files."""
        file_path = tmp_path / "test.msg"
        file_path.write_text("string data")
        store = TypeStore()
        with pytest.raises(TypeStoreError, match="Path is not a directory"):
            store.add_path(file_path)

    def test_add_path_direct_msg_files(self, tmp_path: Path):
        """Test pattern 1: folder with .msg files directly."""
        # Create a package directory with .msg files
        pkg_dir = tmp_path / "my_msgs"
        pkg_dir.mkdir()
        (pkg_dir / "MyMessage.msg").write_text("string data")
        (pkg_dir / "OtherMessage.msg").write_text("int32 value")

        store = TypeStore()
        store.add_path(pkg_dir)

        assert "my_msgs/msg/MyMessage" in store
        assert "my_msgs/msg/OtherMessage" in store
        assert len(store.list_messages()) == 2

    def test_add_path_msg_subfolder(self, tmp_path: Path):
        """Test pattern 2: folder with msg subfolder."""
        # Create a package directory with msg subfolder
        pkg_dir = tmp_path / "my_sensor_msgs"
        msg_dir = pkg_dir / "msg"
        msg_dir.mkdir(parents=True)
        (msg_dir / "Image.msg").write_text("uint32 height\nuint32 width")

        store = TypeStore()
        store.add_path(pkg_dir)

        assert "my_sensor_msgs/msg/Image" in store

    def test_add_path_multiple_packages(self, tmp_path: Path):
        """Test pattern 3: folder containing multiple package folders."""
        # Create multiple package directories
        (tmp_path / "pkg_a" / "msg").mkdir(parents=True)
        (tmp_path / "pkg_a" / "msg" / "MsgA.msg").write_text("int32 a")

        (tmp_path / "pkg_b").mkdir()
        (tmp_path / "pkg_b" / "MsgB.msg").write_text("int32 b")

        store = TypeStore()
        store.add_path(tmp_path)

        assert "pkg_a/msg/MsgA" in store
        assert "pkg_b/msg/MsgB" in store

    def test_find_user_message_with_comments(self, tmp_path: Path):
        """Test that comments are stripped from user messages."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "Commented.msg").write_text(
            "# This is a comment\n"
            "string data  # inline comment\n"
            "int32 value\n"
        )

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('my_pkg/msg/Commented')

        assert '# This is a comment' not in schema.text
        assert '# inline comment' not in schema.text
        assert 'string data' in schema.text
        assert 'int32 value' in schema.text

    def test_user_message_overrides_builtin(self, tmp_path: Path):
        """Test that user messages override built-in messages."""
        # Create a custom std_msgs/String that differs from built-in
        pkg_dir = tmp_path / "std_msgs"
        pkg_dir.mkdir()
        (pkg_dir / "String.msg").write_text("string custom_data")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('std_msgs/msg/String')

        # Should get the custom version
        assert 'custom_data' in schema.text
        assert schema.text.count('data') == 1  # Only custom_data, not built-in data


class TestTypeStoreFindBuiltin:
    """Tests for finding built-in pybag messages."""

    def test_find_ros2_std_msgs_string(self):
        """Test finding std_msgs/msg/String with ROS2 encoding."""
        store = TypeStore(encoding='ros2msg')
        schema = store.find('std_msgs/msg/String')

        assert isinstance(schema, SchemaText)
        assert schema.name == 'std_msgs/msg/String'
        assert 'string data' in schema.text

    @pytest.mark.parametrize("key", ["std_msgs/String", "std_msgs/msg/String"])
    def test_find_ros1_std_msgs_string(self, key):
        """Test finding std_msgs/String with ROS1 encoding."""
        store = TypeStore(encoding='ros1msg', distro='noetic')
        schema = store.find(key)

        assert isinstance(schema, SchemaText)
        assert schema.name == 'std_msgs/String'
        assert 'string data' in schema.text

    def test_find_unknown_message(self):
        """Test that finding an unknown message raises an error."""
        store = TypeStore(encoding='ros2msg')
        with pytest.raises(TypeStoreError, match="Message not found"):
            store.find('unknown_pkg/msg/UnknownMsg')
        with pytest.raises(TypeStoreError, match="Invalid message name"):
            store.find('invalid')
        with pytest.raises(TypeStoreError, match="Invalid message name"):
            store.find('too/many/parts/here')


class TestTypeStoreDependencies:
    """Tests for dependency resolution."""

    def test_user_message_with_user_dependency(self, tmp_path: Path):
        """Test resolving dependencies between user messages."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        # Create a nested message structure
        (pkg_dir / "Inner.msg").write_text("int32 value")
        (pkg_dir / "Outer.msg").write_text("Inner inner\nstring name")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('my_pkg/msg/Outer')

        assert 'my_pkg/Inner inner' in schema.text
        assert 'MSG: my_pkg/Inner' in schema.text
        assert 'int32 value' in schema.text

    def test_user_message_with_builtin_dependency(self, tmp_path: Path):
        """Test resolving dependencies on built-in messages."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        # Create a message that uses Header
        (pkg_dir / "Stamped.msg").write_text("Header header\nint32 data")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('my_pkg/msg/Stamped')

        # Should include std_msgs/Header definition
        assert 'std_msgs/Header header' in schema.text
        assert 'MSG: std_msgs/Header' in schema.text
        # Should also include nested builtin_interfaces/Time definition
        assert 'MSG: builtin_interfaces/Time' in schema.text
        assert 'int32 sec' in schema.text
        assert 'uint32 nanosec' in schema.text

    def test_user_message_with_array_dependency(self, tmp_path: Path):
        """Test resolving dependencies for array types."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        (pkg_dir / "Item.msg").write_text("string name")
        (pkg_dir / "Container.msg").write_text("Item[] items")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('my_pkg/msg/Container')

        assert 'my_pkg/Item[] items' in schema.text
        assert 'MSG: my_pkg/Item' in schema.text

    def test_user_message_with_fixed_array_dependency(self, tmp_path: Path):
        """Test resolving dependencies for fixed-size array types."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        (pkg_dir / "Point.msg").write_text("float64 x\nfloat64 y")
        (pkg_dir / "Triangle.msg").write_text("Point[3] vertices")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)
        schema = store.find('my_pkg/msg/Triangle')

        assert 'my_pkg/Point[3] vertices' in schema.text
        assert 'MSG: my_pkg/Point' in schema.text

    def test_cross_package_dependency(self, tmp_path: Path):
        """Test resolving dependencies across packages."""
        # Create two packages
        pkg_a = tmp_path / "pkg_a"
        pkg_a.mkdir()
        (pkg_a / "TypeA.msg").write_text("int32 a")

        pkg_b = tmp_path / "pkg_b"
        pkg_b.mkdir()
        (pkg_b / "TypeB.msg").write_text("pkg_a/TypeA ref")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_a)
        store.add_path(pkg_b)

        schema = store.find('pkg_b/msg/TypeB')

        assert 'MSG: pkg_a/TypeA' in schema.text
        assert 'int32 a' in schema.text

    def test_circular_dependency(self, tmp_path: Path):
        """Test that circular dependencies don't cause infinite recursion."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        # Create messages with circular reference: A -> B -> A
        (pkg_dir / "TypeA.msg").write_text("TypeB b\nint32 a_value")
        (pkg_dir / "TypeB.msg").write_text("TypeA a\nint32 b_value")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)

        # This should not cause infinite recursion
        with pytest.raises(TypeStoreError, match="Recursion detected"):
            schema = store.find('my_pkg/msg/TypeA')

    def test_self_referential_message(self, tmp_path: Path):
        """Test that self-referential messages don't cause infinite recursion."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()

        # Create a message that references itself (like a linked list node)
        (pkg_dir / "Node.msg").write_text("int32 value\nNode next")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)

        # This should not cause infinite recursion
        with pytest.raises(TypeStoreError, match="Recursion detected"):
            schema = store.find('my_pkg/msg/Node')


class TestTypeStoreEncodingCompatibility:
    """Tests for encoding compatibility validation."""

    def test_ros1_time_type_in_ros2_encoding(self, tmp_path: Path):
        """Test that ROS1 'time' type raises error with ros2msg encoding."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "Stamped.msg").write_text("time stamp\nint32 data")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)

        with pytest.raises(TypeStoreError, match="time"):
            store.find('my_pkg/msg/Stamped')

    def test_ros1_duration_type_in_ros2_encoding(self, tmp_path: Path):
        """Test that ROS1 'duration' type raises error with ros2msg encoding."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "Timed.msg").write_text("duration timeout\nstring name")

        store = TypeStore(encoding='ros2msg')
        store.add_path(pkg_dir)

        with pytest.raises(TypeStoreError, match="duration"):
            store.find('my_pkg/msg/Timed')

    def test_ros1_time_type_works_with_ros1_encoding(self, tmp_path: Path):
        """Test that ROS1 'time' type works correctly with ros1msg encoding."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "Stamped.msg").write_text("time stamp\nint32 data")

        store = TypeStore(encoding='ros1msg', distro='noetic')
        store.add_path(pkg_dir)

        schema = store.find('my_pkg/msg/Stamped')
        assert 'time stamp' in schema.text
        assert 'int32 data' in schema.text

    def test_ros1_duration_type_works_with_ros1_encoding(self, tmp_path: Path):
        """Test that ROS1 'duration' type works correctly with ros1msg encoding."""
        pkg_dir = tmp_path / "my_pkg"
        pkg_dir.mkdir()
        (pkg_dir / "Timed.msg").write_text("duration timeout\nstring name")

        store = TypeStore(encoding='ros1msg', distro='noetic')
        store.add_path(pkg_dir)

        schema = store.find('my_pkg/msg/Timed')
        assert 'duration timeout' in schema.text
        assert 'string name' in schema.text


class TestTypeStoreIntegration:
    """Integration tests for TypeStore with McapFileWriter."""

    def test_schema_with_mcap_writer(self, tmp_path: Path):
        """Test that TypeStore schemas work with McapFileWriter."""
        from pybag.mcap_writer import McapFileWriter

        store = TypeStore(encoding='ros2msg')
        schema = store.find('std_msgs/msg/String')

        mcap_path = tmp_path / "test.mcap"
        with McapFileWriter.open(mcap_path, profile='ros2') as writer:
            channel_id = writer.add_channel('/test', schema=schema)
            assert channel_id is not None

    def test_schema_with_mcap_writer_ros1(self, tmp_path: Path):
        """Test that TypeStore schemas work with McapFileWriter in ROS1 mode."""
        from pybag.mcap_writer import McapFileWriter

        store = TypeStore(encoding='ros1msg', distro='noetic')
        schema = store.find('std_msgs/msg/String')

        mcap_path = tmp_path / "test.mcap"
        with McapFileWriter.open(mcap_path, profile='ros1') as writer:
            channel_id = writer.add_channel('/test', schema=schema)
            assert channel_id is not None
