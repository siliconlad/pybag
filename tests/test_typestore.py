"""Tests for the TypeStore class."""
import tempfile
from dataclasses import is_dataclass
from pathlib import Path

import pytest

from pybag import TypeStore, Stores, TypeStoreError, MsgParseError, get_typestore, get_types_from_msg


class TestTypeStoreBasic:
    """Test basic TypeStore functionality."""

    def test_create_empty_store(self):
        """An empty store should have no types."""
        store = TypeStore()
        assert len(store) == 0
        assert len(store.types) == 0

    def test_types_property(self):
        """The types property should return the internal types dict."""
        store = TypeStore()
        assert store.types == {}

    def test_contains(self):
        """The __contains__ method should check type existence."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test", "int32 value")
        assert "pkg/msg/Test" in store
        assert "pkg/msg/Other" not in store

    def test_len(self):
        """The __len__ method should return the number of types."""
        store = TypeStore()
        assert len(store) == 0
        store.register_msg("pkg/msg/Test1", "int32 value")
        assert len(store) == 1
        store.register_msg("pkg/msg/Test2", "int32 value")
        assert len(store) == 2

    def test_iter(self):
        """The __iter__ method should iterate over type names."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test1", "int32 value")
        store.register_msg("pkg/msg/Test2", "int32 value")
        names = list(store)
        assert "pkg/msg/Test1" in names
        assert "pkg/msg/Test2" in names

    def test_getitem(self):
        """The __getitem__ method should return types by name."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test", "int32 value")
        msg_type = store["pkg/msg/Test"]
        assert hasattr(msg_type, "__msg_name__")
        assert msg_type.__msg_name__ == "pkg/msg/Test"

    def test_getitem_not_found(self):
        """The __getitem__ method should raise KeyError for missing types."""
        store = TypeStore()
        with pytest.raises(KeyError, match="Type not found"):
            _ = store["pkg/msg/Missing"]

    def test_get(self):
        """The get method should return types or None."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test", "int32 value")
        assert store.get("pkg/msg/Test") is not None
        assert store.get("pkg/msg/Missing") is None


class TestTypeStoreHumble:
    """Test ROS2 Humble type loading."""

    def test_ros2_humble_factory(self):
        """The ros2_humble() factory should load Humble types."""
        store = TypeStore.ros2_humble()
        assert len(store) > 0
        assert "std_msgs/msg/String" in store
        assert "std_msgs/msg/Header" in store
        assert "geometry_msgs/msg/Point" in store
        assert "sensor_msgs/msg/Image" in store

    def test_from_store_humble(self):
        """The from_store() factory should work with Stores enum."""
        store = TypeStore.from_store(Stores.ROS2_HUMBLE)
        assert "std_msgs/msg/String" in store

    def test_from_store_invalid(self):
        """The from_store() factory should raise for invalid stores."""
        # There's no such store, so we can't actually test this
        # without adding a fake enum value
        pass

    def test_humble_types_are_dataclasses(self):
        """Humble types should be valid dataclasses."""
        store = TypeStore.ros2_humble()
        String = store["std_msgs/msg/String"]
        assert is_dataclass(String)
        assert hasattr(String, "__msg_name__")

    def test_humble_types_can_be_instantiated(self):
        """Humble types should be instantiable."""
        store = TypeStore.ros2_humble()
        String = store["std_msgs/msg/String"]
        msg = String(data="hello")
        assert msg.data == "hello"


class TestRegisterMsg:
    """Test registering types from .msg text."""

    def test_register_simple_msg(self):
        """Should register a simple message type."""
        store = TypeStore()
        msg_type = store.register_msg("my_msgs/msg/Point3D", """
            float64 x
            float64 y
            float64 z
        """)
        assert "my_msgs/msg/Point3D" in store
        assert is_dataclass(msg_type)
        assert msg_type.__msg_name__ == "my_msgs/msg/Point3D"

    def test_register_msg_instantiate(self):
        """Registered types should be instantiable."""
        store = TypeStore()
        store.register_msg("my_msgs/msg/Point3D", """
            float64 x
            float64 y
            float64 z
        """)
        Point3D = store["my_msgs/msg/Point3D"]
        point = Point3D(x=1.0, y=2.0, z=3.0)
        assert point.x == 1.0
        assert point.y == 2.0
        assert point.z == 3.0

    def test_register_msg_normalizes_name(self):
        """Should normalize message names (add /msg/ if missing)."""
        store = TypeStore()
        store.register_msg("my_msgs/Point3D", "float64 x")
        assert "my_msgs/msg/Point3D" in store

    def test_register_msg_with_comments(self):
        """Should handle comments in .msg text."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test", """
            # This is a comment
            float64 x  # inline comment
            float64 y
        """)
        Test = store["pkg/msg/Test"]
        msg = Test(x=1.0, y=2.0)
        assert msg.x == 1.0

    def test_register_msg_all_primitives(self):
        """Should handle all primitive types."""
        store = TypeStore()
        store.register_msg("pkg/msg/AllPrimitives", """
            bool b
            int8 i8
            int16 i16
            int32 i32
            int64 i64
            uint8 u8
            uint16 u16
            uint32 u32
            uint64 u64
            float32 f32
            float64 f64
            string s
            byte by
            char ch
        """)
        AllPrimitives = store["pkg/msg/AllPrimitives"]
        assert is_dataclass(AllPrimitives)

    def test_register_msg_with_array(self):
        """Should handle array types."""
        store = TypeStore()
        store.register_msg("pkg/msg/WithArray", "int32[] values")
        WithArray = store["pkg/msg/WithArray"]
        msg = WithArray(values=[1, 2, 3])
        assert msg.values == [1, 2, 3]

    def test_register_msg_with_fixed_array(self):
        """Should handle fixed-length array types."""
        store = TypeStore()
        store.register_msg("pkg/msg/WithFixedArray", "int32[3] values")
        WithFixedArray = store["pkg/msg/WithFixedArray"]
        msg = WithFixedArray(values=[1, 2, 3])
        assert msg.values == [1, 2, 3]

    def test_register_msg_with_default_value(self):
        """Should handle default values."""
        store = TypeStore()
        store.register_msg("pkg/msg/WithDefault", "int32 count 100")
        WithDefault = store["pkg/msg/WithDefault"]
        msg = WithDefault()
        assert msg.count == 100

    def test_register_msg_with_string_default(self):
        """Should handle string default values."""
        store = TypeStore()
        store.register_msg("pkg/msg/WithStringDefault", 'string name "default"')
        WithStringDefault = store["pkg/msg/WithStringDefault"]
        msg = WithStringDefault()
        assert msg.name == "default"

    def test_register_msg_with_constant(self):
        """Should handle constants."""
        store = TypeStore()
        store.register_msg("pkg/msg/WithConstant", """
            int32 MAX_VALUE=100
            int32 value
        """)
        WithConstant = store["pkg/msg/WithConstant"]
        assert WithConstant.MAX_VALUE == 100

    def test_register_msg_invalid_name(self):
        """Should raise error for invalid message names."""
        store = TypeStore()
        with pytest.raises(MsgParseError, match="must include package"):
            store.register_msg("InvalidName", "int32 value")


class TestRegisterMsgNested:
    """Test registering nested message types."""

    def test_register_msg_with_nested_type(self):
        """Should handle nested types from sub-schema."""
        store = TypeStore()
        msg_text = """
geometry_msgs/Point point
================================================================================
MSG: geometry_msgs/Point
float64 x
float64 y
float64 z
"""
        store.register_msg("pkg/msg/WithPoint", msg_text)
        assert "pkg/msg/WithPoint" in store
        assert "geometry_msgs/msg/Point" in store

    def test_register_msg_with_preloaded_nested(self):
        """Should work with pre-loaded nested types."""
        store = TypeStore.ros2_humble()
        # std_msgs/Header is already loaded
        store.register_msg("pkg/msg/Stamped", """
            std_msgs/Header header
            float64 value
        """)
        Stamped = store["pkg/msg/Stamped"]
        Header = store["std_msgs/msg/Header"]
        Time = store["builtin_interfaces/msg/Time"]

        time = Time(sec=1, nanosec=0)
        header = Header(stamp=time, frame_id="test")
        msg = Stamped(header=header, value=42.0)
        assert msg.value == 42.0


class TestRegisterFromFile:
    """Test registering types from .msg files."""

    def test_register_from_file(self):
        """Should register a type from a .msg file."""
        store = TypeStore()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create standard ROS package structure
            msg_dir = Path(tmpdir) / "test_pkg" / "msg"
            msg_dir.mkdir(parents=True)

            msg_file = msg_dir / "MyMessage.msg"
            msg_file.write_text("float64 x\nfloat64 y")

            store.register_from_file(msg_file)

            assert "test_pkg/msg/MyMessage" in store

    def test_register_from_file_explicit_name(self):
        """Should accept explicit message name."""
        store = TypeStore()

        with tempfile.TemporaryDirectory() as tmpdir:
            msg_file = Path(tmpdir) / "MyMessage.msg"
            msg_file.write_text("float64 x\nfloat64 y")

            store.register_from_file(msg_file, "custom_pkg/msg/CustomName")

            assert "custom_pkg/msg/CustomName" in store

    def test_register_from_file_not_found(self):
        """Should raise error for missing files."""
        store = TypeStore()
        with pytest.raises(TypeStoreError, match="File not found"):
            store.register_from_file("/nonexistent/path/Message.msg")


class TestRegisterFromPackage:
    """Test batch registration from a package directory."""

    def test_register_from_package(self):
        """Should register all .msg files from a package."""
        store = TypeStore()

        with tempfile.TemporaryDirectory() as tmpdir:
            msg_dir = Path(tmpdir) / "test_pkg" / "msg"
            msg_dir.mkdir(parents=True)

            (msg_dir / "Point.msg").write_text("float64 x\nfloat64 y\nfloat64 z")
            (msg_dir / "Vector.msg").write_text("float64 x\nfloat64 y\nfloat64 z")

            registered = store.register_from_package(msg_dir.parent, "test_pkg")

            assert "test_pkg/msg/Point" in store
            assert "test_pkg/msg/Vector" in store
            assert len(registered) == 2

    def test_register_from_package_msg_subdir(self):
        """Should find msg/ subdirectory automatically."""
        store = TypeStore()

        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_dir = Path(tmpdir) / "test_pkg"
            msg_dir = pkg_dir / "msg"
            msg_dir.mkdir(parents=True)

            (msg_dir / "Test.msg").write_text("int32 value")

            store.register_from_package(pkg_dir, "test_pkg")

            assert "test_pkg/msg/Test" in store

    def test_register_from_package_infer_name(self):
        """Should infer package name from directory structure."""
        store = TypeStore()

        with tempfile.TemporaryDirectory() as tmpdir:
            pkg_dir = Path(tmpdir) / "my_pkg"
            msg_dir = pkg_dir / "msg"
            msg_dir.mkdir(parents=True)

            (msg_dir / "Test.msg").write_text("int32 value")

            store.register_from_package(pkg_dir)

            assert "my_pkg/msg/Test" in store

    def test_register_from_package_not_found(self):
        """Should raise error for missing directories."""
        store = TypeStore()
        with pytest.raises(TypeStoreError, match="Directory not found"):
            store.register_from_package("/nonexistent/path")

    def test_register_from_package_no_msg_files(self):
        """Should raise error for empty directories."""
        store = TypeStore()
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(TypeStoreError, match="No .msg files"):
                store.register_from_package(tmpdir)


class TestRegisterType:
    """Test registering pre-defined type classes."""

    def test_register_type(self):
        """Should register a dataclass with __msg_name__."""
        from dataclasses import dataclass
        import pybag.types as t

        @dataclass(kw_only=True)
        class CustomMsg:
            __msg_name__ = "custom/msg/CustomMsg"
            value: t.int32

        store = TypeStore()
        store.register_type(CustomMsg)

        assert "custom/msg/CustomMsg" in store
        assert store["custom/msg/CustomMsg"] is CustomMsg

    def test_register_type_without_msg_name(self):
        """Should raise error for types without __msg_name__."""
        from dataclasses import dataclass

        @dataclass
        class InvalidMsg:
            value: int

        store = TypeStore()
        with pytest.raises(TypeStoreError, match="__msg_name__"):
            store.register_type(InvalidMsg)

    def test_register_dict(self):
        """Should register multiple types at once."""
        from dataclasses import dataclass
        import pybag.types as t

        @dataclass(kw_only=True)
        class Msg1:
            __msg_name__ = "pkg/msg/Msg1"
            value: t.int32

        @dataclass(kw_only=True)
        class Msg2:
            __msg_name__ = "pkg/msg/Msg2"
            value: t.int32

        store = TypeStore()
        store.register({
            "pkg/msg/Msg1": Msg1,
            "pkg/msg/Msg2": Msg2,
        })

        assert "pkg/msg/Msg1" in store
        assert "pkg/msg/Msg2" in store


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_get_typestore(self):
        """get_typestore() should return pre-configured store."""
        store = get_typestore(Stores.ROS2_HUMBLE)
        assert isinstance(store, TypeStore)
        assert "std_msgs/msg/String" in store

    def test_get_types_from_msg(self):
        """get_types_from_msg() should parse .msg text into types."""
        types = get_types_from_msg(
            msg_text="float64 x\nfloat64 y\nfloat64 z",
            name="my_msgs/msg/Point3D"
        )
        assert "my_msgs/msg/Point3D" in types
        Point3D = types["my_msgs/msg/Point3D"]
        point = Point3D(x=1.0, y=2.0, z=3.0)
        assert point.x == 1.0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_msg(self):
        """Should handle empty .msg content."""
        store = TypeStore()
        # Empty messages are valid in ROS2 (though uncommon)
        store.register_msg("pkg/msg/Empty", "")
        Empty = store["pkg/msg/Empty"]
        # Should be instantiable with no arguments
        msg = Empty()
        assert is_dataclass(msg)

    def test_msg_with_only_whitespace(self):
        """Should handle whitespace-only .msg content."""
        store = TypeStore()
        store.register_msg("pkg/msg/Whitespace", "   \n\n   ")
        Whitespace = store["pkg/msg/Whitespace"]
        msg = Whitespace()
        assert is_dataclass(msg)

    def test_overwrites_existing_type(self):
        """Should overwrite existing types without error."""
        store = TypeStore()
        store.register_msg("pkg/msg/Test", "int32 value")
        store.register_msg("pkg/msg/Test", "float64 other_value")

        Test = store["pkg/msg/Test"]
        # Should have the new field, not the old one
        msg = Test(other_value=1.0)
        assert hasattr(msg, "other_value")
