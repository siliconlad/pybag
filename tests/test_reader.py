"""Tests for the unified Reader class."""

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import pybag.ros2.humble.std_msgs as std_msgs
import pybag.types as t
from pybag.bag_writer import BagFileWriter
from pybag.mcap_writer import McapFileWriter
from pybag.reader import DecodedMessage, Reader


# Message types for bag files (ROS 1 style with pybag type annotations)
@dataclass(kw_only=True)
class BagString:
    """String message for bag files."""
    __msg_name__ = 'std_msgs/String'
    data: t.string


@dataclass(kw_only=True)
class BagInt32:
    """Int32 message for bag files."""
    __msg_name__ = 'std_msgs/Int32'
    data: t.int32


class TestFormatDetection:
    """Tests for file format detection."""

    def test_detect_mcap_extension(self):
        """Test that .mcap extension is detected correctly."""
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test.mcap"
            with McapFileWriter.open(path) as writer:
                writer.write_message("/test", 1000, std_msgs.String(data="hello"))

            with Reader.from_file(path) as reader:
                assert reader.format == 'mcap'

    def test_detect_bag_extension(self):
        """Test that .bag extension is detected correctly."""
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "test.bag"
            with BagFileWriter.open(path) as writer:
                writer.write_message("/test", 1000, BagString(data="hello"))

            with Reader.from_file(path) as reader:
                assert reader.format == 'bag'

    def test_unknown_extension_raises_error(self):
        """Test that unknown extensions raise ValueError."""
        with pytest.raises(ValueError, match="Unknown file extension"):
            Reader.from_file("/path/to/file.unknown")


class TestBasicReading:
    """Tests for basic reading operations."""

    @pytest.fixture
    def mcap_file(self, tmp_path):
        """Create a test MCAP file."""
        path = tmp_path / "test.mcap"
        with McapFileWriter.open(path, chunk_compression=None) as writer:
            writer.write_message("/topic1", 1000, std_msgs.String(data="msg1"))
            writer.write_message("/topic1", 2000, std_msgs.String(data="msg2"))
            writer.write_message("/topic2", 1500, std_msgs.Int32(data=42))
            writer.write_message("/topic2", 2500, std_msgs.Int32(data=100))
        return path

    @pytest.fixture
    def bag_file(self, tmp_path):
        """Create a test bag file."""
        path = tmp_path / "test.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/topic1", 1000, BagString(data="msg1"))
            writer.write_message("/topic1", 2000, BagString(data="msg2"))
            writer.write_message("/topic2", 1500, BagInt32(data=42))
            writer.write_message("/topic2", 2500, BagInt32(data=100))
        return path

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_get_topics(self, file_fixture, request):
        """Test getting all topics from a file."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            topics = reader.get_topics()
            assert set(topics) == {"/topic1", "/topic2"}

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_get_message_count(self, file_fixture, request):
        """Test getting message count for a topic."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            assert reader.get_message_count("/topic1") == 2
            assert reader.get_message_count("/topic2") == 2

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_start_end_time(self, file_fixture, request):
        """Test start and end time properties."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            assert reader.start_time == 1000
            assert reader.end_time == 2500

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_read_single_topic(self, file_fixture, request):
        """Test reading messages from a single topic."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages("/topic1"))
            assert len(messages) == 2
            assert all(msg.topic == "/topic1" for msg in messages)

            assert messages[0].topic == '/topic1'
            assert messages[0].msg_type == 'std_msgs/String' if file_fixture == "bag_file" else 'std_msgs/msg/String'
            assert messages[0].log_time == 1000
            assert messages[0].data.data == "msg1"

            assert messages[1].topic == '/topic1'
            assert messages[1].msg_type == 'std_msgs/String' if file_fixture == "bag_file" else 'std_msgs/msg/String'
            assert messages[1].log_time == 2000
            assert messages[1].data.data == "msg2"

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_read_multiple_topics(self, file_fixture, request):
        """Test reading messages from multiple topics."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages(["/topic1", "/topic2"], in_log_time_order=True))
            assert len(messages) == 4
            # Check log time ordering
            log_times = [msg.log_time for msg in messages]
            assert log_times == sorted(log_times)

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_time_range_filtering(self, file_fixture, request):
        """Test filtering messages by time range."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages(
                ["/topic1", "/topic2"],
                start_time=1200,
                end_time=2200,
            ))
            # Should include: topic2@1500, topic1@2000
            assert len(messages) == 2
            assert [msg.log_time for msg in messages] == [1500, 2000]

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_custom_filter(self, file_fixture, request):
        """Test filtering messages with a custom filter function."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages(
                "/topic1",
                filter=lambda msg: msg.data.data == "msg1",
            ))
            assert len(messages) == 1
            assert messages[0].data.data == "msg1"

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_write_order(self, file_fixture, request):
        """Test reading messages in write order (not log time order)."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages(
                ["/topic1", "/topic2"],
                in_log_time_order=False,
            ))
            assert len(messages) == 4
            assert messages[0].data.data == "msg1"
            assert messages[1].data.data == "msg2"
            assert messages[2].data.data == 42
            assert messages[3].data.data == 100

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_in_reverse(self, file_fixture, request):
        """Test reverse iteration works for MCAP files."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages("/topic2", in_reverse=True))
            assert len(messages) == 2
            assert messages[0].data.data == 100
            assert messages[1].data.data == 42

    @pytest.mark.parametrize("file_fixture", ["mcap_file", "bag_file"])
    def test_write_order_reverse(self, file_fixture, request):
        """Test reading messages in write order (not log time order)."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages(
                ["/topic1", "/topic2"],
                in_log_time_order=False,
                in_reverse=True,
            ))
            assert len(messages) == 4
            assert messages[0].data.data == 100
            assert messages[1].data.data == 42
            assert messages[2].data.data == "msg2"
            assert messages[3].data.data == "msg1"

class TestMcapSpecificFeatures:
    """Tests for MCAP-specific features."""

    def test_get_attachments(self, tmp_path):
        """Test getting attachments from MCAP files."""
        path = tmp_path / "test.mcap"
        with McapFileWriter.open(path, chunk_compression=None) as writer:
            writer.write_message("/test", 1000, std_msgs.String(data="hello"))
            writer.write_attachment("config.yaml", b"key: value", media_type="text/yaml")

        with Reader.from_file(path) as reader:
            attachments = reader.get_attachments()
            assert len(attachments) == 1
            assert attachments[0].name == "config.yaml"
            assert attachments[0].data == b"key: value"

    def test_get_attachments_by_name(self, tmp_path):
        """Test filtering attachments by name."""
        path = tmp_path / "test.mcap"
        with McapFileWriter.open(path, chunk_compression=None) as writer:
            writer.write_message("/test", 1000, std_msgs.String(data="hello"))
            writer.write_attachment("config.yaml", b"key: value", media_type="text/yaml")
            writer.write_attachment("other.txt", b"other data", media_type="text/plain")

        with Reader.from_file(path) as reader:
            attachments = reader.get_attachments(name="config.yaml")
            assert len(attachments) == 1
            assert attachments[0].name == "config.yaml"

    def test_get_metadata(self, tmp_path):
        """Test getting metadata from MCAP files."""
        path = tmp_path / "test.mcap"
        with McapFileWriter.open(path, chunk_compression=None) as writer:
            writer.write_message("/test", 1000, std_msgs.String(data="hello"))
            writer.write_metadata("session", {"location": "lab", "operator": "alice"})

        with Reader.from_file(path) as reader:
            metadata = reader.get_metadata()
            assert len(metadata) == 1
            assert metadata[0].name == "session"
            assert metadata[0].metadata == {"location": "lab", "operator": "alice"}


class TestBagSpecificBehavior:
    """Tests for bag-specific behavior."""

    def test_get_attachments_returns_empty(self, tmp_path):
        """Test that get_attachments returns empty list for bag files."""
        path = tmp_path / "test.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/test", 1000, BagString(data="hello"))

        with Reader.from_file(path) as reader:
            attachments = reader.get_attachments()
            assert attachments == []

    def test_get_metadata_returns_empty(self, tmp_path):
        """Test that get_metadata returns empty list for bag files."""
        path = tmp_path / "test.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/test", 1000, BagString(data="hello"))

        with Reader.from_file(path) as reader:
            metadata = reader.get_metadata()
            assert metadata == []


class TestGlobPatterns:
    """Tests for glob pattern matching in topic selection."""

    @pytest.fixture
    def multi_topic_mcap(self, tmp_path):
        """Create an MCAP file with multiple topics."""
        path = tmp_path / "test.mcap"
        with McapFileWriter.open(path, chunk_compression=None) as writer:
            writer.write_message("/sensor/camera", 1000, std_msgs.String(data="cam"))
            writer.write_message("/sensor/lidar", 2000, std_msgs.String(data="lidar"))
            writer.write_message("/control/cmd", 3000, std_msgs.String(data="cmd"))
        return path

    @pytest.fixture
    def multi_topic_bag(self, tmp_path):
        """Create a bag file with multiple topics."""
        path = tmp_path / "test.bag"
        with BagFileWriter.open(path) as writer:
            writer.write_message("/sensor/camera", 1000, BagString(data="cam"))
            writer.write_message("/sensor/lidar", 2000, BagString(data="lidar"))
            writer.write_message("/control/cmd", 3000, BagString(data="cmd"))
        return path

    @pytest.mark.parametrize("file_fixture", ["multi_topic_mcap", "multi_topic_bag"])
    def test_glob_pattern(self, file_fixture, request):
        """Test glob pattern matching for topic selection."""
        path = request.getfixturevalue(file_fixture)
        with Reader.from_file(path) as reader:
            messages = list(reader.messages("/sensor/*"))
            assert len(messages) == 2
            topics = {msg.topic for msg in messages}
            assert topics == {"/sensor/camera", "/sensor/lidar"}
