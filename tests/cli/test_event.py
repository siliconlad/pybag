"""Tests for the event CLI command."""

import shutil
from pathlib import Path

import pytest

from pybag.cli.event import EVENT_METADATA_NAME
from pybag.cli.main import main as cli_main
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32


def test_cli_event_list_empty(tmp_path: Path) -> None:
    """Test listing events when there are none."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # List events (should be empty)
    cli_main(["event", "list", str(input_path)])


def test_cli_event_add_and_list(tmp_path: Path) -> None:
    """Test adding an event and listing it."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/foo", int(2e9), Int32(data=2))

    # Add an event (modifies file in place)
    cli_main([
        "event", "add", str(input_path),
        "start", "1.5",
    ])

    # Verify event was added
    with McapRecordReaderFactory.from_file(input_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 1
        assert metadata[0].metadata["name"] == "start"
        assert metadata[0].metadata["timestamp"] == str(int(1.5e9))

        # Verify messages are preserved
        channels = reader.get_channels()
        assert len(channels) == 1


def test_cli_event_add_with_description(tmp_path: Path) -> None:
    """Test adding an event with description."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # Add event with description
    cli_main([
        "event", "add", str(input_path),
        "collision", "5.0",
        "--description", "Hit obstacle at corner",
    ])

    with McapRecordReaderFactory.from_file(input_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 1
        assert metadata[0].metadata["name"] == "collision"
        assert metadata[0].metadata["description"] == "Hit obstacle at corner"


def test_cli_event_add_with_extra_fields(tmp_path: Path) -> None:
    """Test adding an event with extra key-value pairs."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # Add event with extra fields
    cli_main([
        "event", "add", str(input_path),
        "waypoint", "10.0",
        "--extra", "waypoint_id=42",
        "--extra", "location=entrance",
    ])

    with McapRecordReaderFactory.from_file(input_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 1
        assert metadata[0].metadata["name"] == "waypoint"
        assert metadata[0].metadata["waypoint_id"] == "42"
        assert metadata[0].metadata["location"] == "entrance"


def test_cli_event_add_multiple_events(tmp_path: Path) -> None:
    """Test adding multiple events."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/foo", int(5e9), Int32(data=2))
        writer.write_message("/foo", int(10e9), Int32(data=3))

    # Add first event
    cli_main([
        "event", "add", str(input_path),
        "start", "0.0",
    ])

    # Add second event
    cli_main([
        "event", "add", str(input_path),
        "end", "10.0",
    ])

    with McapRecordReaderFactory.from_file(input_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 2
        names = [m.metadata["name"] for m in metadata]
        assert "start" in names
        assert "end" in names


def test_cli_event_delete_all(tmp_path: Path) -> None:
    """Test deleting all events."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    # Create MCAP with messages
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # Add an event
    cli_main([
        "event", "add", str(input_path),
        "event1", "1.0",
    ])

    # Delete all events
    cli_main([
        "event", "delete", str(input_path),
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 0


def test_cli_event_delete_by_name(tmp_path: Path) -> None:
    """Test deleting events by name."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # Add first event
    cli_main([
        "event", "add", str(input_path),
        "start", "1.0",
    ])

    # Add second event
    cli_main([
        "event", "add", str(input_path),
        "collision", "5.0",
    ])

    # Delete only "collision" events
    cli_main([
        "event", "delete", str(input_path),
        "--name", "collision",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 1
        assert metadata[0].metadata["name"] == "start"


def test_cli_event_delete_by_time_range(tmp_path: Path) -> None:
    """Test deleting events by time range."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    # Add events at different times
    cli_main([
        "event", "add", str(input_path),
        "event1", "1.0",
    ])
    cli_main([
        "event", "add", str(input_path),
        "event2", "5.0",
    ])
    cli_main([
        "event", "add", str(input_path),
        "event3", "10.0",
    ])

    # Delete events between 4s and 6s
    cli_main([
        "event", "delete", str(input_path),
        "--start-time", "4.0",
        "--end-time", "6.0",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        metadata = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(metadata) == 2
        names = [m.metadata["name"] for m in metadata]
        assert "event1" in names
        assert "event3" in names
        assert "event2" not in names


def test_cli_event_list_filter_by_name(tmp_path: Path, capsys) -> None:
    """Test listing events filtered by name."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    cli_main([
        "event", "add", str(input_path),
        "start", "1.0",
    ])
    cli_main([
        "event", "add", str(input_path),
        "collision", "5.0",
    ])

    # Clear captured output from add commands
    capsys.readouterr()

    # List only "collision" events
    cli_main([
        "event", "list", str(input_path),
        "--name", "collision",
    ])

    captured = capsys.readouterr()
    assert "collision" in captured.out
    assert "start" not in captured.out or "start" in captured.out.split("collision")[0]  # Should not be in data rows


def test_cli_event_list_json_output(tmp_path: Path, capsys) -> None:
    """Test listing events in JSON format."""
    import json

    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    cli_main([
        "event", "add", str(input_path),
        "test_event", "2.5",
        "--description", "Test description",
    ])

    # Clear captured output from the add command
    capsys.readouterr()

    cli_main([
        "event", "list", str(input_path),
        "--json",
    ])

    captured = capsys.readouterr()
    # The JSON output should be the entire output when --json is used
    events = json.loads(captured.out.strip())
    assert len(events) == 1
    assert events[0]["name"] == "test_event"
    assert events[0]["timestamp"] == int(2.5e9)
    assert events[0]["description"] == "Test description"


def test_cli_event_preserves_messages(tmp_path: Path) -> None:
    """Test that adding events preserves all messages."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

    cli_main([
        "event", "add", str(input_path),
        "test", "1.5",
    ])

    # Verify all messages are preserved
    with McapRecordReaderFactory.from_file(input_path) as reader:
        channels = reader.get_channels()
        assert len(channels) == 2

        messages = list(reader.get_messages())
        assert len(messages) == 3


def test_cli_event_preserves_attachments_and_metadata(tmp_path: Path) -> None:
    """Test that adding events preserves attachments and other metadata."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_attachment("test.txt", b"test data", "text/plain")
        writer.write_metadata("config", {"key": "value"})

    cli_main([
        "event", "add", str(input_path),
        "test", "1.0",
    ])

    with McapRecordReaderFactory.from_file(input_path) as reader:
        attachments = reader.get_attachments()
        assert len(attachments) == 1
        assert attachments[0].name == "test.txt"

        all_metadata = reader.get_metadata()
        # Should have config + event
        assert len(all_metadata) == 2


def test_cli_event_bag_not_supported(tmp_path: Path, capsys) -> None:
    """Test that events are not supported for bag files."""
    from pybag.bag_writer import BagFileWriter
    from pybag.ros1.noetic.std_msgs import Int32 as Ros1Int32

    input_path = tmp_path / "input.bag"

    with BagFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Ros1Int32(data=1))

    # List should print message about not supported
    cli_main(["event", "list", str(input_path)])
    captured = capsys.readouterr()
    assert "not supported" in captured.out.lower()

    # Add should raise error
    with pytest.raises(ValueError, match="not supported in bag"):
        cli_main([
            "event", "add", str(input_path),
            "test", "1.0",
        ])


def test_cli_event_delete_preserves_non_event_metadata(tmp_path: Path) -> None:
    """Test that deleting events preserves non-event metadata."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_metadata("config", {"setting": "value"})

    # Add an event
    cli_main([
        "event", "add", str(input_path),
        "test_event", "1.0",
    ])

    # Delete all events
    cli_main([
        "event", "delete", str(input_path),
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        all_metadata = reader.get_metadata()
        # Should only have config metadata (event should be deleted)
        assert len(all_metadata) == 1
        assert all_metadata[0].name == "config"
        assert all_metadata[0].metadata["setting"] == "value"


#####################
# Clip Command Tests
#####################

def test_cli_event_clip_basic(tmp_path: Path) -> None:
    """Test basic clip around an event."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    # Create MCAP with messages spread over time
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(20):
            # Messages at 0s, 1s, 2s, ..., 19s
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    # Add an event at 10s
    cli_main([
        "event", "add", str(input_path),
        "incident", "10.0",
    ])

    # Clip around the event with 3s before and after
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "3",
        "--after", "3",
        "-o", str(output_path),
    ])

    # Verify clipped file contains messages from 7s to 13s
    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        timestamps = [m.log_time / 1e9 for m in messages]

        # Should have messages at 7, 8, 9, 10, 11, 12, 13 = 7 messages
        assert len(messages) == 7
        assert min(timestamps) >= 7.0
        assert max(timestamps) <= 13.0


def test_cli_event_clip_symmetric_single_arg(tmp_path: Path) -> None:
    """Test that clip uses symmetric margin when only --before is given."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(20):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    cli_main([
        "event", "add", str(input_path),
        "incident", "10.0",
    ])

    # Only specify --before, should use same value for after
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "2",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        timestamps = [m.log_time / 1e9 for m in messages]

        # Should have messages at 8, 9, 10, 11, 12 = 5 messages
        assert len(messages) == 5
        assert min(timestamps) >= 8.0
        assert max(timestamps) <= 12.0


def test_cli_event_clip_symmetric_after_only(tmp_path: Path) -> None:
    """Test that clip uses symmetric margin when only --after is given."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(20):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    cli_main([
        "event", "add", str(input_path),
        "incident", "10.0",
    ])

    # Only specify --after, should use same value for before
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--after", "2",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        timestamps = [m.log_time / 1e9 for m in messages]

        # Should have messages at 8, 9, 10, 11, 12 = 5 messages
        assert len(messages) == 5
        assert min(timestamps) >= 8.0
        assert max(timestamps) <= 12.0


def test_cli_event_clip_default_margin(tmp_path: Path) -> None:
    """Test that clip uses default 5s margin when no --before/--after given."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(20):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    cli_main([
        "event", "add", str(input_path),
        "incident", "10.0",
    ])

    # No margin specified, should default to 5s before and after
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        timestamps = [m.log_time / 1e9 for m in messages]

        # Should have messages at 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 = 11 messages
        assert len(messages) == 11
        assert min(timestamps) >= 5.0
        assert max(timestamps) <= 15.0


def test_cli_event_clip_asymmetric(tmp_path: Path) -> None:
    """Test clip with different before and after values."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(20):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    cli_main([
        "event", "add", str(input_path),
        "incident", "10.0",
    ])

    # 2s before, 5s after
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "2",
        "--after", "5",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        timestamps = [m.log_time / 1e9 for m in messages]

        # Should have messages at 8, 9, 10, 11, 12, 13, 14, 15 = 8 messages
        assert len(messages) == 8
        assert min(timestamps) >= 8.0
        assert max(timestamps) <= 15.0


def test_cli_event_clip_event_not_found(tmp_path: Path) -> None:
    """Test that clip raises error when event not found."""
    input_path = tmp_path / "input.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))

    with pytest.raises(ValueError, match="No event found"):
        cli_main([
            "event", "clip", str(input_path),
            "nonexistent",
        ])


def test_cli_event_clip_with_topic_filter(tmp_path: Path) -> None:
    """Test clip with topic filtering."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(10):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))
            writer.write_message("/bar", int(i * 1e9), Int32(data=i * 10))

    cli_main([
        "event", "add", str(input_path),
        "incident", "5.0",
    ])

    # Clip but only include /foo topic
    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "2",
        "--after", "2",
        "--include-topic", "/foo",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        channels = reader.get_channels()
        # Should only have /foo channel
        assert len(channels) == 1
        topic_names = [ch.topic for ch in channels.values()]
        assert "/foo" in topic_names
        assert "/bar" not in topic_names


def test_cli_event_clip_default_output_name(tmp_path: Path) -> None:
    """Test that clip generates appropriate default output filename."""
    input_path = tmp_path / "recording.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(10):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))

    cli_main([
        "event", "add", str(input_path),
        "collision", "5.0",
    ])

    # Don't specify output path
    cli_main([
        "event", "clip", str(input_path),
        "collision",
        "--before", "2",
        "--after", "2",
    ])

    # Check that default output file was created
    expected_output = tmp_path / "recording_clip_collision.mcap"
    assert expected_output.exists()


def test_cli_event_clip_preserves_metadata(tmp_path: Path) -> None:
    """Test that clip preserves metadata and attachments within time range."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(10):
            writer.write_message("/foo", int(i * 1e9), Int32(data=i))
        writer.write_metadata("config", {"key": "value"})
        # Attachment at 5s (within clip range of 3s-7s)
        writer.write_attachment("test.txt", b"test data", "text/plain", log_time=int(5e9))

    cli_main([
        "event", "add", str(input_path),
        "incident", "5.0",
    ])

    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "2",
        "--after", "2",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        all_metadata = reader.get_metadata()
        # Should have config + event metadata
        assert len(all_metadata) == 2

        attachments = reader.get_attachments()
        assert len(attachments) == 1
        assert attachments[0].name == "test.txt"
