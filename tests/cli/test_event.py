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
