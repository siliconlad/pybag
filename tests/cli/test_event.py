"""Tests for the event CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pybag.cli.event import EVENT_METADATA_NAME
from pybag.cli.main import main as cli_main
from pybag.mcap.record_reader import McapRecordReaderFactory
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32


def _write_messages(path: Path, topics: tuple[str, ...], start_s: int, end_s: int) -> None:
    with McapFileWriter.open(path, chunk_size=1024) as writer:
        for second in range(start_s, end_s + 1):
            for idx, topic in enumerate(topics):
                writer.write_message(topic, int(second * 1e9), Int32(data=second * 10 + idx))


def test_cli_event_list_empty(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "input.mcap"
    _write_messages(input_path, ("/foo",), 0, 2)

    cli_main(["event", "list", str(input_path)])

    out = capsys.readouterr().out
    assert "Events (0):" in out
    assert "No events found." in out


def test_cli_event_add_and_list_filters_json(tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "input.mcap"
    _write_messages(input_path, ("/foo",), 0, 10)

    cli_main([
        "event", "add", str(input_path),
        "start", "1.0",
        "--description", "start marker",
        "--extra", "source=operator",
    ])
    cli_main([
        "event", "add", str(input_path),
        "collision", "5.0",
    ])

    capsys.readouterr()
    cli_main([
        "event", "list", str(input_path),
        "--name", "start",
        "--start-time", "0.5",
        "--end-time", "2.0",
        "--json",
    ])

    events = json.loads(capsys.readouterr().out)
    assert len(events) == 1
    assert events[0]["name"] == "start"
    assert events[0]["timestamp"] == int(1e9)
    assert events[0]["description"] == "start marker"
    assert events[0]["source"] == "operator"


def test_cli_event_add_with_output_copies_file(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"
    _write_messages(input_path, ("/foo",), 0, 2)

    cli_main([
        "event", "add", str(input_path),
        "incident", "1.0",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(input_path) as reader:
        assert len(reader.get_metadata(name=EVENT_METADATA_NAME)) == 0

    with McapRecordReaderFactory.from_file(output_path) as reader:
        events = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert len(events) == 1
        assert events[0].metadata["name"] == "incident"
        assert len(list(reader.get_messages())) == 3


def test_cli_event_delete_by_name_preserves_other_data(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for second in range(3):
            writer.write_message("/foo", int(second * 1e9), Int32(data=second))
        writer.write_attachment("note.txt", b"hello", "text/plain")
        writer.write_metadata("config", {"mode": "test"})

    cli_main(["event", "add", str(input_path), "start", "0.0"])
    cli_main(["event", "add", str(input_path), "collision", "2.0"])

    cli_main([
        "event", "delete", str(input_path),
        "--name", "collision",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        events = reader.get_metadata(name=EVENT_METADATA_NAME)
        assert [event.metadata["name"] for event in events] == ["start"]
        assert len(list(reader.get_messages())) == 3
        assert len(reader.get_attachments()) == 1

        all_metadata = reader.get_metadata()
        config = [m for m in all_metadata if m.name == "config"]
        assert len(config) == 1
        assert config[0].metadata["mode"] == "test"


def test_cli_event_delete_by_time_range(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"
    _write_messages(input_path, ("/foo",), 0, 10)

    cli_main(["event", "add", str(input_path), "early", "1.0"])
    cli_main(["event", "add", str(input_path), "mid", "5.0"])
    cli_main(["event", "add", str(input_path), "late", "9.0"])

    cli_main([
        "event", "delete", str(input_path),
        "--start-time", "4.0",
        "--end-time", "6.0",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        names = [m.metadata["name"] for m in reader.get_metadata(name=EVENT_METADATA_NAME)]
        assert names == ["early", "late"]


def test_cli_event_delete_requires_output(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    _write_messages(input_path, ("/foo",), 0, 1)

    with pytest.raises(SystemExit):
        cli_main(["event", "delete", str(input_path)])


def test_cli_event_clip_before_after_with_topic_filter(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"
    _write_messages(input_path, ("/foo", "/bar"), 0, 10)

    cli_main(["event", "add", str(input_path), "incident", "5.0"])

    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--before", "2",
        "--after", "1",
        "--include-topic", "/foo",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        seconds = [m.log_time / 1e9 for m in messages]

        assert len(messages) == 4
        assert min(seconds) >= 3.0
        assert max(seconds) <= 6.0

        topics = {reader.get_channels()[m.channel_id].topic for m in messages}
        assert topics == {"/foo"}


def test_cli_event_clip_default_uses_event_timestamp_only(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"
    _write_messages(input_path, ("/foo",), 0, 10)

    cli_main(["event", "add", str(input_path), "incident", "5.0"])

    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        messages = list(reader.get_messages())
        seconds = [m.log_time / 1e9 for m in messages]

        assert len(messages) == 1
        assert seconds == [5.0]


def test_cli_event_clip_margin_is_symmetric(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"
    _write_messages(input_path, ("/foo",), 0, 10)

    cli_main(["event", "add", str(input_path), "incident", "5.0"])

    cli_main([
        "event", "clip", str(input_path),
        "incident",
        "--margin", "2",
        "-o", str(output_path),
    ])

    with McapRecordReaderFactory.from_file(output_path) as reader:
        seconds = [m.log_time / 1e9 for m in reader.get_messages()]
        assert min(seconds) >= 3.0
        assert max(seconds) <= 7.0
        assert len(seconds) == 5


def test_cli_event_clip_margin_conflicts_with_before_after(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    _write_messages(input_path, ("/foo",), 0, 5)
    cli_main(["event", "add", str(input_path), "incident", "2.0"])

    with pytest.raises(ValueError, match="Cannot use --margin together"):
        cli_main([
            "event", "clip", str(input_path),
            "incident",
            "--margin", "2",
            "--before", "1",
        ])


def test_cli_event_clip_event_not_found(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    _write_messages(input_path, ("/foo",), 0, 3)

    with pytest.raises(ValueError, match="No event found"):
        cli_main(["event", "clip", str(input_path), "missing"])


def test_cli_event_bag_not_supported(tmp_path: Path) -> None:
    from pybag.bag_writer import BagFileWriter
    from pybag.ros1.noetic.std_msgs import Int32 as Ros1Int32

    input_path = tmp_path / "input.bag"

    with BagFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Ros1Int32(data=1))

    with pytest.raises(ValueError, match="not supported in bag"):
        cli_main(["event", "list", str(input_path)])
