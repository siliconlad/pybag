from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pybag.ros2.humble.std_msgs as std_msgs
from pybag.cli.main import main
from pybag.cli.structure import describe_mcap_structure
from pybag.mcap_writer import McapFileWriter

if TYPE_CHECKING:
    import pytest


def _write_simple_mcap(path: Path) -> None:
    with McapFileWriter.open(path, profile="ros2") as writer:
        writer.write_message("/example", 1, std_msgs.Int32(data=5))


def _write_chunked_mcap(path: Path) -> None:
    with McapFileWriter.open(
        path,
        profile="ros2",
        chunk_size=1,
        chunk_compression=None,
    ) as writer:
        writer.write_message("/example", 1, std_msgs.Int32(data=1))
        writer.write_message("/example", 2, std_msgs.Int32(data=2))


def test_describe_mcap_structure_lists_records(tmp_path: Path) -> None:
    mcap_path = tmp_path / "simple.mcap"
    _write_simple_mcap(mcap_path)

    output = describe_mcap_structure(mcap_path)
    lines = output.splitlines()

    assert any("HEADER" in line for line in lines)
    assert any("SCHEMA (id=" in line for line in lines)
    assert any("CHANNEL (id=" in line for line in lines)
    assert any("MESSAGE (channel=" in line for line in lines)
    assert any("STATISTICS" in line for line in lines)
    assert any("MAGIC_BYTES (end)" in line for line in lines)


def test_describe_mcap_structure_shows_chunk_contents(tmp_path: Path) -> None:
    mcap_path = tmp_path / "chunked.mcap"
    _write_chunked_mcap(mcap_path)

    output = describe_mcap_structure(mcap_path)
    lines = output.splitlines()

    assert any("CHUNK" in line for line in lines)
    # Nested message box should be indented beneath the chunk.
    assert any(line.startswith("    │ MESSAGE") for line in lines)
    assert any("channel: 1 (/example)" in line for line in lines)


def test_structure_command_prints_output(
    tmp_path: Path, capsys: "pytest.CaptureFixture[str]"
) -> None:
    mcap_path = tmp_path / "cli.mcap"
    _write_simple_mcap(mcap_path)

    expected = describe_mcap_structure(mcap_path)

    main(["structure", str(mcap_path)])
    captured = capsys.readouterr()

    assert captured.err == ""
    assert captured.out.rstrip("\n") == expected

