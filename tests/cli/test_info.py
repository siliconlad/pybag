from pathlib import Path

import pytest

from pybag.bag_writer import BagFileWriter
from pybag.cli.main import main as cli_main
from pybag.cli.utils import get_file_format
from pybag.mcap_writer import McapFileWriter
from pybag.ros1.noetic.std_msgs import Int32 as Ros1Int32
from pybag.ros2.humble.std_msgs import Int32


def test_cli_info_basic(tmp_path: Path, capsys) -> None:
    """Test basic info command output."""
    input_path = tmp_path / "input.mcap"

    # Create a simple MCAP with messages
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify output contains expected information
    assert "input.mcap" in output
    assert "/foo" in output
    assert "/bar" in output


def test_cli_info_bag_basic(tmp_path: Path, capsys) -> None:
    """Test basic info command output for bag files."""
    input_path = tmp_path / "input.bag"

    # Create a simple bag file with messages
    with BagFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Ros1Int32(data=1))
        writer.write_message("/bar", int(2e9), Ros1Int32(data=2))
        writer.write_message("/foo", int(3e9), Ros1Int32(data=3))

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify output contains expected information
    assert "input.bag" in output
    assert "/foo" in output
    assert "/bar" in output
    assert "std_msgs/Int32" in output

    # Count how many times each topic appears in the Topics section
    # The topic should only appear once in the output
    lines = output.split('\n')
    foo_count = sum(1 for line in lines if '/foo' in line and 'std_msgs' in line)
    bar_count = sum(1 for line in lines if '/bar' in line and 'std_msgs' in line)
    # Each topic should appear exactly once in the topic listing
    assert foo_count == 1, f"Expected /foo to appear once, but found {foo_count} times"
    assert bar_count == 1, f"Expected /bar to appear once, but found {bar_count} times"

    # Verify message counts are correct (not double-counted)
    # /foo has 2 messages, /bar has 1 message
    assert "Topics:         2" in output  # 2 unique topics
    assert "Messages:       3" in output  # 3 total messages
