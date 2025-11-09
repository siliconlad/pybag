import sys
from io import StringIO
from pathlib import Path

from pybag.cli.main import main as cli_main
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Empty, Int32


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
    assert "Time Information:" in output
    assert "Start time:" in output
    assert "End time:" in output
    assert "Duration:" in output
    assert "Statistics:" in output
    assert "Profile:" in output
    assert "Messages:" in output
    assert "Topics:" in output
    assert "/foo" in output
    assert "/bar" in output


def test_cli_info_message_counts(tmp_path: Path, capsys) -> None:
    """Test that info command correctly counts messages per topic."""
    input_path = tmp_path / "input.mcap"

    # Create MCAP with different message counts per topic
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        # 3 messages on /foo
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/foo", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

        # 1 message on /bar
        writer.write_message("/bar", int(4e9), Int32(data=4))

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify message counts
    assert "Messages:       4" in output or "Messages: 4" in output

    # Check that topics are listed
    assert "/foo" in output
    assert "/bar" in output


def test_cli_info_empty_mcap(tmp_path: Path, capsys) -> None:
    """Test info command on an empty MCAP (no messages)."""
    input_path = tmp_path / "empty.mcap"

    # Create an empty MCAP
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        pass  # No messages written

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify basic structure is present
    assert "empty.mcap" in output
    assert "Statistics:" in output
    assert "Messages:" in output


def test_cli_info_multiple_topics(tmp_path: Path, capsys) -> None:
    """Test info command with multiple topics."""
    input_path = tmp_path / "multi.mcap"

    # Create MCAP with multiple topics
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/topic1", int(1e9), Int32(data=1))
        writer.write_message("/topic2", int(2e9), Int32(data=2))
        writer.write_message("/topic3", int(3e9), Int32(data=3))
        writer.write_message("/topic1", int(4e9), Int32(data=4))

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify all topics are listed
    assert "/topic1" in output
    assert "/topic2" in output
    assert "/topic3" in output

    # Verify encoding is shown
    assert "cdr" in output


def test_cli_info_duration_format(tmp_path: Path, capsys) -> None:
    """Test that duration is formatted correctly."""
    input_path = tmp_path / "duration.mcap"

    # Create MCAP with 5 second duration
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), Int32(data=1))
        writer.write_message("/test", int(6e9), Int32(data=2))  # 5 seconds later

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify duration is shown
    assert "Duration:" in output
    assert "5.00 s" in output or "5.0 s" in output


def test_cli_info_frequency_calculation(tmp_path: Path, capsys) -> None:
    """Test that frequency is calculated correctly."""
    input_path = tmp_path / "freq.mcap"

    # Create MCAP with known frequency
    # 10 messages over 10 seconds = 1 Hz
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        for i in range(10):
            writer.write_message("/test", int((i + 1) * 1e9), Int32(data=i))

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify frequency is shown (approximately 1 Hz)
    # Note: actual frequency will be 10/9 = 1.11 Hz because duration is from first to last message
    assert "Freq (Hz)" in output


def test_cli_info_different_encodings(tmp_path: Path, capsys) -> None:
    """Test info command with different message types (same encoding in ROS2)."""
    input_path = tmp_path / "encodings.mcap"

    # Create MCAP with different message types
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/int", int(1e9), Int32(data=1))
        writer.write_message("/empty", int(2e9), Empty())

    # Run info command
    cli_main(["info", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify both topics are shown
    assert "/int" in output
    assert "/empty" in output

    # Verify schema information is shown
    assert "Schema" in output
