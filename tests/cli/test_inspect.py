from pathlib import Path

from pybag.cli.main import main as cli_main
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32


def test_cli_inspect_basic(tmp_path: Path, capsys) -> None:
    """Test basic inspect command output."""
    input_path = tmp_path / "input.mcap"

    # Create a simple MCAP with messages
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

    # Run inspect command
    cli_main(["inspect", str(input_path)])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify output contains expected information
    assert "MCAP File Structure" in output
    assert "Header:" in output
    assert "Record Counts:" in output
    assert "Messages:" in output
    assert "Channels:" in output
    assert "Schemas:" in output
    assert "Chunks:" in output
    assert "Summary Section:" in output


def test_cli_inspect_chunks(tmp_path: Path, capsys) -> None:
    """Test inspect command with --chunks flag."""
    input_path = tmp_path / "input.mcap"

    # Create a MCAP with multiple chunks
    with McapFileWriter.open(input_path, chunk_size=512) as writer:
        for i in range(50):
            writer.write_message("/topic1", int(1e9 + i * 1e8), Int32(data=i))
            writer.write_message("/topic2", int(1e9 + i * 1e8), Int32(data=i * 2))

    # Run inspect command with --chunks
    cli_main(["inspect", str(input_path), "--chunks"])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify chunk details are present
    assert "Chunk Details" in output
    assert "Start Time" in output
    assert "End Time" in output
    assert "Duration" in output
    assert "Compression" in output
    assert "Chunk Summary:" in output
    assert "Total chunks:" in output
    assert "Overall compression:" in output


def test_cli_inspect_summary(tmp_path: Path, capsys) -> None:
    """Test inspect command with --summary flag."""
    input_path = tmp_path / "input.mcap"

    # Create a MCAP with messages
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))

    # Run inspect command with --summary
    cli_main(["inspect", str(input_path), "--summary"])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify summary section details are present
    assert "Summary Section Details" in output
    assert "Schemas" in output
    assert "Channels" in output
    assert "Chunk Indexes" in output


def test_cli_inspect_all(tmp_path: Path, capsys) -> None:
    """Test inspect command with --all flag."""
    input_path = tmp_path / "input.mcap"

    # Create a MCAP with messages
    with McapFileWriter.open(input_path, chunk_size=512) as writer:
        for i in range(20):
            writer.write_message("/test", int(1e9 + i * 1e8), Int32(data=i))

    # Run inspect command with --all
    cli_main(["inspect", str(input_path), "--all"])

    # Capture output
    captured = capsys.readouterr()
    output = captured.out

    # Verify both chunk and summary details are present
    assert "MCAP File Structure" in output
    assert "Chunk Details" in output
    assert "Summary Section Details" in output
    assert "Schemas" in output
    assert "Channels" in output
