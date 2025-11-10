from pathlib import Path

from pybag.cli.main import main as cli_main
from pybag.mcap_writer import McapFileWriter
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
