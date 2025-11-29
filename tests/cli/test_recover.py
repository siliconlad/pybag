from pathlib import Path

import pytest
from pybag.cli.main import main as cli_main
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32


def test_cli_recover_valid_file(tmp_path: Path) -> None:
    """Test that recovery works on a valid MCAP file."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid MCAP file
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))

    # Run recovery command
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
        ]
    )

    # Verify that all messages were recovered
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/foo", "/bar"}

        foo_messages = list(reader.messages("/foo"))
        assert len(foo_messages) == 2
        assert [m.data.data for m in foo_messages] == [1, 3]

        bar_messages = list(reader.messages("/bar"))
        assert len(bar_messages) == 1
        assert [m.data.data for m in bar_messages] == [2]


def test_cli_recover_with_compression(tmp_path: Path) -> None:
    """Test recovery with compression options."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create a valid MCAP file
    with McapFileWriter.open(input_path, chunk_size=1024, chunk_compression="lz4") as writer:
        for i in range(10):
            writer.write_message("/test", int((i + 1) * 1e9), Int32(data=i))

    # Run recovery command with zstd compression
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
            "--chunk-compression",
            "zstd",
            "--chunk-size",
            "512",
        ]
    )

    # Verify that all messages were recovered
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 10
        assert [m.data.data for m in messages] == list(range(10))


def test_cli_recover_overwrite(tmp_path: Path) -> None:
    """Test that recovery respects overwrite flag."""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "recovered.mcap"

    # Create input file
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), Int32(data=1))

    # Create existing output file
    with McapFileWriter.open(output_path, chunk_size=1024) as writer:
        writer.write_message("/old", int(1e9), Int32(data=999))

    # Try to recover without overwrite flag - should raise error
    with pytest.raises(ValueError):
        cli_main(
            [
                "recover",
                str(input_path),
                "--output",
                str(output_path),
            ]
        )

    # Now recover with overwrite flag - should succeed
    cli_main(
        [
            "recover",
            str(input_path),
            "--output",
            str(output_path),
            "--overwrite",
        ]
    )

    # Verify the output has the new data
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/test"}  # Not /old
        messages = list(reader.messages("/test"))
        assert len(messages) == 1
        assert messages[0].data.data == 1  # Not 999
