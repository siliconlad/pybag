from pathlib import Path

from pybag.cli.main import main as cli_main
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32, Empty


def _create_mcap(path: Path) -> None:
    with McapFileWriter.open(path, chunk_size=1024) as writer:
        writer.write_message("/foo", int(1e9), Int32(data=1))
        writer.write_message("/bar", int(2e9), Int32(data=2))
        writer.write_message("/foo", int(3e9), Int32(data=3))


def test_cli_filter_include_and_time(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"
    _create_mcap(input_path)

    cli_main(
        [
            "filter",
            str(input_path),
            "--include-topic",
            "/foo",
            "--start-time",
            "2",
            "--end-time",
            "4",
            "--output",
            str(output_path),
        ]
    )

    with McapFileReader.from_file(output_path) as reader:
        assert set(reader.get_topics()) == {"/foo"}

        messages = list(reader.messages("/foo"))
        assert [m.log_time for m in messages] == [int(3e9)]
        assert [m.data.data for m in messages] == [3]


def test_cli_filter_exclude(tmp_path: Path) -> None:
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"
    _create_mcap(input_path)

    cli_main(
        [
            "filter",
            str(input_path),
            "--exclude-topic",
            "/bar",
            "--output",
            str(output_path),
        ]
    )

    with McapFileReader.from_file(output_path) as reader:
        assert set(reader.get_topics()) == {"/foo"}

        messages = list(reader.messages("/foo"))
        assert [m.log_time for m in messages] == [int(1e9), int(3e9)]
        assert [m.data.data for m in messages] == [1, 3]


def test_cli_filter_empty_messages(tmp_path: Path) -> None:
    """Test filtering MCAP files with Empty messages"""
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"

    # Create MCAP with Empty messages
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/empty", int(1e9), Empty(structure_needs_at_least_one_member=0))
        writer.write_message("/foo", int(2e9), Int32(data=42))
        writer.write_message("/empty", int(3e9), Empty(structure_needs_at_least_one_member=0))

    # Filter to include only /empty topic
    cli_main(
        [
            "filter",
            str(input_path),
            "--include-topic",
            "/empty",
            "--output",
            str(output_path),
        ]
    )

    # Verify the output
    with McapFileReader.from_file(output_path) as reader:
        assert set(reader.get_topics()) == {"/empty"}

        messages = list(reader.messages("/empty"))
        assert len(messages) == 2
        assert [m.log_time for m in messages] == [int(1e9), int(3e9)]
        # Empty messages should be proper dataclass instances, not None
        assert hasattr(messages[0].data, '__msg_name__')
        assert messages[0].data.__msg_name__ == 'std_msgs/msg/Empty'
        assert hasattr(messages[1].data, '__msg_name__')
        assert messages[1].data.__msg_name__ == 'std_msgs/msg/Empty'
