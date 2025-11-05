from pathlib import Path

from pybag.cli.main import main as cli_main
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Empty, Int32


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
        writer.write_message("/empty", int(1e9), Empty())
        writer.write_message("/foo", int(2e9), Int32(data=42))
        writer.write_message("/empty", int(3e9), Empty())

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


def test_cli_filter_glob_with_exclude(tmp_path: Path) -> None:
    """Test that exclude filters work correctly with glob patterns in include.

    This ensures that exclusions are applied AFTER glob expansion, not before.
    """
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"

    # Create MCAP with multiple topics matching a glob pattern
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo/a", int(1e9), Int32(data=1))
        writer.write_message("/foo/b", int(2e9), Int32(data=2))
        writer.write_message("/foo/c", int(3e9), Int32(data=3))
        writer.write_message("/bar", int(4e9), Int32(data=4))

    # Filter to include all /foo/* topics but exclude /foo/b
    cli_main(
        [
            "filter",
            str(input_path),
            "--include-topic",
            "/foo/*",
            "--exclude-topic",
            "/foo/b",
            "--output",
            str(output_path),
        ]
    )

    # Verify the output - should have /foo/a and /foo/c but NOT /foo/b
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        expected_topics = {"/foo/a", "/foo/c"}
        assert topics == expected_topics, f"Expected {expected_topics}, got {topics}"

        # Verify messages
        messages_a = list(reader.messages("/foo/a"))
        assert len(messages_a) == 1
        assert messages_a[0].data.data == 1

        messages_c = list(reader.messages("/foo/c"))
        assert len(messages_c) == 1
        assert messages_c[0].data.data == 3
