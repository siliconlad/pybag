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


def test_cli_filter_exclude_with_glob(tmp_path: Path) -> None:
    """Test that glob patterns work in exclude filters.

    This ensures that --exclude-topic with wildcards actually excludes matching topics.
    """
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"

    # Create MCAP with multiple topics
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/foo/a", int(1e9), Int32(data=1))
        writer.write_message("/foo/b", int(2e9), Int32(data=2))
        writer.write_message("/bar", int(3e9), Int32(data=3))

    # Filter to exclude all /foo/* topics using glob pattern
    cli_main(
        [
            "filter",
            str(input_path),
            "--exclude-topic",
            "/foo/*",
            "--output",
            str(output_path),
        ]
    )

    # Verify the output - should only have /bar (all /foo/* should be excluded)
    with McapFileReader.from_file(output_path) as reader:
        topics = set(reader.get_topics())
        expected_topics = {"/bar"}
        assert topics == expected_topics, f"Expected {expected_topics}, got {topics}"

        # Verify message
        messages = list(reader.messages("/bar"))
        assert len(messages) == 1
        assert messages[0].data.data == 3


def test_cli_filter_preserves_publish_time(tmp_path: Path) -> None:
    """Test that filtering preserves both log_time and publish_time.

    Many MCAPs (e.g. rosbag2 recordings) have distinct log_time and publish_time,
    and downstream tooling relies on both being preserved accurately.
    """
    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"

    # Create MCAP with messages that have different log_time and publish_time
    log_time_1 = int(1e9)
    publish_time_1 = int(1.5e9)  # Different from log_time
    log_time_2 = int(2e9)
    publish_time_2 = int(2.7e9)  # Different from log_time

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        # Write messages with different log_time and publish_time using the public API
        writer.write_message("/test", log_time_1, Int32(data=1), publish_time=publish_time_1)
        writer.write_message("/test", log_time_2, Int32(data=2), publish_time=publish_time_2)

    # Filter the MCAP (should preserve all timestamps)
    cli_main(
        [
            "filter",
            str(input_path),
            "--output",
            str(output_path),
        ]
    )

    # Verify that both log_time and publish_time are preserved
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 2

        # Check first message
        assert messages[0].log_time == log_time_1
        assert messages[0].publish_time == publish_time_1, \
            f"Expected publish_time {publish_time_1}, got {messages[0].publish_time}"
        assert messages[0].data.data == 1

        # Check second message
        assert messages[1].log_time == log_time_2
        assert messages[1].publish_time == publish_time_2, \
            f"Expected publish_time {publish_time_2}, got {messages[1].publish_time}"
        assert messages[1].data.data == 2


def test_cli_filter_preserves_constants(tmp_path: Path) -> None:
    """Test that filtering preserves schema constants.

    When filtering an MCAP, the constants defined in message schemas should be
    preserved in the output file. This is critical for schema compatibility.

    For example, sensor_msgs/BatteryState has POWER_SUPPLY_* constants that
    downstream tools rely on.
    """
    from pybag.ros2.humble.sensor_msgs import BatteryState
    from pybag.ros2.humble.std_msgs import Header
    from pybag.ros2.humble.builtin_interfaces import Time

    input_path = tmp_path / "input.mcap"
    output_path = tmp_path / "out.mcap"

    # Write an MCAP with BatteryState message (which has constants)
    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message(
            "/battery",
            int(1e9),
            BatteryState(
                header=Header(stamp=Time(sec=0, nanosec=0), frame_id="battery"),
                voltage=12.0,
                temperature=25.0,
                current=1.5,
                charge=10.0,
                capacity=100.0,
                design_capacity=100.0,
                percentage=0.1,
                power_supply_status=BatteryState.POWER_SUPPLY_STATUS_DISCHARGING,
                power_supply_health=BatteryState.POWER_SUPPLY_HEALTH_GOOD,
                power_supply_technology=BatteryState.POWER_SUPPLY_TECHNOLOGY_UNKNOWN,
                present=True,
                cell_voltage=[3.7, 3.7, 3.7],
                cell_temperature=[25.0, 25.0, 25.0],
                location="base",
                serial_number="123456"
            )
        )

    # Filter the MCAP
    cli_main([
        "filter",
        str(input_path),
        "--output",
        str(output_path),
    ])

    # Read the filtered MCAP and verify constants are preserved on the message object
    with McapFileReader.from_file(output_path) as reader:
        messages = list(reader.messages("/battery"))
        assert len(messages) == 1, "Expected 1 message in filtered MCAP"

        msg = messages[0].data

        # Verify that the decoded message has all the POWER_SUPPLY_* constants
        assert hasattr(msg, "POWER_SUPPLY_STATUS_UNKNOWN")
        assert hasattr(msg, "POWER_SUPPLY_STATUS_CHARGING")
        assert hasattr(msg, "POWER_SUPPLY_STATUS_DISCHARGING")
        assert hasattr(msg, "POWER_SUPPLY_HEALTH_GOOD")
        assert hasattr(msg, "POWER_SUPPLY_TECHNOLOGY_UNKNOWN")

        # Verify constant values are correct
        assert msg.POWER_SUPPLY_STATUS_UNKNOWN == 0
        assert msg.POWER_SUPPLY_STATUS_CHARGING == 1
        assert msg.POWER_SUPPLY_STATUS_DISCHARGING == 2
        assert msg.POWER_SUPPLY_HEALTH_GOOD == 1
        assert msg.POWER_SUPPLY_TECHNOLOGY_UNKNOWN == 0
