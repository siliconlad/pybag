"""Tests for the convert CLI command."""

from dataclasses import dataclass
from pathlib import Path

import pytest

import pybag
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.cli.mcap_convert import convert
from pybag.cli.main import main as cli_main
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


# Define ROS1-compatible test messages (works with both bag and mcap)
@dataclass(kw_only=True)
class SimpleInt:
    """A simple int32 message compatible with both ROS1 and ROS2."""
    __msg_name__ = 'test_msgs/SimpleInt'
    data: pybag.int32


@dataclass(kw_only=True)
class SimpleString:
    """A simple string message compatible with both ROS1 and ROS2."""
    __msg_name__ = 'test_msgs/SimpleString'
    data: pybag.string


# --- bag to mcap conversion tests ---


def test_convert_bag_to_mcap_basic(tmp_path: Path) -> None:
    """Test bag to mcap conversion with multiple topics."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file with multiple topics
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/topic_a", int(1e9), SimpleInt(data=1))
        writer.write_message("/topic_b", int(2e9), SimpleString(data="hello"))
        writer.write_message("/topic_a", int(3e9), SimpleInt(data=2))

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the mcap content
    with McapFileReader.from_file(mcap_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/topic_a", "/topic_b"}

        msgs_a = list(reader.messages("/topic_a"))
        assert len(msgs_a) == 2
        assert msgs_a[0].data.data == 1
        assert msgs_a[1].data.data == 2

        msgs_b = list(reader.messages("/topic_b"))
        assert len(msgs_b) == 1
        assert msgs_b[0].data.data == "hello"


def test_convert_bag_to_mcap_empty(tmp_path: Path) -> None:
    """Test converting an empty bag file."""
    bag_path = tmp_path / "empty.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create an empty bag file
    with BagFileWriter.open(bag_path) as writer:
        pass  # No messages

    # Convert to mcap
    convert(bag_path, mcap_path)

    # Verify the mcap is valid but empty
    with McapFileReader.from_file(mcap_path) as reader:
        assert reader.get_topics() == []


# --- mcap to bag conversion tests ---


def test_convert_mcap_to_bag_basic(tmp_path: Path) -> None:
    """Test mcap to bag conversion with multiple topics."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an mcap file with multiple topics
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/topic_a", int(1e9), SimpleInt(data=1))
        writer.write_message("/topic_b", int(2e9), SimpleString(data="hello"))
        writer.write_message("/topic_a", int(3e9), SimpleInt(data=2))

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the bag content
    with BagFileReader.from_file(bag_path) as reader:
        topics = set(reader.get_topics())
        assert topics == {"/topic_a", "/topic_b"}

        msgs_a = list(reader.messages("/topic_a"))
        assert len(msgs_a) == 2
        assert msgs_a[0].data.data == 1
        assert msgs_a[1].data.data == 2

        msgs_b = list(reader.messages("/topic_b"))
        assert len(msgs_b) == 1
        assert msgs_b[0].data.data == "hello"


def test_convert_mcap_to_bag_empty(tmp_path: Path) -> None:
    """Test converting an empty mcap file."""
    mcap_path = tmp_path / "empty.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an empty mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        pass  # No messages

    # Convert to bag
    convert(mcap_path, bag_path)

    # Verify the bag is valid but empty
    with BagFileReader.from_file(bag_path) as reader:
        assert reader.get_topics() == []


# --- CLI tests ---


def test_cli_convert_bag_to_mcap(tmp_path: Path) -> None:
    """Test CLI conversion from bag to mcap."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/cli_test", int(1e9), SimpleInt(data=99))

    # Convert via CLI
    cli_main(["convert", str(bag_path), "-o", str(mcap_path)])

    # Verify
    with McapFileReader.from_file(mcap_path) as reader:
        messages = list(reader.messages("/cli_test"))
        assert len(messages) == 1
        assert messages[0].data.data == 99


def test_cli_convert_mcap_to_bag(tmp_path: Path) -> None:
    """Test CLI conversion from mcap to bag."""
    mcap_path = tmp_path / "input.mcap"
    bag_path = tmp_path / "output.bag"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/cli_test", int(1e9), SimpleInt(data=99))

    # Convert via CLI
    cli_main(["convert", str(mcap_path), "-o", str(bag_path)])

    # Verify
    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/cli_test"))
        assert len(messages) == 1
        assert messages[0].data.data == 99


def test_cli_convert_overwrite(tmp_path: Path) -> None:
    """Test CLI conversion with overwrite flag."""
    bag_path = tmp_path / "input.bag"
    mcap_path = tmp_path / "output.mcap"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    # Create an existing mcap file
    mcap_path.touch()

    # Without overwrite, should fail
    with pytest.raises(ValueError, match="Output file exists"):
        convert(bag_path, mcap_path)

    # With overwrite, should succeed
    convert(bag_path, mcap_path, overwrite=True)
    assert mcap_path.exists()


# --- Error handling tests ---


def test_convert_same_format_error(tmp_path: Path) -> None:
    """Test that converting to the same format raises an error."""
    mcap_path = tmp_path / "input.mcap"
    output_path = tmp_path / "output.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="same"):
        convert(mcap_path, output_path)


def test_convert_same_path_error(tmp_path: Path) -> None:
    """Test that converting to the same path raises an error."""
    mcap_path = tmp_path / "input.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="Use a different output format"):
        convert(mcap_path, mcap_path)


def test_convert_input_not_found(tmp_path: Path) -> None:
    """Test that a missing input file raises an error."""
    with pytest.raises(FileNotFoundError):
        convert(tmp_path / "nonexistent.bag", tmp_path / "output.mcap")


def test_convert_unknown_input_format(tmp_path: Path) -> None:
    """Test that an unknown input format raises an error."""
    input_path = tmp_path / "input.xyz"
    input_path.touch()

    with pytest.raises(ValueError, match="Cannot detect input format"):
        convert(input_path, tmp_path / "output.mcap")


def test_convert_unknown_output_format(tmp_path: Path) -> None:
    """Test that an unknown output format raises an error."""
    bag_path = tmp_path / "input.bag"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=1))

    with pytest.raises(ValueError, match="Cannot detect output format"):
        convert(bag_path, tmp_path / "output.xyz")


# --- Compression tests ---


@pytest.mark.parametrize("compression", ["lz4", "zstd"])
def test_convert_bag_to_mcap_with_compression(tmp_path: Path, compression) -> None:
    """Test bag to mcap conversion with different compression options."""
    bag_path = tmp_path / "input.bag"

    # Create a bag file
    with BagFileWriter.open(bag_path) as writer:
        for i in range(100):
            writer.write_message("/test", i * int(1e6), SimpleInt(data=i))

    mcap_lz4 = tmp_path / "output_lz4.mcap"
    convert(bag_path, mcap_lz4, mcap_compression=compression, chunk_size=1024)

    with McapFileReader.from_file(mcap_lz4) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 100


def test_convert_mcap_to_bag_with_compression(tmp_path: Path) -> None:
    """Test mcap to bag conversion with bz2 compression."""
    mcap_path = tmp_path / "input.mcap"

    # Create an mcap file
    with McapFileWriter.open(mcap_path, chunk_size=1024) as writer:
        for i in range(100):
            writer.write_message("/test", i * int(1e6), SimpleInt(data=i))

    # Convert with bz2 compression
    bag_path = tmp_path / "output.bag"
    convert(mcap_path, bag_path, bag_compression="bz2")

    with BagFileReader.from_file(bag_path) as reader:
        messages = list(reader.messages("/test"))
        assert len(messages) == 100


# --- Round-trip tests ---


def test_convert_roundtrip_bag_mcap_bag(tmp_path: Path) -> None:
    """Test that bag -> mcap -> bag preserves message data."""
    original_bag = tmp_path / "original.bag"
    mcap_path = tmp_path / "intermediate.mcap"
    final_bag = tmp_path / "final.bag"

    # Create original bag
    with BagFileWriter.open(original_bag) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=42))
        writer.write_message("/test", int(2e9), SimpleInt(data=43))

    # Convert bag -> mcap -> bag
    convert(original_bag, mcap_path)
    convert(mcap_path, final_bag)

    # Compare original and final
    with BagFileReader.from_file(original_bag) as orig:
        with BagFileReader.from_file(final_bag) as final:
            orig_msgs = list(orig.messages("/test"))
            final_msgs = list(final.messages("/test"))

            assert len(orig_msgs) == len(final_msgs)
            for o, f in zip(orig_msgs, final_msgs, strict=True):
                assert o.log_time == f.log_time
                assert o.data.data == f.data.data


def test_convert_roundtrip_mcap_bag_mcap(tmp_path: Path) -> None:
    """Test that mcap -> bag -> mcap preserves message data."""
    original_mcap = tmp_path / "original.mcap"
    bag_path = tmp_path / "intermediate.bag"
    final_mcap = tmp_path / "final.mcap"

    # Create original mcap
    with McapFileWriter.open(original_mcap, chunk_size=1024) as writer:
        writer.write_message("/test", int(1e9), SimpleInt(data=42))
        writer.write_message("/test", int(2e9), SimpleInt(data=43))

    # Convert mcap -> bag -> mcap
    convert(original_mcap, bag_path)
    convert(bag_path, final_mcap)

    # Compare original and final
    with McapFileReader.from_file(original_mcap) as orig:
        with McapFileReader.from_file(final_mcap) as final:
            orig_msgs = list(orig.messages("/test"))
            final_msgs = list(final.messages("/test"))

            assert len(orig_msgs) == len(final_msgs)
            for o, f in zip(orig_msgs, final_msgs, strict=True):
                assert o.log_time == f.log_time
                assert o.data.data == f.data.data
