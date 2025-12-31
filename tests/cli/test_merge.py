from pathlib import Path

import pytest

import pybag.ros1.noetic.std_msgs as ros1_std_msgs
import pybag.ros2.humble.std_msgs as std_msgs
from pybag.bag_reader import BagFileReader
from pybag.bag_writer import BagFileWriter
from pybag.cli.main import main
from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter


def test_merge_cli(tmp_path: Path) -> None:
    input1 = tmp_path / "one.mcap"
    input2 = tmp_path / "two.mcap"
    output = tmp_path / "merged.mcap"

    with McapFileWriter.open(input1) as writer:
        writer.write_message("/one", 1, std_msgs.String(data="a"))
    with McapFileWriter.open(input2) as writer:
        writer.write_message("/two", 2, std_msgs.String(data="b"))

    main(["merge", str(input1), str(input2), "-o", str(output)])

    with McapFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == set(["/one", "/two"])
        # Get /one topic message
        messages = list(reader.messages("/one"))
        assert len(messages) == 1
        # Get /two topic message
        messages = list(reader.messages("/two"))
        assert len(messages) == 1


def test_merge_cli_same_topic(tmp_path: Path) -> None:
    input1 = tmp_path / "one.mcap"
    input2 = tmp_path / "two.mcap"
    output = tmp_path / "merged.mcap"

    with McapFileWriter.open(input1) as writer:
        writer.write_message("/one", 1, std_msgs.String(data="a"))
    with McapFileWriter.open(input2) as writer:
        writer.write_message("/one", 2, std_msgs.String(data="b"))

    main(["merge", str(input1), str(input2), "-o", str(output)])

    with McapFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == set(["/one"])
        # Get /one topic message
        messages = list(reader.messages("/one"))
        assert len(messages) == 2


def test_merge_bag_cli(tmp_path: Path) -> None:
    """Test merging two bag files with different topics."""
    input1 = tmp_path / "one.bag"
    input2 = tmp_path / "two.bag"
    output = tmp_path / "merged.bag"

    with BagFileWriter.open(input1) as writer:
        writer.write_message("/one", 1_000_000_000, ros1_std_msgs.String(data="a"))
    with BagFileWriter.open(input2) as writer:
        writer.write_message("/two", 2_000_000_000, ros1_std_msgs.String(data="b"))

    main(["merge", str(input1), str(input2), "-o", str(output)])

    with BagFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == {"/one", "/two"}
        # Get /one topic message
        messages = list(reader.messages("/one"))
        assert len(messages) == 1
        assert messages[0].data.data == "a"
        # Get /two topic message
        messages = list(reader.messages("/two"))
        assert len(messages) == 1
        assert messages[0].data.data == "b"


def test_merge_bag_cli_same_topic(tmp_path: Path) -> None:
    """Test merging two bag files with the same topic."""
    input1 = tmp_path / "one.bag"
    input2 = tmp_path / "two.bag"
    output = tmp_path / "merged.bag"

    with BagFileWriter.open(input1) as writer:
        writer.write_message("/one", 1_000_000_000, ros1_std_msgs.String(data="a"))
    with BagFileWriter.open(input2) as writer:
        writer.write_message("/one", 2_000_000_000, ros1_std_msgs.String(data="b"))

    main(["merge", str(input1), str(input2), "-o", str(output)])

    with BagFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == {"/one"}
        # Get /one topic messages - should have 2 messages merged in time order
        messages = list(reader.messages("/one"))
        assert len(messages) == 2
        assert messages[0].data.data == "a"
        assert messages[1].data.data == "b"


def test_merge_bag_cli_with_compression(tmp_path: Path) -> None:
    """Test merging bag files with bz2 compression."""
    input1 = tmp_path / "one.bag"
    input2 = tmp_path / "two.bag"
    output = tmp_path / "merged.bag"

    with BagFileWriter.open(input1) as writer:
        writer.write_message("/one", 1_000_000_000, ros1_std_msgs.String(data="a"))
    with BagFileWriter.open(input2) as writer:
        writer.write_message("/two", 2_000_000_000, ros1_std_msgs.String(data="b"))

    main(["merge", str(input1), str(input2), "-o", str(output), "--chunk-compression", "bz2"])

    with BagFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == {"/one", "/two"}
        messages = list(reader.messages(["/one", "/two"]))
        assert len(messages) == 2


def test_merge_bag_empty_file(tmp_path: Path) -> None:
    """Test merging when one file has no messages."""
    input1 = tmp_path / "one.bag"
    input2 = tmp_path / "two.bag"
    output = tmp_path / "merged.bag"

    # File 1 has messages
    with BagFileWriter.open(input1) as writer:
        writer.write_message("/one", 1_000_000_000, ros1_std_msgs.String(data="a"))
    # File 2 is empty
    with BagFileWriter.open(input2):
        pass

    main(["merge", str(input1), str(input2), "-o", str(output)])

    with BagFileReader.from_file(output) as reader:
        assert set(reader.get_topics()) == {"/one"}
        messages = list(reader.messages("/one"))
        assert len(messages) == 1


def test_merge_mixed_formats_fails(tmp_path: Path) -> None:
    """Test that merging mixed formats raises an error."""
    mcap_file = tmp_path / "file.mcap"
    bag_file = tmp_path / "file.bag"
    output = tmp_path / "merged.bag"

    with McapFileWriter.open(mcap_file) as writer:
        writer.write_message("/one", 1, std_msgs.String(data="a"))
    with BagFileWriter.open(bag_file) as writer:
        writer.write_message("/two", 2_000_000_000, ros1_std_msgs.String(data="b"))

    with pytest.raises(ValueError, match="All input files must have the same format"):
        main(["merge", str(mcap_file), str(bag_file), "-o", str(output)])
