from pathlib import Path

import pybag.ros2.humble.std_msgs as std_msgs
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
