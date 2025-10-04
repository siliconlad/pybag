from pathlib import Path

import pybag.ros2.humble.std_msgs as std_msgs
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from pybag.cli.main import main
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

    with open(output, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        topics = {channel.topic for _, channel, _, _ in reader.iter_decoded_messages()}
        summary = reader.get_summary()

    assert topics == {"/one", "/two"}
    assert summary.statistics.message_count == 2
