from pathlib import Path
from tempfile import TemporaryDirectory

from pybag.mcap_reader import McapFileReader
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble import std_msgs


def test_read_multiple_files_as_one() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        file1 = temp_path / "one.mcap"
        file2 = temp_path / "two.mcap"

        with McapFileWriter.open(file1, chunk_size=1) as writer:
            writer.write_message("/chatter", 1, std_msgs.String(data="hello"))
            writer.write_message("/chatter", 3, std_msgs.String(data="again"))
        with McapFileWriter.open(file2, chunk_size=1) as writer:
            writer.write_message("/chatter", 2, std_msgs.String(data="world"))
            writer.write_message("/chatter", 4, std_msgs.String(data="!!"))

        reader = McapFileReader.from_file([file1, file2])

        assert reader.get_message_count("/chatter") == 4
        messages = list(reader.messages("/chatter"))
        assert [m.data.data for m in messages] == ["hello", "world", "again", "!!"]
        assert reader.start_time == 1
        assert reader.end_time == 4

