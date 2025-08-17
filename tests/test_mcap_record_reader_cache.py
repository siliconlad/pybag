from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap.record_parser import McapRecordParser
from pybag.mcap.record_reader import McapRecordRandomAccessReader


def _write_mcap(temp_dir: str) -> Path:
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    Bool = typestore.types["std_msgs/msg/Bool"]
    msg = Bool(data=True)
    with Writer(Path(temp_dir) / "rosbags", version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        conn = writer.add_connection("/rosbags", msg.__msgtype__, typestore=typestore)
        writer.write(conn, 0, typestore.serialize_cdr(msg, msg.__msgtype__))
    return next(Path(temp_dir).rglob("*.mcap"))


def test_chunk_and_message_index_caching() -> None:
    with TemporaryDirectory() as temp_dir:
        mcap = _write_mcap(temp_dir)
        with mock.patch.object(
            McapRecordParser,
            "parse_chunk_index",
            wraps=McapRecordParser.parse_chunk_index,
        ) as mock_chunk, mock.patch.object(
            McapRecordParser,
            "parse_message_index",
            wraps=McapRecordParser.parse_message_index,
        ) as mock_msg:
            reader = McapRecordRandomAccessReader.from_file(mcap)
            chunk = reader.get_chunk_indexes()[0]
            reader.get_message_indexes(chunk)
            reader.get_chunk_indexes()
            reader.get_message_indexes(chunk)
            reader.get_chunk_indexes()
        assert mock_chunk.call_count == 1
        assert mock_msg.call_count == 1
