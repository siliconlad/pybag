from pathlib import Path

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def test_mcap_reader_context_manager(tmp_path: Path) -> None:
    typestore = get_typestore(Stores.ROS2_HUMBLE)
    Bool = typestore.types['std_msgs/msg/Bool']
    msg = Bool(data=True)

    bag_dir = tmp_path / 'rosbags'
    with Writer(bag_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        connection = writer.add_connection('/bool', msg.__msgtype__, typestore=typestore)
        writer.write(connection, 0, typestore.serialize_cdr(msg, msg.__msgtype__))

    mcap_file = next(bag_dir.rglob('*.mcap'))

    with McapFileReader.from_file(mcap_file) as reader:
        messages = list(reader.messages('/bool'))

    assert len(messages) == 1
    assert messages[0].data.data is True
