from pathlib import Path
from tempfile import TemporaryDirectory

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def _find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob('*.mcap'))


def test_read_multiple_topics_and_patterns():
    typestore = get_typestore(Stores.ROS2_JAZZY)
    String = typestore.types['std_msgs/msg/String']

    with TemporaryDirectory() as temp_dir:
        with Writer(Path(temp_dir) / 'rosbags', version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            conn1 = writer.add_connection('/pose/first', String.__msgtype__, typestore=typestore)
            writer.write(conn1, 0, typestore.serialize_cdr(String(data='pose'), String.__msgtype__))
            conn2 = writer.add_connection('/cmd_vel', String.__msgtype__, typestore=typestore)
            writer.write(conn2, 1, typestore.serialize_cdr(String(data='cmd'), String.__msgtype__))

        mcap_file = _find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)
        messages = list(reader.messages(['/pose/*', '/cmd_vel']))

        assert [m.log_time for m in messages] == [0, 1]
        assert [m.data.data for m in messages] == ['pose', 'cmd']
