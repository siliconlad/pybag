"""Test the reading of geometry_msgs messages."""
from pathlib import Path
from tempfile import TemporaryDirectory

from rosbags.rosbag2 import StoragePlugin, Writer
from rosbags.typesys import Stores, get_typestore

from pybag.mcap_reader import McapFileReader


def find_mcap_file(temp_dir: str) -> Path:
    return next(Path(temp_dir).rglob('*.mcap'))


def test_vector3_rosbags():
    # Create a typestore for the desired ROS release and get the string class.
    typestore = get_typestore(Stores.LATEST)
    Vector3 = typestore.types['geometry_msgs/msg/Vector3']

    with TemporaryDirectory() as temp_dir:
        with Writer(Path(temp_dir) / 'rosbags', version=9, storage_plugin=StoragePlugin.MCAP) as writer:
            connection = writer.add_connection('/rosbags', Vector3.__msgtype__, typestore=typestore)
            # Write first message
            msg = Vector3(x=1.0, y=2.0, z=3.0)
            serialized_msg = typestore.serialize_cdr(msg, Vector3.__msgtype__)
            writer.write(connection, 0, serialized_msg)

        mcap_file = find_mcap_file(temp_dir)
        reader = McapFileReader.from_file(mcap_file)

        messages = list(reader.messages('/rosbags'))
        assert len(messages) == 1
        assert messages[0].log_time == 0
        assert messages[0].publish_time == 0
        assert messages[0].sequence == 0
        assert messages[0].channel_id == connection.id

        # TODO: Test the actual message data
