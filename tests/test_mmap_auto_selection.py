"""Test automatic mmap selection based on file size."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import pytest

from pybag.mcap.record_reader import McapRecordReaderFactory


# Helper to create a test MCAP file
def create_dummy_mcap(path: Path, size_mb: int = 1):
    """Create a dummy MCAP file for testing."""
    import random
    import numpy as np
    from rosbags.rosbag2 import StoragePlugin, Writer
    from rosbags.typesys import Stores, get_typestore

    TYPESTORE = get_typestore(Stores.LATEST)
    Odometry = TYPESTORE.types["nav_msgs/msg/Odometry"]
    Header = TYPESTORE.types["std_msgs/msg/Header"]
    Time = TYPESTORE.types["builtin_interfaces/msg/Time"]
    PoseWithCovariance = TYPESTORE.types["geometry_msgs/msg/PoseWithCovariance"]
    Pose = TYPESTORE.types["geometry_msgs/msg/Pose"]
    Point = TYPESTORE.types["geometry_msgs/msg/Point"]
    Quaternion = TYPESTORE.types["geometry_msgs/msg/Quaternion"]
    TwistWithCovariance = TYPESTORE.types["geometry_msgs/msg/TwistWithCovariance"]
    Twist = TYPESTORE.types["geometry_msgs/msg/Twist"]
    Vector3 = TYPESTORE.types["geometry_msgs/msg/Vector3"]

    rng = random.Random(0)

    # Create a sample message to determine size
    sample_odom = Odometry(
        header=Header(stamp=Time(sec=0, nanosec=0), frame_id='frame_id'),
        child_frame_id='child_frame_id',
        pose=PoseWithCovariance(
            pose=Pose(
                position=Point(x=rng.random(), y=rng.random(), z=rng.random()),
                orientation=Quaternion(x=rng.random(), y=rng.random(), z=rng.random(), w=rng.random()),
            ),
            covariance=np.array([rng.random() for _ in range(36)]),
        ),
        twist=TwistWithCovariance(
            twist=Twist(
                linear=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
                angular=Vector3(x=rng.random(), y=rng.random(), z=rng.random()),
            ),
            covariance=np.array([rng.random() for _ in range(36)]),
        ),
    )

    serialized = TYPESTORE.serialize_cdr(sample_odom, Odometry.__msgtype__)
    target_size = size_mb * 1024 * 1024
    message_count = max(1, target_size // len(serialized))

    with Writer(path, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        odom_conn = writer.add_connection("/odom", Odometry.__msgtype__, typestore=TYPESTORE)
        for i in range(message_count):
            timestamp = int(i * 1_500_000_000)
            writer.write(odom_conn, timestamp, serialized)

    return next(Path(path).rglob("*.mcap"))


def test_auto_uses_file_reader_for_small_files():
    """Verify FileReader is used for small files (<512MB)."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_dummy_mcap(Path(tmpdir) / "test", size_mb=1)

        with patch('pybag.mcap.record_reader.FileReader') as mock_file_reader, \
             patch('pybag.mcap.record_reader.MmapReader') as mock_mmap_reader:

            # Make FileReader mock return a working reader
            mock_file_reader.return_value.__enter__ = lambda self: self
            mock_file_reader.return_value.__exit__ = lambda self, *args: None

            try:
                McapRecordReaderFactory.from_file(mcap)
            except Exception:
                pass  # We just care about which reader was called

            # FileReader should be called, not MmapReader
            assert mock_file_reader.called, "FileReader should be used for small files"
            assert not mock_mmap_reader.called, "MmapReader should not be used for small files"


def test_auto_uses_mmap_reader_for_large_files():
    """Verify MmapReader is used for large files (>512MB)."""
    with TemporaryDirectory() as tmpdir:
        # Create a 600MB file
        mcap = create_dummy_mcap(Path(tmpdir) / "test", size_mb=600)

        with patch('pybag.mcap.record_reader.FileReader') as mock_file_reader, \
             patch('pybag.mcap.record_reader.MmapReader') as mock_mmap_reader:

            # Make MmapReader mock return a working reader
            mock_mmap_reader.return_value.__enter__ = lambda self: self
            mock_mmap_reader.return_value.__exit__ = lambda self, *args: None

            try:
                McapRecordReaderFactory.from_file(mcap)
            except Exception:
                pass  # We just care about which reader was called

            # MmapReader should be called, not FileReader
            assert mock_mmap_reader.called, "MmapReader should be used for large files"
            assert not mock_file_reader.called, "FileReader should not be used for large files"


def test_manual_override_use_mmap_true():
    """Verify use_mmap=True forces MmapReader even for small files."""
    with TemporaryDirectory() as tmpdir:
        mcap = create_dummy_mcap(Path(tmpdir) / "test", size_mb=1)

        with patch('pybag.mcap.record_reader.FileReader') as mock_file_reader, \
             patch('pybag.mcap.record_reader.MmapReader') as mock_mmap_reader:

            mock_mmap_reader.return_value.__enter__ = lambda self: self
            mock_mmap_reader.return_value.__exit__ = lambda self, *args: None

            try:
                McapRecordReaderFactory.from_file(mcap, use_mmap=True)
            except Exception:
                pass

            assert mock_mmap_reader.called, "MmapReader should be used when use_mmap=True"
            assert not mock_file_reader.called, "FileReader should not be used when use_mmap=True"


def test_manual_override_use_mmap_false():
    """Verify use_mmap=False forces FileReader even for large files."""
    with TemporaryDirectory() as tmpdir:
        # Create a 600MB file
        mcap = create_dummy_mcap(Path(tmpdir) / "test", size_mb=600)

        with patch('pybag.mcap.record_reader.FileReader') as mock_file_reader, \
             patch('pybag.mcap.record_reader.MmapReader') as mock_mmap_reader:

            mock_file_reader.return_value.__enter__ = lambda self: self
            mock_file_reader.return_value.__exit__ = lambda self, *args: None

            try:
                McapRecordReaderFactory.from_file(mcap, use_mmap=False)
            except Exception:
                pass

            assert mock_file_reader.called, "FileReader should be used when use_mmap=False"
            assert not mock_mmap_reader.called, "MmapReader should not be used when use_mmap=False"
