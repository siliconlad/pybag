import pytest

import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.sensor_msgs as sensor_msgs
import pybag.ros2.humble.std_msgs as std_msgs


def _make_pose() -> geometry_msgs.Pose:
    return geometry_msgs.Pose(
        position=geometry_msgs.Point(x=0.0, y=0.0, z=0.0),
        orientation=geometry_msgs.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )


@pytest.mark.parametrize("value", [-1, 256])
def test_uint8_out_of_range(value: int) -> None:
    with pytest.raises(ValueError, match="between 0 and 255"):
        std_msgs.UInt8(data=value)


def test_pose_with_covariance_enforces_length() -> None:
    pose = _make_pose()
    with pytest.raises(ValueError, match="covariance"):
        geometry_msgs.PoseWithCovariance(pose=pose, covariance=[0.0] * 35)


def test_pose_with_covariance_accepts_valid_length() -> None:
    pose = _make_pose()
    msg = geometry_msgs.PoseWithCovariance(pose=pose, covariance=[0.0] * 36)
    assert len(msg.covariance) == 36


def test_constant_field_cannot_be_overridden() -> None:
    with pytest.raises(ValueError, match="TYPE_LED"):
        sensor_msgs.JoyFeedback(TYPE_LED=1, type=0, id=0, intensity=0.0)


def test_complex_field_requires_expected_type() -> None:
    with pytest.raises(TypeError, match="orientation"):
        geometry_msgs.Pose(
            position=geometry_msgs.Point(x=0.0, y=0.0, z=0.0),
            orientation="not a quaternion",
        )


def test_array_elements_are_validated() -> None:
    with pytest.raises(TypeError, match="array"):
        sensor_msgs.JoyFeedbackArray(array=[object()])


def test_byte_field_validates_length() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        std_msgs.Byte(data=b"too long")
