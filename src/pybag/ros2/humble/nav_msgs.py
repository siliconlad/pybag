from dataclasses import dataclass

import pybag.ros2.humble.builtin_interfaces as builtin_interfaces
import pybag.ros2.humble.geometry_msgs as geometry_msgs
import pybag.ros2.humble.std_msgs as std_msgs
import pybag.types as t


@dataclass(kw_only=True)
class Goals:
    __msg_name__ = 'nav_msgs/msg/Goals'

    header: t.Complex[std_msgs.Header]
    goals: t.Array[t.Complex[geometry_msgs.PoseStamped]]


@dataclass(kw_only=True)
class GridCells:
    __msg_name__ = 'nav_msgs/msg/GridCells'

    header: t.Complex[std_msgs.Header]
    cell_width: t.float32
    cell_height: t.float32
    cells: t.Array[t.Complex[geometry_msgs.Point]]


@dataclass(kw_only=True)
class MapMetaData:
    __msg_name__ = 'nav_msgs/msg/MapMetaData'

    map_load_time: t.Complex[builtin_interfaces.Time]
    resolution: t.float32
    width: t.uint32
    height: t.uint32
    origin: t.Complex[geometry_msgs.Pose]


@dataclass(kw_only=True)
class OccupancyGrid:
    __msg_name__ = 'nav_msgs/msg/OccupancyGrid'

    header: t.Complex[std_msgs.Header]
    info: t.Complex[MapMetaData]
    data: t.Array[t.int8]


@dataclass(kw_only=True)
class Odometry:
    __msg_name__ = 'nav_msgs/msg/Odometry'

    header: t.Complex[std_msgs.Header]
    child_frame_id: t.string
    pose: t.Complex[geometry_msgs.PoseWithCovariance]
    twist: t.Complex[geometry_msgs.TwistWithCovariance]


@dataclass(kw_only=True)
class Path:
    __msg_name__ = 'nav_msgs/msg/Path'

    header: t.Complex[std_msgs.Header]
    poses: t.Array[t.Complex[geometry_msgs.PoseStamped]]
