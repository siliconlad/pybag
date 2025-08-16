from dataclasses import dataclass

import pybag.types as t
from .builtin_interfaces import *
from .std_msgs import *
from .geometry_msgs import *


@dataclass
class GridCells:
    __msg_name__ = 'nav_msgs/msg/GridCells'

    header: t.Complex(Header)
    cell_width: t.float32
    cell_height: t.float32
    cells: t.Array(t.Complex(Point))


@dataclass
class MapMetaData:
    __msg_name__ = 'nav_msgs/msg/MapMetaData'

    map_load_time: t.Complex(Time)
    resolution: t.float32
    width: t.uint32
    height: t.uint32
    origin: t.Complex(Pose)


@dataclass
class Odometry:
    __msg_name__ = 'nav_msgs/msg/Odometry'

    header: t.Complex(Header)
    child_frame_id: t.string
    pose: t.Complex(PoseWithCovariance)
    twist: t.Complex(TwistWithCovariance)


@dataclass
class Path:
    __msg_name__ = 'nav_msgs/msg/Path'

    header: t.Complex(Header)
    poses: t.Array(t.Complex(PoseStamped))


@dataclass
class OccupancyGrid:
    __msg_name__ = 'nav_msgs/msg/OccupancyGrid'

    header: t.Complex(Header)
    info: t.Complex(MapMetaData)
    data: t.Array(t.int8)
