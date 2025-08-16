from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from . import types as t
from .builtin_interfaces import *
from .std_msgs import *
from .geometry_msgs import *

@dataclass
class GridCells:
    """Class for nav_msgs/msg/GridCells."""

    header: t.Complex(Header)
    cell_width: t.float32
    cell_height: t.float32
    cells: t.Array(t.Complex(Point))
    __msgtype__: ClassVar[str] = 'nav_msgs/msg/GridCells'

@dataclass
class MapMetaData:
    """Class for nav_msgs/msg/MapMetaData."""

    map_load_time: t.Complex(Time)
    resolution: t.float32
    width: t.uint32
    height: t.uint32
    origin: t.Complex(Pose)
    __msgtype__: ClassVar[str] = 'nav_msgs/msg/MapMetaData'

@dataclass
class Odometry:
    """Class for nav_msgs/msg/Odometry."""

    header: t.Complex(Header)
    child_frame_id: t.string
    pose: t.Complex(PoseWithCovariance)
    twist: t.Complex(TwistWithCovariance)
    __msgtype__: ClassVar[str] = 'nav_msgs/msg/Odometry'

@dataclass
class Path:
    """Class for nav_msgs/msg/Path."""

    header: t.Complex(Header)
    poses: t.Array(t.Complex(PoseStamped))
    __msgtype__: ClassVar[str] = 'nav_msgs/msg/Path'

@dataclass
class OccupancyGrid:
    """Class for nav_msgs/msg/OccupancyGrid."""

    header: t.Complex(Header)
    info: t.Complex(MapMetaData)
    data: t.Array(t.int8)
    __msgtype__: ClassVar[str] = 'nav_msgs/msg/OccupancyGrid'
