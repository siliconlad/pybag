from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from . import types as t

@dataclass
class Duration:
    """Class for builtin_interfaces/msg/Duration."""

    sec: t.int32
    nanosec: t.uint32
    __msgtype__: ClassVar[str] = 'builtin_interfaces/msg/Duration'

@dataclass
class Time:
    """Class for builtin_interfaces/msg/Time."""

    sec: t.int32
    nanosec: t.uint32
    __msgtype__: ClassVar[str] = 'builtin_interfaces/msg/Time'
