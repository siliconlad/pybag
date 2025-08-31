from dataclasses import dataclass

import pybag.types as t


@dataclass(kw_only=True)
class Duration:
    __msg_name__ = 'builtin_interfaces/msg/Duration'

    sec: t.int32
    nanosec: t.uint32


@dataclass(kw_only=True)
class Time:
    __msg_name__ = 'builtin_interfaces/msg/Time'

    sec: t.int32
    nanosec: t.uint32
