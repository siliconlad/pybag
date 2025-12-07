"""ROS2 builtin_interfaces message types for humble.

Auto-generated with embedded encode/decode methods.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pybag.types as t


@dataclass(kw_only=True)
class Duration:
    __msg_name__ = 'builtin_interfaces/msg/Duration'

    sec: t.int32
    nanosec: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        _fields['sec'] = struct.unpack(fmt_prefix + 'i', _data.read(4))[0]
        _data.align(4)
        _fields['nanosec'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        return Duration(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'i', self.sec))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.nanosec))


@dataclass(kw_only=True)
class Time:
    __msg_name__ = 'builtin_interfaces/msg/Time'

    sec: t.int32
    nanosec: t.uint32

    @staticmethod
    def decode(decoder):
        """Decode message from decoder."""
        fmt_prefix = '<' if decoder._is_little_endian else '>'
        _data = decoder._data
        _fields = {}

        _data.align(4)
        _fields['sec'] = struct.unpack(fmt_prefix + 'i', _data.read(4))[0]
        _data.align(4)
        _fields['nanosec'] = struct.unpack(fmt_prefix + 'I', _data.read(4))[0]
        return Time(**_fields)

    def encode(self, encoder):
        """Encode message to encoder."""
        fmt_prefix = '<' if encoder._is_little_endian else '>'
        _payload = encoder._payload

        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'i', self.sec))
        _payload.align(4)
        _payload.write(struct.pack(fmt_prefix + 'I', self.nanosec))


