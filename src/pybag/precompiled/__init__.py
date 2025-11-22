"""Pre-compiled ROS2 message encoders/decoders.

This package contains pre-compiled encoder and decoder functions for standard
ROS2 message types to improve first-use performance.
"""

from __future__ import annotations

from typing import Any, Callable

# Lazy import to avoid circular dependencies
_humble_initialized = False


def get_decoder(msg_name: str) -> Callable[[Any], type] | None:
    """Get pre-compiled decoder function for a message type.

    Args:
        msg_name: The full message name (e.g., 'std_msgs/msg/Header')

    Returns:
        The pre-compiled decoder function, or None if not available
    """
    global _humble_initialized

    try:
        from pybag.precompiled import humble

        if not _humble_initialized:
            humble.initialize_dataclass_types()
            _humble_initialized = True

        return humble.get_decoder(msg_name)
    except ImportError:
        return None


def get_encoder(msg_name: str) -> Callable[[Any, Any], None] | None:
    """Get pre-compiled encoder function for a message type.

    Args:
        msg_name: The full message name (e.g., 'std_msgs/msg/Header')

    Returns:
        The pre-compiled encoder function, or None if not available
    """
    global _humble_initialized

    try:
        from pybag.precompiled import humble

        if not _humble_initialized:
            humble.initialize_dataclass_types()
            _humble_initialized = True

        return humble.get_encoder(msg_name)
    except ImportError:
        return None


__all__ = ['get_decoder', 'get_encoder']
