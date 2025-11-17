from importlib.metadata import version, PackageNotFoundError

from .types import (
    Array,
    Complex,
    Constant,
    Message,
    bool,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    string,
    uint8,
    uint16,
    uint32,
    uint64,
    wstring
)

try:
    __version__ = version("pybag-sdk")
except PackageNotFoundError:
    __version__ = "0.5.0-dev"
