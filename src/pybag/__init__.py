from importlib.metadata import version

__version__ = version("pybag-sdk")

from .reader import DecodedMessage, Reader
