class McapError(Exception):
    """Base exception for all MCAP errors."""


class McapNoSummarySectionError(McapError):
    """Exception raised when a MCAP file has no summary section."""
    def __init__(self, message: str):
        super().__init__(message)


class McapNoSummaryIndexError(McapError):
    """Exception raised when a MCAP file has no summary index."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownSchemaError(McapError):
    """Exception raised when a MCAP file has an unknown schema."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownCompressionError(McapError):
    """Exception raised when a MCAP file has an unknown compression type."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownEncodingError(McapError):
    """Exception raised when a MCAP file has an unknown encoding type."""
    def __init__(self, message: str):
        super().__init__(message)


class McapNoStatisticsError(McapError):
    """Exception raised when a MCAP file has no statistics."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnknownTopicError(McapError):
    """Exception raised when a topic is not found in a MCAP file."""
    def __init__(self, message: str):
        super().__init__(message)
