class McapError(Exception):
    """Base exception for all MCAP errors."""


class McapNoChunkIndexError(McapError):
    """Exception raised when a MCAP file has no chunk index records."""
    def __init__(self, message: str):
        super().__init__(message)


class McapNoChunkError(McapError):
    """Exception raised when a MCAP file has no chunk records."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnexpectedChunkIndexError(McapError):
    """Exception raised when a MCAP file has a chunk index record."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnexpectedChunkError(McapError):
    """Exception raised when a MCAP file has a chunk record."""
    def __init__(self, message: str):
        super().__init__(message)


class McapUnexpectedRecordError(McapError):
    """Exception raised when a record is found in an unexpected place."""
    def __init__(self, message: str):
        super().__init__(message)


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
