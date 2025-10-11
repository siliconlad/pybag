from typing import Literal, TypeAlias

from pybag.io.raw_reader import BaseReader
from pybag.mcap.records import (
    ChannelRecord,
    ChunkIndexRecord,
    SchemaRecord,
    StatisticsRecord,
    SummaryOffsetRecord
)


ChannelId: TypeAlias = int
"""Integer representing the channel ID."""

SchemaId: TypeAlias = int
"""Integer representing the schema ID."""

RecordId: TypeAlias = int
"""ID of an MCAP record (i.e. the record type)."""

LogTime: TypeAlias = int
"""Integer representing the log time."""

Offset: TypeAlias = int
"""Integer representing an offset from the start of a file/bytes."""

class McapChunkedSummary:
    def __init__(
        self,
        file: BaseReader,
        *,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:

    @property
    def schemas(self) -> dict[SchemaId, SchemaRecord]:
        # TODO: Implement

    @property
    def channels(self) -> dict[ChannelId, ChannelRecord]:
        # TODO: Implement

    @property
    def chunk_indexes(self) -> list[ChunkIndexRecord]:
        # TODO: Implement

    @property
    def statistics(self) -> StatisticsRecord:
        # TODO: Implement

    @property
    def offsets(self) -> dict[RecordId, SummaryOffsetRecord]:
        # TODO: Implement

    # TODO: Implement attachment index
    # TODO: Implement metadata index


class McapNonChunkedSummary:
    def __init__(
        self,
        file: BaseReader,
        *,
        enable_reconstruction: Literal['never', 'missing', 'always'] = 'missing',
    ) -> None:
        # Summary section start
        self._summary_start = footer.summary_start
        if self._summary_start == 0:
            error_msg = 'No summary section detected in MCAP'
            raise McapNoSummarySectionError(error_msg)

        # Summary offset section start
        self._summary_offset_start = footer.summary_offset_start
        if self._summary_offset_start == 0:
            error_msg = 'No summary offset section detected in MCAP'
            raise McapNoSummaryIndexError(error_msg)

    @property
    def schemas(self) -> dict[SchemaId, SchemaRecord]:
        # TODO: Implement

    @property
    def channels(self) -> dict[ChannelId, ChannelRecord]:
        # TODO: Implement

    @property
    def statistics(self) -> StatisticsRecord:
        # TODO: Implement

    @property
    def offsets(self) -> dict[RecordId, SummaryOffsetRecord]:
        # TODO: Implement

    @property
    def message_indexes(self) -> dict[ChannelId, dict[LogTime, list[Offset]]]:
        # TODO: Implement
