from enum import Enum
from pathlib import Path

import pytest
from mcap.writer import CompressionType, IndexType, Writer
from mcap_ros2.writer import SchemaEncoding, serialize_dynamic

from pybag.io.raw_reader import FileReader
from pybag.io.raw_writer import BytesWriter
from pybag.mcap.record_parser import (
    FOOTER_SIZE,
    MAGIC_BYTES_SIZE,
    McapRecordParser
)
from pybag.mcap.record_writer import McapRecordWriter
from pybag.mcap.records import FooterRecord
from pybag.mcap_reader import McapFileReader


def _read_footer(path: Path) -> FooterRecord:
    reader = FileReader(path)
    try:
        _ = reader.seek_from_end(FOOTER_SIZE + MAGIC_BYTES_SIZE)
        return McapRecordParser.parse_footer(reader)
    finally:
        reader.close()


def _strip_summary(path: Path) -> None:
    footer = _read_footer(path)
    if footer.summary_start == 0:
        return

    payload = bytearray(path.read_bytes())
    truncated = payload[:footer.summary_start]

    footer_writer = BytesWriter()
    McapRecordWriter.write_footer(
        footer_writer,
        FooterRecord(summary_start=0, summary_offset_start=0, summary_crc=0),
    )
    _ = McapRecordWriter.write_magic_bytes(footer_writer)

    truncated.extend(footer_writer.as_bytes())
    _ = path.write_bytes(truncated)


def _write_mcap(
    path: Path,
    messages: list[tuple[int, str]],
    *,
    chunk_size: int = 64,
    compression: CompressionType = CompressionType.ZSTD,
    index_types: IndexType = IndexType.ALL,
    repeat_channels: bool = True,
    repeat_schemas: bool = True,
    use_chunking: bool = True,
    use_statistics: bool = True,
    use_summary_offsets: bool = True,
    use_summary: bool = True,
    output_topic: str = '/topic',
) -> None:
    schema_name = 'std_msgs/msg/String'
    schema_text = 'string data\n'
    encoder = serialize_dynamic(schema_name, schema_text)[schema_name]

    with path.open('wb') as stream:
        writer = Writer(
            stream,
            chunk_size=chunk_size,
            compression=compression,
            repeat_channels=repeat_channels,
            repeat_schemas=repeat_schemas,
            use_chunking=use_chunking,
            use_statistics=use_statistics,
            use_summary_offsets=use_summary_offsets,
            index_types=index_types,
        )
        writer.start(profile='ros2', library='pybag-test')

        schema_id = writer.register_schema(
            name=schema_name,
            encoding=SchemaEncoding.ROS2,
            data=schema_text.encode(),
        )
        channel_id = writer.register_channel(
            topic=output_topic,
            message_encoding='cdr',
            schema_id=schema_id,
        )

        for timestamp, message in messages:
            payload = encoder({'data': message})
            writer.add_message(
                channel_id=channel_id,
                log_time=timestamp,
                publish_time=timestamp,
                data=payload,
            )

        writer.finish()

    if not use_summary:
        _strip_summary(path)


def _bool_params(name: str) -> list[object]:
    return [
        pytest.param(False, id=f'no_{name}'),
        pytest.param(True, id=f'with_{name}'),
    ]


def _enum_params(e: type[Enum]) -> list[object]:
    return [
        pytest.param(e_value, id=f'{e.__name__}_{e_value.name.lower()}')
        for e_value in e
    ]


@pytest.mark.parametrize('compression', _enum_params(CompressionType))
@pytest.mark.parametrize('index_types', _enum_params(IndexType))
@pytest.mark.parametrize('repeat_channels', _bool_params('repeat_channels'))
@pytest.mark.parametrize('repeat_schemas', _bool_params('repeat_schemas'))
@pytest.mark.parametrize('use_chunking', _bool_params('chunking'))
@pytest.mark.parametrize('use_statistics', _bool_params('statistics'))
@pytest.mark.parametrize('use_summary_offsets', _bool_params('summary_offsets'))
@pytest.mark.parametrize('use_summary', _bool_params('summary'))
def test_read_messages_across_writer_configs(
    tmp_path: Path,
    compression: CompressionType,
    index_types: IndexType,
    repeat_channels: bool,
    repeat_schemas: bool,
    use_chunking: bool,
    use_statistics: bool,
    use_summary_offsets: bool,
    use_summary: bool,
) -> None:
    output_topic = '/topic'
    path = tmp_path / 'test.mcap'
    messages = [(index, f'msg_{index}') for index in range(5)]

    _write_mcap(
        path,
        messages,
        compression=compression,
        index_types=index_types,
        repeat_channels=repeat_channels,
        repeat_schemas=repeat_schemas,
        use_chunking=use_chunking,
        use_statistics=use_statistics,
        use_summary_offsets=use_summary_offsets,
        use_summary=use_summary,
        output_topic=output_topic,
    )

    with McapFileReader.from_file(path) as reader:
        assert reader.get_topics() == [output_topic]
        assert reader.get_message_count(output_topic) == 5
        assert (reader.start_time, reader.end_time) == (0, 4)

        actual = list(reader.messages(output_topic))
        expected_time = [timestamp for timestamp, _ in messages]
        expected_data = [message for _, message in messages]

        assert len(actual) == len(messages)
        assert [message.log_time for message in actual] == expected_time
        assert [message.publish_time for message in actual] == expected_time
        assert [message.data.data for message in actual] == expected_data
