from dataclasses import dataclass
from pathlib import Path

import pybag.types as t
from pybag.cli.main import main
from pybag.mcap_writer import McapFileWriter


def test_info_outputs_statistics(tmp_path, capsys) -> None:
    @dataclass
    class Example:
        value: t.int32

    mcap_path = tmp_path / "test.mcap"
    with McapFileWriter.open(mcap_path) as writer:
        writer.write_message("/example", 1, Example(1))
        writer.write_message("/example", 2, Example(2))

    main(["info", str(mcap_path)])
    output = capsys.readouterr().out
    assert "message_count" in output
    assert "2" in output
    assert "/example" in output
