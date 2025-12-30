"""Tests for MCAP inspect CLI command."""

import json
from pathlib import Path

import pytest

from pybag.cli.main import main as cli_main
from pybag.mcap_writer import McapFileWriter
from pybag.ros2.humble.std_msgs import Int32, String


@pytest.fixture
def mcap_with_data(tmp_path: Path) -> Path:
    """Create an MCAP file with schemas, channels, metadata, and attachments."""
    input_path = tmp_path / "test.mcap"

    with McapFileWriter.open(input_path, chunk_size=1024) as writer:
        writer.write_message("/sensor/temperature", int(1e9), Int32(data=25))
        writer.write_message("/sensor/humidity", int(2e9), Int32(data=50))
        writer.write_message("/control/status", int(3e9), String(data="OK"))
        writer.write_metadata("robot_info", {"name": "robot1", "version": "1.0"})
        writer.write_metadata("session", {"id": "12345"})
        writer.write_attachment("config.json", b'{"key": "value"}', "application/json", int(1e9))
        writer.write_attachment("data.txt", b"Hello World!", "text/plain", int(2e9))

    return input_path


class TestInspectSchemas:
    """Tests for inspect schemas subcommand."""

    def test_list_schemas(self, mcap_with_data: Path, capsys) -> None:
        """Test listing all schemas."""
        cli_main(["inspect", "schemas", str(mcap_with_data)])

        output = capsys.readouterr().out
        assert "Schemas (2):" in output
        assert "std_msgs/msg/Int32" in output
        assert "std_msgs/msg/String" in output
        assert "ros2msg" in output

    def test_schema_detail_by_id(self, mcap_with_data: Path, capsys) -> None:
        """Test showing schema details by ID."""
        cli_main(["inspect", "schemas", str(mcap_with_data), "--id", "1"])

        output = capsys.readouterr().out
        assert "Schema ID: 1" in output
        assert "Name:" in output
        assert "Encoding:" in output
        assert "Data:" in output

    def test_filter_schemas_by_name(self, mcap_with_data: Path, capsys) -> None:
        """Test filtering schemas by name pattern."""
        cli_main(["inspect", "schemas", str(mcap_with_data), "--name", "*Int32"])

        output = capsys.readouterr().out
        assert "std_msgs/msg/Int32" in output
        assert "std_msgs/msg/String" not in output

    def test_schemas_json_output(self, mcap_with_data: Path, capsys) -> None:
        """Test JSON output for schemas."""
        cli_main(["inspect", "schemas", str(mcap_with_data), "--json"])

        output = capsys.readouterr().out
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 2
        assert all("id" in s and "name" in s and "encoding" in s for s in data)

    def test_schema_not_found(self, mcap_with_data: Path, capsys) -> None:
        """Test error when schema ID is not found."""
        cli_main(["inspect", "schemas", str(mcap_with_data), "--id", "999"])

        output = capsys.readouterr().out
        assert "not found" in output


class TestInspectChannels:
    """Tests for inspect channels subcommand."""

    def test_list_channels(self, mcap_with_data: Path, capsys) -> None:
        """Test listing all channels."""
        cli_main(["inspect", "channels", str(mcap_with_data)])

        output = capsys.readouterr().out
        assert "Channels (3):" in output
        assert "/sensor/temperature" in output
        assert "/sensor/humidity" in output
        assert "/control/status" in output

    def test_channel_detail_by_id(self, mcap_with_data: Path, capsys) -> None:
        """Test showing channel details by ID."""
        cli_main(["inspect", "channels", str(mcap_with_data), "--id", "1"])

        output = capsys.readouterr().out
        assert "Channel ID:" in output
        assert "Topic:" in output
        assert "Message Encoding:" in output
        assert "Schema ID:" in output

    def test_filter_channels_by_topic(self, mcap_with_data: Path, capsys) -> None:
        """Test filtering channels by topic pattern."""
        cli_main(["inspect", "channels", str(mcap_with_data), "--topic", "/sensor/*"])

        output = capsys.readouterr().out
        assert "Channels (2):" in output
        assert "/sensor/temperature" in output
        assert "/sensor/humidity" in output
        assert "/control/status" not in output

    def test_channels_json_output(self, mcap_with_data: Path, capsys) -> None:
        """Test JSON output for channels."""
        cli_main(["inspect", "channels", str(mcap_with_data), "--json"])

        output = capsys.readouterr().out
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 3
        assert all("id" in c and "topic" in c for c in data)

    def test_channel_not_found(self, mcap_with_data: Path, capsys) -> None:
        """Test error when channel ID is not found."""
        cli_main(["inspect", "channels", str(mcap_with_data), "--id", "999"])

        output = capsys.readouterr().out
        assert "not found" in output


class TestInspectMetadata:
    """Tests for inspect metadata subcommand."""

    def test_list_metadata(self, mcap_with_data: Path, capsys) -> None:
        """Test listing all metadata."""
        cli_main(["inspect", "metadata", str(mcap_with_data)])

        output = capsys.readouterr().out
        assert "Metadata (2):" in output
        assert "robot_info" in output
        assert "session" in output

    def test_metadata_detail_by_name(self, mcap_with_data: Path, capsys) -> None:
        """Test showing metadata details by name."""
        cli_main(["inspect", "metadata", str(mcap_with_data), "--name", "robot_info"])

        output = capsys.readouterr().out
        assert "Name: robot_info" in output
        assert "name: robot1" in output
        assert "version: 1.0" in output

    def test_metadata_json_output(self, mcap_with_data: Path, capsys) -> None:
        """Test JSON output for metadata."""
        cli_main(["inspect", "metadata", str(mcap_with_data), "--json"])

        output = capsys.readouterr().out
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 2


class TestInspectAttachments:
    """Tests for inspect attachments subcommand."""

    def test_list_attachments(self, mcap_with_data: Path, capsys) -> None:
        """Test listing all attachments."""
        cli_main(["inspect", "attachments", str(mcap_with_data)])

        output = capsys.readouterr().out
        assert "Attachments (2):" in output
        assert "config.json" in output
        assert "data.txt" in output
        assert "application/json" in output
        assert "text/plain" in output

    def test_attachment_detail_by_name(self, mcap_with_data: Path, capsys) -> None:
        """Test showing attachment details by name."""
        cli_main(["inspect", "attachments", str(mcap_with_data), "--name", "data.txt"])

        output = capsys.readouterr().out
        assert "Name:        data.txt" in output
        assert "Media Type:  text/plain" in output
        assert "Data Size:   12 bytes" in output

    def test_attachment_with_data(self, mcap_with_data: Path, capsys) -> None:
        """Test showing attachment with data content."""
        cli_main(["inspect", "attachments", str(mcap_with_data), "--name", "data.txt", "--data"])

        output = capsys.readouterr().out
        assert "Hello World!" in output

    def test_filter_attachments_by_time(self, mcap_with_data: Path, capsys) -> None:
        """Test filtering attachments by time range."""
        cli_main(["inspect", "attachments", str(mcap_with_data), "--start-time", "1.5", "--end-time", "2.5"])

        output = capsys.readouterr().out
        assert "Attachments (1):" in output
        assert "data.txt" in output
        assert "config.json" not in output

    def test_attachments_json_output(self, mcap_with_data: Path, capsys) -> None:
        """Test JSON output for attachments."""
        cli_main(["inspect", "attachments", str(mcap_with_data), "--json"])

        output = capsys.readouterr().out
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) == 2
        assert all("name" in a and "media_type" in a for a in data)

    def test_attachment_json_with_data(self, mcap_with_data: Path, capsys) -> None:
        """Test JSON output with attachment data."""
        cli_main(["inspect", "attachments", str(mcap_with_data), "--name", "data.txt", "--data", "--json"])

        output = capsys.readouterr().out
        data = json.loads(output)
        assert data["name"] == "data.txt"
        assert data["data"] == "Hello World!"


class TestInspectEmpty:
    """Tests for inspect commands on empty MCAP files."""

    def test_empty_schemas(self, tmp_path: Path, capsys) -> None:
        """Test inspecting schemas in empty MCAP."""
        input_path = tmp_path / "empty.mcap"
        with McapFileWriter.open(input_path, chunk_size=1024) as writer:
            pass  # Empty file

        cli_main(["inspect", "schemas", str(input_path)])
        output = capsys.readouterr().out
        assert "Schemas (0):" in output
        assert "No schemas found" in output

    def test_empty_metadata(self, tmp_path: Path, capsys) -> None:
        """Test inspecting metadata in MCAP with no metadata."""
        input_path = tmp_path / "no_metadata.mcap"
        with McapFileWriter.open(input_path, chunk_size=1024) as writer:
            writer.write_message("/foo", int(1e9), Int32(data=1))

        cli_main(["inspect", "metadata", str(input_path)])
        output = capsys.readouterr().out
        assert "Metadata (0):" in output
        assert "No metadata found" in output

    def test_empty_attachments(self, tmp_path: Path, capsys) -> None:
        """Test inspecting attachments in MCAP with no attachments."""
        input_path = tmp_path / "no_attachments.mcap"
        with McapFileWriter.open(input_path, chunk_size=1024) as writer:
            writer.write_message("/foo", int(1e9), Int32(data=1))

        cli_main(["inspect", "attachments", str(input_path)])
        output = capsys.readouterr().out
        assert "Attachments (0):" in output
        assert "No attachments found" in output

    def test_empty_channels(self, tmp_path: Path, capsys) -> None:
        """Test inspecting attachments in MCAP with no attachments."""
        input_path = tmp_path / "no_attachments.mcap"
        with McapFileWriter.open(input_path, chunk_size=1024) as writer:
            pass

        cli_main(["inspect", "channels", str(input_path)])
        output = capsys.readouterr().out
        assert "Channels (0):" in output
        assert "No channels found" in output


class TestInspectHelp:
    """Tests for help output."""

    def test_inspect_help(self, capsys) -> None:
        """Test main inspect help."""
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["inspect", "--help"])
        assert exc_info.value.code == 0

        output = capsys.readouterr().out
        assert "schemas" in output
        assert "channels" in output
        assert "metadata" in output
        assert "attachments" in output

    def test_schemas_help(self, capsys) -> None:
        """Test schemas subcommand help."""
        with pytest.raises(SystemExit) as exc_info:
            cli_main(["inspect", "schemas", "--help"])
        assert exc_info.value.code == 0

        output = capsys.readouterr().out
        assert "--id" in output
        assert "--name" in output
        assert "--json" in output
