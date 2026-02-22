import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from query_cost_guard.cli import app

runner = CliRunner()

FAKE_PRICE_PER_BYTE = 6.25 / (1024**4)


def _mock_dry_run_job(*, total_bytes_processed=5_000_000_000):
    job = MagicMock()
    job.total_bytes_processed = total_bytes_processed
    return job


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_json_output(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job()
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["estimated_bytes"] == 5_000_000_000
    assert data["estimated_cost_usd"] > 0
    assert data["price_per_tib_usd"] == 6.25


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_human_output(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job()
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1"])
    assert result.exit_code == 0
    assert "Estimated bytes:" in result.output
    assert "Estimated cost:" in result.output


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_max_cost_pass(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job(total_bytes_processed=1_000_000)
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--max-cost", "100.0"])
    assert result.exit_code == 0
    assert "PASS" in result.output


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_max_cost_fail(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job(total_bytes_processed=5_000_000_000_000)
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--max-cost", "0.001"])
    assert result.exit_code == 1
    assert "FAIL" in result.output


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_max_cost_json_includes_pass_field(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job(total_bytes_processed=1_000)
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--max-cost", "10.0", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["pass"] is True
    assert data["max_cost_usd"] == 10.0


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_from_file(mock_build_client, _mock_pricing, tmp_path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT * FROM my_table")
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job()
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--file", str(sql_file), "--json"])
    assert result.exit_code == 0
    mock_client.query.assert_called_once()
    called_sql = mock_client.query.call_args[0][0]
    assert called_sql == "SELECT * FROM my_table"


def test_estimate_rejects_both_query_and_file(tmp_path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text("SELECT 1")
    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--file", str(sql_file)])
    assert result.exit_code != 0


def test_estimate_requires_query_input():
    result = runner.invoke(app, ["estimate"])
    assert result.exit_code != 0


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_passes_project_and_credentials(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job()
    mock_build_client.return_value = mock_client

    runner.invoke(app, ["--project", "my-proj", "--credentials", "/tmp/creds.json", "estimate", "--query", "SELECT 1"])
    mock_build_client.assert_called_once_with(project="my-proj", credentials=Path("/tmp/creds.json"))


@patch("query_cost_guard.cli._resolve_pricing", return_value=FAKE_PRICE_PER_BYTE)
@patch("query_cost_guard.cli._build_client")
def test_estimate_zero_bytes(mock_build_client, _mock_pricing):
    mock_client = MagicMock()
    mock_client.query.return_value = _mock_dry_run_job(total_bytes_processed=0)
    mock_build_client.return_value = mock_client

    result = runner.invoke(app, ["estimate", "--query", "SELECT 1", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["estimated_bytes"] == 0
    assert data["estimated_cost_usd"] == 0.0
