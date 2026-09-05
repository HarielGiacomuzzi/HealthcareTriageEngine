import pathlib

import pytest
from typer.testing import CliRunner

from ecet.cli import app

runner = CliRunner()


def test_help_lists_both_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "api" in result.stdout
    assert "worker" in result.stdout


def test_api_command_starts_uvicorn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)  # no local .env
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")
    calls: list[dict[str, object]] = []

    def fake_run(application: object, **kwargs: object) -> None:
        calls.append({"app": application, **kwargs})

    monkeypatch.setattr("uvicorn.run", fake_run)

    result = runner.invoke(app, ["api", "--port", "9999"])

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["port"] == 9999
    assert calls[0]["host"] == "0.0.0.0"


def test_worker_command_runs_the_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "tok")
    monkeypatch.setenv("ECET_API_KEY", "key")
    started: list[str] = []

    async def fake_run(settings: object, stop: object = None) -> None:
        started.append(getattr(settings, "env", "?"))

    monkeypatch.setattr("ecet.interfaces.worker.main.run", fake_run)

    result = runner.invoke(app, ["worker"])

    assert result.exit_code == 0
    assert started == ["dev"]


def test_invalid_config_exits_one_and_names_the_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ECET_S3_EVENT_TOKEN", raising=False)
    monkeypatch.delenv("ECET_API_KEY", raising=False)

    result = runner.invoke(app, ["worker"])

    assert result.exit_code == 1
    assert "s3_event_token" in result.output
    assert "api_key" in result.output
