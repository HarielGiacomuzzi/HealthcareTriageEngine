import pytest
from pydantic import ValidationError

from ecet.config import LlmProvider, Settings

REQUIRED = {"ECET_S3_EVENT_TOKEN": "tok", "ECET_API_KEY": "key"}


def test_defaults_match_config_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, value in REQUIRED.items():
        monkeypatch.setenv(var, value)

    settings = Settings(_env_file=None)

    assert settings.env == "dev"
    assert settings.log_level == "INFO"
    assert settings.auto_migrate is False
    assert settings.worker_prefetch == 4
    assert settings.s3_endpoint == "http://minio:9000"
    assert settings.max_pdf_bytes == 20_000_000
    assert settings.max_pdf_pages == 50
    assert settings.pii_concurrency == 2
    assert settings.spacy_model == "en_core_web_lg"
    assert settings.confidence_threshold == 0.85
    assert settings.llm_provider is LlmProvider.FAKE
    assert settings.llm_model == "claude-sonnet-5"
    assert settings.llm_timeout_s == 60
    assert settings.prompt_version == "v1"
    assert settings.webhook_timeout_s == 10
    assert settings.webhook_max_attempts == 3
    assert settings.metrics_port == 9100
    assert settings.database_url.get_secret_value().startswith("postgresql+asyncpg://")
    assert settings.amqp_url.get_secret_value().startswith("amqp://")


def test_env_prefix_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, value in REQUIRED.items():
        monkeypatch.setenv(var, value)
    monkeypatch.setenv("ECET_ENV", "prod")
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", "0.5")

    settings = Settings(_env_file=None)

    assert settings.env == "prod"
    assert settings.confidence_threshold == 0.5


def test_missing_required_secrets_are_reported_together() -> None:
    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    missing = {error["loc"][0] for error in exc_info.value.errors()}
    assert missing == {"s3_event_token", "api_key"}


def test_invalid_log_level_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_LOG_LEVEL", "verbose")

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["0", "0.0", "1.5", "-0.2"])
def test_confidence_threshold_out_of_range_is_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_confidence_threshold_one_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_CONFIDENCE_THRESHOLD", "1")

    assert Settings(_env_file=None).confidence_threshold == 1.0


def test_openai_provider_requires_an_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_LLM_PROVIDER", "openai")

    with pytest.raises(ValidationError, match="ECET_LLM_API_KEY"):
        Settings(_env_file=None)


def test_openai_provider_with_api_key_is_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    for var, secret in REQUIRED.items():
        monkeypatch.setenv(var, secret)
    monkeypatch.setenv("ECET_LLM_PROVIDER", "openai")
    monkeypatch.setenv("ECET_LLM_API_KEY", "sk-test")

    settings = Settings(_env_file=None)

    assert settings.llm_provider is LlmProvider.OPENAI
    assert settings.llm_api_key is not None
    assert settings.llm_api_key.get_secret_value() == "sk-test"


def test_secrets_are_not_leaked_by_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ECET_S3_EVENT_TOKEN", "super-secret-token")
    monkeypatch.setenv("ECET_API_KEY", "super-secret-key")

    dumped = repr(Settings(_env_file=None))

    assert "super-secret-token" not in dumped
    assert "super-secret-key" not in dumped
