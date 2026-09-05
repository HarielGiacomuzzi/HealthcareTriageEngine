"""Application settings. Built once in the CLI and passed down — never a global."""

from enum import StrEnum
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LlmProvider(StrEnum):
    FAKE = "fake"
    OPENAI = "openai"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ECET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: str = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    database_url: SecretStr = SecretStr("postgresql+asyncpg://ecet:ecet@postgres:5432/ecet")
    auto_migrate: bool = False

    amqp_url: SecretStr = SecretStr("amqp://guest:guest@rabbitmq:5672/")
    worker_prefetch: int = 4

    s3_endpoint: str | None = "http://minio:9000"
    s3_access_key: SecretStr = SecretStr("minioadmin")
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_event_token: SecretStr
    api_key: SecretStr

    max_pdf_bytes: int = 20_000_000
    max_pdf_pages: int = 50
    pii_concurrency: int = 2
    spacy_model: str = "en_core_web_lg"

    confidence_threshold: float = Field(default=0.85, gt=0, le=1)

    llm_provider: LlmProvider = LlmProvider.FAKE
    llm_base_url: str = "https://api.anthropic.com/v1/"
    llm_api_key: SecretStr | None = None
    llm_model: str = "claude-sonnet-5"
    llm_timeout_s: int = 60
    prompt_version: str = "v1"

    webhook_timeout_s: int = 10
    webhook_max_attempts: int = 3

    metrics_port: int = 9100

    @model_validator(mode="after")
    def _openai_provider_needs_a_key(self) -> Self:
        if self.llm_provider is LlmProvider.OPENAI and self.llm_api_key is None:
            raise ValueError("ECET_LLM_API_KEY is required when ECET_LLM_PROVIDER=openai")
        return self
