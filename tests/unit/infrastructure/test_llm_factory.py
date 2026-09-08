"""One switch, so nothing else in the codebase branches on the provider."""

import pytest
from pydantic import ValidationError

from ecet.config import LlmProvider, Settings
from ecet.infrastructure.llm.factory import build_gateway
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway


def build_settings(**overrides: object) -> Settings:
    fields: dict[str, object] = {
        "_env_file": None,
        "s3_event_token": "t",
        "api_key": "k",
    }
    fields.update(overrides)
    return Settings(**fields)  # type: ignore[arg-type]


def test_the_default_provider_is_the_fake_one() -> None:
    assert isinstance(build_gateway(build_settings()), FakeLlmGateway)


def test_the_openai_provider_builds_the_vendor_adapter() -> None:
    settings = build_settings(llm_provider=LlmProvider.OPENAI, llm_api_key="sk-test")

    assert isinstance(build_gateway(settings), OpenAiLlmGateway)


def test_the_openai_provider_without_a_key_fails_at_settings_time() -> None:
    # The guard lives in `Settings`, so a misconfigured worker exits at startup with a
    # field name rather than at the first message with a 401.
    with pytest.raises(ValidationError):
        build_settings(llm_provider=LlmProvider.OPENAI)
