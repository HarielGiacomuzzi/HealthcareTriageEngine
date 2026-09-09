"""Provider selection. The only place in the codebase that reads `llm_provider`."""

from ecet.application.ports.llm_gateway import LLMGateway
from ecet.config import LlmProvider, Settings
from ecet.infrastructure.llm.fake_gateway import FakeLlmGateway
from ecet.infrastructure.llm.openai_gateway import OpenAiLlmGateway


def build_gateway(settings: Settings) -> LLMGateway:
    match settings.llm_provider:
        case LlmProvider.FAKE:
            return FakeLlmGateway()
        case LlmProvider.OPENAI:
            key = settings.llm_api_key
            if key is None:  # pragma: no cover - Settings refuses this combination
                raise ValueError("ECET_LLM_API_KEY is required when ECET_LLM_PROVIDER=openai")
            return OpenAiLlmGateway(
                base_url=settings.llm_base_url,
                api_key=key.get_secret_value(),
                model=settings.llm_model,
                timeout_s=settings.llm_timeout_s,
            )
