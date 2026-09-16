"""`build_container` opens an engine, a broker connection and an HTTP client before it
can fail on the next step (a schema behind head, a broker refusing, a missing spaCy
model). Whatever was already open must be released on the way out — the api process
exits on a startup failure, but a test or a supervisor retrying in-process would not."""

from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from ecet.config import Settings
from ecet.interfaces.api import container as api_container


class _Engine:
    def __init__(self) -> None:
        self.disposed = False
        self.pool = SimpleNamespace(checkedout=lambda: 0)

    async def dispose(self) -> None:
        self.disposed = True


class _Queue:
    built: ClassVar[list["_Queue"]] = []

    def __init__(self, url: str) -> None:
        self.stopped = False
        _Queue.built.append(self)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    async def is_healthy(self) -> bool:
        return True


async def _at_head(engine: Any) -> None:
    return None


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> _Engine:
    built = _Engine()
    monkeypatch.setattr(api_container, "create_engine", lambda url: built)
    return built


@pytest.fixture
def queues(monkeypatch: pytest.MonkeyPatch) -> list[_Queue]:
    _Queue.built = []
    monkeypatch.setattr(api_container, "RabbitMqEvaluationQueue", _Queue)
    return _Queue.built


async def test_a_schema_behind_head_disposes_the_engine(
    settings: Settings, engine: _Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def behind(engine: Any) -> None:
        raise RuntimeError("pending migration: database at None")

    monkeypatch.setattr(api_container, "assert_at_head", behind)

    with pytest.raises(RuntimeError, match="pending migration"):
        await api_container.build_container(settings)

    assert engine.disposed


async def test_a_broker_that_refuses_still_disposes_the_engine(
    settings: Settings, engine: _Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Refusing(_Queue):
        async def start(self) -> None:
            raise ConnectionError("connection refused")

    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "RabbitMqEvaluationQueue", Refusing)

    with pytest.raises(ConnectionError):
        await api_container.build_container(settings)

    assert engine.disposed


async def test_a_missing_spacy_model_stops_the_queue_and_disposes_the_engine(
    settings: Settings,
    engine: _Engine,
    queues: list[_Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_model(**kwargs: Any) -> None:
        raise OSError("[E050] Can't find model 'en_core_web_lg'")

    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "PresidioPiiRedactor", no_model)

    with pytest.raises(OSError, match="E050"):
        await api_container.build_container(settings)

    (queue,) = queues
    assert queue.stopped
    assert engine.disposed


async def test_a_built_container_releases_everything_on_aclose(
    settings: Settings,
    engine: _Engine,
    queues: list[_Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_container, "assert_at_head", _at_head)
    monkeypatch.setattr(api_container, "PresidioPiiRedactor", lambda **kwargs: object())

    built = await api_container.build_container(settings)
    assert not engine.disposed

    await built.aclose()

    (queue,) = queues
    assert queue.stopped
    assert engine.disposed
