import importlib

import pytest

import ecet

LAYER_PACKAGES = [
    "ecet.domain",
    "ecet.domain.ports",
    "ecet.application",
    "ecet.application.ports",
    "ecet.application.use_cases",
    "ecet.application.prompts",
    "ecet.infrastructure",
    "ecet.infrastructure.observability",
    "ecet.interfaces",
    "ecet.interfaces.api",
    "ecet.interfaces.api.routes",
    "ecet.interfaces.worker",
]


def test_version_is_exposed() -> None:
    assert ecet.__version__ == "0.1.0"


@pytest.mark.parametrize("name", LAYER_PACKAGES)
def test_layer_package_is_importable(name: str) -> None:
    assert importlib.import_module(name) is not None
