from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ecet import __version__
from ecet.config import Settings
from ecet.interfaces.api.container import ApiContainer, build_container
from ecet.interfaces.api.errors import register_error_handlers
from ecet.interfaces.api.routes import health


def create_app(settings: Settings, container: ApiContainer | None = None) -> FastAPI:
    """`container` is injected by tests: passing one skips the lifespan build, so the
    routes can be exercised with fakes and no infrastructure at all."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if container is not None:
            # Already set on app.state below — tests never see the container built
            # here, and never own its shutdown.
            yield
            return
        built = await build_container(settings)
        app.state.container = built
        try:
            yield
        finally:
            await built.aclose()

    app = FastAPI(title="ECET API", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    if container is not None:
        app.state.container = container
    register_error_handlers(app)
    app.include_router(health.router)
    return app
