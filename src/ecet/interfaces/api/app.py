from fastapi import FastAPI

from ecet import __version__
from ecet.config import Settings
from ecet.interfaces.api.routes import health


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="ECET API", version=__version__)
    app.state.settings = settings
    app.include_router(health.router)
    return app
