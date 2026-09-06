"""`ecet` command line: one image, two entrypoints (ADR-007)."""

import asyncio

import typer
from pydantic import ValidationError

from ecet.config import Settings
from ecet.infrastructure.observability.logging import configure_logging

app = typer.Typer(add_completion=False, help="ECET — claims extraction & triage engine")


def _load_settings() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]  # required fields come from env/`.env`
    except ValidationError as exc:
        for error in exc.errors():
            field = ".".join(str(part) for part in error["loc"])
            typer.echo(f"config error: {field}: {error['msg']}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the HTTP API."""
    import uvicorn

    from ecet.interfaces.api.app import create_app

    settings = _load_settings()
    configure_logging(settings.log_level)
    uvicorn.run(create_app(settings), host=host, port=port, log_config=None)


@app.command()
def worker() -> None:
    """Run the queue worker."""
    import ecet.interfaces.worker.main as worker_main

    settings = _load_settings()
    configure_logging(settings.log_level)
    asyncio.run(worker_main.run(settings))


@app.command()
def seed() -> None:
    """Load the dev seed data (ICD-10 catalogue, tenants, policies). Dev only."""
    from ecet.infrastructure.postgres.seed import load_seed
    from ecet.infrastructure.postgres.session import create_engine

    settings = _load_settings()
    configure_logging(settings.log_level)
    if settings.env != "dev":
        typer.echo(f"refusing to seed: ECET_ENV={settings.env!r}, the seed is dev-only", err=True)
        raise typer.Exit(code=1)

    async def run() -> int:
        engine = create_engine(settings.database_url.get_secret_value())
        try:
            return await load_seed(engine)
        finally:
            await engine.dispose()

    typer.echo(f"seed applied: {asyncio.run(run())} statements")
