from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ecet.interfaces.api.dependencies import ContainerDep

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe. No dependencies — it must answer even when Postgres is down."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(container: ContainerDep) -> JSONResponse:
    """Readiness: database, AMQP and the presidio engine. A probe that raises counts
    as not ready rather than as a 500 — an unready service is a normal state."""
    results: dict[str, bool] = {}
    for name, probe in container.probes.items():
        try:
            results[name] = await probe()
        except Exception:
            results[name] = False
    status_code = 200 if all(results.values()) else 503
    return JSONResponse(results, status_code=status_code)
