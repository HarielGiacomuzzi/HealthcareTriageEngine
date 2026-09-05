from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness probe. No dependencies — it must answer even when Postgres is down."""
    return {"status": "ok"}
