from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
def health_check() -> dict[str, str]:
    return {"status": "ok", "component": "quotes"}


@router.post("/symbols/sync")
def trigger_symbol_sync() -> dict[str, str]:
    # TODO: enqueue symbol catalog refresh job
    return {"status": "queued", "detail": "symbol catalog sync scheduled"}


@router.post("/quotes/sync")
def trigger_quote_sync(broker: str | None = None) -> dict[str, str]:
    # TODO: schedule quote download for requested universe
    return {
        "status": "queued",
        "detail": "quote sync job created",
        "broker": broker or "default",
    }
