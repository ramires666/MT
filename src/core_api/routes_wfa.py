from fastapi import APIRouter, HTTPException

from app_config import get_settings
from domain.contracts import WfaRequest
from domain.wfa import run_wfa_request

router = APIRouter(prefix="/wfa", tags=["wfa"])


@router.get("/healthz")
def health_check() -> dict[str, str]:
    return {"status": "ok", "component": "wfa"}


@router.post("/run")
def trigger_wfa(request: WfaRequest) -> dict[str, object]:
    try:
        return run_wfa_request(get_settings().default_broker_id, request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
