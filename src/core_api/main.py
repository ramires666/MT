from fastapi import FastAPI

from app_config import get_settings
from core_api.routes_optimizer import router as optimizer_router
from core_api.routes_quotes import router as quotes_router
from core_api.routes_scan import router as scan_router
from core_api.routes_wfa import router as wfa_router


settings = get_settings()
app = FastAPI(title=settings.app_name)
app.include_router(quotes_router, prefix="/api/v1")
app.include_router(optimizer_router, prefix="/api/v1")
app.include_router(scan_router, prefix="/api/v1")
app.include_router(wfa_router, prefix="/api/v1")


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok", "service": "core_api"}


@app.get("/api/v1/meta")
def service_meta() -> dict[str, str | int]:
    return {
        "app_name": settings.app_name,
        "environment": settings.environment,
        "api_port": settings.api_port,
        "bokeh_port": settings.bokeh_port,
    }