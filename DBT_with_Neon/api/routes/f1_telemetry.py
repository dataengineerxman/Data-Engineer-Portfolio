import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

OPENF1_BASE = "https://api.openf1.org/v1"
TIMEOUT_SECONDS = 30.0

# Shared client stored on app.state (recommended pattern)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=TIMEOUT_SECONDS)
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="DBT with Neon - OpenF1 Ingest API", lifespan=lifespan)


async def openf1_get(path: str, params: dict):
    """
    Calls OpenF1 and returns JSON.
    Raises HTTPException with useful details on errors.
    """
    url = f"{OPENF1_BASE}{path}"

    try:
        resp = await app.state.http.get(url, params=params)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={"message": "Network error calling OpenF1", "url": url, "error": str(e)},
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail={
                "message": "OpenF1 request failed",
                "url": str(resp.request.url),
                "status_code": resp.status_code,
                "response_preview": resp.text[:800],
            },
        )

    try:
        return resp.json()
    except Exception:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "OpenF1 returned non-JSON response",
                "url": str(resp.request.url),
                "status_code": resp.status_code,
                "response_preview": resp.text[:800],
            },
        )


# -------------------------------------------------------
# ROUTES (MATCH YOUR CLIENT REQUESTS)
# -------------------------------------------------------

@app.get("/ingest/sessions/{year}")
async def ingest_sessions(year: int):
    return await openf1_get("/sessions", {"year": year})


@app.get("/ingest/drivers/{session_key}")
async def ingest_drivers(session_key: int, driver_number: int | None = None):
    params = {"session_key": session_key}
    if driver_number is not None:
        params["driver_number"] = driver_number
    return await openf1_get("/drivers", params)


@app.get("/ingest/telemetry/{session_key}/{driver_number}")
async def ingest_telemetry(session_key: int, driver_number: int, limit: int = 200):
    """
    Your endpoint name: /ingest/telemetry/...
    OpenF1 telemetry-like data endpoint is /car_data, so we alias telemetry -> car_data.
    """
    data = await openf1_get("/car_data", {"session_key": session_key, "driver_number": driver_number})
    return data[: max(0, limit)] if isinstance(data, list) else data


@app.get("/health")
async def health():
    return {"status": "ok"}