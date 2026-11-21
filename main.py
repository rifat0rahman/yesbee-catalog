from __future__ import annotations

import asyncio
import os
import sys
import threading
import webbrowser
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import uvicorn
from dotenv import dotenv_values, set_key
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from database import CatalogDatabase
from scraper import ScrapeOptions, ScrapeStatus, YesbeeScraper


# Source files are read-only in PyInstaller builds. Keep user data in LOCALAPPDATA.
ROOT = Path(__file__).resolve().parent
if getattr(sys, "frozen", False):
    RESOURCE_ROOT = Path(sys._MEIPASS)
    local_app_data = os.environ.get("LOCALAPPDATA")
    DATA_ROOT = (
        Path(local_app_data) / "YesbeeCatalog"
        if local_app_data
        else Path.home() / "AppData" / "Local" / "YesbeeCatalog"
    )
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(RESOURCE_ROOT / "ms-playwright")
else:
    RESOURCE_ROOT = ROOT
    DATA_ROOT = ROOT

ENV_FILE = DATA_ROOT / ".env"
STORAGE_FILE = DATA_ROOT / "storage_state.json"
TEMPLATE_ROOT = RESOURCE_ROOT / "templates"
database = CatalogDatabase(DATA_ROOT / "yesbee_catalog.db")
status = ScrapeStatus()
active_task: asyncio.Task[None] | None = None
active_scraper: YesbeeScraper | None = None


class ConfigPayload(BaseModel):
    username: str = ""
    password: str = ""
    proxy: str = ""


class StartPayload(BaseModel):
    mode: str = Field(pattern="^(fresh|resume|enrich)$")
    headless: bool = True


def _config() -> dict[str, str]:
    values = {**dotenv_values(ENV_FILE), **os.environ}
    return {
        "username": values.get("YESBEE_USERNAME", ""),
        "password": values.get("YESBEE_PASSWORD", ""),
        "proxy": values.get("YESBEE_PROXY", ""),
    }


async def _run_scrape(payload: StartPayload) -> None:
    global active_scraper
    values = _config()
    active_scraper = YesbeeScraper(database, status)
    try:
        await active_scraper.run(
            ScrapeOptions(
                username=values["username"],
                password=values["password"],
                headless=payload.headless,
                mode=payload.mode,
                proxy=values["proxy"],
                storage_path=STORAGE_FILE,
            )
        )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        status.state = "error"
        status.log(f"Execution error: {e}", "error")
    finally:
        active_scraper = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Auto-open browser in local environment after 1 second
    threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:8000")).start()
    yield


app = FastAPI(title="Yesbee Wholesale Catalog Exporter", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=TEMPLATE_ROOT), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(TEMPLATE_ROOT / "index.html")


@app.get("/api/config")
async def get_config() -> dict[str, Any]:
    values = _config()
    return {
        "username": values["username"],
        "password_set": bool(values["password"]),
        "proxy": values["proxy"],
        **database.stats(),
    }


@app.post("/api/config")
async def save_config(payload: ConfigPayload) -> dict[str, str]:
    if not payload.username.strip():
        raise HTTPException(400, "Username is required.")
    current = _config()
    password = payload.password or current["password"]
    if not password:
        raise HTTPException(400, "Password is required.")

    if not ENV_FILE.exists():
        ENV_FILE.touch()

    set_key(ENV_FILE, "YESBEE_USERNAME", payload.username.strip())
    set_key(ENV_FILE, "YESBEE_PASSWORD", password)
    set_key(ENV_FILE, "YESBEE_PROXY", payload.proxy.strip())
    return {"message": "Credentials & configuration saved successfully."}


@app.post("/api/scrape/start")
async def start_scrape(payload: StartPayload) -> dict[str, str]:
    global active_task
    if active_task and not active_task.done():
        raise HTTPException(409, "A scrape is already in progress.")

    values = _config()
    if not values["username"].strip() or not values["password"].strip():
        raise HTTPException(400, "Please enter and save your Yesbee Username and Password first.")

    if payload.mode == "enrich" and database.stats()["cached_products"] == 0:
        raise HTTPException(400, "No cached products found in database to enrich. Please run 'Start Full Scrape' first.")

    status.state = "idle"
    active_task = asyncio.create_task(_run_scrape(payload))
    return {"message": f"Scrape initiated in {payload.mode} mode."}


@app.post("/api/scrape/stop")
async def stop_scrape() -> dict[str, str]:
    global active_scraper, active_task
    if active_scraper:
        active_scraper.request_stop()

    if active_task and not active_task.done():
        status.state = "stopping"
        # Give the scraper up to 2 seconds to gracefully finish in-flight requests and persist session
        try:
            await asyncio.wait_for(asyncio.shield(active_task), timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if active_task and not active_task.done():
                active_task.cancel()
        return {"message": "Scrape stopped; all discovered and enriched rows are preserved."}

    return {"message": "No scrape is currently active."}


@app.get("/api/scrape/status")
async def scrape_status() -> dict[str, Any]:
    return status.as_dict()


@app.get("/api/products")
async def get_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=5, le=100),
    q: str = Query("", alias="q"),
    brand: str = Query("", alias="brand"),
) -> dict[str, Any]:
    return database.get_products(page=page, page_size=page_size, search=q, brand=brand)


@app.post("/api/products/clear")
async def clear_products() -> dict[str, str]:
    global active_task
    if active_task and not active_task.done():
        raise HTTPException(400, "Cannot clear catalog while a scrape is actively running.")
    database.reset_products()
    status.scraped_count = 0
    status.total_estimated = 0
    return {"message": "Catalog products cache cleared."}


@app.get("/api/brands")
async def get_brands() -> list[str]:
    return database.get_brands()


@app.get("/api/runs")
async def get_runs() -> list[dict[str, Any]]:
    return database.get_runs()


@app.get("/api/export")
async def export_csv() -> StreamingResponse:
    if not database.stats()["cached_products"]:
        raise HTTPException(404, "No cached products found to export.")
    filename = f"yesbee_catalog_{datetime.now():%Y-%m-%d_%H%M}.csv"
    return StreamingResponse(
        database.export_csv(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)
