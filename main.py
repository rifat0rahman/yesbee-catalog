from __future__ import annotations

import asyncio
import io
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
from faire_database import FaireDatabase
from faire_scraper import (
    FaireScraper,
    FaireScrapeOptions,
    FaireScrapeStatus,
    open_interactive_login,
)


if getattr(sys, "frozen", False):
    # The PyInstaller --windowed launcher has no console streams.
    if sys.stdout is None:
        sys.stdout = io.StringIO()
    if sys.stderr is None:
        sys.stderr = io.StringIO()

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

FAIRE_STORAGE_FILE = DATA_ROOT / "faire_storage_state.json"
faire_database = FaireDatabase(DATA_ROOT / "faire_catalog.db")
faire_status = FaireScrapeStatus()
active_faire_task: asyncio.Task[None] | None = None
active_faire_scraper: FaireScraper | None = None


class FaireStartPayload(BaseModel):
    targets: list[str] = Field(default_factory=list)
    mode: str = "brand"  # 'brand', 'category', 'bulk'
    scrape_mode: str = "fresh"  # 'fresh', 'resume', 'enrich_all'
    headless: bool = True
    deep_enrich: bool = False
    batch_limit: int = 0


class FaireEnrichSinglePayload(BaseModel):
    product_id: str


class FaireImportSessionPayload(BaseModel):
    storage_state_json: str


class ConfigPayload(BaseModel):
    username: str = ""
    password: str = ""
    proxy: str = ""


class StartPayload(BaseModel):
    mode: str = Field(pattern="^(fresh|resume)$")
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


async def _run_faire_scrape(payload: FaireStartPayload) -> None:
    global active_faire_scraper
    values = _config()
    active_faire_scraper = FaireScraper(faire_database, faire_status)
    try:
        await active_faire_scraper.run(
            FaireScrapeOptions(
                targets=payload.targets,
                mode=payload.mode,
                scrape_mode=payload.scrape_mode,
                headless=payload.headless,
                deep_enrich=payload.deep_enrich,
                batch_limit=payload.batch_limit,
                proxy=values["proxy"],
                storage_path=FAIRE_STORAGE_FILE,
            )
        )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        faire_status.state = "error"
        faire_status.log(f"Faire execution error: {e}", "error")
    finally:
        active_faire_scraper = None


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
        status.state = "stopped"
        return {"message": "Scrape stopped; all cached rows are preserved."}

    status.state = "stopped"
    return {"message": "No scrape is currently active."}


@app.get("/api/scrape/status")
async def scrape_status() -> dict[str, Any]:
    return status.as_dict()


@app.get("/api/products")
async def get_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=5, le=200),
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


# ==========================================
# FAIRE B2B WHOLESALE ROUTES
# ==========================================

@app.get("/api/faire/session/status")
async def get_faire_session_status() -> dict[str, Any]:
    has_session = FAIRE_STORAGE_FILE.exists()
    session_info: dict[str, Any] = {
        "has_session": has_session,
        "storage_path": str(FAIRE_STORAGE_FILE),
        "last_modified": None,
    }
    if has_session:
        try:
            mtime = FAIRE_STORAGE_FILE.stat().st_mtime
            session_info["last_modified"] = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return {
        **session_info,
        **faire_database.stats(),
    }


@app.post("/api/faire/session/login")
async def faire_interactive_login() -> dict[str, str]:
    """Opens a visible browser to allow the user to complete 2FA / email verification once."""
    values = _config()
    try:
        success = await open_interactive_login(FAIRE_STORAGE_FILE, proxy=values.get("proxy", ""))
        if success:
            return {"message": "Faire login completed and session saved successfully."}
        return {"message": "Login session window closed. State saved if completed."}
    except Exception as e:
        raise HTTPException(500, f"Error launching login browser: {e}")


@app.post("/api/faire/session/import")
async def faire_import_session(payload: FaireImportSessionPayload) -> dict[str, str]:
    """Allows importing an exported storage_state.json or cookie JSON directly."""
    raw = payload.storage_state_json.strip()
    if not raw:
        raise HTTPException(400, "Session JSON cannot be empty.")
    try:
        parsed = json.loads(raw)
        FAIRE_STORAGE_FILE.write_text(json.dumps(parsed, indent=2), encoding="utf-8")
        return {"message": "Faire session state imported successfully."}
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON format: {e}")


@app.post("/api/faire/scrape/start")
async def start_faire_scrape(payload: FaireStartPayload) -> dict[str, str]:
    global active_faire_task
    if active_faire_task and not active_faire_task.done():
        raise HTTPException(409, "A Faire scrape is already actively in progress.")

    targets = [t.strip() for t in payload.targets if t.strip()]
    if not targets and payload.scrape_mode != "enrich_all":
        raise HTTPException(400, "Please provide at least one Brand URL or Category URL.")

    faire_status.state = "idle"
    active_faire_task = asyncio.create_task(_run_faire_scrape(payload))
    if payload.scrape_mode == "enrich_all":
        mode_label = f"Batch Enrich ({payload.batch_limit} rows)" if payload.batch_limit > 0 else "Enrich All"
    elif payload.scrape_mode == "resume":
        mode_label = "Resume"
    else:
        mode_label = "Full"
    return {"message": f"Faire {mode_label} scrape started."}


@app.post("/api/faire/scrape/stop")
async def stop_faire_scrape() -> dict[str, str]:
    global active_faire_scraper, active_faire_task
    if active_faire_scraper:
        active_faire_scraper.request_stop()

    if active_faire_task and not active_faire_task.done():
        faire_status.state = "stopping"
        try:
            await asyncio.wait_for(asyncio.shield(active_faire_task), timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if active_faire_task and not active_faire_task.done():
                active_faire_task.cancel()
        faire_status.state = "stopped"
        return {"message": "Faire scrape stopped. All scraped products have been preserved."}

    faire_status.state = "stopped"
    return {"message": "No Faire scrape is currently active."}


@app.get("/api/faire/scrape/status")
async def faire_scrape_status() -> dict[str, Any]:
    return faire_status.as_dict()


@app.get("/api/faire/products")
async def get_faire_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=5, le=200),
    q: str = Query("", alias="q"),
    brand: str = Query("", alias="brand"),
) -> dict[str, Any]:
    return faire_database.get_products(page=page, page_size=page_size, search=q, brand=brand)


@app.post("/api/faire/products/enrich")
async def enrich_single_faire_product(payload: FaireEnrichSinglePayload) -> dict[str, Any]:
    pid = payload.product_id.strip()
    if not pid:
        raise HTTPException(400, "Product ID cannot be empty.")
    scraper = FaireScraper(faire_database, faire_status)
    try:
        updated_items = await scraper.enrich_single_product_by_id(pid, storage_path=FAIRE_STORAGE_FILE)
        return {
            "message": f"Successfully enriched {len(updated_items)} item(s) for {pid}.",
            "items": updated_items,
        }
    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "rate limit" in err_msg.lower():
            raise HTTPException(429, "Faire rate limit encountered ('Too many requests'). Please wait 1 minute before retrying.")
        raise HTTPException(500, f"Error enriching product: {err_msg}")


@app.post("/api/faire/products/clear")
async def clear_faire_products() -> dict[str, str]:
    global active_faire_task
    if active_faire_task and not active_faire_task.done():
        raise HTTPException(400, "Cannot clear catalog while a scrape is actively running.")
    faire_database.reset_products()
    faire_status.scraped_count = 0
    faire_status.total_estimated = 0
    return {"message": "Faire catalog cache cleared."}


@app.get("/api/faire/brands")
async def get_faire_brands() -> list[str]:
    return faire_database.get_brands()


@app.get("/api/faire/export")
async def export_faire_csv() -> StreamingResponse:
    if not faire_database.stats()["faire_cached_products"]:
        raise HTTPException(404, "No cached Faire products found to export.")
    filename = f"faire_catalog_{datetime.now():%Y-%m-%d_%H%M}.csv"
    return StreamingResponse(
        faire_database.export_csv(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        reload=False,
        log_config=None,
    )
