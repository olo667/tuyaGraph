import asyncio
import logging
import sys
import yaml
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from .storage import read_readings, compress_old_data
from .stats import compute_all
from .poller import Poller

LOG = logging.getLogger("tuyaGraph")
# Basic logging configuration so poller logs are visible on the console by default
if not logging.getLogger().handlers:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = BASE_DIR / "config_example.yaml"

app = FastAPI(title="Tuya LAN Sensor Server")
templates = Jinja2Templates(directory=str(BASE_DIR.parent / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR.parent / "static")), name="static")

_poller_task = None
_poller = None
_compressor_task = None


def load_config(path: Path = None) -> dict:
    p = path or (BASE_DIR / "config.yaml")
    if not p.exists():
        p = DEFAULT_CONFIG
    with open(p, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg or {}


@app.on_event("startup")
async def startup_event():
    global _poller_task, _poller
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    interval = int(cfg.get("poll_interval_seconds", 30))
    simulate = bool(cfg.get("simulate_if_missing", True))
    device_id = cfg.get("device_id")
    ip = cfg.get("ip")
    local_key = cfg.get("local_key")

    _poller = Poller(device_id or "", ip or "", local_key or "", csv_path, interval, simulate)
    _poller_task = asyncio.create_task(_poller.start())
    LOG.info("Poller background task started")
    # Start compressor background task (runs once every 24 hours)
    global _compressor_task
    # Read simple compression settings from config, with sensible defaults
    comp_days = int(cfg.get("compress_days_older_than", 7))
    comp_keep = int(cfg.get("compress_keep", 10))
    comp_resample = int(cfg.get("compress_resample_minutes", 1))
    # Background loop
    async def _compress_loop():
        try:
            while True:
                try:
                    LOG.info("Running daily compression: days_older_than=%s keep=%s", comp_days, comp_keep)
                    await compress_old_data(csv_path, days_older_than=comp_days, keep=comp_keep, resample_minutes=comp_resample)
                except Exception:
                    LOG.exception("Error during compression run")
                # Sleep 24 hours
                await asyncio.sleep(24 * 3600)
        except asyncio.CancelledError:
            LOG.info("Compression background task cancelled")
            raise

    _compressor_task = asyncio.create_task(_compress_loop())
    LOG.info("Compressor background task started")


@app.on_event("shutdown")
async def shutdown_event():
    global _poller, _poller_task
    if _poller:
        _poller.stop()
    if _poller_task:
        await _poller_task
    global _compressor_task
    if _compressor_task:
        _compressor_task.cancel()
        try:
            await _compressor_task
        except asyncio.CancelledError:
            pass


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/readings")
async def api_readings(limit: int = 0):
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    rows = await read_readings(csv_path, limit=limit)
    # return as list of dicts
    return JSONResponse([{"timestamp": t, "value": v} for t, v in rows])


@app.get("/api/stats")
async def api_stats(limit: int = 0):
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    rows = await read_readings(csv_path, limit=limit)
    stats = compute_all(rows)
    return JSONResponse(stats)
