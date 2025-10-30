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
from .stats import compute_all, mean_temperature_across_days_hourly, compute_rate_of_change, list_transforms, call_registered
from datetime import datetime, timedelta
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
async def api_stats(limit: int = 0, stat: str = None):
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    rows = await read_readings(csv_path, limit=limit)
    if stat:
        try:
            result = call_registered(stat, rows)
            return JSONResponse({stat: result})
        except KeyError:
            return JSONResponse({"error": f"unknown stat '{stat}'"}, status_code=404)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
    stats = compute_all(rows)
    return JSONResponse(stats)


@app.get("/api/transforms")
async def api_transforms():
    """Return a list of available transforms and metadata useful for the UI."""
    transforms = list_transforms()
    # Add a couple of higher-level endpoints that the UI already knows how to call
    transforms.append({"name": "mean-day", "meta": {"endpoint": "/api/mean-day", "params": ["days", "resolution_minutes", "start", "end"]}})
    transforms.append({"name": "rate-of-change", "meta": {"endpoint": "/api/rate-of-change", "params": ["days", "resolution_minutes", "smoothing_days", "kernel", "start", "end"]}})
    return JSONResponse(transforms)


@app.get("/api/mean-day")
async def api_mean_day(days: int = 30, start: str = None, end: str = None, resolution_minutes: int = 5, min_sample_fraction: float = 0.5):
    """
    Return mean temperature across days aggregated by hour-of-day.

    Query params:
    - days: integer (default 30) — use last `days` days ending now (ignored if start/end provided)
    - start: ISO datetime string (optional)
    - end: ISO datetime string (optional)

    If start or end are provided they override the `days` parameter.
    """
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    rows = await read_readings(csv_path, limit=0)

    # Determine time window
    now = datetime.now()
    start_dt = None
    end_dt = None
    try:
        if start:
            start_dt = datetime.fromisoformat(start)
        if end:
            end_dt = datetime.fromisoformat(end)
    except Exception:
        return JSONResponse({"error": "invalid start/end datetime (use ISO format)"}, status_code=400)

    if not start_dt and not end_dt:
        # use `days` backwards from now
        try:
            d = int(days)
        except Exception:
            d = 30
        start_dt = now - timedelta(days=d)
        end_dt = now
    # resolution validation
    try:
        res = int(resolution_minutes)
        if res <= 0 or res > 24 * 60:
            res = 5
    except Exception:
        res = 5

    try:
        min_frac = float(min_sample_fraction)
    except Exception:
        min_frac = 0.5

    result = mean_temperature_across_days_hourly(rows, start=start_dt, end=end_dt, resolution_minutes=res, min_sample_fraction=min_frac)
    return JSONResponse(result)


@app.get("/api/rate-of-change")
async def api_rate_of_change(days: int = 30, start: str = None, end: str = None, resolution_minutes: int = 60, smoothing_days: int = 1, kernel: str = None, min_sample_fraction: float = 0.5):
    """
    Return per-day rate-of-change computed by applying a linear filter to each day's interpolated temperature series.

    Query params:
    - days: integer (default 30) — use last `days` days ending now (ignored if start/end provided)
    - start, end: ISO datetimes (optional)
    - resolution_minutes: bucket size
    - smoothing_days: integer rolling-window to smooth per-day averages
    - kernel: optional comma-separated floats to override default kernel (e.g. -0.5,-0.5,0.5,0.5)
    """
    cfg = load_config()
    csv_path = cfg.get("csv_path", "data/readings.csv")
    rows = await read_readings(csv_path, limit=0)

    now = datetime.now()
    start_dt = None
    end_dt = None
    try:
        if start:
            start_dt = datetime.fromisoformat(start)
        if end:
            end_dt = datetime.fromisoformat(end)
    except Exception:
        return JSONResponse({"error": "invalid start/end datetime (use ISO format)"}, status_code=400)

    if not start_dt and not end_dt:
        try:
            d = int(days)
        except Exception:
            d = 30
        start_dt = now - timedelta(days=d)
        end_dt = now

    try:
        res = int(resolution_minutes)
        if res <= 0 or res > 24 * 60:
            res = 60
    except Exception:
        res = 60

    try:
        min_frac = float(min_sample_fraction)
    except Exception:
        min_frac = 0.5

    try:
        smooth = int(smoothing_days)
        if smooth < 1:
            smooth = 1
    except Exception:
        smooth = 1

    kernel_list = None
    if kernel:
        try:
            kernel_list = [float(x.strip()) for x in kernel.split(",") if x.strip()]
            if not kernel_list:
                kernel_list = None
        except Exception:
            return JSONResponse({"error": "invalid kernel parameter; provide comma-separated numbers"}, status_code=400)

    result = compute_rate_of_change(rows, start=start_dt, end=end_dt, resolution_minutes=res, kernel=kernel_list, smoothing_days=smooth, min_sample_fraction=min_frac)
    return JSONResponse(result)
