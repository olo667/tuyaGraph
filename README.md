# Tuya LAN Temperature Poller + Web UI

Simple FastAPI app that polls a Tuya LAN temperature sensor (via tinytuya), stores readings to CSV, and exposes a small web UI and API to view history and statistics. The project is intentionally minimal and extensible.

Quick start (Windows PowerShell):

1. Create a virtualenv and activate it:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Copy `src/config_example.yaml` to `src/config.yaml` and fill in your device info (device_id, ip, local_key).

4. Run the server:

```powershell
uvicorn src.app:app --reload --host 0.0.0.0 --port 8000
```

5. Open `http://localhost:8000` in your browser.

Notes:
- If you don't have tinytuya-compatible device details yet, the poller will fall back to a simulated sensor. Replace with real credentials in `src/config.yaml`.
- Data is appended to `data/readings.csv` by default.

Project layout (important files):
- `src/app.py` — FastAPI application and background startup.
- `src/poller.py` — Poller logic (tinytuya integration + simulation fallback).
- `src/storage.py` — CSV storage read/write helpers.
- `src/stats.py` — Pluggable statistics registry.
- `templates/index.html` — Minimal UI using Chart.js.