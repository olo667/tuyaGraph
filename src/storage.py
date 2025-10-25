import asyncio
import csv
import os
from datetime import datetime
from typing import List, Tuple

_lock = asyncio.Lock()


async def append_reading(csv_path: str, timestamp: datetime, value: float) -> None:
    """Append a reading to CSV (timestamp, value). Creates parent dir if needed."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    async with _lock:
        # Use asyncio-friendly file write via threadpool
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write_row, csv_path, timestamp.isoformat(), value)


def _write_row(csv_path: str, ts_iso: str, value: float) -> None:
    write_header = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["timestamp", "value"])
        writer.writerow([ts_iso, value])


async def read_readings(csv_path: str, limit: int = 0) -> List[Tuple[str, float]]:
    """Read readings from CSV. If limit>0 returns only last `limit` rows."""
    if not os.path.exists(csv_path):
        return []
    async with _lock:
        loop = asyncio.get_running_loop()
        rows = await loop.run_in_executor(None, _read_all, csv_path)
    if limit and len(rows) > limit:
        rows = rows[-limit:]
    return rows


def _read_all(csv_path: str) -> List[Tuple[str, float]]:
    result = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                result.append((r["timestamp"], float(r["value"])))
            except Exception:
                continue
    return result
