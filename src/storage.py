import asyncio
import csv
import os
import json
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

import numpy as np

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


async def compress_old_data(
    csv_path: str,
    days_older_than: int = 7,
    keep: int = 10,
    resample_minutes: int = 1,
    compressed_dir: Optional[str] = None,
) -> None:
    """Compress old daily data using FFT and keep only the `keep` most relevant frequencies per day.

    Behavior / assumptions:
    - Rows are grouped by calendar day (local time based on the timestamp's ISO string).
    - To use FFT we resample each day to a uniform grid at `resample_minutes` resolution (minutes).
      Missing samples are linearly interpolated. This is a pragmatic choice for irregular samplings.
    - For each day older than `days_older_than`, we produce a JSON file with retained frequencies
      and remove that day's rows from the original CSV to save space.

    Parameters:
    - csv_path: path to the CSV used by the application.
    - days_older_than: integer days; days strictly older than now-days_older_than are compressed.
    - keep: how many frequency components to retain (largest amplitudes).
    - resample_minutes: resampling interval in minutes for FFT.
    - compressed_dir: optional directory to store per-day compressed JSON files; defaults to
      a `compressed` subdirectory next to the CSV.
    """
    if not os.path.exists(csv_path):
        return

    if compressed_dir is None:
        compressed_dir = os.path.join(os.path.dirname(csv_path), "compressed")
    os.makedirs(compressed_dir, exist_ok=True)

    async with _lock:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            _compress_and_prune,
            csv_path,
            days_older_than,
            keep,
            resample_minutes,
            compressed_dir,
        )


def _compress_and_prune(
    csv_path: str,
    days_older_than: int,
    keep: int,
    resample_minutes: int,
    compressed_dir: str,
) -> None:
    # Read all rows synchronously
    rows = _read_all(csv_path)
    if not rows:
        return

    # Parse timestamps and group by date string
    parsed = []
    for ts_iso, val in rows:
        try:
            ts = datetime.fromisoformat(ts_iso)
        except Exception:
            continue
        parsed.append((ts, val))

    parsed.sort(key=lambda x: x[0])

    # Partition by day
    groups = {}
    for ts, val in parsed:
        day = ts.date().isoformat()
        groups.setdefault(day, []).append((ts, val))

    cutoff = (datetime.now() - timedelta(days=days_older_than)).date()

    days_to_compress = [d for d in groups.keys() if datetime.fromisoformat(d).date() < cutoff]

    compressed_days = set()
    for day in days_to_compress:
        day_rows = groups.get(day, [])
        if len(day_rows) < 2:
            # not enough data to compress reasonably
            continue
        try:
            comp = _compress_day(day, day_rows, keep, resample_minutes)
        except Exception:
            # skip problematic days
            continue
        if comp is None:
            continue
        out_path = os.path.join(compressed_dir, f"{day}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(comp, f, ensure_ascii=False)
        compressed_days.add(day)

    if not compressed_days:
        return

    # Re-write CSV keeping only rows not in compressed_days
    remaining = [r for r in rows if datetime.fromisoformat(r[0]).date().isoformat() not in compressed_days]
    # Write back CSV atomically
    tmp_path = csv_path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "value"])
        for ts_iso, val in remaining:
            writer.writerow([ts_iso, val])
    os.replace(tmp_path, csv_path)


def _compress_day(day: str, rows: List[Tuple[datetime, float]], keep: int, resample_minutes: int):
    """Resample a day's rows to a uniform grid and return a compact dict with top frequencies.

    Returns None on failure.
    """
    # Build uniform grid for that day
    # Start at midnight of that day
    try:
        day_date = datetime.fromisoformat(day).date()
    except Exception:
        return None
    start = datetime.combine(day_date, datetime.min.time())
    end = start + timedelta(days=1)
    dt = resample_minutes * 60.0
    n = int((end - start).total_seconds() / dt)
    if n < 2:
        return None

    grid = np.array([start.timestamp() + i * dt for i in range(n)])
    # Map input rows to seconds since epoch and values
    times = np.array([t.timestamp() for t, _ in rows])
    values = np.array([v for _, v in rows], dtype=float)

    # Compute bin centers and average values per grid cell
    # Use linear interpolation onto grid after handling potential duplicates
    # If there are duplicate timestamps, average them first
    uniq_times, idx = np.unique(times, return_inverse=True)
    if len(uniq_times) != len(times):
        # average duplicates
        agg = {}
        for i, ut in enumerate(uniq_times):
            vals = values[idx == i]
            agg[ut] = float(np.mean(vals))
        times = np.array(list(agg.keys()))
        values = np.array(list(agg.values()))

    # Interpolate linearly to grid. For points outside observed range use nearest
    try:
        interp = np.interp(grid, times, values, left=values[0], right=values[-1])
    except Exception:
        return None

    # Remove mean (store mean separately)
    mean_val = float(np.mean(interp))
    signal = interp - mean_val

    # FFT
    try:
        fft = np.fft.rfft(signal)
        freqs = np.fft.rfftfreq(len(signal), d=dt)
        amps = np.abs(fft)
        phases = np.angle(fft)
    except Exception:
        return None

    # Select top `keep` frequencies by amplitude (excluding the DC component at index 0 unless it's large)
    indices = np.argsort(amps)[::-1]
    # Filter zero-amplitude entries and keep top N
    selected = []
    for idx in indices:
        if len(selected) >= keep:
            break
        if amps[idx] <= 0:
            continue
        selected.append(int(idx))

    freqs_out = []
    for i in selected:
        freqs_out.append([float(freqs[i]), float(amps[i]), float(phases[i])])

    return {
        "date": day,
        "n": int(len(signal)),
        "dt": float(dt),
        "mean": mean_val,
        "components": freqs_out,
    }


def reconstruct_day_from_compressed(comp: dict) -> Tuple[np.ndarray, np.ndarray]:
    """Given a compressed day dict (as produced above), reconstruct a time grid and signal.

    Returns (times_seconds_since_epoch, values_array)
    """
    n = comp.get("n")
    dt = comp.get("dt")
    mean = comp.get("mean", 0.0)
    day = comp.get("date")
    if not n or not dt or not day:
        raise ValueError("Invalid compressed data")
    day_date = datetime.fromisoformat(day).date()
    start = datetime.combine(day_date, datetime.min.time())
    grid = np.array([start.timestamp() + i * dt for i in range(n)])
    signal = np.zeros(n)
    # For each component add its sinusoid
    for freq, amp, phase in comp.get("components", []):
        # For a real FFT component k of length n, the amplitude in `amps` is magnitude of complex coefficient.
        # We need to convert amplitude back to complex coefficient. A simple reconstruction is:
        # contribution = (amp) * cos(2*pi*freq*t + phase)
        signal += amp * np.cos(2.0 * math.pi * freq * (np.arange(n) * dt) + phase)
    # Add mean back
    signal = signal + mean
    return grid, signal


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
