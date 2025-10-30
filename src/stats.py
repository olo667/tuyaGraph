from typing import Callable, Dict, Any, List, Tuple
from statistics import mean
from datetime import datetime
import numpy as np

# Registry interface: name -> {'fn': function, 'meta': metadata}
_registry: Dict[str, Dict[str, Any]] = {}


def register(name: str, meta: Dict[str, Any] = None):
    """Decorator to register a simple stat/transform.

    The registered function must accept a single argument `readings` (list of (iso, value)).
    Optional metadata can describe an external endpoint or parameters for UI discovery.
    """
    def _decorator(fn: Callable[[List[Tuple[str, float]]], Any]):
        _registry[name] = {"fn": fn, "meta": meta or {}}
        return fn

    return _decorator


def compute_all(readings: List[Tuple[str, float]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, entry in _registry.items():
        fn = entry.get("fn")
        try:
            out[name] = fn(readings)
        except Exception as e:
            out[name] = {"error": str(e)}
    return out


def list_transforms() -> List[Dict[str, Any]]:
    """Return a list of registered transform names and their metadata for UI discovery."""
    out: List[Dict[str, Any]] = []
    for name, entry in _registry.items():
        out.append({"name": name, "meta": entry.get("meta", {})})
    return out


def call_registered(name: str, readings: List[Tuple[str, float]]):
    """Invoke a registered stat by name. Raises KeyError if not found."""
    entry = _registry.get(name)
    if not entry:
        raise KeyError(name)
    fn = entry.get("fn")
    return fn(readings)


# Built-in stats
@register("count")
def _count(readings):
    return len(readings)


@register("mean")
def _mean(readings):
    vals = [v for _, v in readings]
    return mean(vals) if vals else None


@register("min")
def _min(readings):
    vals = [v for _, v in readings]
    return min(vals) if vals else None


@register("max")
def _max(readings):
    vals = [v for _, v in readings]
    return max(vals) if vals else None


def _parse_iso(ts: str) -> datetime:
    # helper to parse ISO timestamps saved by storage (datetime.isoformat)
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        # fallback: try to handle Z suffix
        if ts.endswith("Z"):
            try:
                return datetime.fromisoformat(ts[:-1])
            except Exception:
                pass
        raise


def mean_temperature_across_days_hourly(
    readings: List[Tuple[str, float]],
    start: datetime = None,
    end: datetime = None,
    resolution_minutes: int = 60,
    min_sample_fraction: float = 0.5,
) -> dict:
    """
    Compute the mean temperature for each time-bucket across days in the given time window.

    Parameters:
    - readings: list of (timestamp_iso, value)
    - start, end: optional datetimes to filter readings. If None, no bound is applied on that side.
    - resolution_minutes: bucket size in minutes (must divide 24*60 reasonably). Default 60.

    Returns: list of buckets covering a 24h day. Each bucket is a dict with keys:
      - label: 'HH:MM' start of bucket
      - start_minute: minutes since midnight of bucket start (0..1435)
      - avg: float or None
      - count: number of samples in that bucket
    """
    # clamp/validate resolution
    try:
        res = int(resolution_minutes)
        if res <= 0 or res > 24 * 60:
            res = 60
    except Exception:
        res = 60

    buckets_count = (24 * 60) // res

    # Build uniform grid (minutes since midnight) for a day
    grid = np.array([i * res for i in range(buckets_count)], dtype=float)

    # Group and interpolate readings into per-day arrays on the common grid
    grid, days, warnings = _interpolate_readings_per_day(
        readings, start=start, end=end, resolution_minutes=res, min_sample_fraction=min_sample_fraction
    )

    if not days:
        # return empty result structure
        out = []
        for i in range(buckets_count):
            start_min = (i * res) % (24 * 60)
            hh = start_min // 60
            mm = start_min % 60
            label = f"{hh:02d}:{mm:02d}"
            out.append({"label": label, "start_minute": start_min, "avg": None, "count": 0})
        return {"series": out, "warnings": [], "days": 0}

    # Sum of values per grid point and count of days contributing to that point
    sum_vals = np.zeros(buckets_count, dtype=float)
    days_count = np.zeros(buckets_count, dtype=int)

    for day, arr in days.items():
        # arr is already an interpolated numpy array on the grid
        if arr is None or len(arr) == 0:
            continue
        sum_vals += arr
        days_count += 1

    # Build output: average per grid point across days that contributed
    out = []
    for i in range(buckets_count):
        start_min = int((i * res) % (24 * 60))
        hh = start_min // 60
        mm = start_min % 60
        label = f"{hh:02d}:{mm:02d}"
        cnt = int(days_count[i])
        if cnt > 0:
            avg = float(sum_vals[i] / cnt)
            out.append({"label": label, "start_minute": start_min, "avg": avg, "count": cnt})
        else:
            out.append({"label": label, "start_minute": start_min, "avg": None, "count": 0})
    return {"series": out, "warnings": warnings, "days": len(days)}
    # end of function


def _interpolate_readings_per_day(
    readings: List[Tuple[str, float]],
    start: datetime = None,
    end: datetime = None,
    resolution_minutes: int = 60,
    min_sample_fraction: float = 0.5,
) -> Tuple[np.ndarray, Dict[str, np.ndarray], List[Dict[str, Any]]]:
    """
    Helper: group readings by day and interpolate each day's values onto a common daily grid.

    Returns (grid_minutes_array, days_dict, warnings)
    - grid: numpy array of minutes-since-midnight for each bucket
    - days_dict: mapping day_iso -> numpy array of floats (length = len(grid))
    - warnings: list of warning dicts about sparse days
    """
    try:
        res = int(resolution_minutes)
        if res <= 0 or res > 24 * 60:
            res = 60
    except Exception:
        res = 60

    buckets_count = (24 * 60) // res
    grid = np.array([i * res for i in range(buckets_count)], dtype=float)

    days_raw = {}
    for ts_iso, val in readings:
        try:
            ts = _parse_iso(ts_iso)
        except Exception:
            continue
        if start and ts < start:
            continue
        if end and ts > end:
            continue
        day = ts.date().isoformat()
        minute_of_day = ts.hour * 60 + ts.minute
        days_raw.setdefault(day, []).append((minute_of_day, float(val)))

    days_out: Dict[str, np.ndarray] = {}
    warnings: List[Dict[str, Any]] = []

    for day, rows in days_raw.items():
        times = np.array([r[0] for r in rows], dtype=float)
        vals = np.array([r[1] for r in rows], dtype=float)
        if len(times) == 0:
            continue
        order = np.argsort(times)
        times = times[order]
        vals = vals[order]
        uniq_times, inv = np.unique(times, return_inverse=True)
        if len(uniq_times) != len(times):
            agg_vals = []
            for j in range(len(uniq_times)):
                agg_vals.append(float(np.mean(vals[inv == j])))
            times = uniq_times
            vals = np.array(agg_vals, dtype=float)

        raw_count = int(len(times))
        expected = buckets_count
        frac = raw_count / expected if expected > 0 else 0.0
        if frac < float(min_sample_fraction):
            warnings.append({
                "day": day,
                "samples": raw_count,
                "expected": expected,
                "fraction": frac,
                "message": f"Only {raw_count} samples for {day} (< {min_sample_fraction*100:.0f}% of expected {expected})",
            })

        if len(times) == 1:
            interp = np.full_like(grid, vals[0], dtype=float)
        else:
            try:
                interp = np.interp(grid, times, vals, left=vals[0], right=vals[-1])
            except Exception:
                interp = None

        days_out[day] = interp

    return grid, days_out, warnings


def compute_rate_of_change(
    readings: List[Tuple[str, float]],
    start: datetime = None,
    end: datetime = None,
    resolution_minutes: int = 60,
    kernel: List[float] = None,
    smoothing_days: int = 1,
    min_sample_fraction: float = 0.5,
) -> Dict[str, Any]:
    """
    Compute rate-of-change (ROC) per day using a linear filter kernel applied to each day's interpolated temperature series.

    Parameters:
    - readings: list of (iso_ts, value)
    - start/end: optional filter datetimes
    - resolution_minutes: bucket size used for interpolation
    - kernel: list of filter weights (e.g. [-0.5, -0.5, 0.5, 0.5])
    - smoothing_days: integer rolling window to average per-day ROC (1 = no smoothing)
    - min_sample_fraction: when to warn about sparse days

    Returns a dict with:
    - grid_minutes: list of bucket start minutes
    - per_day: list of {day, series_per_hour: [...], avg_per_hour: float, avg_abs_per_hour: float}
    - rolling_avg: list of smoothed avg_per_hour aligned to per_day (same length)
    - warnings: list
    """
    if kernel is None:
        kernel = [-0.5, -0.5, 0.5, 0.5]

    grid, days_dict, warnings = _interpolate_readings_per_day(
        readings, start=start, end=end, resolution_minutes=resolution_minutes, min_sample_fraction=min_sample_fraction
    )

    # prepare kernel and normalization: offsets relative to center (can be fractional)
    k = np.array(kernel, dtype=float)
    n = len(k)
    center = (n - 1) / 2.0
    offsets = np.arange(n, dtype=float) - center
    denom = float(np.sum(k * offsets)) * float(resolution_minutes)
    if denom == 0:
        raise ValueError("Kernel normalization resulted in zero denominator; choose a different kernel")

    per_day = []
    days_sorted = sorted(days_dict.keys())
    for day in days_sorted:
        arr = days_dict.get(day)
        if arr is None:
            series_per_hour = [None] * len(grid)
            avg_per_hour = None
            avg_abs_per_hour = None
        else:
            conv = np.convolve(arr, k, mode="same")
            # slope in degC per minute
            slope_per_min = conv / denom
            # convert to degC per hour for readability
            slope_per_hour = slope_per_min * 60.0
            series_per_hour = [float(x) for x in slope_per_hour]
            vals = np.array([v for v in series_per_hour if v is not None])
            if vals.size:
                avg_per_hour = float(np.mean(vals))
                avg_abs_per_hour = float(np.mean(np.abs(vals)))
            else:
                avg_per_hour = None
                avg_abs_per_hour = None

        per_day.append({"day": day, "series_per_hour": series_per_hour, "avg_per_hour": avg_per_hour, "avg_abs_per_hour": avg_abs_per_hour})

    # compute rolling average over days for avg_per_hour
    avgs = [d["avg_per_hour"] for d in per_day]
    rolled: List[Any] = []
    if smoothing_days <= 1:
        rolled = avgs
    else:
        half = smoothing_days // 2
        for i in range(len(avgs)):
            # window [i-half, i+half]
            lo = max(0, i - half)
            hi = min(len(avgs), i + half + 1)
            window = [v for v in avgs[lo:hi] if v is not None]
            rolled.append(float(np.mean(window)) if window else None)

    return {"grid_minutes": [int(x) for x in grid], "per_day": per_day, "rolling_avg": rolled, "warnings": warnings}


@register("rate_of_change_per_day", meta={"endpoint": "/api/rate-of-change", "params": ["resolution_minutes", "smoothing_days", "kernel", "start", "end"]})
def _rate_of_change_registered(readings: List[Tuple[str, float]]):
    """
    Registered convenience function returning rate-of-change per day with sensible defaults.
    Users or callers who need custom kernels or smoothing should call compute_rate_of_change directly.
    """
    return compute_rate_of_change(readings)
