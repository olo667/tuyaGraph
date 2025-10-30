import asyncio
import importlib
import logging
import re
from datetime import datetime
from typing import Optional

from .storage import append_reading

logger = logging.getLogger(__name__)


class Poller:
    def __init__(self, device_id: str, ip: str, local_key: str, csv_path: str, interval: int = 30, simulate_if_missing: bool = True):
        self.device_id = device_id
        self.ip = ip
        self.local_key = local_key
        self.csv_path = csv_path
        self.interval = interval
        self.simulate_if_missing = simulate_if_missing
        self._stop = False
        self._device = None

    async def start(self):
        """Start the polling loop until stop() is called."""
        # Try to import tinytuya
        try:
            tinytuya = importlib.import_module("tinytuya")
        except Exception:
            tinytuya = None
            logger.info("tinytuya not available; falling back to simulation if enabled")

        if tinytuya:
            try:
                # Use generic TuyaDevice interface (works for many devices), user may need to adapt for sensor types
                self._device = tinytuya.Device(self.device_id, self.ip, self.local_key)
                try:
                    self._device.set_version(3.3)
                except Exception:
                    pass
                logger.info("Initialized tinytuya device for polling")
            except Exception as e:
                logger.exception("Failed to initialize tinytuya device: %s", e)
                self._device = None

        count = 0
        while not self._stop:
            try:
                value = await asyncio.get_running_loop().run_in_executor(None, self._read_temp_sync)
                if value is not None:
                    await append_reading(self.csv_path, datetime.utcnow(), float(value))
                    logger.debug("Appended reading %s", value)
                else:
                    logger.debug("No reading obtained")
            except Exception:
                logger.exception("Error during poller loop")
            count += 1
            await asyncio.sleep(self.interval)

    def stop(self):
        self._stop = True

    def _read_temp_sync(self) -> Optional[float]:
        """Synchronous read helper used in threadpool. Returns temperature in Celsius or None."""
        # If device was created, attempt to read
        try:
            if self._device:
                # Generic get status
                try:
                    logger.info("Requesting status from device %s at %s", self.device_id, self.ip)
                    data = self._device.status()
                    logger.info("Received raw response from device: %s", repr(data))
                except Exception:
                    logger.exception("Exception while requesting status from device %s", self.device_id)
                    return None

                # Try common keys - user may need to adapt mapping for their device
                # Search for a numeric value in the status payload, but ignore
                # numeric values that are actually error codes (keys like err/error/code).
                if isinstance(data, dict):
                    # Pattern to detect error-like keys (err, error, code, etc.)
                    error_pattern = re.compile(r"\b(err(?:or)?|error|code)\b", re.I)

                    def find_numeric(obj):
                        """Recursively find the first numeric value in obj while skipping
                        dictionary keys that match error_pattern.
                        Returns a float or None.
                        """
                        # dict: iterate items but skip keys that look like errors
                        if isinstance(obj, dict):
                            for kk, vv in obj.items():
                                try:
                                    if isinstance(kk, str) and error_pattern.search(kk):
                                        logger.debug("Skipping error-like key %s -> %s", kk, vv)
                                        continue
                                except Exception:
                                    # defensive: if key isn't str or regex fails, ignore
                                    pass

                                res = find_numeric(vv)
                                if res is not None:
                                    return res
                            return None

                        # list/tuple: search elements
                        if isinstance(obj, (list, tuple)):
                            for item in obj:
                                res = find_numeric(item)
                                if res is not None:
                                    return res
                            return None

                        # scalar: try to convert to float
                        try:
                            # guard against None, booleans, etc.
                            if obj is None:
                                return None
                            # strings may contain whitespace
                            if isinstance(obj, str):
                                s = obj.strip()
                                # avoid interpreting empty strings
                                if s == "":
                                    return None
                                return float(s)
                            return float(obj)
                        except Exception:
                            return None

                    fv = find_numeric(data)
                    if fv is not None:
                        logger.debug("Interpreted numeric value from device response -> %s", fv)
                        return fv
                else:
                    logger.debug("Device returned non-dict status: %s", type(data))
                return None
        except Exception:
            logger.exception("tinytuya read failed")

        # Fallback simulation
        if self.simulate_if_missing:
            import math, time

            t = time.time()
            # simple sine wave to simulate temperature changes
            return 20.0 + 3.0 * math.sin(t / 300.0)

        return None
