from typing import Callable, Dict, Any, List, Tuple
from statistics import mean

# Registry interface: name -> function(readings)->result
_registry: Dict[str, Callable[[List[Tuple[str, float]]], Any]] = {}


def register(name: str):
    def _decorator(fn: Callable[[List[Tuple[str, float]]], Any]):
        _registry[name] = fn
        return fn

    return _decorator


def compute_all(readings: List[Tuple[str, float]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, fn in _registry.items():
        try:
            out[name] = fn(readings)
        except Exception as e:
            out[name] = {"error": str(e)}
    return out


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
