"""Timing, memory and environment facts recorded in manifests."""

import platform
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib import metadata

import psutil


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@contextmanager
def timed(timings: dict[str, float], name: str) -> Iterator[None]:
    """Store the wall time of the ``with`` block in ``timings[name]`` (seconds)."""
    start = time.perf_counter()
    yield
    timings[name] = round(time.perf_counter() - start, 3)


def peak_memory_bytes() -> int:
    """OS-reported peak resident memory of this process over its lifetime."""
    info = psutil.Process().memory_info()
    if hasattr(info, "peak_wset"):  # Windows
        return int(info.peak_wset)
    import resource  # noqa: PLC0415 - POSIX only

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)  # Linux reports KiB


def environment_info() -> dict[str, object]:
    packages = ("nyc-housing-pipeline", "polars", "dagster", "psutil")
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "logical_cpus": psutil.cpu_count(),
        "total_memory_bytes": psutil.virtual_memory().total,
        "packages": {name: metadata.version(name) for name in packages},
    }
