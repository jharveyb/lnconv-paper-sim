#!/usr/bin/env python3
"""Sample whole-machine + lnconv resource use, one key=value line per interval."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

import psutil


def _dirty_kb() -> int:
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("Dirty:"):
                return int(line.split()[1])
    return 0


def _lnconv_aggregate() -> tuple[int, int]:
    pids = 0
    rss_kb = 0
    for proc in psutil.process_iter(["name", "memory_info"]):
        try:
            if proc.info["name"] == "lnconv":
                rss_kb += proc.info["memory_info"].rss // 1024
                pids += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids, rss_kb


def sample(interval: float) -> str:
    # psutil.cpu_percent(interval=...) blocks for `interval` seconds and
    # returns the busy% over that window — does sampling AND pacing in one call.
    cpu_pct = psutil.cpu_percent(interval=interval)
    load1, load5, load15 = psutil.getloadavg()
    mem = psutil.virtual_memory()
    pids, rss_kb = _lnconv_aggregate()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    fields = {
        "ts": ts,
        "load1": f"{load1:.2f}",
        "load5": f"{load5:.2f}",
        "load15": f"{load15:.2f}",
        "mem_total_kb": mem.total // 1024,
        "mem_avail_kb": mem.available // 1024,
        "mem_dirty_kb": _dirty_kb(),
        "cpu_pct": f"{cpu_pct:.1f}",
        "lnconv_pids": pids,
        "lnconv_rss_kb_sum": rss_kb,
    }
    return " ".join(f"{k}={v}" for k, v in fields.items())


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--interval", type=float, default=30.0,
                   help="seconds between samples (default 30)")
    args = p.parse_args()
    try:
        while True:
            print(sample(args.interval), flush=True)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
