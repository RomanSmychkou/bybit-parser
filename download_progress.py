"""Low-overhead terminal progress reporting for bulk downloads."""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass


@dataclass
class _Snapshot:
    total: int = 0
    completed: int = 0
    skipped: int = 0
    failed: int = 0
    active: int = 0
    bytes_done: int = 0


class DownloadProgress:
    """A non-blocking progress display fed by download lifecycle events."""

    def __init__(self, total: int, interval: float = 1.0) -> None:
        self._snapshot = _Snapshot(total=total)
        self._lock = threading.Lock()
        self._started_at = time.monotonic()
        self._stop = threading.Event()
        self._interval = interval
        self._thread = threading.Thread(
            target=self._render_loop,
            name="download-progress",
            daemon=True,
        )

    def start(self) -> None:
        if self._snapshot.total:
            self._thread.start()

    def task_started(self) -> None:
        with self._lock:
            self._snapshot.active += 1

    def task_finished(self, size: int) -> None:
        with self._lock:
            self._snapshot.active -= 1
            self._snapshot.completed += 1
            self._snapshot.bytes_done += size

    def task_skipped(self) -> None:
        with self._lock:
            self._snapshot.skipped += 1
            self._snapshot.completed += 1

    def task_failed(self) -> None:
        with self._lock:
            self._snapshot.active -= 1
            self._snapshot.failed += 1
            self._snapshot.completed += 1

    def close(self) -> None:
        self._stop.set()
        self._render(final=True)

    def _render_loop(self) -> None:
        while not self._stop.wait(self._interval):
            self._render()

    def _render(self, final: bool = False) -> None:
        with self._lock:
            snapshot = _Snapshot(**vars(self._snapshot))

        elapsed = max(time.monotonic() - self._started_at, 0.001)
        rate = snapshot.bytes_done / elapsed
        remaining = max(snapshot.total - snapshot.completed, 0)
        eta = remaining / (snapshot.completed / elapsed) if snapshot.completed else 0
        progress = (
            f"{snapshot.completed}/{snapshot.total} "
            f"({snapshot.completed / snapshot.total:.0%})"
        )
        line = (
            f"downloads {progress} | active {snapshot.active} | "
            f"failed {snapshot.failed} | {format_bytes(snapshot.bytes_done)} | "
            f"{format_bytes(rate)}/s | ETA {format_duration(eta)}"
        )
        if snapshot.skipped:
            line += f" | skipped {snapshot.skipped}"

        if sys.stderr.isatty():
            end = "\n" if final else ""
            print(f"\r\033[K{line}", end=end, file=sys.stderr, flush=True)
        else:
            # IDE terminals and redirected stderr may not report themselves
            # as TTYs, so keep progress visible instead of suppressing it.
            print(line, file=sys.stderr, flush=True)


def format_bytes(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{value:.0f} B"
        value /= 1024
    return f"{value:.1f} TiB"


def format_duration(seconds: float) -> str:
    if not seconds:
        return "--:--"
    seconds = int(seconds)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"
