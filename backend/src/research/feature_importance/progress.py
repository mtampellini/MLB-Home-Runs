"""Timestamped progress logging.

Every line printed by this module includes a wall-clock timestamp and an
elapsed-since-start counter. The script is designed to run for hours
unattended; when you check on it in the morning, you should be able to:

  1. See immediately whether it finished (final "DONE" banner).
  2. If still running, know which phase and which sub-step it's on.
  3. Know how long each phase took (per-phase wall time).
  4. Estimate completion when you see chunk i/N progress.

Output goes to stdout and (if STDOUT is a file) is unbuffered so tail -f works.
"""

from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator


_SCRIPT_START_TS: float | None = None
_PHASE_START_TS: dict[str, float] = {}


def _ensure_started() -> None:
    global _SCRIPT_START_TS
    if _SCRIPT_START_TS is None:
        _SCRIPT_START_TS = time.time()


def _format_elapsed(seconds: float) -> str:
    return str(timedelta(seconds=int(seconds)))


def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _emit(level: str, msg: str) -> None:
    _ensure_started()
    elapsed = time.time() - _SCRIPT_START_TS
    line = f"[{_stamp()}] [+{_format_elapsed(elapsed)}] [{level}] {msg}"
    print(line, flush=True)


def info(msg: str) -> None:
    _emit("INFO", msg)


def warn(msg: str) -> None:
    _emit("WARN", msg)


def error(msg: str) -> None:
    _emit("ERROR", msg)


def banner(msg: str) -> None:
    _emit("====", "=" * 60)
    _emit("====", msg)
    _emit("====", "=" * 60)


@contextmanager
def phase(name: str) -> Iterator[None]:
    """Context manager: prints start/finish for a phase with elapsed time."""
    _PHASE_START_TS[name] = time.time()
    banner(f"PHASE START: {name}")
    try:
        yield
    finally:
        dur = time.time() - _PHASE_START_TS.pop(name, time.time())
        banner(f"PHASE DONE:  {name}  (took {_format_elapsed(dur)})")


def progress_chunk(i: int, total: int, label: str, eta_chunks_per_sec: float | None = None) -> None:
    """Per-chunk progress with optional ETA based on observed throughput."""
    pct = (i / total * 100) if total else 0.0
    eta_str = ""
    if eta_chunks_per_sec and i < total:
        remaining_chunks = total - i
        seconds_left = remaining_chunks / eta_chunks_per_sec
        eta_str = f"  ETA {_format_elapsed(seconds_left)}"
    info(f"chunk {i}/{total} ({pct:5.1f}%): {label}{eta_str}")


def script_done(extra: str = "") -> None:
    elapsed = time.time() - (_SCRIPT_START_TS or time.time())
    banner(f"SCRIPT DONE — total wall time {_format_elapsed(elapsed)}.  {extra}".strip())
