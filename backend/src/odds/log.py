"""Daily odds-snapshot logger.

Every fetch writes a timestamped JSON to data/odds/. Snapshots are NEVER
overwritten — collectively they ARE our historical odds dataset, accumulated
day by day. Project rule: commit data/odds/ to git.

Filename pattern: `YYYY-MM-DD-HHMM[-tag].json`
- The HHMM is local-time minute precision; collisions inside a minute get
  `-1`, `-2`, ... appended so we never clobber an earlier write.
- Optional `tag` lets the caller distinguish e.g. "prematch" vs "closing"
  snapshots taken in the same minute.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from src.odds import fetch as fetch_module  # noqa: F401  (forward ref for type hints)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
ODDS_DIR = _DATA_DIR / "odds"


def _to_serializable(obj: Any) -> Any:
    """Recursively coerce dataclasses, dates, and datetimes to JSON-friendly forms."""
    if is_dataclass(obj):
        return _to_serializable(asdict(obj))
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_serializable(x) for x in obj]
    return obj


def _resolve_path_in(target_dir: Path, when: datetime, tag: Optional[str]) -> Path:
    """Pick a non-conflicting filename in `target_dir` for the given timestamp + tag."""
    target_dir.mkdir(parents=True, exist_ok=True)
    base = when.strftime("%Y-%m-%d-%H%M")
    if tag:
        base = f"{base}-{tag}"
    candidate = target_dir / f"{base}.json"
    if not candidate.exists():
        return candidate
    n = 1
    while True:
        candidate = target_dir / f"{base}-{n}.json"
        if not candidate.exists():
            return candidate
        n += 1


def write_snapshot(
    payload: dict,
    when: Optional[datetime] = None,
    tag: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """Write a snapshot. Returns the file path.

    `output_dir=None` resolves to the module-global `ODDS_DIR` at call time so
    tests can monkeypatch it. Caller passes a Path to redirect.
    """
    when = when or datetime.now().astimezone()
    target_dir = Path(output_dir) if output_dir is not None else ODDS_DIR
    out = _resolve_path_in(target_dir, when, tag)

    full = dict(payload)
    full["_snapshot_written_at"] = datetime.now().astimezone().isoformat()
    serializable = _to_serializable(full)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, sort_keys=False)
    return out


def list_snapshots(start: Optional[date] = None, end: Optional[date] = None) -> list[Path]:
    """List all snapshot files, optionally filtered to a date range (inclusive)."""
    if not ODDS_DIR.exists():
        return []
    files = sorted(ODDS_DIR.glob("*.json"))
    if start is None and end is None:
        return files
    out: list[Path] = []
    for f in files:
        # Filename starts with YYYY-MM-DD-HHMM
        try:
            d = date.fromisoformat(f.stem[:10])
        except ValueError:
            continue
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(f)
    return out


def load_snapshot(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
