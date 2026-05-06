"""Shared 'enough data?' gate used by train.py and walk_forward.py.

The gate counts distinct dates of logged odds snapshots. The model SHALL NOT
produce trustworthy results below the threshold (default 60 days) and MUST
emit a loud warning when invoked with insufficient data.

Per project rule (README): no synthetic odds. No backfill. We just wait.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
ODDS_DIR = _DATA_DIR / "odds"

DEFAULT_MIN_DAYS = 60


@dataclass(frozen=True)
class GateDecision:
    days_logged: int
    threshold: int
    sufficient: bool
    warning_text: str


def count_logged_odds_days(odds_dir: Path = ODDS_DIR) -> int:
    """Count distinct YYYY-MM-DD prefixes among snapshot filenames."""
    if not odds_dir.exists():
        return 0
    days: set[str] = set()
    for f in odds_dir.glob("*.json"):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", f.stem)
        if m:
            days.add(m.group(1))
    return len(days)


def gate(
    *, min_days: int = DEFAULT_MIN_DAYS,
    odds_dir: Path = ODDS_DIR,
    allow_unsafe: bool = False,
) -> GateDecision:
    days = count_logged_odds_days(odds_dir)
    sufficient = days >= min_days
    text = (
        f"data gate: {days} distinct odds-snapshot days logged "
        f"(threshold = {min_days})."
    )
    if not sufficient:
        text += (
            "  ⚠️  RESULTS ARE NOT RELIABLE. Backtest infrastructure is dormant "
            "by design until the project has accumulated enough real odds. "
            "Do not tune weights, ship a model, or claim CLV based on what "
            "comes out below this gate. "
            "Pass allow_unsafe=True only for SCAFFOLDING smoke-tests."
        )
    decision = GateDecision(
        days_logged=days, threshold=min_days,
        sufficient=sufficient, warning_text=text,
    )
    if sufficient:
        logger.info(text)
    else:
        # Log at ERROR level so it shows up in any reasonable handler.
        logger.error(text)
    if not sufficient and not allow_unsafe:
        raise InsufficientOddsError(text)
    return decision


class InsufficientOddsError(RuntimeError):
    """Raised when the gate is closed and the caller didn't opt-in."""
