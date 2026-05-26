"""
Closing-line odds capture (data collection only — NOT a model run).

WHY
    The daily-picks pipeline fires through ~23:00 UTC, so its last odds
    snapshot sits a median ~60 min before first pitch. That makes recovered
    CLV a "T-minus" proxy rather than a true close. This job pulls additional
    odds snapshots in the late window (near/at first pitches) so the back half
    of the filter experiment (2026-05-26 .. 2026-06-18) gets TRUE closing-line
    value. clv_recover.py reads these automatically — same dir, same format.

WHAT IT DOES NOT DO
    No predictions, no filters, no feature engineering, no MODEL_VERSION.
    It reuses ONLY the odds fetch + snapshot writer (src.odds.fetch /
    src.odds.log), so it is lock-safe during the pre-registered experiment.
    Snapshots are tagged `-close` in the filename to distinguish them from
    pipeline fires, but are otherwise byte-identical in structure.

USAGE
    python -m scripts.capture_closing_odds
"""

from __future__ import annotations

import logging
import sys
from dataclasses import asdict, is_dataclass
from datetime import date as _date
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

# Odds-only imports: these pull in `requests` and stdlib, NOT the model.
from src.odds.fetch import DEFAULT_BOOKS, fetch_today_hr_props  # noqa: E402
from src.odds.log import write_snapshot  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("capture_closing_odds")


def _to_snapshot_dict(fetch, cutoff_date: _date) -> dict:
    """Plain-dict snapshot, identical shape to run_daily._to_snapshot_dict."""

    def _coerce(o):
        if is_dataclass(o):
            return _coerce(asdict(o))
        if isinstance(o, (datetime, _date)):
            return o.isoformat()
        if isinstance(o, dict):
            return {k: _coerce(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_coerce(x) for x in o]
        return o

    return {
        "fetched_at": fetch.fetched_at,
        "as_of_date": cutoff_date.isoformat(),
        "books_filtered": list(fetch.books),
        "markets": fetch.markets,
        "requests_remaining": fetch.requests_remaining,
        "requests_used": fetch.requests_used,
        "events": _coerce(fetch.events),
        "quotes": _coerce(fetch.quotes),
        "errors": fetch.errors,
    }


def main() -> int:
    # No slate-team filter: we want the closing line for every still-pregame
    # game. skip_started_clock_skew_min (default 5) drops games already underway,
    # so a late fire naturally captures only the games yet to start.
    fetch = fetch_today_hr_props(books=DEFAULT_BOOKS, relevant_team_pairs=None)
    today = datetime.now().astimezone().date()
    path = write_snapshot(_to_snapshot_dict(fetch, today), tag="close")
    logger.info(
        "closing capture: %d quotes from %d events (remaining=%s) -> %s",
        len(fetch.quotes), len(fetch.events), fetch.requests_remaining, path,
    )
    if not fetch.quotes:
        logger.info("no pregame quotes (likely no games left to start) — empty snapshot is fine")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
