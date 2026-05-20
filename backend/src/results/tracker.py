"""Aggregate settled picks → running ROI / hit rate / CLV / calibration.

Reads:
  data/daily_archives/YYYY-MM-DD.json   (per-day picks + settlement block)
  data/odds/*.json                      (snapshot history, used for CLV)

Writes:
  data/processed/tracker.json    (web-friendly running stats)

CLV (closing line value): for each pick, find the LATEST odds snapshot whose
fetched_at is BEFORE the game's commence time. Compare the pick's taken
American odds to the closing American odds for the same batter on the same
side / line. CLV % = decimal(taken) / decimal(close) - 1, in pp.

Positive CLV is the cleanest edge proof — it doesn't depend on small-sample
realized outcomes. Per the project README, prioritize CLV over short-window ROI.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from src.odds.ev import american_to_decimal
from src.pipeline.filters import annotate_filter_status, passes_quad, passes_triple
from src.pipeline.slate import normalize_name

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
PROCESSED_DIR = _DATA_DIR / "processed"
DAILY_ARCHIVES_DIR = _DATA_DIR / "daily_archives"
ODDS_DIR = _DATA_DIR / "odds"
TRACKER_PATH = PROCESSED_DIR / "tracker.json"

CALIBRATION_BUCKETS = (
    (0.00, 0.05), (0.05, 0.10), (0.10, 0.15),
    (0.15, 0.20), (0.20, 0.25), (0.25, 0.30),
    (0.30, 0.40), (0.40, 0.60),
)

TIERS = ("primary", "secondary", "shadow")

# Tracker reports post-build picks only. The 2026-05-13 calibration release
# (commits 9c65339, ac6f23a, 24b1ea5, d27fea2) shipped four structural fixes
# to the Bayesian blend, p_per_pa clip, pitcher_factor clip, and breakout
# weights. Pre-build picks were generated against a known-miscalibrated
# model — their CLV and ROI reflect noise from that model, not the one in
# production now. Exclude them from headline metrics.
TRACKER_START_DATE = _date(2026, 5, 13)


# ---------------------------------------------------------------------------
# Loading archives + snapshots
# ---------------------------------------------------------------------------

_ARCHIVE_PAT = re.compile(r"(\d{4}-\d{2}-\d{2})\.json")


def _list_archive_files(start: Optional[_date], end: Optional[_date],
                         archives_dir: Path) -> list[tuple[_date, Path]]:
    if not archives_dir.exists():
        return []
    out: list[tuple[_date, Path]] = []
    for f in sorted(archives_dir.glob("*.json")):
        m = _ARCHIVE_PAT.match(f.name)
        if not m:
            continue
        d = _date.fromisoformat(m.group(1))
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append((d, f))
    return out


def _load_snapshots_for_date(d: _date, odds_dir: Path) -> list[dict]:
    """All snapshots whose filename starts with the given date (sorted by filename → time)."""
    if not odds_dir.exists():
        return []
    out: list[dict] = []
    for f in sorted(odds_dir.glob(f"{d.isoformat()}-*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                out.append(json.load(fh))
        except Exception as e:    # noqa: BLE001
            logger.warning("failed to read %s: %s", f, e)
    return out


# ---------------------------------------------------------------------------
# CLV
# ---------------------------------------------------------------------------

def _closing_quote_for_pick(
    pick: dict, snapshots_by_date: dict[_date, list[dict]],
) -> Optional[int]:
    """Return the closing AMERICAN price for the same batter on the same side.

    Closing = the latest snapshot taken BEFORE the game's commence_time, on
    the snapshot day matching the game date. Picks the BEST book (max American)
    for an apples-to-apples comparison with how we computed our 'taken' price.
    """
    game_dt_iso = pick.get("game_datetime")
    if not game_dt_iso:
        return None
    try:
        game_dt = datetime.fromisoformat(game_dt_iso)
    except ValueError:
        return None

    snaps = snapshots_by_date.get(game_dt.date()) or []
    if not snaps:
        return None

    target_norm = normalize_name(pick.get("batter", ""))

    # Walk snapshots in reverse chronological order until we find one taken before commence.
    closing: Optional[int] = None
    for snap in reversed(snaps):
        fetched = snap.get("fetched_at")
        if not fetched:
            continue
        try:
            f_dt = datetime.fromisoformat(fetched)
        except ValueError:
            continue
        if f_dt >= game_dt:
            continue
        # First (most recent) snapshot before game time wins.
        # Snapshot quotes are alt-HR market (implicitly point=0.5/Over); the
        # batter price lives in `bet_over_american`.
        prices: list[int] = []
        for q in snap.get("quotes", []) or []:
            if normalize_name(q.get("batter_name", "")) != target_norm:
                continue
            price = q.get("bet_over_american")
            if price is None:
                continue
            prices.append(int(price))
        if prices:
            closing = max(prices)
            break
    return closing


def _clv_pct(taken_american: int, closing_american: int) -> float:
    """CLV % = (decimal(taken) / decimal(close) − 1) × 100."""
    return (american_to_decimal(taken_american) /
            american_to_decimal(closing_american) - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

@dataclass
class TrackerSummary:
    total_picks: int = 0
    wins: int = 0
    losses: int = 0
    voids: int = 0
    units_staked: float = 0.0
    units_profit: float = 0.0
    roi_pct: float = 0.0
    hit_rate: float = 0.0
    avg_clv_pct: Optional[float] = None
    n_picks_with_clv: int = 0


@dataclass
class TrackerOutput:
    last_updated: datetime
    date_range: tuple[Optional[_date], Optional[_date]]
    summary: TrackerSummary
    by_book: dict = field(default_factory=dict)
    rolling_30d: TrackerSummary = field(default_factory=TrackerSummary)
    calibration: list[dict] = field(default_factory=list)


def _summarize(rows: Iterable[dict], picks_lookup: dict[str, dict]) -> TrackerSummary:
    s = TrackerSummary()
    clvs: list[float] = []
    for r in rows:
        s.total_picks += 1
        if r["outcome"] == "W":
            s.wins += 1
        elif r["outcome"] == "L":
            s.losses += 1
        else:
            s.voids += 1
        s.units_profit += float(r.get("profit_units", 0.0))

        # CLV — only when we recorded a closing price for this pick.
        key = f"{r['batter_id']}|{r.get('game_pk') or ''}"
        meta = picks_lookup.get(key, {})
        if "clv_pct" in meta and meta["clv_pct"] is not None:
            clvs.append(meta["clv_pct"])

    s.units_staked = float(s.wins + s.losses)
    if s.units_staked > 0:
        s.roi_pct = round(s.units_profit / s.units_staked * 100.0, 2)
        s.hit_rate = round(s.wins / s.units_staked, 4)
    if clvs:
        s.avg_clv_pct = round(sum(clvs) / len(clvs), 2)
        s.n_picks_with_clv = len(clvs)
    return s


def _calibration_buckets(rows: Iterable[dict]) -> list[dict]:
    rows = list(rows)
    out: list[dict] = []
    for lo, hi in CALIBRATION_BUCKETS:
        bucket = [r for r in rows if r["outcome"] in ("W", "L")
                  and lo <= float(r["model_prob"]) < hi]
        if not bucket:
            continue
        n = len(bucket)
        wins = sum(1 for r in bucket if r["outcome"] == "W")
        expected = sum(float(r["model_prob"]) for r in bucket) / n
        out.append({
            "model_prob_min": round(lo, 2),
            "model_prob_max": round(hi, 2),
            "n_picks": n,
            "expected_hit_rate": round(expected, 4),
            "actual_hit_rate": round(wins / n, 4),
        })
    return out


def _by_book_breakdown(rows: list[dict], picks_lookup: dict[str, dict]) -> dict:
    out: dict[str, TrackerSummary] = {}
    for r in rows:
        key = f"{r['batter_id']}|{r.get('game_pk') or ''}"
        book = (picks_lookup.get(key, {}) or {}).get("best_book", "unknown")
        out.setdefault(book, TrackerSummary())
    for r in rows:
        key = f"{r['batter_id']}|{r.get('game_pk') or ''}"
        book = (picks_lookup.get(key, {}) or {}).get("best_book", "unknown")
        s = out[book]
        s.total_picks += 1
        if r["outcome"] == "W": s.wins += 1
        elif r["outcome"] == "L": s.losses += 1
        else: s.voids += 1
        s.units_profit += float(r.get("profit_units", 0.0))
    for book, s in out.items():
        s.units_staked = float(s.wins + s.losses)
        if s.units_staked > 0:
            s.roi_pct = round(s.units_profit / s.units_staked * 100.0, 2)
            s.hit_rate = round(s.wins / s.units_staked, 4)
        s.units_profit = round(s.units_profit, 4)
    return {book: s.__dict__ for book, s in out.items()}


# ---------------------------------------------------------------------------
# Build tracker
# ---------------------------------------------------------------------------

def _load_all_tiers_rows(
    start: Optional[_date], end: Optional[_date],
    archives_dir: Path, odds_dir: Path,
    snapshots_cache: dict[_date, list[dict]],
) -> dict[str, tuple[list[dict], dict[str, dict]]]:
    """Walk daily archives once and return per-tier (rows, picks_meta)."""
    per_tier: dict[str, tuple[list[dict], dict[str, dict]]] = {
        tier: ([], {}) for tier in TIERS
    }
    files = _list_archive_files(start, end, archives_dir)
    for d, af in files:
        with open(af, "r", encoding="utf-8") as f:
            archive = json.load(f)
        settle = archive.get("settlement") or {}
        if not settle:
            continue
        if d not in snapshots_cache:
            snapshots_cache[d] = _load_snapshots_for_date(d, odds_dir)
        snaps_by_date = {d: snapshots_cache[d]}

        for tier in TIERS:
            results = settle.get(f"{tier}_results") or []
            if not results:
                continue
            rows, picks_meta = per_tier[tier]
            picks_list = archive.get(f"{tier}_picks") or []
            # Backfill filter_status for archives written before 2026-05-20
            # (the day the triple-filter experiment shipped). Computing it
            # on the fly means pre-experiment data lines up with post.
            if any("filter_status" not in p for p in picks_list):
                for p in picks_list:
                    p.setdefault("tier", tier)
                annotate_filter_status(picks_list)
            picks_index: dict[str, dict] = {}
            for p in picks_list:
                key = f"{p['batter_id']}|{p.get('game_pk') or ''}"
                picks_index[key] = p
            for r in results:
                r = dict(r)
                r["_tier"] = tier
                r["settled_date"] = d.isoformat()
                key = f"{r['batter_id']}|{r.get('game_pk') or ''}"
                pick = picks_index.get(key) or {}
                fs = pick.get("filter_status") or {}
                # Stamp the filter flags onto the row itself so downstream
                # filtering doesn't need a secondary lookup.
                r["passes_triple"] = bool(fs.get("passes_triple", passes_triple(pick)))
                r["passes_quad"] = bool(fs.get("passes_quad", passes_quad(pick)))
                rows.append(r)
                taken = r.get("over_american") or pick.get("dk_odds") or pick.get("fd_odds")
                close = _closing_quote_for_pick(pick, snaps_by_date) if pick else None
                clv = _clv_pct(int(taken), int(close)) if (taken and close) else None
                picks_meta[key] = {**pick, "taken_american": taken,
                                    "closing_american": close, "clv_pct": clv,
                                    "_tier": tier}
    return per_tier


def build_tracker(
    *, start: Optional[_date] = None, end: Optional[_date] = None,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
    processed_dir: Path = PROCESSED_DIR,
    odds_dir: Path = ODDS_DIR,
) -> TrackerOutput:
    snapshots_cache: dict[_date, list[dict]] = {}
    per_tier = _load_all_tiers_rows(start, end, archives_dir, odds_dir, snapshots_cache)

    primary_rows, primary_meta = per_tier["primary"]
    secondary_rows, secondary_meta = per_tier["secondary"]
    shadow_rows, shadow_meta = per_tier["shadow"]

    # Per-tier summaries (ROI, hit rate, CLV per tier).
    summary = _summarize(primary_rows, primary_meta)
    summary_secondary = _summarize(secondary_rows, secondary_meta)
    summary_shadow = _summarize(shadow_rows, shadow_meta)
    by_book = _by_book_breakdown(primary_rows, primary_meta) # primary only — what we'd actually bet

    # Filter-level rollups. Baseline = every settled pick (the three summary_*
    # fields above). Triple/quad = subset that would have cleared each filter.
    # See backend/docs/filter_experiment.md.
    def _filter_rows(rows: list[dict], flag: str) -> list[dict]:
        return [r for r in rows if r.get(flag)]

    by_filter: dict[str, dict] = {}
    for filter_name, flag in (("triple", "passes_triple"), ("quad", "passes_quad")):
        f_primary = _filter_rows(primary_rows, flag)
        f_secondary = _filter_rows(secondary_rows, flag)
        f_shadow = _filter_rows(shadow_rows, flag)
        by_filter[filter_name] = {
            "summary_primary": _summarize(f_primary, primary_meta).__dict__,
            "summary_secondary": _summarize(f_secondary, secondary_meta).__dict__,
            "summary_shadow": _summarize(f_shadow, shadow_meta).__dict__,
            "calibration_combined": _calibration_buckets(f_primary + f_secondary + f_shadow),
            "calibration_n_picks_primary": len(f_primary),
            "calibration_n_picks_secondary": len(f_secondary),
            "calibration_n_picks_shadow": len(f_shadow),
        }
    # Dropped = passed baseline but failed triple. Surfaces H3 directly.
    dropped_primary = [r for r in primary_rows if not r.get("passes_triple")]
    dropped_secondary = [r for r in secondary_rows if not r.get("passes_triple")]
    dropped_shadow = [r for r in shadow_rows if not r.get("passes_triple")]
    by_filter["dropped_by_triple"] = {
        "summary_primary": _summarize(dropped_primary, primary_meta).__dict__,
        "summary_secondary": _summarize(dropped_secondary, secondary_meta).__dict__,
        "summary_shadow": _summarize(dropped_shadow, shadow_meta).__dict__,
        "calibration_n_picks_primary": len(dropped_primary),
        "calibration_n_picks_secondary": len(dropped_secondary),
        "calibration_n_picks_shadow": len(dropped_shadow),
    }

    rolling_cutoff = (end or _date.today()) - timedelta(days=30)
    rolling_rows = [
        r for r in primary_rows
        if _date.fromisoformat(r.get("settled_date", "")) >= rolling_cutoff
    ] if any("settled_date" in r for r in primary_rows) else primary_rows[-300:]
    rolling = _summarize(rolling_rows, primary_meta)

    # Calibration uses COMBINED data (all three tiers) — bigger sample, faster signal.
    calibration_rows = primary_rows + secondary_rows + shadow_rows
    calibration = _calibration_buckets(calibration_rows)

    out = TrackerOutput(
        last_updated=datetime.now().astimezone(),
        date_range=(start, end),
        summary=summary,
        by_book=by_book,
        rolling_30d=rolling,
        calibration=calibration,
    )

    out_path = processed_dir / "tracker.json"
    processed_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "last_updated": out.last_updated.isoformat(),
            "date_range": [d.isoformat() if d else None for d in out.date_range],
            "summary_primary": summary.__dict__,
            "summary_secondary": summary_secondary.__dict__,
            "summary_shadow": summary_shadow.__dict__,
            # Calibration is on combined (all three tiers) data — bigger sample.
            "calibration_combined": calibration,
            "calibration_n_picks_primary": len(primary_rows),
            "calibration_n_picks_secondary": len(secondary_rows),
            "calibration_n_picks_shadow": len(shadow_rows),
            "by_book": out.by_book,
            "rolling_30d": out.rolling_30d.__dict__,
            # Back-compat: keep `summary` pointing at primary so existing UI works.
            "summary": out.summary.__dict__,
            "calibration": out.calibration,
            # Filter-level rollups (per filter_experiment.md). The top-level
            # summary_* fields above remain baseline; by_filter contains the
            # triple, quad, and dropped-by-triple equivalents.
            "by_filter": by_filter,
        }, f, indent=2)
    logger.info("wrote %s (primary=%d, secondary=%d, shadow=%d)",
                out_path, len(primary_rows), len(secondary_rows), len(shadow_rows))
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    out = build_tracker(start=TRACKER_START_DATE)
    print(json.dumps({
        "total_picks": out.summary.total_picks,
        "roi_pct": out.summary.roi_pct,
        "hit_rate": out.summary.hit_rate,
        "avg_clv_pct": out.summary.avg_clv_pct,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
