"""Join actual HR outcomes onto the full-slate log.

`run_daily._write_full_slate_log` records EVERY non-skipped prediction — picks
and passes alike — but deliberately stops short of settling them: "Outcomes are
NOT settled here — join actual HR by (batter_id, game_pk) offline when building
the dataset." This module is that join.

Why it matters: the daily archives only retain picks that cleared the EV gate,
a sample selected for model-vs-market disagreement (the 2026-06-11 P1 backtest
measured zero incremental signal in model_prob conditional on that selection).
Calibrating on picks alone is statistically poisoned. The full-slate log is the
unbiased frame; it just needed labels.

Two label sources, tried in that order:

  1. `archive`  — settlement blocks already committed in data/daily_archives/.
     Free, offline, and authoritative (written by settle.py). Covers only the
     rows that were also picks, so ~17% of the frame — and precisely the biased
     17%, which is why source 2 is not optional.
  2. `boxscore` — /v1/game/{game_pk}/boxscore via the MLB Stats API, gated on
     an authoritative 'Final' status from /v1/schedule. Reuses settle.py's
     `_hr_count_for_batter` verbatim so a backfilled label and a live-settled
     label can never disagree.

Output is a compact label store at data/processed/full_slate_outcomes.json,
keyed by "batter_id|game_pk". It stores ONLY labels — never a copy of the
features — so it stays small and the full-slate log remains the single source
of truth for model inputs. `build_labeled_dataset()` performs the join.

Resumable and idempotent: games whose boxscore has been read are recorded in
`games_processed` and never re-fetched, and the store is flushed periodically
so an interrupted run keeps its progress. Safe to re-run at any time.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
FULL_SLATE_DIR = _DATA_DIR / "full_slate"
DAILY_ARCHIVES_DIR = _DATA_DIR / "daily_archives"
STORE_PATH = _DATA_DIR / "processed" / "full_slate_outcomes.json"

TIERS = ("primary", "secondary", "shadow")

# Flush the store to disk every N games so a timeout or rate-limit doesn't
# throw away an hour of fetching.
FLUSH_EVERY_GAMES = 25


def _key(batter_id: int, game_pk: int) -> str:
    return f"{int(batter_id)}|{int(game_pk)}"


# ---------------------------------------------------------------------------
# Label store
# ---------------------------------------------------------------------------

@dataclass
class LabelStore:
    """Persistent (batter_id, game_pk) -> HR count map.

    `labels` holds only batters who actually took a plate appearance. A key
    absent from `labels` whose game IS in `games_processed` means the batter
    was on the projected slate but never batted (scratched, defensive sub,
    pinch-hit that never came) — no label exists and the row must be dropped
    from training rather than scored as a zero.
    """
    labels: dict[str, int] = field(default_factory=dict)
    sources: dict[str, str] = field(default_factory=dict)
    games_processed: set[int] = field(default_factory=set)

    def has(self, batter_id: int, game_pk: int) -> bool:
        return _key(batter_id, game_pk) in self.labels

    def put(self, batter_id: int, game_pk: int, hr: int, source: str) -> None:
        k = _key(batter_id, game_pk)
        # An existing label wins: `archive` labels come from settle.py and are
        # already committed to the tracker's record. Re-deriving them from a
        # boxscore must never silently rewrite settled history.
        if k in self.labels:
            return
        self.labels[k] = int(hr)
        self.sources[k] = source

    def get(self, batter_id: int, game_pk: int) -> Optional[int]:
        return self.labels.get(_key(batter_id, game_pk))


def load_store(path: Path = STORE_PATH) -> LabelStore:
    if not path.exists():
        return LabelStore()
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return LabelStore(
        labels={k: int(v) for k, v in (raw.get("labels") or {}).items()},
        sources=dict(raw.get("sources") or {}),
        games_processed={int(g) for g in (raw.get("games_processed") or [])},
    )


def save_store(store: LabelStore, path: Path = STORE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    by_source: dict[str, int] = {}
    for s in store.sources.values():
        by_source[s] = by_source.get(s, 0) + 1
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "n_labels": len(store.labels),
        "n_games_processed": len(store.games_processed),
        "labels_by_source": by_source,
        "games_processed": sorted(store.games_processed),
        "sources": store.sources,
        "labels": store.labels,
    }
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), sort_keys=True)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Reading the full-slate log
# ---------------------------------------------------------------------------

def iter_full_slate_rows(
    full_slate_dir: Path = FULL_SLATE_DIR,
) -> Iterator[tuple[str, dict]]:
    """Yield (slate_date, row) for every logged prediction, oldest first."""
    for path in sorted(full_slate_dir.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("skipping unreadable full-slate file %s: %s", path, e)
            continue
        day = doc.get("date") or path.stem
        for row in doc.get("rows") or []:
            yield day, row


def games_by_date(full_slate_dir: Path = FULL_SLATE_DIR) -> dict[str, set[int]]:
    """{slate_date: {game_pk, ...}} across the whole log."""
    out: dict[str, set[int]] = {}
    for day, row in iter_full_slate_rows(full_slate_dir):
        gpk = row.get("game_pk")
        if gpk is not None:
            out.setdefault(day, set()).add(int(gpk))
    return out


# ---------------------------------------------------------------------------
# Source 1: archive settlements (offline)
# ---------------------------------------------------------------------------

def harvest_archive_labels(
    store: LabelStore,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
) -> int:
    """Pull settled outcomes out of committed daily archives. No network.

    VOID results carry no usable label — a void means the game didn't finish or
    the batter never hit, not that he failed to homer — so they are skipped.
    """
    added = 0
    for path in sorted(archives_dir.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                archive = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("skipping unreadable archive %s: %s", path, e)
            continue
        settlement = archive.get("settlement") or {}
        for tier in TIERS:
            for res in settlement.get(f"{tier}_results") or []:
                if res.get("outcome") not in ("W", "L"):
                    continue
                gpk = res.get("game_pk")
                if gpk is None:
                    continue
                bid = res.get("batter_id")
                if bid is None:
                    continue
                if store.has(int(bid), int(gpk)):
                    continue
                store.put(int(bid), int(gpk), int(res.get("actual_hr", 0) or 0),
                          source="archive")
                added += 1
    return added


# ---------------------------------------------------------------------------
# Source 2: boxscores (network)
# ---------------------------------------------------------------------------

def _settle_helpers():
    """Import settle.py's parsers lazily.

    Reusing settle.py guarantees a backfilled label and a live-settled label
    are produced by identical code. The cost is that settle -> slate -> predict
    -> as_of_context drags in pandas, so the import is deferred: `--report-only`
    and `--offline-only` never pay for it.
    """
    from src.results.settle import _fetch_game_statuses, _hr_count_for_batter
    return _fetch_game_statuses, _hr_count_for_batter


@dataclass
class BoxscorePass:
    games_fetched: int = 0
    labels_added: int = 0
    games_skipped_not_final: int = 0
    boxscore_failures: int = 0
    # Slate dates whose /v1/schedule call came back empty. settle.py's
    # `_fetch_game_statuses` swallows request errors and returns {}, which is
    # indistinguishable from "no games" at the call site — but a date only
    # appears in the full-slate log because it HAD games, so an empty map means
    # the API call failed. Counted separately: without this, a total network
    # outage reports as "all 627 games still in progress" and exits clean.
    schedule_failures: int = 0


def fetch_boxscore_labels(
    store: LabelStore,
    *,
    client=None,
    full_slate_dir: Path = FULL_SLATE_DIR,
    store_path: Path = STORE_PATH,
    max_games: Optional[int] = None,
) -> BoxscorePass:
    """Fill labels from MLB box scores for games not yet processed.

    One /v1/schedule call per slate date supplies the authoritative game state;
    only 'Final' games are read, so a backfill run mid-slate leaves today's
    games alone and picks them up on the next run.
    """
    fetch_statuses, hr_count_for_batter = _settle_helpers()
    if client is None:
        from src.pipeline.slate import MlbStatsClient
        client = MlbStatsClient()

    per_date = games_by_date(full_slate_dir)
    # Batters we actually need a label for, grouped by game.
    need_by_game: dict[int, set[int]] = {}
    for _day, row in iter_full_slate_rows(full_slate_dir):
        gpk, bid = row.get("game_pk"), row.get("batter_id")
        if gpk is None or bid is None:
            continue
        need_by_game.setdefault(int(gpk), set()).add(int(bid))

    out = BoxscorePass()

    for day in sorted(per_date):
        pending = [g for g in sorted(per_date[day]) if g not in store.games_processed]
        if not pending:
            continue
        if max_games is not None and out.games_fetched >= max_games:
            break

        try:
            statuses = fetch_statuses(client, _date.fromisoformat(day))
        except (ValueError, TypeError) as e:
            logger.warning("bad slate date %r (%s); skipping", day, e)
            continue

        if not statuses:
            out.schedule_failures += 1
            logger.warning("schedule for %s returned no games — treating as an API "
                           "failure, not an empty slate; %d games left for a retry",
                           day, len(pending))
            continue

        for gpk in pending:
            if max_games is not None and out.games_fetched >= max_games:
                break
            state = statuses.get(gpk)
            # A game missing from the schedule, or not yet Final, is left alone
            # so a later run retries it rather than recording a blank game.
            if state != "Final":
                out.games_skipped_not_final += 1
                continue
            try:
                box = client._get(f"/game/{gpk}/boxscore")
            except Exception as e:  # noqa: BLE001 — one bad game must not end the run
                logger.warning("boxscore fetch failed for game_pk=%s: %s: %s",
                               gpk, type(e).__name__, e)
                out.boxscore_failures += 1
                continue

            for bid in sorted(need_by_game.get(gpk, ())):
                if store.has(bid, gpk):
                    continue
                hr = hr_count_for_batter(box, bid)
                if hr is None:
                    continue          # did not bat — no label, drop downstream
                store.put(bid, gpk, hr, source="boxscore")
                out.labels_added += 1

            store.games_processed.add(gpk)
            out.games_fetched += 1
            if out.games_fetched % FLUSH_EVERY_GAMES == 0:
                save_store(store, store_path)
                logger.info("… %d games fetched, %d labels",
                            out.games_fetched, out.labels_added)

    return out


# ---------------------------------------------------------------------------
# Coverage reporting
# ---------------------------------------------------------------------------

@dataclass
class Coverage:
    rows_total: int
    rows_labeled: int
    rows_unlabeled: int
    rows_no_game_pk: int
    labeled_by_source: dict[str, int]
    hr_rate: Optional[float]
    date_min: Optional[str]
    date_max: Optional[str]
    rows_with_odds: int
    rows_with_odds_labeled: int

    @property
    def pct_labeled(self) -> float:
        return (self.rows_labeled / self.rows_total * 100.0) if self.rows_total else 0.0


def coverage(
    store: LabelStore,
    full_slate_dir: Path = FULL_SLATE_DIR,
) -> Coverage:
    total = labeled = no_gpk = with_odds = with_odds_labeled = 0
    hrs = 0
    by_source: dict[str, int] = {}
    dates: list[str] = []
    for day, row in iter_full_slate_rows(full_slate_dir):
        total += 1
        dates.append(day)
        gpk, bid = row.get("game_pk"), row.get("batter_id")
        has_odds = bool(row.get("matched_odds"))
        if has_odds:
            with_odds += 1
        if gpk is None or bid is None:
            no_gpk += 1
            continue
        lab = store.get(int(bid), int(gpk))
        if lab is None:
            continue
        labeled += 1
        hrs += 1 if lab >= 1 else 0
        src = store.sources.get(_key(int(bid), int(gpk)), "?")
        by_source[src] = by_source.get(src, 0) + 1
        if has_odds:
            with_odds_labeled += 1
    return Coverage(
        rows_total=total,
        rows_labeled=labeled,
        rows_unlabeled=total - labeled - no_gpk,
        rows_no_game_pk=no_gpk,
        labeled_by_source=by_source,
        hr_rate=(hrs / labeled) if labeled else None,
        date_min=min(dates) if dates else None,
        date_max=max(dates) if dates else None,
        rows_with_odds=with_odds,
        rows_with_odds_labeled=with_odds_labeled,
    )


# ---------------------------------------------------------------------------
# The join
# ---------------------------------------------------------------------------

def build_labeled_dataset(
    store: Optional[LabelStore] = None,
    *,
    full_slate_dir: Path = FULL_SLATE_DIR,
    store_path: Path = STORE_PATH,
    require_odds: bool = False,
    model_version: Optional[str] = None,
) -> list[dict]:
    """Join labels onto full-slate rows. One dict per labeled prediction.

    Every row carries `label` (1 if the batter homered, else 0) plus
    `actual_hr` and `slate_date`. Unlabeled rows are dropped, so a batter who
    never took a plate appearance never becomes a negative example.

    `require_odds` keeps only rows with a real market price attached — needed
    for EV/CLV work, unnecessary for pure calibration. `model_version` filters
    to a single model generation, which matters whenever a downstream consumer
    reads `model_prob` or `components`: those are not comparable across the
    2026-07-01 rebuild.
    """
    if store is None:
        store = load_store(store_path)
    out: list[dict] = []
    for day, row in iter_full_slate_rows(full_slate_dir):
        gpk, bid = row.get("game_pk"), row.get("batter_id")
        if gpk is None or bid is None:
            continue
        if require_odds and not row.get("matched_odds"):
            continue
        hr = store.get(int(bid), int(gpk))
        if hr is None:
            continue
        rec = dict(row)
        rec["slate_date"] = day
        rec["actual_hr"] = int(hr)
        rec["label"] = 1 if hr >= 1 else 0
        rec["label_source"] = store.sources.get(_key(int(bid), int(gpk)), "?")
        out.append(rec)

    if model_version is not None:
        versions = _version_by_date(full_slate_dir)
        out = [r for r in out if versions.get(r["slate_date"]) == model_version]
    return out


def _version_by_date(full_slate_dir: Path = FULL_SLATE_DIR) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(full_slate_dir.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        day = doc.get("date") or path.stem
        if doc.get("model_version"):
            out[day] = doc["model_version"]
    return out


# ---------------------------------------------------------------------------
# Orchestration + CLI
# ---------------------------------------------------------------------------

def backfill(
    *,
    offline_only: bool = False,
    max_games: Optional[int] = None,
    full_slate_dir: Path = FULL_SLATE_DIR,
    archives_dir: Path = DAILY_ARCHIVES_DIR,
    store_path: Path = STORE_PATH,
    client=None,
) -> dict:
    """Run both label sources and persist the store. Idempotent."""
    store = load_store(store_path)
    before = len(store.labels)

    from_archive = harvest_archive_labels(store, archives_dir)
    save_store(store, store_path)
    logger.info("archive pass: +%d labels", from_archive)

    box = BoxscorePass()
    network_error: Optional[str] = None
    if not offline_only:
        try:
            box = fetch_boxscore_labels(
                store, client=client, full_slate_dir=full_slate_dir,
                store_path=store_path, max_games=max_games,
            )
        except Exception as e:  # noqa: BLE001 — report, keep offline progress
            network_error = f"{type(e).__name__}: {e}"
            logger.warning("boxscore pass failed: %s", network_error)
        save_store(store, store_path)

    cov = coverage(store, full_slate_dir)
    return {
        "labels_before": before,
        "labels_after": len(store.labels),
        "added_from_archive": from_archive,
        "added_from_boxscore": box.labels_added,
        "games_fetched": box.games_fetched,
        "games_skipped_not_final": box.games_skipped_not_final,
        "games_processed_total": len(store.games_processed),
        "boxscore_failures": box.boxscore_failures,
        "schedule_failures": box.schedule_failures,
        "network_error": network_error,
        "coverage": {
            "rows_total": cov.rows_total,
            "rows_labeled": cov.rows_labeled,
            "rows_unlabeled": cov.rows_unlabeled,
            "rows_no_game_pk": cov.rows_no_game_pk,
            "pct_labeled": round(cov.pct_labeled, 1),
            "labeled_by_source": cov.labeled_by_source,
            "observed_hr_rate": round(cov.hr_rate, 4) if cov.hr_rate is not None else None,
            "rows_with_odds": cov.rows_with_odds,
            "rows_with_odds_labeled": cov.rows_with_odds_labeled,
            "date_range": [cov.date_min, cov.date_max],
        },
        "store_path": str(store_path),
    }


def main() -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(
        description="Backfill actual HR outcomes onto the full-slate log.",
    )
    p.add_argument("--offline-only", action="store_true",
                   help="Use committed archive settlements only; no API calls.")
    p.add_argument("--max-games", type=int, default=None,
                   help="Stop after fetching N box scores (for incremental runs).")
    p.add_argument("--report-only", action="store_true",
                   help="Print current coverage without fetching anything.")
    args = p.parse_args()

    if args.report_only:
        store = load_store()
        cov = coverage(store)
        print(json.dumps({
            "labels": len(store.labels),
            "games_processed": len(store.games_processed),
            "rows_total": cov.rows_total,
            "rows_labeled": cov.rows_labeled,
            "pct_labeled": round(cov.pct_labeled, 1),
            "labeled_by_source": cov.labeled_by_source,
            "observed_hr_rate": round(cov.hr_rate, 4) if cov.hr_rate is not None else None,
            "rows_with_odds_labeled": cov.rows_with_odds_labeled,
            "date_range": [cov.date_min, cov.date_max],
        }, indent=2))
        return 0

    result = backfill(offline_only=args.offline_only, max_games=args.max_games)
    print(json.dumps(result, indent=2))
    # Offline progress is already saved either way; a non-zero exit just tells
    # CI the run was degraded. Schedule failures count here specifically so a
    # dead API can't masquerade as "every game is still in progress".
    degraded = (
        result.get("network_error")
        or result.get("schedule_failures")
        or result.get("boxscore_failures")
    )
    return 1 if degraded else 0


if __name__ == "__main__":
    raise SystemExit(main())
