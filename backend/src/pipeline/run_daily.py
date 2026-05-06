"""Daily orchestrator: slate → predictions → odds → EV → picks.json.

Step-by-step:
    1. Build today's slate from MLB Stats (skipping games with no posted lineup).
    2. predict_slate() → P(HR) per batter (skipped batters carry skip reasons).
    3. Fetch odds via The Odds API. Log a timestamped snapshot to data/odds/.
    4. Match odds to slate by normalized batter name. For each matched batter:
       - Compute consensus de-vigged market prob (or fall back per book).
       - Pick the best Over price across books → that drives EV.
       - Compute EV %; keep picks with EV >= 25.
    5. Write picks.json (project root) and skipped_batters_YYYY-MM-DD.json.

External clients are injectable so the orchestrator is testable without
hitting MLB Stats, The Odds API, pybaseball, or Open-Meteo.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date as _date
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.backtest.as_of_context import AsOfContext
from src.model.baseline import BaselineConfig
from src.model.predict import (
    FeatureProvider,
    PredictionRow,
    predict_slate,
    top_n_features,
)
from src.odds.ev import (
    american_to_implied_prob,
    devig_consensus,
    devig_two_way,
    ev_pct,
)
from src.odds.fetch import (
    DEFAULT_BOOKS,
    FetchResult,
    HRPropQuote,
    OddsAPIClient,
    fetch_today_hr_props,
)
from src.odds.log import write_snapshot
from src.pipeline.slate import MlbStatsClient, build_slate, normalize_name


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
PROCESSED_DIR = _DATA_DIR / "processed"
PICKS_PATH_DEFAULT = PROJECT_ROOT / "picks.json"

MODEL_VERSION = "v7-baseline-0.1.0"
EV_THRESHOLD_PCT = 25.0
PICK_LINE = 0.5


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class DailyReport:
    cutoff_date: _date
    slate_size: int
    predictions: int
    skipped_batters: int
    odds_snapshot_path: Optional[Path]
    matched_with_odds: int
    picks_count: int
    picks_path: Path
    skipped_path: Path
    requests_remaining: Optional[int]
    requests_used: Optional[int]
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Picks assembly
# ---------------------------------------------------------------------------

def _index_quotes_by_norm_name(quotes: list[HRPropQuote]) -> dict[str, list[HRPropQuote]]:
    out: dict[str, list[HRPropQuote]] = {}
    for q in quotes:
        out.setdefault(normalize_name(q.batter_name), []).append(q)
    return out


def _market_prob_devig(quotes_for_batter: list[HRPropQuote]) -> Optional[float]:
    """Best-effort de-vig.

    - If both FD and DK have BOTH sides (Over+Under) → consensus across them.
    - If exactly one book has both sides → de-vig that book alone.
    - If no book has Under → return None (caller should skip — single-side
      implied prob systematically overstates the market).
    """
    pairs: list[tuple[int, int]] = []
    for q in quotes_for_batter:
        if q.under_american is not None:
            pairs.append((q.over_american, q.under_american))
    if not pairs:
        return None
    if len(pairs) == 1:
        return devig_two_way(*pairs[0])
    return devig_consensus(pairs)


def _best_over_prices_by_book(quotes: list[HRPropQuote]) -> dict[str, int]:
    """Map book key → best Over price for that book (in case of dupes)."""
    out: dict[str, int] = {}
    for q in quotes:
        cur = out.get(q.book)
        if cur is None or q.over_american > cur:
            out[q.book] = q.over_american
    return out


def _assemble_pick(
    row: PredictionRow,
    quotes: list[HRPropQuote],
    market_prob: float,
    ev_result,
    best_book: str,
    best_price: int,
    book_prices: dict[str, int],
) -> dict:
    entry = row.entry
    pred = row.prediction
    return {
        "batter": entry.batter_name,
        "batter_id": entry.batter_id,
        "batter_hand": entry.batter_hand,
        "team": entry.team,
        "lineup_spot": entry.lineup_spot,
        "pitcher": entry.pitcher_name,
        "pitcher_id": entry.pitcher_id,
        "pitcher_hand": entry.pitcher_hand,
        "park": entry.park,
        "game_pk": entry.game_pk,
        "game_datetime": entry.game_datetime.isoformat(),

        "line": PICK_LINE,
        "fd_odds": book_prices.get("fanduel"),
        "dk_odds": book_prices.get("draftkings"),
        "best_book": best_book,
        "market_prob_devig": round(market_prob, 4),

        "model_prob": round(pred.p_hr, 4),
        "ev_pct": round(ev_result.ev_pct, 2),

        "blended_hr_per_pa": round(pred.blended_hr_per_pa, 5),
        "breakout_score": round(row.breakout.score if row.breakout else 0.0, 4),
        "low_confidence": bool(row.low_confidence),

        # Recent-form diagnostics — surfaced for human review only, not scored.
        "trend_signal": (
            round(row.recent_form.trend_signal, 4)
            if row.recent_form and row.recent_form.trend_signal is not None
            else None
        ),
        "unstable_recent": bool(row.recent_form.unstable_recent) if row.recent_form else False,

        "top_3_features": [
            {
                "name": item["name"],
                "value": round(item["value"], 4),
                "deviation": round(item["deviation"], 4),
            }
            for item in top_n_features(pred, n=3)
        ],
    }


# ---------------------------------------------------------------------------
# JSON writers
# ---------------------------------------------------------------------------

def _write_picks_json(picks: list[dict], cutoff_date: _date,
                      league_hr_per_pa: float, skipped_count: int,
                      skipped_path: Path, output_path: Path) -> Path:
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "as_of_date": cutoff_date.isoformat(),
        "model_version": MODEL_VERSION,
        "league_hr_per_pa": league_hr_per_pa,
        "ev_threshold_pct": EV_THRESHOLD_PCT,
        "picks": picks,
        "skipped_count": skipped_count,
        "skipped_reference": str(skipped_path.relative_to(PROJECT_ROOT))
                             if skipped_path.is_relative_to(PROJECT_ROOT) else str(skipped_path),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return output_path


def _write_skipped_json(rows: list[PredictionRow], cutoff_date: _date,
                        output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"skipped_batters_{cutoff_date.isoformat()}.json"
    payload = {
        "as_of_date": cutoff_date.isoformat(),
        "skipped": [
            {
                "batter_id": r.entry.batter_id,
                "batter_name": r.entry.batter_name,
                "team": r.entry.team,
                "skip_code": r.skip_code,
                "skip_reason": r.skip_reason,
            }
            for r in rows if r.skipped
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Snapshot serialization
# ---------------------------------------------------------------------------

def _to_snapshot_dict(fetch: FetchResult, cutoff_date: _date) -> dict:
    """Convert the FetchResult dataclasses to a plain dict ready for log.write_snapshot."""
    def _coerce(o):
        if is_dataclass(o):
            return _coerce(asdict(o))
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, _date):
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
        "market": fetch.market,
        "requests_remaining": fetch.requests_remaining,
        "requests_used": fetch.requests_used,
        "events": _coerce(fetch.events),
        "quotes": _coerce(fetch.quotes),
        "errors": fetch.errors,
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_daily(
    cutoff_date: Optional[_date] = None,
    *,
    config: BaselineConfig = BaselineConfig(),
    breakout_weights: Optional[dict] = None,
    feature_provider: Optional[FeatureProvider] = None,
    odds_client: Optional[OddsAPIClient] = None,
    slate_client: Optional[MlbStatsClient] = None,
    picks_path: Path = PICKS_PATH_DEFAULT,
    skipped_dir: Path = PROCESSED_DIR,
    ev_threshold_pct: float = EV_THRESHOLD_PCT,
) -> DailyReport:
    cutoff_date = cutoff_date or _date.today()
    ctx = AsOfContext(cutoff_date=cutoff_date)
    logger.info("daily run starting; as_of=%s", cutoff_date)

    # --- 1. Slate ----------------------------------------------------------
    slate, slate_meta = build_slate(cutoff_date, client=slate_client)
    logger.info(
        "slate: %d entries from %d games (skipped %d games without lineups)",
        len(slate), slate_meta["games_with_lineups"],
        slate_meta["games_no_lineup_skipped"],
    )

    # --- 2. Predictions ----------------------------------------------------
    rows = predict_slate(
        slate, ctx, config=config,
        breakout_weights=breakout_weights, provider=feature_provider,
    )
    skipped_path = _write_skipped_json(rows, cutoff_date, skipped_dir)
    skipped_count = sum(1 for r in rows if r.skipped)
    logger.info(
        "predictions: %d kept, %d skipped (skips → %s)",
        len(rows) - skipped_count, skipped_count, skipped_path,
    )

    # --- 3. Odds + log -----------------------------------------------------
    snapshot_path: Optional[Path] = None
    fetch: Optional[FetchResult] = None
    requests_remaining = None
    requests_used = None
    try:
        fetch = fetch_today_hr_props(client=odds_client, books=DEFAULT_BOOKS)
        snapshot_path = write_snapshot(_to_snapshot_dict(fetch, cutoff_date))
        requests_remaining = fetch.requests_remaining
        requests_used = fetch.requests_used
        logger.info(
            "odds: %d quotes from %d events (remaining=%s) → %s",
            len(fetch.quotes), len(fetch.events),
            fetch.requests_remaining, snapshot_path,
        )
    except Exception as e:    # noqa: BLE001
        logger.warning("odds fetch failed (%s: %s) — picks.json will be empty",
                       type(e).__name__, e)

    # --- 4. Match + EV + filter --------------------------------------------
    picks: list[dict] = []
    matched = 0
    if fetch is not None:
        quotes_idx = _index_quotes_by_norm_name(fetch.quotes)
        for row in rows:
            if row.skipped or row.prediction is None:
                continue
            qs = quotes_idx.get(normalize_name(row.entry.batter_name)) or []
            if not qs:
                continue
            matched += 1
            mkt = _market_prob_devig(qs)
            if mkt is None:
                continue
            book_prices = _best_over_prices_by_book(qs)
            best_book = max(book_prices, key=book_prices.get)
            best_price = book_prices[best_book]
            ev = ev_pct(
                model_prob=row.prediction.p_hr,
                over_american=best_price,
                market_prob_devig=mkt,
            )
            if ev.ev_pct < ev_threshold_pct:
                continue
            picks.append(_assemble_pick(
                row=row, quotes=qs, market_prob=mkt, ev_result=ev,
                best_book=best_book, best_price=best_price, book_prices=book_prices,
            ))
    picks.sort(key=lambda p: p["ev_pct"], reverse=True)
    logger.info(
        "EV filter: %d picks above %.0f%% (matched %d / %d slate batters)",
        len(picks), ev_threshold_pct, matched, len(rows) - skipped_count,
    )

    # --- 5. Write picks.json + dated copy ---------------------------------
    final_picks_path = _write_picks_json(
        picks=picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=picks_path,
    )
    # Permanent dated copy for settle.py / tracker.py — picks.json gets
    # overwritten daily, but data/processed/picks_YYYY-MM-DD.json is forever.
    dated_picks_path = skipped_dir / f"picks_{cutoff_date.isoformat()}.json"
    _write_picks_json(
        picks=picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=dated_picks_path,
    )
    logger.info("wrote %s (%d picks; dated copy: %s)",
                final_picks_path, len(picks), dated_picks_path)

    return DailyReport(
        cutoff_date=cutoff_date,
        slate_size=len(slate),
        predictions=len(rows) - skipped_count,
        skipped_batters=skipped_count,
        odds_snapshot_path=snapshot_path,
        matched_with_odds=matched,
        picks_count=len(picks),
        picks_path=final_picks_path,
        skipped_path=skipped_path,
        requests_remaining=requests_remaining,
        requests_used=requests_used,
        metadata=slate_meta,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Load backend/.env if present (gitignored). Used for ODDS_API_KEY in local
    # runs. CI workflows pass the secret via env, so load_dotenv is a no-op there.
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except ImportError:
        pass
    report = run_daily()
    print(json.dumps({
        "as_of_date": report.cutoff_date.isoformat(),
        "slate_size": report.slate_size,
        "skipped": report.skipped_batters,
        "matched_with_odds": report.matched_with_odds,
        "picks_count": report.picks_count,
        "picks_path": str(report.picks_path),
        "odds_snapshot": str(report.odds_snapshot_path) if report.odds_snapshot_path else None,
        "requests_remaining": report.requests_remaining,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
