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
    DEFAULT_VIG_TIERS,
    VigTiers,
    american_to_implied_prob,
    devig_consensus,
    devig_two_way,
    ev_pct,
    single_sided_consensus,
    single_sided_fair_prob,
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

# Tier-config constants. These are pipeline-level — kept here (rather than
# in BaselineConfig, which is the model's config) so the separation between
# "what the model produces" and "what we choose to bet" stays clean.
EV_THRESHOLD_PCT = 25.0           # primary tier: would-bet floor
SHADOW_EV_THRESHOLD_PCT = 10.0    # shadow tier: calibration-only floor
PRIMARY_PICK_LIMIT = 10           # cap top-N by edge_pct for the primary tier;
                                  # rank 11+ at EV>=25% (or above price cap) → SECONDARY.
PRIMARY_MAX_PRICE = 900           # primary tier ALSO requires best_price <= +900.
                                  # Above +900 implied prob < 10%; small calibration
                                  # errors translate to outsized betting losses. Long
                                  # shots flow to secondary, tracked but not bet.
                                  # See docs/known_issues.md item #4c.

PICK_LINE = 0.5
PRIMARY_PICKS_FILENAME = "picks.json"
SECONDARY_PICKS_FILENAME = "secondary_picks.json"
SHADOW_PICKS_FILENAME = "shadow_picks.json"


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
    picks_count: int                    # PRIMARY tier picks
    picks_path: Path                    # primary picks.json path
    skipped_path: Path
    requests_remaining: Optional[int]
    requests_used: Optional[int]
    secondary_picks_count: int = 0
    secondary_picks_path: Optional[Path] = None
    shadow_picks_count: int = 0
    shadow_picks_path: Optional[Path] = None
    metadata: dict = field(default_factory=dict)
    funnel: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Picks assembly
# ---------------------------------------------------------------------------

def _index_quotes_by_norm_name(quotes: list[HRPropQuote]) -> dict[str, list[HRPropQuote]]:
    out: dict[str, list[HRPropQuote]] = {}
    for q in quotes:
        out.setdefault(normalize_name(q.batter_name), []).append(q)
    return out


def _market_prob_devig(
    quotes_for_batter: list[HRPropQuote],
    tiers: VigTiers = DEFAULT_VIG_TIERS,
) -> tuple[Optional[float], str]:
    """Estimate fair Over probability with method tag.

    Returns (fair_prob, method) where method is one of:
      - 'two_way'           main market both sides, single book
      - 'two_way_consensus' main market both sides, multiple books
      - 'single_sided'      Over price only, with vig haircut (fallback)
      - 'none'              no usable price → caller skips
    """
    pairs: list[tuple[int, int]] = []
    for q in quotes_for_batter:
        if q.main_over_american is not None and q.main_under_american is not None:
            pairs.append((q.main_over_american, q.main_under_american))
    if pairs:
        if len(pairs) == 1:
            return devig_two_way(*pairs[0]), "two_way"
        return devig_consensus(pairs), "two_way_consensus"

    # Fallback: single-sided from the alt-market Over price.
    bet_prices = [q.bet_over_american for q in quotes_for_batter
                  if q.bet_over_american is not None]
    if not bet_prices:
        return None, "none"
    return single_sided_consensus(bet_prices, tiers=tiers), "single_sided"


def _best_bet_prices_by_book(quotes: list[HRPropQuote]) -> dict[str, int]:
    """Map book key → best alt @ 0.5 Over price (the actual bet price).

    Skips books that didn't quote the alt market for this batter.
    """
    out: dict[str, int] = {}
    for q in quotes:
        if q.bet_over_american is None:
            continue
        cur = out.get(q.book)
        if cur is None or q.bet_over_american > cur:
            out[q.book] = q.bet_over_american
    return out




def _devig_inputs_per_book(quotes: list[HRPropQuote]) -> dict:
    """Surface the raw main-market prices each book contributed to the de-vig.
    Auditability for picks.json — lets the human eyeball the math."""
    out: dict[str, dict] = {}
    for q in quotes:
        if q.main_over_american is None or q.main_under_american is None:
            continue
        # If a book somehow appears twice (shouldn't), keep the higher Over.
        cur = out.get(q.book)
        if cur is None or q.main_over_american > cur["over"]:
            out[q.book] = {"over": q.main_over_american, "under": q.main_under_american}
    return out


def _assemble_pick(
    row: PredictionRow,
    quotes: list[HRPropQuote],
    market_prob: float,
    ev_result,
    best_book: str,
    best_price: int,
    book_prices: dict[str, int],
    devig_method: str = "two_way",
    tier: str = "primary",
    daily_rank: int = 0,
) -> dict:
    entry = row.entry
    pred = row.prediction
    return {
        "tier": tier,                                 # 'primary', 'secondary', 'shadow'
        "daily_rank": daily_rank,                     # 1-based rank by EV across all tiers
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
        "fd_odds": book_prices.get("fanduel"),       # alt @ 0.5 Over (bet price)
        "dk_odds": book_prices.get("draftkings"),
        "best_book": best_book,
        "market_prob_devig": round(market_prob, 4),
        "devig_method": devig_method,                # 'two_way', 'two_way_consensus', 'single_sided'
        # Raw main-market prices used to compute the de-vigged fair prob (when available).
        "devig_inputs": _devig_inputs_per_book(quotes),

        "model_prob": round(pred.p_hr, 4),
        "ev_pct": round(ev_result.ev_pct, 2),         # Option A: model × payout - (1-model)
        "edge_pct": round(ev_result.edge_pct, 2),     # Option B framing: model_prob - market_prob_devig (pp)

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

def _write_picks_json(
    picks: list[dict],
    cutoff_date: _date,
    league_hr_per_pa: float,
    skipped_count: int,
    skipped_path: Path,
    output_path: Path,
    *,
    tier: str = "primary",
    ev_threshold_pct_min: float = EV_THRESHOLD_PCT,
    ev_threshold_pct_max: Optional[float] = None,
    slate_meta: Optional[dict] = None,
) -> Path:
    sm = slate_meta or {}
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "as_of_date": cutoff_date.isoformat(),
        "model_version": MODEL_VERSION,
        "league_hr_per_pa": league_hr_per_pa,
        "tier": tier,
        "ev_threshold_pct_min": ev_threshold_pct_min,
        "ev_threshold_pct_max": ev_threshold_pct_max,
        # Back-compat: primary tier also exposes the old field name.
        **({"ev_threshold_pct": EV_THRESHOLD_PCT} if tier == "primary" else {}),
        # Slate-level transparency — surfaces the pre-game filter so a glance
        # at picks.json answers "did the cron run too late or too early?"
        "total_games_today": sm.get("games_total"),
        "games_pregame": sm.get("games_pregame"),
        "games_excluded_live_or_complete": sm.get("games_excluded_live_or_complete"),
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
        "markets": fetch.markets,
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
    secondary_picks_path: Optional[Path] = None,
    shadow_picks_path: Optional[Path] = None,
    skipped_dir: Path = PROCESSED_DIR,
    ev_threshold_pct: float = EV_THRESHOLD_PCT,
    shadow_ev_threshold_pct: float = SHADOW_EV_THRESHOLD_PCT,
    primary_pick_limit: int = PRIMARY_PICK_LIMIT,
    primary_max_price: int = PRIMARY_MAX_PRICE,
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
    # Slate teams to filter Odds API events — saves credits (each unfiltered
    # event call costs 2 credits with both markets active).
    slate_team_pairs = set(slate_meta.get("team_pairs", []))
    try:
        fetch = fetch_today_hr_props(
            client=odds_client, books=DEFAULT_BOOKS,
            relevant_team_pairs=slate_team_pairs or None,
        )
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

    # --- 4. Match + EV + tier classification (3 tiers, top-N cap on primary) -
    funnel = {
        "predictions_kept": len(rows) - skipped_count,
        "matched_any_quote": 0,
        "matched_main_market": 0,
        "matched_alt_market": 0,
        "two_way_devig": 0,
        "single_sided_devig": 0,
        "survived_devig": 0,
        "below_shadow_threshold": 0,
        "above_primary_floor_pre_cap": 0,   # cleared 25% before top-N cap applied
        "primary_tier": 0,
        "secondary_tier": 0,
        "shadow_tier": 0,
    }

    # Pass 1: build all eligibility candidates (anyone clearing the SHADOW floor).
    candidates: list[dict] = []
    if fetch is not None:
        quotes_idx = _index_quotes_by_norm_name(fetch.quotes)
        for row in rows:
            if row.skipped or row.prediction is None:
                continue
            qs = quotes_idx.get(normalize_name(row.entry.batter_name)) or []
            if not qs:
                continue
            funnel["matched_any_quote"] += 1
            has_main = any(q.main_over_american is not None and q.main_under_american is not None
                           for q in qs)
            has_alt = any(q.bet_over_american is not None for q in qs)
            if has_main:
                funnel["matched_main_market"] += 1
            if has_alt:
                funnel["matched_alt_market"] += 1
            if not has_alt:
                continue

            mkt, devig_method = _market_prob_devig(qs)
            if mkt is None:
                continue
            funnel["survived_devig"] += 1
            if devig_method == "single_sided":
                funnel["single_sided_devig"] += 1
            else:
                funnel["two_way_devig"] += 1

            book_prices = _best_bet_prices_by_book(qs)
            if not book_prices:
                continue
            best_book = max(book_prices, key=book_prices.get)
            best_price = book_prices[best_book]
            ev = ev_pct(
                model_prob=row.prediction.p_hr,
                over_american=best_price,
                market_prob_devig=mkt,
            )

            if ev.ev_pct < shadow_ev_threshold_pct:
                funnel["below_shadow_threshold"] += 1
                continue
            if ev.ev_pct >= ev_threshold_pct:
                funnel["above_primary_floor_pre_cap"] += 1

            candidates.append({
                "row": row, "qs": qs, "mkt": mkt, "ev": ev,
                "devig_method": devig_method,
                "book_prices": book_prices,
                "best_book": best_book, "best_price": best_price,
            })

    # Pass 2: assign tiers.
    #   PRIMARY: ev>=25%  AND  best_price <= +900   (top N by EDGE_PCT desc)
    #   SECONDARY: ev>=25%  AND  (price > +900  OR  rank > N by edge)
    #   SHADOW: 10% <= ev < 25%
    # daily_rank is the global rank by EV across ALL candidates, preserved for
    # post-deploy "did rank-K outperform rank-J" analysis regardless of tier.
    candidates.sort(key=lambda c: c["ev"].ev_pct, reverse=True)
    for global_rank, c in enumerate(candidates, start=1):
        c["_daily_rank"] = global_rank

    above_primary_floor = [c for c in candidates if c["ev"].ev_pct >= ev_threshold_pct]
    primary_eligible = [c for c in above_primary_floor
                        if c["best_price"] <= primary_max_price]
    funnel["primary_eligible_after_price_cap"] = len(primary_eligible)
    funnel["above_price_cap_pushed_to_secondary"] = (
        len(above_primary_floor) - len(primary_eligible)
    )

    # Top N by edge_pct for primary.
    primary_eligible.sort(key=lambda c: c["ev"].edge_pct, reverse=True)
    primary_chosen = primary_eligible[:primary_pick_limit]
    primary_chosen_keys = {
        (c["row"].entry.batter_id, c["row"].entry.game_pk) for c in primary_chosen
    }

    primary_picks: list[dict] = []
    secondary_picks: list[dict] = []
    shadow_picks: list[dict] = []

    for c in candidates:
        key = (c["row"].entry.batter_id, c["row"].entry.game_pk)
        if c["ev"].ev_pct >= ev_threshold_pct:
            if key in primary_chosen_keys:
                tier = "primary"
                funnel["primary_tier"] += 1
            else:
                tier = "secondary"
                funnel["secondary_tier"] += 1
        else:
            tier = "shadow"
            funnel["shadow_tier"] += 1

        pick = _assemble_pick(
            row=c["row"], quotes=c["qs"], market_prob=c["mkt"], ev_result=c["ev"],
            best_book=c["best_book"], best_price=c["best_price"],
            book_prices=c["book_prices"], devig_method=c["devig_method"],
            tier=tier, daily_rank=c["_daily_rank"],
        )
        if tier == "primary":
            primary_picks.append(pick)
        elif tier == "secondary":
            secondary_picks.append(pick)
        else:
            shadow_picks.append(pick)

    # File ordering: primary by edge_pct desc (the conviction view); the
    # other tiers by ev_pct desc (the leverage view).
    primary_picks.sort(key=lambda p: p["edge_pct"], reverse=True)
    secondary_picks.sort(key=lambda p: p["ev_pct"], reverse=True)
    shadow_picks.sort(key=lambda p: p["ev_pct"], reverse=True)

    logger.info(
        "EV funnel: preds=%d → matched=%d → alt=%d → devig(2-way=%d, 1-sided=%d) → "
        "ev>=%.0f%%(pre-cap)=%d → eligible<=+%d=%d → primary(top%d by edge)=%d → "
        "secondary=%d → shadow(>=%.0f%%)=%d",
        funnel["predictions_kept"], funnel["matched_any_quote"],
        funnel["matched_alt_market"],
        funnel["two_way_devig"], funnel["single_sided_devig"],
        ev_threshold_pct, funnel["above_primary_floor_pre_cap"],
        primary_max_price, funnel["primary_eligible_after_price_cap"],
        primary_pick_limit, funnel["primary_tier"],
        funnel["secondary_tier"],
        shadow_ev_threshold_pct, funnel["shadow_tier"],
    )

    # --- 5. Write picks.json (primary) + shadow_picks.json + dated copies --
    final_picks_path = _write_picks_json(
        picks=primary_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=picks_path,
        tier="primary",
        ev_threshold_pct_min=ev_threshold_pct,
        ev_threshold_pct_max=None,
        slate_meta=slate_meta,
    )
    # Dated permanent copy for settle.py + tracker.py.
    dated_picks_path = skipped_dir / f"picks_{cutoff_date.isoformat()}.json"
    _write_picks_json(
        picks=primary_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=dated_picks_path,
        tier="primary",
        ev_threshold_pct_min=ev_threshold_pct,
        ev_threshold_pct_max=None,
        slate_meta=slate_meta,
    )

    # Secondary tier — picks above EV floor but past the top-N cap. Same
    # schema as primary; web doesn't consume; tracker compares vs primary.
    secondary_path_resolved = (
        secondary_picks_path or (picks_path.parent / SECONDARY_PICKS_FILENAME)
    )
    final_secondary_path = _write_picks_json(
        picks=secondary_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=secondary_path_resolved,
        tier="secondary",
        ev_threshold_pct_min=ev_threshold_pct,
        ev_threshold_pct_max=None,
        slate_meta=slate_meta,
    )
    dated_secondary_path = skipped_dir / f"secondary_picks_{cutoff_date.isoformat()}.json"
    _write_picks_json(
        picks=secondary_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=dated_secondary_path,
        tier="secondary",
        ev_threshold_pct_min=ev_threshold_pct,
        ev_threshold_pct_max=None,
        slate_meta=slate_meta,
    )

    # Shadow tier — same schema, separate file. Web does NOT consume this;
    # it feeds tracker.py + post-deploy calibration analysis.
    shadow_path_resolved = (
        shadow_picks_path
        or (picks_path.parent / SHADOW_PICKS_FILENAME)
    )
    final_shadow_path = _write_picks_json(
        picks=shadow_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=shadow_path_resolved,
        tier="shadow",
        ev_threshold_pct_min=shadow_ev_threshold_pct,
        ev_threshold_pct_max=ev_threshold_pct,
        slate_meta=slate_meta,
    )
    dated_shadow_path = skipped_dir / f"shadow_picks_{cutoff_date.isoformat()}.json"
    _write_picks_json(
        picks=shadow_picks,
        cutoff_date=cutoff_date,
        league_hr_per_pa=config.league_hr_per_pa,
        skipped_count=skipped_count,
        skipped_path=skipped_path,
        output_path=dated_shadow_path,
        tier="shadow",
        ev_threshold_pct_min=shadow_ev_threshold_pct,
        ev_threshold_pct_max=ev_threshold_pct,
        slate_meta=slate_meta,
    )

    logger.info(
        "wrote primary=%s (%d picks) + secondary=%s (%d) + shadow=%s (%d)",
        final_picks_path, len(primary_picks),
        final_secondary_path, len(secondary_picks),
        final_shadow_path, len(shadow_picks),
    )

    return DailyReport(
        cutoff_date=cutoff_date,
        slate_size=len(slate),
        predictions=len(rows) - skipped_count,
        skipped_batters=skipped_count,
        odds_snapshot_path=snapshot_path,
        matched_with_odds=funnel["matched_any_quote"],
        funnel=dict(funnel),
        picks_count=len(primary_picks),
        picks_path=final_picks_path,
        secondary_picks_count=len(secondary_picks),
        secondary_picks_path=final_secondary_path,
        shadow_picks_count=len(shadow_picks),
        shadow_picks_path=final_shadow_path,
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
