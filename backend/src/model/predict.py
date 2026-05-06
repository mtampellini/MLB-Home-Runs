"""Slate orchestrator: features → skip → blend → breakout → baseline.

For each batter on today's slate this:
  1. Pulls batter season + 30d + prior-year via the FeatureProvider.
  2. Applies skip_logic — drops batters with no meaningful track record.
  3. Pulls pitcher features (with vs-RHB / vs-LHB splits) and park+weather.
  4. Blends batter HR/PA across season + recent + (early-season) prior year.
  5. Computes the breakout signal vs prior-year underlying metrics.
  6. Picks the matched platoon split for the pitcher and blends his HR/9.
  7. Calls baseline.predict() to get P(HR ≥ 1) and component breakdown.

Returns one PredictionRow per slate entry. Skipped batters carry skip_reason
so run_daily.py can write them to data/processed/skipped_batters_*.json.

The FeatureProvider abstraction lets tests inject canned dicts so this module
can be exercised end-to-end without touching pybaseball, Open-Meteo, or disk.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

from src.backtest.as_of_context import AsOfContext
from src.features.batter_recent import batter_recent_features
from src.features.batter_season import batter_season_features
from src.features.blend import BlendResult, blend_features
from src.features.breakout import BreakoutScore, compute_breakout_score
from src.features.park_weather import park_weather_features
from src.features.pitcher import pitcher_features
from src.features.skip_logic import should_skip_batter
from src.model.baseline import BaselineConfig, BaselinePrediction, predict

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Inputs and outputs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SlateEntry:
    """One row of today's slate: a batter facing a specific starter at a park."""
    batter_id: int
    batter_name: str
    batter_hand: str        # 'R' or 'L'
    team: str
    pitcher_id: int
    pitcher_name: str
    pitcher_hand: str       # 'R' or 'L'
    park: str               # park code (matches data/park_metadata.json)
    game_datetime: datetime
    lineup_spot: Optional[int] = None
    game_pk: Optional[int] = None     # MLBAM gamePk; used by settle.py


@dataclass
class PredictionRow:
    """Output of predict_slate(). Skipped rows carry reason; otherwise a full prediction."""
    entry: SlateEntry
    skipped: bool
    skip_reason: Optional[str]
    skip_code: Optional[str]
    prediction: Optional[BaselinePrediction] = None
    breakout: Optional[BreakoutScore] = None
    batter_blend: Optional[BlendResult] = None
    pitcher_blend: Optional[BlendResult] = None
    low_confidence: bool = False        # True when prior-year fully drives the prediction

    season: Optional[dict] = None
    recent: Optional[dict] = None
    prior_year: Optional[dict] = None
    pitcher_split_season: Optional[dict] = None
    pitcher_split_recent: Optional[dict] = None
    park_weather: Optional[dict] = None


# ---------------------------------------------------------------------------
# Feature provider — real or stubbed
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeatureProvider:
    """Pluggable feature pulls. Default = real (pybaseball + Open-Meteo); tests pass stubs."""
    batter_season: Callable[..., dict]
    batter_recent: Callable[..., dict]
    pitcher_features: Callable[..., dict]
    park_weather: Callable[..., dict]


def default_feature_provider() -> FeatureProvider:
    return FeatureProvider(
        batter_season=batter_season_features,
        batter_recent=batter_recent_features,
        pitcher_features=pitcher_features,
        park_weather=park_weather_features,
    )


# ---------------------------------------------------------------------------
# Slate prediction
# ---------------------------------------------------------------------------

def predict_slate(
    slate: list[SlateEntry],
    ctx: AsOfContext,
    *,
    config: BaselineConfig = BaselineConfig(),
    breakout_weights: Optional[dict[str, float]] = None,
    provider: Optional[FeatureProvider] = None,
) -> list[PredictionRow]:
    """Run the full pipeline against an injectable FeatureProvider.

    Returns one PredictionRow per slate entry, in the same order as input.
    Caller is responsible for writing picks.json (filtering by EV happens
    in src/pipeline/run_daily.py once odds are available — Phase 5).
    """
    if provider is None:
        provider = default_feature_provider()

    rows: list[PredictionRow] = []
    for entry in slate:
        rows.append(_predict_entry(entry, ctx, config, breakout_weights, provider))
    return rows


def _predict_entry(
    entry: SlateEntry,
    ctx: AsOfContext,
    config: BaselineConfig,
    breakout_weights: Optional[dict[str, float]],
    provider: FeatureProvider,
) -> PredictionRow:
    season = provider.batter_season(entry.batter_id, ctx, batter_hand=entry.batter_hand)
    recent = provider.batter_recent(entry.batter_id, ctx, batter_hand=entry.batter_hand)
    prior_year = provider.batter_season(
        entry.batter_id, ctx,
        batter_hand=entry.batter_hand,
        season_year=ctx.cutoff_date.year - 1,
    )

    season_pa = int((season or {}).get("pa", 0) or 0)
    prior_year_pa = int((prior_year or {}).get("pa", 0) or 0)

    # ---- Skip rule (project rule: never median-fill) ----------------------
    skip = should_skip_batter(season_pa=season_pa, prior_year_pa=prior_year_pa)
    if skip.skip:
        return PredictionRow(
            entry=entry, skipped=True,
            skip_reason=skip.reason, skip_code=skip.code,
            season=season, recent=recent, prior_year=prior_year,
        )

    # ---- Pitcher features on the matched platoon split --------------------
    pitcher_data = provider.pitcher_features(entry.pitcher_id, ctx)
    split_key = f"vs_{entry.batter_hand}"
    try:
        pit_season_split = pitcher_data["season"][split_key]
        pit_recent_split = pitcher_data["recent"][split_key]
    except (KeyError, TypeError):
        # Pitcher data unavailable → bail with a skip rather than guess.
        return PredictionRow(
            entry=entry, skipped=True,
            skip_reason=f"pitcher features missing split '{split_key}'",
            skip_code="NO_PITCHER_SPLIT",
            season=season, recent=recent, prior_year=prior_year,
            pitcher_split_season=pitcher_data.get("season") if pitcher_data else None,
        )

    # PA on the platoon split. Blend metric key is HR/9; PA-weighted is fine
    # because PA is proportional to IP (≈ 4.3 × IP).
    pitcher_blend = blend_features(
        pit_season_split, pit_recent_split,
        metric_key="hr_per_9", pa_key="pa",
    )

    # ---- Park + weather ---------------------------------------------------
    pw = provider.park_weather(entry.park, entry.batter_hand, entry.game_datetime, ctx)

    # ---- Blend batter HR/PA (dynamic prior-year weight handled inside) ----
    batter_blend = blend_features(
        season, recent,
        prior_year=prior_year if prior_year_pa > 0 else None,
        metric_key="hr_per_pa", pa_key="pa",
    )

    # ---- Breakout vs prior year ------------------------------------------
    bk_kwargs = {"weights": breakout_weights} if breakout_weights else {}
    breakout = compute_breakout_score(
        current=season if season_pa > 0 else None,
        prior_year=prior_year if prior_year_pa > 0 else None,
        **bk_kwargs,
    )

    # ---- Run the baseline -------------------------------------------------
    pred = predict(
        blended_hr_per_pa=batter_blend.rate,
        reliable_breakout=breakout.score,
        pitcher_hr_per_9=pitcher_blend.rate,
        pitcher_hand_split_pa=int(pit_season_split.get("pa", 0) or 0),
        park_hr_factor=pw.get("park_hr_factor", 1.0) if pw else 1.0,
        temperature_f=pw.get("temperature_f", config.temp_baseline_f) if pw else config.temp_baseline_f,
        wind_out_to_cf_mph=pw.get("wind_out_to_cf_mph", 0.0) if pw else 0.0,
        is_indoor=pw.get("is_indoor", True) if pw else True,
        lineup_spot=entry.lineup_spot,
        config=config,
    )

    # Vet hasn't appeared yet this season but prior-year carries us → flag it.
    low_confidence = (season_pa == 0 and prior_year_pa > 0)

    return PredictionRow(
        entry=entry,
        skipped=pred.skipped,
        skip_reason=pred.skip_reason,
        skip_code="MODEL_SKIP" if pred.skipped else None,
        prediction=pred,
        breakout=breakout,
        batter_blend=batter_blend,
        pitcher_blend=pitcher_blend,
        low_confidence=low_confidence,
        season=season, recent=recent, prior_year=prior_year,
        pitcher_split_season=pit_season_split,
        pitcher_split_recent=pit_recent_split,
        park_weather=pw,
    )


# ---------------------------------------------------------------------------
# Top-N feature ranking for picks.json
# ---------------------------------------------------------------------------

def top_n_features(prediction: BaselinePrediction, n: int = 3) -> list[dict]:
    """Rank components by abs(value - 1) and return top N as picks.json shape.

    For multiplicative factors (park, pitcher, temperature, wind, batter_skill)
    the value is the raw scalar. For breakout_signal the value is encoded as
    1 + (bump / blended) — see README "picks.json schema" → "Field notes".
    """
    items = [
        {"name": name, "value": float(v), "deviation": abs(float(v) - 1.0)}
        for name, v in (prediction.components or {}).items()
        if v is not None
    ]
    items.sort(key=lambda x: x["deviation"], reverse=True)
    return items[:n]
