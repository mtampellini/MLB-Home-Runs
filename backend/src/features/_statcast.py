"""Shared Statcast fetch + metric helpers used by batter_*.py and pitcher.py.

Two concerns live here:
1. Cached pulls via pybaseball (per player, per date range) → parquet under data/raw/.
2. Pure metric calculators that take a pitch-level dataframe and return scalars.

Keeping these separate from the feature modules means batter_season,
batter_recent, and pitcher all share the same definitions of barrel%,
xwOBAcon, etc. — no chance of drift.
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.backtest.as_of_context import AsOfContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = _DATA_DIR / "raw"
BATTER_CACHE_DIR = RAW_DIR / "statcast_batter"
PITCHER_CACHE_DIR = RAW_DIR / "statcast_pitcher"


def _ensure_dirs() -> None:
    BATTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PITCHER_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# PA / BBE classification
# ---------------------------------------------------------------------------

# Events that end a plate appearance. Statcast `events` is non-null on the
# final pitch of each PA. Anything else is mid-PA pitch noise.
PA_END_EVENTS = frozenset({
    "single", "double", "triple", "home_run",
    "walk", "intent_walk", "hit_by_pitch",
    "strikeout", "strikeout_double_play",
    "field_out", "force_out", "grounded_into_double_play", "double_play",
    "triple_play", "fielders_choice", "fielders_choice_out",
    "field_error", "sac_fly", "sac_fly_double_play",
    "sac_bunt", "sac_bunt_double_play",
    "catcher_interf",
})

# Events that count as at-bats (PA minus walks/HBP/sacs/interf).
AB_EVENTS = frozenset({
    "single", "double", "triple", "home_run",
    "strikeout", "strikeout_double_play",
    "field_out", "force_out", "grounded_into_double_play", "double_play",
    "triple_play", "fielders_choice", "fielders_choice_out",
    "field_error",
})


def _pa_rows(df: pd.DataFrame) -> pd.DataFrame:
    """One row per PA: the pitch that ended it (events column non-null)."""
    if df is None or df.empty or "events" not in df.columns:
        return df.iloc[0:0] if df is not None else pd.DataFrame()
    return df.loc[df["events"].notna() & df["events"].isin(PA_END_EVENTS)].copy()


def _bbe_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Batted ball events: pitches put in play."""
    if df is None or df.empty or "description" not in df.columns:
        return df.iloc[0:0] if df is not None else pd.DataFrame()
    return df.loc[df["description"] == "hit_into_play"].copy()


# ---------------------------------------------------------------------------
# Batter metrics
# ---------------------------------------------------------------------------

def compute_batter_metrics(pitches: pd.DataFrame, batter_hand: str | None = None) -> dict:
    """Aggregate pitch-level Statcast → batter feature dict.

    Returns NaN for any metric where the denominator is zero. Callers must
    decide skip-vs-impute (per project rule: skip, never median-fill).
    """
    pa = _pa_rows(pitches)
    bbe = _bbe_rows(pitches)
    pa_count = len(pa)
    bbe_count = len(bbe)

    # HR / PA
    hr = int((pa["events"] == "home_run").sum()) if pa_count else 0

    # ISO via slash-line components
    ab = int(pa["events"].isin(AB_EVENTS).sum()) if pa_count else 0
    singles = int((pa["events"] == "single").sum()) if pa_count else 0
    doubles = int((pa["events"] == "double").sum()) if pa_count else 0
    triples = int((pa["events"] == "triple").sum()) if pa_count else 0

    if ab > 0:
        avg = (singles + doubles + triples + hr) / ab
        slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab
        iso = slg - avg
    else:
        iso = float("nan")

    # Barrel% — Statcast classifies via launch_speed_angle; 6 = barrel.
    if bbe_count and "launch_speed_angle" in bbe.columns:
        barrels = int((bbe["launch_speed_angle"] == 6).sum())
        barrel_pct = barrels / bbe_count
    elif bbe_count:
        barrel_pct = float(_manual_barrel_mask(bbe).mean())
    else:
        barrel_pct = float("nan")

    # xwOBAcon — mean estimated_woba_using_speedangle on BBE
    if bbe_count and "estimated_woba_using_speedangle" in bbe.columns:
        xwobacon = float(bbe["estimated_woba_using_speedangle"].dropna().mean())
    else:
        xwobacon = float("nan")

    # Hard-hit% — % BBE with EV >= 95. Also capture avg_ev for breakout detection.
    if bbe_count and "launch_speed" in bbe.columns:
        ev = pd.to_numeric(bbe["launch_speed"], errors="coerce")
        denom = ev.notna().sum()
        hardhit_pct = float((ev >= 95).sum() / denom) if denom else float("nan")
        avg_ev = float(ev.mean()) if denom else float("nan")
    else:
        hardhit_pct = float("nan")
        avg_ev = float("nan")

    # Sweet-spot LA% — % BBE with launch_angle in [8, 32]
    if bbe_count and "launch_angle" in bbe.columns:
        la = pd.to_numeric(bbe["launch_angle"], errors="coerce")
        denom = la.notna().sum()
        sweet_pct = float(((la >= 8) & (la <= 32)).sum() / denom) if denom else float("nan")
    else:
        sweet_pct = float("nan")

    # Pull% — handedness-aware spray angle
    pull_pct = _compute_pull_pct(bbe, batter_hand)

    return {
        "pa": pa_count,
        "ab": ab,
        "hr": hr,
        "bbe": bbe_count,
        "hr_per_pa": hr / pa_count if pa_count else float("nan"),
        "iso": iso,
        "barrel_pct": barrel_pct,
        "xwobacon": xwobacon,
        "hardhit_pct": hardhit_pct,
        "sweetspot_pct": sweet_pct,
        "pull_pct": pull_pct,
        "avg_ev": avg_ev,
    }


def _manual_barrel_mask(bbe: pd.DataFrame) -> pd.Series:
    """Fallback barrel classification when launch_speed_angle is absent.

    Approximates the official Savant rule:
      EV >= 98, with LA window expanding ±~1° per mph above 98, capped at [8, 50].
    """
    ev = pd.to_numeric(bbe.get("launch_speed"), errors="coerce")
    la = pd.to_numeric(bbe.get("launch_angle"), errors="coerce")
    eligible = ev >= 98
    spread = (ev - 98).clip(lower=0)
    la_min = (30 - spread).clip(lower=8)
    la_max = (30 + spread).clip(upper=50)
    return eligible & la.between(la_min, la_max)


def _compute_pull_pct(bbe: pd.DataFrame, batter_hand: str | None) -> float:
    """Spray-angle based pull rate. Threshold: |angle from CF| >= 15° on pull side.

    Statcast hc_x/hc_y are pixel-ish coords with home plate near (125.42, 198.27).
    Angle is measured from CF; negative = LF side, positive = RF side.
    For RHB, pull = LF (angle < -15). For LHB, pull = RF (angle > 15).
    """
    if bbe is None or bbe.empty or batter_hand not in ("R", "L"):
        return float("nan")
    if "hc_x" not in bbe.columns or "hc_y" not in bbe.columns:
        return float("nan")
    hc_x = pd.to_numeric(bbe["hc_x"], errors="coerce")
    hc_y = pd.to_numeric(bbe["hc_y"], errors="coerce")
    valid = hc_x.notna() & hc_y.notna()
    if valid.sum() == 0:
        return float("nan")
    dx = hc_x[valid] - 125.42
    dy = 198.27 - hc_y[valid]
    angle_deg = np.degrees(np.arctan2(dx, dy))
    if batter_hand == "R":
        pulled = angle_deg < -15
    else:
        pulled = angle_deg > 15
    return float(pulled.sum() / valid.sum())


# ---------------------------------------------------------------------------
# Pitcher metrics
# ---------------------------------------------------------------------------

def compute_pitcher_metrics(pitches: pd.DataFrame, vs_hand: str | None = None) -> dict:
    """Pitcher metrics, optionally filtered to vs LHB or vs RHB.

    Returns: ip_estimate, hr, hr_per_9, barrel_pct_allowed, xwobacon_allowed,
             hardhit_pct_allowed, pa, bbe.
    """
    df = pitches
    if vs_hand in ("R", "L") and df is not None and not df.empty and "stand" in df.columns:
        df = df.loc[df["stand"] == vs_hand]

    pa = _pa_rows(df)
    bbe = _bbe_rows(df)
    pa_count = len(pa)
    bbe_count = len(bbe)

    hr = int((pa["events"] == "home_run").sum()) if pa_count else 0

    # IP estimate: outs recorded / 3. Approximate via PA-ending events.
    OUT_EVENTS = {
        "field_out", "force_out", "strikeout", "strikeout_double_play",
        "grounded_into_double_play", "double_play", "triple_play",
        "sac_fly", "sac_fly_double_play", "sac_bunt", "sac_bunt_double_play",
        "fielders_choice_out",
    }
    if pa_count:
        outs = int(pa["events"].isin(OUT_EVENTS).sum())
        # Rough adjustment: DPs/triple plays produce extra outs.
        outs += int((pa["events"].isin({"strikeout_double_play", "grounded_into_double_play",
                                        "double_play", "sac_fly_double_play", "sac_bunt_double_play"})).sum())
        outs += 2 * int((pa["events"] == "triple_play").sum())
        ip = outs / 3.0
    else:
        ip = 0.0

    hr_per_9 = (hr / ip * 9) if ip > 0 else float("nan")

    if bbe_count and "launch_speed_angle" in bbe.columns:
        barrels = int((bbe["launch_speed_angle"] == 6).sum())
        barrel_pct = barrels / bbe_count
    elif bbe_count:
        barrel_pct = float(_manual_barrel_mask(bbe).mean())
    else:
        barrel_pct = float("nan")

    if bbe_count and "estimated_woba_using_speedangle" in bbe.columns:
        xwobacon = float(bbe["estimated_woba_using_speedangle"].dropna().mean())
    else:
        xwobacon = float("nan")

    if bbe_count and "launch_speed" in bbe.columns:
        ev = pd.to_numeric(bbe["launch_speed"], errors="coerce")
        denom = ev.notna().sum()
        hardhit_pct = float((ev >= 95).sum() / denom) if denom else float("nan")
    else:
        hardhit_pct = float("nan")

    return {
        "pa": pa_count,
        "bbe": bbe_count,
        "hr": hr,
        "ip_estimate": ip,
        "hr_per_9": hr_per_9,
        "barrel_pct_allowed": barrel_pct,
        "xwobacon_allowed": xwobacon,
        "hardhit_pct_allowed": hardhit_pct,
    }


# ---------------------------------------------------------------------------
# Cached fetches
# ---------------------------------------------------------------------------

def _cache_path(kind: str, player_id: int, start: date, end: date) -> Path:
    base = BATTER_CACHE_DIR if kind == "batter" else PITCHER_CACHE_DIR
    return base / f"{player_id}_{start.isoformat()}_{end.isoformat()}.parquet"


def fetch_batter_pitches(
    player_id: int,
    start: date,
    end: date,
    ctx: AsOfContext,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Pull pitch-level Statcast for one batter over [start, end] inclusive.

    `end` is automatically clipped to ctx.last_allowed_date — leakage is
    impossible from this entry point.
    """
    return _fetch_player_pitches("batter", player_id, start, end, ctx, use_cache)


def fetch_pitcher_pitches(
    player_id: int,
    start: date,
    end: date,
    ctx: AsOfContext,
    use_cache: bool = True,
) -> pd.DataFrame:
    return _fetch_player_pitches("pitcher", player_id, start, end, ctx, use_cache)


def _fetch_player_pitches(
    kind: str,
    player_id: int,
    start: date,
    end: date,
    ctx: AsOfContext,
    use_cache: bool,
) -> pd.DataFrame:
    clipped = ctx.clip_range(start, end)
    if clipped is None:
        logger.info("AsOfContext clipped entire range [%s, %s] for %s id=%s — returning empty",
                    start, end, kind, player_id)
        return pd.DataFrame()
    s, e = clipped
    _ensure_dirs()
    path = _cache_path(kind, player_id, s, e)
    if use_cache and path.exists():
        df = pd.read_parquet(path)
    else:
        df = _pybaseball_fetch(kind, player_id, s, e)
        if not df.empty:
            df.to_parquet(path, index=False)

    if not df.empty:
        ctx.assert_no_leakage(df, date_col="game_date")
    return df


def _pybaseball_fetch(kind: str, player_id: int, start: date, end: date) -> pd.DataFrame:
    """Thin wrapper around pybaseball. Imported lazily so tests don't need it installed."""
    from pybaseball import statcast_batter, statcast_pitcher  # type: ignore

    fn = statcast_batter if kind == "batter" else statcast_pitcher
    df = fn(start.isoformat(), end.isoformat(), player_id)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    return df
