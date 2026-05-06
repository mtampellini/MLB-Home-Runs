"""Paths, constants, and feature definitions for the research script.

This module is the single source of truth for layout decisions. If you
relocate the cache directory or tweak the chunk size, do it here.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))

RESEARCH_DIR     = _DATA_DIR / "research" / "feature_importance"
CHUNKS_DIR       = RESEARCH_DIR / "chunks"
DATASETS_DIR     = RESEARCH_DIR / "datasets"
MODELS_DIR       = RESEARCH_DIR / "models"

REPORT_PATH      = PROJECT_ROOT / "src" / "research" / "feature_importance" / "research_report.md"


def ensure_dirs() -> None:
    for d in (CHUNKS_DIR, DATASETS_DIR, MODELS_DIR):
        d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Data scope
# ---------------------------------------------------------------------------

YEARS_ALL    = (2022, 2023, 2024, 2025)
YEARS_TRAIN  = (2022, 2023, 2024)
YEARS_TEST   = (2025,)

# Regular-season window per year (conservative — Statcast quietly returns no
# rows outside the season anyway, but narrowing the request speeds chunked pulls).
SEASON_BOUNDS: dict[int, tuple[date, date]] = {
    y: (date(y, 3, 15), date(y, 11, 1)) for y in YEARS_ALL
}

# Statcast chunk size in days. Smaller = more requests but smaller blast radius
# on a failure. 5 days is a reasonable balance — ~10–30s per chunk.
STATCAST_CHUNK_DAYS = 5


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

# Per spec: starter ≈ batter with ≥ 2 PAs in the game (true starters almost
# always reach 2 PA; pinch hitters almost never do).
MIN_PA_FOR_STARTER  = 2

# Per spec: drop batter-games where the batter has < 100 career PAs as of game start.
MIN_CAREER_PA       = 100


# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------

# Metrics computed in BOTH season-to-date and last-30-day windows.
# Names match the columns we'll produce in build_dataset.py.
ROLLING_METRICS = (
    "barrel_pct",
    "sweet_spot_pct",
    "pull_air_pct",
    "max_ev",
    "xwobacon",
    "hardhit_pct",
    "avg_ev",
    "iso",
    "fb_pct",
    "bat_speed",       # 2024+ only; NaN otherwise
)

# Per-row scalar/categorical features (one value each).
SCALAR_FEATURES = (
    "blended_hr_per_pa",      # the production model's main signal
    "batter_handedness",      # 'L' / 'R' / 'S'
    "pitcher_handedness",     # 'L' / 'R'
    "platoon_advantage",      # bool: batter on platoon-favored side
    "park_hr_factor",         # handedness-specific, computed from train years only
    "pitcher_hr_per_9",       # season-to-date
    "pitcher_barrel_pct_allowed",  # season-to-date
)


def all_features_variant_c() -> list[str]:
    """Variant C: every feature available."""
    feats = list(SCALAR_FEATURES)
    for m in ROLLING_METRICS:
        feats.append(f"{m}_season")
        feats.append(f"{m}_30d")
    return feats


def features_variant_a() -> list[str]:
    """Variant A: production model's main signal only."""
    return ["blended_hr_per_pa"]


def features_variant_b() -> list[str]:
    """Variant B: blended_hr_per_pa + the 4 'current breakout' metrics
    (barrel, sweet_spot, pull_air, max_ev), 30-day window.

    NOTE: the *spec* names these as 'the 4 current breakout metrics', but the
    actual breakout.py implementation uses {xwobacon, barrel, hardhit, avg_ev}.
    The mismatch is flagged in the report — this variant follows the user's
    explicit list, not breakout.py.
    """
    return [
        "blended_hr_per_pa",
        "barrel_pct_30d",
        "sweet_spot_pct_30d",
        "pull_air_pct_30d",
        "max_ev_30d",
    ]


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

RANDOM_SEED = 20260506   # date this was built — easy to spot in logs

# SHAP is the slowest report step. We subsample the holdout for SHAP only.
SHAP_SAMPLE_SIZE = 30_000
