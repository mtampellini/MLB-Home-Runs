"""Pre-compute handedness-specific park HR factors from 2022-2025 Statcast.

Uses the research script's chunked, resumable pull infrastructure so:
  - the Statcast pull is resumable on Savant timeouts,
  - chunks land in `data/research/feature_importance/chunks/`,
  - the feature-importance research script reuses the same cache (no refetch).

Run:
    python -m scripts.precompute_park_factors

Output: data/processed/park_factors.parquet (committed to git).

Re-running after success is a no-op (~5s) — chunks are cached, the script
re-loads them and rewrites the parquet idempotently.
"""

from __future__ import annotations

import sys

from src.features.park_weather import (
    PARK_FACTORS_PATH,
    compute_park_factors_from_statcast,
)
from src.research.feature_importance import progress
from src.research.feature_importance.config import YEARS_ALL
from src.research.feature_importance.pull_data import load_cached, pull_all


def main() -> int:
    progress.banner(f"Park factors precompute — Statcast {YEARS_ALL}")

    with progress.phase("Statcast pull (chunked + resumable)"):
        pull_all(YEARS_ALL)

    with progress.phase("load cached chunks → DataFrame"):
        pitches = load_cached(YEARS_ALL)
        progress.info(f"loaded {len(pitches):,} pitch rows")

    with progress.phase("compute handedness-specific park HR factors"):
        out = compute_park_factors_from_statcast(
            start_year=YEARS_ALL[0],
            end_year=YEARS_ALL[-1],
            pitches_df=pitches,
        )
        progress.info(f"wrote {PARK_FACTORS_PATH}: {len(out)} (park, bat_side) rows")

    progress.script_done(f"park_factors.parquet → {PARK_FACTORS_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
