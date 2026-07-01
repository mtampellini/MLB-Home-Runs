"""Build data/processed/park_factors.parquet from committed raw HR/PA counts.

Reproducible and fast (no multi-hour Statcast pull): reads
data/processed/park_factor_counts.parquet — one row per (park, bat_side, source)
of summed hr/pa, already keyed on INTERNAL park codes — applies park-relocation
overrides, runs empirical-Bayes shrinkage (park_weather.regress_park_factors),
and writes the regressed factors the pipeline loads.

Refresh workflow when a new season completes:
  1. Pull the new season's counts (see scripts notes / pull_2025 pattern),
     translate Statcast->internal codes, append as a new `source` row.
  2. Re-run this script. Add/adjust RELOCATION_OVERRIDES if a park moved.

RELOCATION_OVERRIDES: a park whose stadium changed must not blend old + new
ballparks under one code. Map internal code -> the set of `source` labels to
KEEP (all others dropped for that park only).
  - OAK: Oakland Coliseum (2022-2024) -> Sutter Health Park / Sacramento (2025+).
    Keep 2025 only. Small single-season sample -> heavily shrunk toward 1.0;
    firms up as 2026 lands. (Vegas move will require another override later.)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # backend/ on path

from src.features.park_weather import (  # noqa: E402
    PARK_FACTORS_PATH,
    PROCESSED_DIR,
    regress_park_factors,
    validate_park_factor_coverage,
)

COUNTS_PATH = PROCESSED_DIR / "park_factor_counts.parquet"
RELOCATION_OVERRIDES: dict[str, set[str]] = {"OAK": {"2025"}}


def build() -> pd.DataFrame:
    counts = pd.read_parquet(COUNTS_PATH)
    keep = []
    for (park,), g in counts.groupby(["park"]):
        if park in RELOCATION_OVERRIDES:
            g = g[g["source"].isin(RELOCATION_OVERRIDES[park])]
        keep.append(g)
    counts = pd.concat(keep, ignore_index=True)

    factors = regress_park_factors(counts[["park", "bat_side", "hr", "pa"]])
    factors = factors.sort_values(["park", "bat_side"]).reset_index(drop=True)
    factors.to_parquet(PARK_FACTORS_PATH, index=False)
    print(f"wrote {PARK_FACTORS_PATH}  ({len(factors)} rows)")
    return factors


if __name__ == "__main__":
    factors = build()
    missing = validate_park_factor_coverage()
    if missing:
        print(f"WARNING: {len(missing)} internal codes still uncovered: {missing}")
        sys.exit(1)
    print(f"coverage OK — all {factors['park'].nunique()} parks x2 hands resolve")
    print(factors.pivot(index="park", columns="bat_side", values="factor").round(3).to_string())
