"""Turn pitch-level Statcast into a labeled batter-game modeling table.

Pipeline (all leakage-safe):

    pitches  ->  per-pitch flags (pa-end, hr, bbe, barrel, hard-hit, sweet spot,
                                  pull, pull-air, fb, hit-into-play)
             ->  batter-day daily totals (group by batter + game_date)
             ->  cumulative season + last-30-day rolling, SHIFTED by 1 day
                so today's totals are NOT in today's features
             ->  starters filter (pa >= 2 today)
             ->  100-PA career-PA floor as of game start
             ->  joined with pitcher-day rolling features (HR/9, barrel%-allowed)
             ->  joined with handedness-specific park HR factors built from train years
             ->  saved to parquet, split by train/test years

Outputs (resumable: each is skipped if already on disk):
    data/research/feature_importance/datasets/dataset_train.parquet
    data/research/feature_importance/datasets/dataset_test.parquet
    data/research/feature_importance/datasets/park_factors.parquet  (intermediate)
"""

from __future__ import annotations

import math
from datetime import date

import numpy as np
import pandas as pd

from src.features._statcast import AB_EVENTS, PA_END_EVENTS  # reuse production definitions
from src.features.blend import PRIOR_PA_EQUIVALENT, bayesian_blend
from src.research.feature_importance import progress
from src.research.feature_importance.config import (
    DATASETS_DIR,
    MIN_CAREER_PA,
    MIN_PA_FOR_STARTER,
    YEARS_TEST,
    YEARS_TRAIN,
    ensure_dirs,
)
from src.research.feature_importance.pull_data import load_cached


# ---------------------------------------------------------------------------
# Per-pitch flags
# ---------------------------------------------------------------------------

def _annotate_pitches(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean / numeric flag columns used by the daily aggregation."""
    df = df.copy()

    # PA-ending pitch
    events = df["events"]
    df["is_pa_end"] = events.notna() & events.isin(PA_END_EVENTS)
    df["is_ab"] = events.isin(AB_EVENTS).astype(int)
    df["is_single"]  = (events == "single").astype(int)
    df["is_double"]  = (events == "double").astype(int)
    df["is_triple"]  = (events == "triple").astype(int)
    df["is_hr"]      = (events == "home_run").astype(int)

    # Batted-ball event
    df["is_bbe"] = (df["description"] == "hit_into_play").astype(int)

    # NA-safe boolean→int. Pandas raises on .astype(int) when the series
    # contains NA. fillna(False) on the boolean mask before the cast.
    def _b2i(mask) -> pd.Series:
        return mask.fillna(False).astype(int)

    # Statcast classifies barrels via launch_speed_angle == 6.
    df["is_barrel"] = _b2i((df["launch_speed_angle"] == 6) & df["is_bbe"].astype(bool))

    ev = pd.to_numeric(df["launch_speed"], errors="coerce")
    la = pd.to_numeric(df["launch_angle"], errors="coerce")
    df["ev"] = ev.where(df["is_bbe"].astype(bool))
    df["is_hard_hit"]   = _b2i((ev >= 95) & df["is_bbe"].astype(bool))
    df["is_sweet_spot"] = _b2i(la.between(8, 32) & df["is_bbe"].astype(bool))

    # Pull / pull-in-the-air using hc_x / hc_y spray angle.
    hc_x = pd.to_numeric(df["hc_x"], errors="coerce")
    hc_y = pd.to_numeric(df["hc_y"], errors="coerce")
    angle_deg = np.degrees(np.arctan2(hc_x - 125.42, 198.27 - hc_y))
    pulled_R = (df["stand"] == "R") & (angle_deg < -15)
    pulled_L = (df["stand"] == "L") & (angle_deg > 15)
    df["is_pull"] = _b2i((pulled_R | pulled_L) & df["is_bbe"].astype(bool))

    # Fly ball / line drive (hit in the air).
    air_types = {"fly_ball", "line_drive"}
    df["is_fb"]  = _b2i((df["bb_type"] == "fly_ball") & df["is_bbe"].astype(bool))
    df["is_air"] = _b2i(df["bb_type"].isin(air_types) & df["is_bbe"].astype(bool))
    df["is_pull_air"] = _b2i(df["is_pull"].astype(bool) & df["is_air"].astype(bool))

    # xwOBAcon -- only on BBE, otherwise NaN.
    xw = pd.to_numeric(df["estimated_woba_using_speedangle"], errors="coerce")
    df["xwobacon_val"] = xw.where(df["is_bbe"].astype(bool))

    # Bat speed -- only available 2024+, NaN before.
    df["bat_speed_val"] = pd.to_numeric(df.get("bat_speed"), errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Daily aggregation
# ---------------------------------------------------------------------------

def _batter_day_totals(pitches: pd.DataFrame) -> pd.DataFrame:
    """Group pitches -> one row per (batter, game_date) with daily totals."""
    progress.info("aggregating pitches -> batter-day daily totals...")
    g = pitches.groupby(["batter", "game_date"], sort=False)
    daily = g.agg(
        pa=("is_pa_end", "sum"),
        ab=("is_ab", "sum"),
        hr=("is_hr", "sum"),
        singles=("is_single", "sum"),
        doubles=("is_double", "sum"),
        triples=("is_triple", "sum"),
        bbe=("is_bbe", "sum"),
        barrels=("is_barrel", "sum"),
        hard_hit=("is_hard_hit", "sum"),
        sweet_spot=("is_sweet_spot", "sum"),
        pull=("is_pull", "sum"),
        pull_air=("is_pull_air", "sum"),
        fb=("is_fb", "sum"),
        ev_sum=("ev", "sum"),
        ev_n=("ev", "count"),
        ev_max=("ev", "max"),
        xwobacon_sum=("xwobacon_val", "sum"),
        xwobacon_n=("xwobacon_val", "count"),
        bat_speed_sum=("bat_speed_val", "sum"),
        bat_speed_n=("bat_speed_val", "count"),
        stand=("stand", "first"),
        game_pk=("game_pk", "first"),
        home_team=("home_team", "first"),
    ).reset_index()
    daily["game_date"] = pd.to_datetime(daily["game_date"])
    daily["game_year"] = daily["game_date"].dt.year
    daily = daily.sort_values(["batter", "game_date"]).reset_index(drop=True)
    progress.info(f"  -> {len(daily):,} batter-day rows")
    return daily


# ---------------------------------------------------------------------------
# As-of cumulative + 30-day rolling features
# ---------------------------------------------------------------------------

# Columns aggregated as cumulative sums (or rolling sums for 30-day windows).
SUM_COLS = (
    "pa", "ab", "hr", "singles", "doubles", "triples",
    "bbe", "barrels", "hard_hit", "sweet_spot", "pull", "pull_air", "fb",
    "ev_sum", "ev_n",
    "xwobacon_sum", "xwobacon_n",
    "bat_speed_sum", "bat_speed_n",
)


def _cumulative_season(daily: pd.DataFrame) -> pd.DataFrame:
    """Per (batter, year), running sums shifted by 1 day so today's PAs aren't in today's features."""
    progress.info("computing season-to-date cumulative as-of features...")
    df = daily.copy()
    grp = df.groupby(["batter", "game_year"], sort=False)
    for col in SUM_COLS:
        df[f"{col}_season"] = grp[col].cumsum().sub(df[col]).astype(float)

    # Career PA across all years (for the 100-PA floor) -- does NOT reset per year.
    grp_all = df.groupby("batter", sort=False)
    df["career_pa"] = grp_all["pa"].cumsum().sub(df["pa"]).astype(int)

    # Rolling max EV over season (cumulative max BEFORE today). Use cummax.
    df["ev_max_season"] = grp["ev_max"].cummax()
    # cummax includes today; we want strictly prior -- use shift within group.
    df["ev_max_season"] = df.groupby(["batter", "game_year"])["ev_max_season"].shift(1)
    return df


def _rolling_30d(daily: pd.DataFrame) -> pd.DataFrame:
    """Last-30-day rolling sums (and rolling max for ev_max). Excludes today via shift."""
    progress.info("computing 30-day rolling features (per batter)...")
    df = daily.copy().reset_index(drop=True)

    # We compute rolling per batter using a time-window. Set game_date as index.
    out_frames = []
    for batter, sub in df.groupby("batter", sort=False):
        sub = sub.sort_values("game_date").set_index("game_date")
        roll = sub[list(SUM_COLS)].rolling("30D").sum().shift(1)
        roll.columns = [f"{c}_30d" for c in roll.columns]
        roll["ev_max_30d"] = sub["ev_max"].rolling("30D").max().shift(1)
        roll["batter"] = batter
        roll = roll.reset_index()
        out_frames.append(roll)

    rolled = pd.concat(out_frames, ignore_index=True)
    df = df.reset_index(drop=True).merge(
        rolled, on=["batter", "game_date"], how="left",
    )
    return df


# ---------------------------------------------------------------------------
# Rate calculations from the cumulative sums
# ---------------------------------------------------------------------------

def _rates_from_sums(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """Compute rate features from {col}_{suffix} sum columns."""
    pa = df[f"pa_{suffix}"].replace(0, np.nan)
    ab = df[f"ab_{suffix}"].replace(0, np.nan)
    bbe = df[f"bbe_{suffix}"].replace(0, np.nan)

    df[f"hr_per_pa_{suffix}"] = df[f"hr_{suffix}"] / pa
    slg = (df[f"singles_{suffix}"] + 2*df[f"doubles_{suffix}"]
           + 3*df[f"triples_{suffix}"] + 4*df[f"hr_{suffix}"]) / ab
    avg = (df[f"singles_{suffix}"] + df[f"doubles_{suffix}"]
           + df[f"triples_{suffix}"] + df[f"hr_{suffix}"]) / ab
    df[f"iso_{suffix}"] = slg - avg

    df[f"barrel_pct_{suffix}"]    = df[f"barrels_{suffix}"]    / bbe
    df[f"hardhit_pct_{suffix}"]   = df[f"hard_hit_{suffix}"]   / bbe
    df[f"sweet_spot_pct_{suffix}"] = df[f"sweet_spot_{suffix}"] / bbe
    df[f"pull_air_pct_{suffix}"]  = df[f"pull_air_{suffix}"]   / bbe
    df[f"fb_pct_{suffix}"]        = df[f"fb_{suffix}"]         / bbe

    ev_n = df[f"ev_n_{suffix}"].replace(0, np.nan)
    df[f"avg_ev_{suffix}"] = df[f"ev_sum_{suffix}"] / ev_n

    xw_n = df[f"xwobacon_n_{suffix}"].replace(0, np.nan)
    df[f"xwobacon_{suffix}"] = df[f"xwobacon_sum_{suffix}"] / xw_n

    bs_n = df[f"bat_speed_n_{suffix}"].replace(0, np.nan)
    df[f"bat_speed_{suffix}"] = df[f"bat_speed_sum_{suffix}"] / bs_n

    # max_ev rate is just the rolling max we already computed.
    df[f"max_ev_{suffix}"] = df[f"ev_max_{suffix}"]
    return df


# ---------------------------------------------------------------------------
# Pitcher rolling features
# ---------------------------------------------------------------------------

def _pitcher_day_features(pitches: pd.DataFrame) -> pd.DataFrame:
    """Per (pitcher, game_date) season-to-date HR/9 and barrel%-allowed, shifted by 1."""
    progress.info("computing pitcher-day rolling features...")
    df = pitches.copy()
    OUT_EVENTS = {
        "field_out", "force_out", "strikeout", "strikeout_double_play",
        "grounded_into_double_play", "double_play", "triple_play",
        "sac_fly", "sac_fly_double_play", "sac_bunt", "sac_bunt_double_play",
        "fielders_choice_out",
    }
    df["is_out"] = df["events"].isin(OUT_EVENTS).astype(int)
    g = df.groupby(["pitcher", "game_date"], sort=False).agg(
        pa=("is_pa_end", "sum"),
        hr=("is_hr", "sum"),
        bbe=("is_bbe", "sum"),
        barrels=("is_barrel", "sum"),
        outs=("is_out", "sum"),
        p_throws=("p_throws", "first"),
    ).reset_index()
    g["game_date"] = pd.to_datetime(g["game_date"])
    g["game_year"] = g["game_date"].dt.year
    g = g.sort_values(["pitcher", "game_date"]).reset_index(drop=True)

    grp = g.groupby(["pitcher", "game_year"], sort=False)
    g["pitcher_outs_season"]    = grp["outs"].cumsum().sub(g["outs"]).astype(float)
    g["pitcher_hr_season"]      = grp["hr"].cumsum().sub(g["hr"]).astype(float)
    g["pitcher_bbe_season"]     = grp["bbe"].cumsum().sub(g["bbe"]).astype(float)
    g["pitcher_barrels_season"] = grp["barrels"].cumsum().sub(g["barrels"]).astype(float)

    ip = (g["pitcher_outs_season"] / 3.0).replace(0, np.nan)
    g["pitcher_hr_per_9"] = g["pitcher_hr_season"] / ip * 9
    bbe = g["pitcher_bbe_season"].replace(0, np.nan)
    g["pitcher_barrel_pct_allowed"] = g["pitcher_barrels_season"] / bbe

    keep = ["pitcher", "game_date", "p_throws",
            "pitcher_hr_per_9", "pitcher_barrel_pct_allowed"]
    return g[keep]


# ---------------------------------------------------------------------------
# Park HR factors -- built from TRAIN years only (avoids leakage on holdout)
# ---------------------------------------------------------------------------

def _build_park_factors_from_train(daily: pd.DataFrame) -> pd.DataFrame:
    """Handedness-specific HR/PA park factors, normalized so league avg = 1.0.

    Built from `daily` rows whose game_year is in YEARS_TRAIN. Saved to
    datasets/park_factors.parquet for inspection.
    """
    progress.info(f"building park HR factors from train years {YEARS_TRAIN}...")
    train_only = daily[daily["game_year"].isin(YEARS_TRAIN)].copy()
    train_only = train_only[train_only["stand"].isin(["L", "R"])]
    grp = train_only.groupby(["home_team", "stand"]).agg(
        hr=("hr", "sum"), pa=("pa", "sum"),
    ).reset_index()
    grp["hr_per_pa"] = grp["hr"] / grp["pa"].replace(0, np.nan)
    league_avg = grp.groupby("stand")["hr_per_pa"].transform("mean")
    grp["park_hr_factor"] = grp["hr_per_pa"] / league_avg

    out = grp.rename(columns={"home_team": "park", "stand": "bat_side"})[
        ["park", "bat_side", "park_hr_factor", "hr", "pa"]
    ]
    ensure_dirs()
    out.to_parquet(DATASETS_DIR / "park_factors.parquet", index=False)
    progress.info(f"  -> wrote park_factors.parquet ({len(out)} rows)")
    return out


# ---------------------------------------------------------------------------
# Blended HR/PA -- match production formula
# ---------------------------------------------------------------------------

def _add_blended_hr_per_pa(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the production blend formula row-by-row on the as-of season + 30d HR/PA.

    Prior-year is approximated using the prior `game_year`'s end-of-season totals
    via a self-join. Rows with no prior year (true rookies) get None for prior.
    """
    progress.info("applying production blend formula for blended_hr_per_pa...")

    # Build a (batter, year) -> end-of-season totals lookup.
    eos = (
        df.groupby(["batter", "game_year"])
          .apply(lambda g: pd.Series({
              "eos_pa": g["pa_season"].iloc[-1] + g["pa"].iloc[-1],
              "eos_hr": g["hr_season"].iloc[-1] + g["hr"].iloc[-1],
          }))
          .reset_index()
    )
    eos["eos_hr_per_pa"] = eos["eos_hr"] / eos["eos_pa"].replace(0, np.nan)
    eos["join_year"] = eos["game_year"] + 1

    out = df.merge(
        eos[["batter", "join_year", "eos_pa", "eos_hr_per_pa"]].rename(
            columns={"join_year": "game_year", "eos_pa": "prior_year_pa",
                     "eos_hr_per_pa": "prior_year_hr_per_pa"}
        ),
        on=["batter", "game_year"], how="left",
    )

    blended_vals = []
    for season_pa, season_rate, recent_pa, recent_rate, py_pa, py_rate in zip(
        out["pa_season"].fillna(0).astype(int),
        out["hr_per_pa_season"],
        out["pa_30d"].fillna(0).astype(int),
        out["hr_per_pa_30d"],
        out["prior_year_pa"].fillna(0).astype(int),
        out["prior_year_hr_per_pa"],
    ):
        # Anchor on prior-year HR/PA when available; otherwise no anchor.
        # (Production also falls back to league HR/PA, but that requires a
        # slate-level value the offline dataset doesn't carry; rows where
        # py_rate is NaN simply skip the anchor here.)
        has_prior = int(py_pa) > 0 and not _isnan(py_rate)
        r = bayesian_blend(
            season_pa=season_pa,
            season_rate=float(season_rate) if not _isnan(season_rate) else float("nan"),
            recent_pa=recent_pa,
            recent_rate=float(recent_rate) if not _isnan(recent_rate) else float("nan"),
            prior_pa=PRIOR_PA_EQUIVALENT if has_prior else 0,
            prior_rate=float(py_rate) if has_prior else float("nan"),
        )
        blended_vals.append(r.rate)
    out["blended_hr_per_pa"] = blended_vals
    return out


def _isnan(x) -> bool:
    try:
        return math.isnan(float(x))
    except (TypeError, ValueError):
        return True


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Full build. Resumable: if the train+test parquets exist, just read them."""
    ensure_dirs()
    train_path = DATASETS_DIR / "dataset_train.parquet"
    test_path  = DATASETS_DIR / "dataset_test.parquet"
    if train_path.exists() and test_path.exists():
        progress.info("dataset parquets already exist -- loading from disk")
        return pd.read_parquet(train_path), pd.read_parquet(test_path)

    with progress.phase("load cached chunks -> DataFrame"):
        all_years = tuple(sorted(set(YEARS_TRAIN) | set(YEARS_TEST)))
        pitches = load_cached(all_years)

    if pitches.empty:
        raise RuntimeError(
            "No cached pitch data. Run pull_data first: "
            "`python -m src.research.feature_importance.pull_data`"
        )

    with progress.phase("annotate per-pitch flags"):
        pitches = _annotate_pitches(pitches)

    with progress.phase("batter-day daily totals"):
        daily = _batter_day_totals(pitches)

    with progress.phase("season cumulative + career_pa"):
        daily = _cumulative_season(daily)

    with progress.phase("30-day rolling features"):
        daily = _rolling_30d(daily)

    with progress.phase("derive rate features (season + 30d)"):
        daily = _rates_from_sums(daily, "season")
        daily = _rates_from_sums(daily, "30d")

    with progress.phase("pitcher-day rolling features"):
        pit = _pitcher_day_features(pitches)

    with progress.phase("park HR factors (train years only)"):
        park_factors = _build_park_factors_from_train(daily)

    with progress.phase("blended_hr_per_pa via production formula"):
        daily = _add_blended_hr_per_pa(daily)

    with progress.phase("filter starters + 100-PA career floor + join pitcher/park"):
        # Need pitcher_id per game -- read from pitches via game_pk + batter.
        starter = (
            pitches[pitches["is_pa_end"]]
            .groupby(["batter", "game_pk"])
            .agg(pitcher=("pitcher", "first"))
            .reset_index()
        )
        # Already aggregated in daily under game_pk, just merge pitcher in.
        daily = daily.merge(starter, on=["batter", "game_pk"], how="left")

        # Starters + sample-size filter
        before_n = len(daily)
        daily = daily[daily["pa"] >= MIN_PA_FOR_STARTER]
        daily = daily[daily["career_pa"] >= MIN_CAREER_PA]
        progress.info(
            f"  filtered {before_n:,} -> {len(daily):,} rows "
            f"(starters + career_pa >= {MIN_CAREER_PA})"
        )

        # Join pitcher rolling features
        pit["game_date"] = pd.to_datetime(pit["game_date"])
        daily = daily.merge(pit, on=["pitcher", "game_date"], how="left")

        # Join park factors (handedness-specific). Select only the factor
        # column — park_factors carries 'hr' / 'pa' for inspection but those
        # would collide with daily's per-game 'hr' / 'pa' on merge.
        pf = (
            park_factors[["park", "bat_side", "park_hr_factor"]]
            .rename(columns={"park": "home_team", "bat_side": "stand"})
        )
        daily = daily.merge(pf, on=["home_team", "stand"], how="left")

        # Categorical / derived
        daily["batter_handedness"] = daily["stand"]
        daily["pitcher_handedness"] = daily["p_throws"]
        daily["platoon_advantage"] = (
            ((daily["stand"] == "L") & (daily["p_throws"] == "R")) |
            ((daily["stand"] == "R") & (daily["p_throws"] == "L"))
        ).astype(int)

        daily["label"] = (daily["hr"] > 0).astype(int)

    train_df = daily[daily["game_year"].isin(YEARS_TRAIN)].copy()
    test_df  = daily[daily["game_year"].isin(YEARS_TEST)].copy()

    progress.info(
        f"final dataset: train={len(train_df):,}  test={len(test_df):,}  "
        f"train HR rate={train_df['label'].mean():.4f}  test HR rate={test_df['label'].mean():.4f}"
    )

    train_df.to_parquet(train_path, index=False)
    test_df.to_parquet(test_path, index=False)
    progress.info(f"wrote {train_path.name} and {test_path.name}")

    return train_df, test_df


if __name__ == "__main__":
    build()
