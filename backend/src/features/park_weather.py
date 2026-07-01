"""Park HR factors + game-time weather + wind-relative-to-park.

Three concerns:

1. **Park HR factors (handedness-specific)** — built once from 2022-2024
   Statcast data via `compute_park_factors_from_statcast`. Loads via
   `get_park_factor(park, batter_hand)`. Falls back to neutral 1.0 with a
   warning if the parquet hasn't been built yet (so the pipeline is still
   runnable on a fresh clone).

2. **Game-time weather** — Open-Meteo (free, no key). Forecast endpoint for
   future games, archive endpoint for past games. Cached per (park, date).

3. **Wind relative to park** — given park CF compass bearing and observed
   wind direction, compute "out-to-CF" component (positive = blowing toward
   CF / out, negative = blowing in toward home). HR-friendly when positive.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from src.backtest.as_of_context import AsOfContext

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = Path(os.environ.get("HR_V7_DATA_DIR", PROJECT_ROOT / "data"))
PROCESSED_DIR = _DATA_DIR / "processed"
WEATHER_CACHE_DIR = _DATA_DIR / "raw" / "weather"
PARK_METADATA_PATH = _DATA_DIR / "park_metadata.json"
PARK_FACTORS_PATH = PROCESSED_DIR / "park_factors.parquet"

# Statcast's `home_team` uses a handful of codes that differ from our internal
# park/team codes (data/park_metadata.json + _teams.TEAM_CODE_BY_MLBAM_ID). Park
# factors are LOOKED UP by the internal code the pipeline emits, so they must be
# STORED under the internal code — otherwise the lookup misses and silently
# returns neutral 1.0. (This bug shipped 2026-05: ARI/CHW/OAK were stored as
# AZ/CWS/ATH and got no park adjustment at all.) Any code Statcast emits that is
# not in this map is assumed to already match our internal code.
STATCAST_TO_INTERNAL = {"AZ": "ARI", "CWS": "CHW", "ATH": "OAK"}

OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
# For strict backtests we'd swap to the historical-forecast endpoint to avoid
# using post-game observations. Today we use forecast for future games and
# archive for past — fine for live picks; flag this in any future backtest work.


# ---------------------------------------------------------------------------
# Park metadata
# ---------------------------------------------------------------------------

_PARK_META_CACHE: Optional[dict] = None


def load_park_metadata() -> dict:
    """Load and cache the static park metadata JSON."""
    global _PARK_META_CACHE
    if _PARK_META_CACHE is None:
        with open(PARK_METADATA_PATH, "r", encoding="utf-8") as f:
            _PARK_META_CACHE = json.load(f)["parks"]
    return _PARK_META_CACHE


def get_park_info(park_code: str) -> dict:
    meta = load_park_metadata()
    if park_code not in meta:
        raise KeyError(f"unknown park code '{park_code}' — add to data/park_metadata.json")
    return meta[park_code]


# ---------------------------------------------------------------------------
# Park HR factors (handedness-specific)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ParkFactor:
    park: str
    bat_side: str  # 'R' or 'L'
    factor: float  # 1.0 = neutral; >1 inflates HR rate
    sample_hr: int


def get_park_factor(park_code: str, batter_hand: str) -> float:
    """Return HR factor for (park, batter_hand). Neutral 1.0 if not built yet."""
    if not PARK_FACTORS_PATH.exists():
        logger.warning(
            "park factors not built (%s missing) — returning neutral 1.0. "
            "Run compute_park_factors_from_statcast() to populate.",
            PARK_FACTORS_PATH,
        )
        return 1.0
    df = pd.read_parquet(PARK_FACTORS_PATH)
    row = df[(df["park"] == park_code) & (df["bat_side"] == batter_hand)]
    if row.empty:
        logger.warning("no park factor for park=%s bat_side=%s — returning 1.0", park_code, batter_hand)
        return 1.0
    return float(row.iloc[0]["factor"])


def validate_park_factor_coverage() -> list[str]:
    """Return internal (park_code, bat_side) pairs the pipeline can emit that are
    MISSING from the park_factors artifact.

    Guards against the 2026-05 code-space bug: factors keyed on the wrong code
    space (AZ vs ARI) look present in the file but resolve to neutral 1.0 at
    lookup time. This checks every code the schedule parser can produce
    (TEAM_CODE_BY_MLBAM_ID values) against the stored codes, for both hands.
    Empty list = full coverage. Returns all codes as missing if the file is absent.
    """
    from src.pipeline._teams import TEAM_CODE_BY_MLBAM_ID

    expected = sorted(set(TEAM_CODE_BY_MLBAM_ID.values()))
    if not PARK_FACTORS_PATH.exists():
        return [f"{code}/{hand}" for code in expected for hand in ("L", "R")]
    df = pd.read_parquet(PARK_FACTORS_PATH)
    have = {(str(r["park"]), str(r["bat_side"])) for _, r in df.iterrows()}
    return [
        f"{code}/{hand}"
        for code in expected
        for hand in ("L", "R")
        if (code, hand) not in have
    ]


def regress_park_factors(counts: pd.DataFrame) -> pd.DataFrame:
    """Empirical-Bayes shrinkage of raw HR park factors toward 1.0.

    Input: one row per (park, bat_side) with summed `hr` and `pa`. Output adds
    `factor` (regressed), `factor_raw` (unregressed), and `weight` (shrinkage
    applied, 1.0 = none). Rows are re-summed by (park, bat_side) first, so it is
    safe to pass a multi-year/multi-source stack.

    Method (per handedness, so L and R use their own league rate + spread):
    method-of-moments — split the observed cross-park variance of the raw factor
    into true signal vs binomial sampling noise, then shrink each cell by its
    reliability `w = V_true / (V_true + sampling_var)`. Equivalent to
    `w = PA / (PA + K)` with a fitted K. Large-sample parks barely move; noisy
    small-sample tails pull hardest toward neutral. Raw factors are normalized to
    the PA-weighted pooled league rate within each hand (league park = 1.0).
    """
    c = counts.groupby(["park", "bat_side"], as_index=False)[["hr", "pa"]].sum()
    parts: list[pd.DataFrame] = []
    for _, g in c.groupby("bat_side"):
        g = g.copy()
        league_rate = g["hr"].sum() / g["pa"].sum()          # PA-weighted pooled
        raw = (g["hr"] / g["pa"]) / league_rate
        sampling_var = (1.0 - league_rate) / (league_rate * g["pa"])  # in factor units
        w = g["pa"].to_numpy(dtype=float)
        mean_f = np.average(raw, weights=w)
        v_obs = np.average((raw - mean_f) ** 2, weights=w)
        v_noise = np.average(sampling_var, weights=w)
        v_true = max(1e-9, v_obs - v_noise)
        weight = v_true / (v_true + sampling_var)
        g["factor_raw"] = raw
        g["weight"] = weight
        g["factor"] = 1.0 + weight * (raw - 1.0)
        parts.append(g)
    out = pd.concat(parts, ignore_index=True)
    return out[["park", "bat_side", "factor", "factor_raw", "hr", "pa", "weight"]]


def compute_park_factors_from_statcast(
    start_year: int = 2022,
    end_year: int = 2024,
    output_path: Path = PARK_FACTORS_PATH,
    pitches_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build handedness-specific HR park factors from Statcast.

    Methodology: HR / PA at each park (split by batter handedness). PA
    denominator (not BBE) is intentional — the factor absorbs both ball-flight
    effects and batter-pool composition, fine because the downstream model
    already conditions on batter + pitcher. The raw ratios are then passed
    through `regress_park_factors` (empirical-Bayes shrinkage toward 1.0) so
    small-sample tails don't ship as overconfident extremes.

    Statcast `home_team` codes are translated to our internal codes
    (STATCAST_TO_INTERNAL) BEFORE aggregating, so the stored codes match what the
    pipeline looks up with. See validate_park_factor_coverage().

    `pitches_df`: if provided, use that DataFrame instead of pulling Statcast
    fresh. Must have columns {events, home_team, stand}. Lets callers reuse a
    cached chunked pull (e.g. the research script's chunk cache).

    NOTE — park relocations: a naive multi-year window blends two stadiums under
    one code (e.g. OAK = Oakland Coliseum 2022-2024 vs Sutter Health Park /
    Sacramento 2025+). This function cannot detect that; relocated parks must be
    rebuilt from single-season counts and overridden. See scripts/build_park_factors.py.

    Writes to data/processed/park_factors.parquet and returns the DataFrame
    (columns: park, bat_side, factor, factor_raw, hr, pa, weight).
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if pitches_df is None:
        from pybaseball import statcast  # type: ignore
        rows: list[pd.DataFrame] = []
        for year in range(start_year, end_year + 1):
            logger.info("pulling Statcast %s for park factors (this is slow)...", year)
            df = statcast(start_dt=f"{year}-03-15", end_dt=f"{year}-11-01")
            if df is None or df.empty:
                continue
            df = df.loc[df["events"].notna()].copy()
            df["is_hr"] = (df["events"] == "home_run").astype(int)
            if "home_team" not in df.columns:
                continue
            df = df.loc[df["stand"].isin(["L", "R"])]
            rows.append(df[["home_team", "stand", "is_hr"]])
        if not rows:
            raise RuntimeError("no Statcast data fetched for park factor calculation")
        all_pa = pd.concat(rows, ignore_index=True)
    else:
        df = pitches_df
        if "events" not in df.columns or "home_team" not in df.columns:
            raise ValueError(
                f"pitches_df missing required columns (need events, home_team, stand); "
                f"got {list(df.columns)}"
            )
        all_pa = df.loc[df["events"].notna()].copy()
        all_pa["is_hr"] = (all_pa["events"] == "home_run").astype(int)
        all_pa = all_pa.loc[all_pa["stand"].isin(["L", "R"])]
        all_pa = all_pa[["home_team", "stand", "is_hr"]]
        logger.info("using pre-loaded pitches DataFrame: %d PA rows", len(all_pa))

    all_pa["home_team"] = all_pa["home_team"].replace(STATCAST_TO_INTERNAL)
    counts = all_pa.groupby(["home_team", "stand"]).agg(
        hr=("is_hr", "sum"), pa=("is_hr", "size")
    ).reset_index().rename(columns={"home_team": "park", "stand": "bat_side"})
    out = regress_park_factors(counts)
    out.to_parquet(output_path, index=False)
    logger.info("wrote regressed park factors to %s (%d rows)", output_path, len(out))
    return out


# ---------------------------------------------------------------------------
# Weather (Open-Meteo)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GameWeather:
    park: str
    game_datetime: datetime
    temperature_f: float
    wind_speed_mph: float
    wind_direction_deg: float  # meteorological: direction wind is FROM, 0=N, 90=E
    precipitation_in: float
    is_indoor: bool
    # When the source already gives a FIELD-RELATIVE out-to-CF component (MLB
    # Stats API: "13 mph, Out To CF"), we store the projected mph here directly
    # and skip the bearing math. None => derive from wind_direction_deg + cf_bearing
    # (the Open-Meteo path). This also sidesteps Open-Meteo's 10m-height wind,
    # which overstates field-level wind.
    wind_out_to_cf_mph: Optional[float] = None

    def out_to_cf_component(self, cf_bearing_deg: float) -> float:
        """Wind component pushing toward CF (positive = blowing out / HR-friendly).

        If a field-relative component was supplied by the source, return it
        directly. Otherwise project the meteorological wind onto the CF axis.

        Convention (Open-Meteo path): wind_direction_deg is the direction the
        wind is *coming from*. A wind from home plate toward CF is *coming from*
        the opposite of CF (wind_direction = (cf_bearing + 180) mod 360). Returns
        wind_speed_mph projected onto the CF axis, positive when blowing out.
        """
        if self.is_indoor:
            return 0.0
        if self.wind_out_to_cf_mph is not None:
            return self.wind_out_to_cf_mph
        # Direction wind is blowing TO:
        wind_to_deg = (self.wind_direction_deg + 180) % 360
        # Angle between "blowing-to" direction and CF bearing:
        diff = math.radians(wind_to_deg - cf_bearing_deg)
        return self.wind_speed_mph * math.cos(diff)


# MLB Stats API reports wind FIELD-RELATIVE ("13 mph, Out To CF"). Map the
# direction phrase to its projection onto the home->CF axis (out = HR-friendly).
# CF = full; the RF/LF gaps sit ~45deg off center (cos45 ~= 0.7); pure crosswinds
# and calm/unknown contribute nothing to carry over the fence.
_WIND_PHRASE_TO_CF_MULT = {
    "out to cf": 1.0, "in from cf": -1.0,
    "out to rf": 0.7, "out to lf": 0.7,
    "in from rf": -0.7, "in from lf": -0.7,
    "l to r": 0.0, "r to l": 0.0, "none": 0.0, "varies": 0.0,
}
# Conditions that mean the roof is closed / it's a dome (no wind, controlled temp).
_INDOOR_CONDITIONS = {"dome", "roof closed"}


def parse_mlb_weather(
    mlb_weather: Optional[dict],
    park_code: str,
    game_datetime: datetime,
) -> Optional[GameWeather]:
    """Build a GameWeather from an MLB Stats API `weather` block, or None.

    The block looks like {'condition': 'Sunny', 'temp': '92',
    'wind': '9 mph, Out To LF'}. This is the AUTHORITATIVE roof-state + weather
    signal: `condition` tells us Dome / Roof Closed vs open explicitly (so a
    retractable roof that's OPEN gets real weather, and a hot open-roof game is
    no longer forced to 72F/no-wind), and wind is already field-relative.

    Returns None when the block is absent or unparseable so callers fall back to
    Open-Meteo. NOTE: at Preview status this is a pre-game forecast (as-of safe);
    at Final it is the observed game weather (do NOT use in a strict backtest).
    """
    if not mlb_weather:
        return None
    cond = str(mlb_weather.get("condition", "")).strip().lower()
    is_indoor = cond in _INDOOR_CONDITIONS or "roof closed" in cond
    try:
        temp_f = float(str(mlb_weather.get("temp", "")).strip()) if mlb_weather.get("temp") else None
    except (TypeError, ValueError):
        temp_f = None
    if temp_f is None and not is_indoor:
        # No usable temperature and not a known dome -> let Open-Meteo handle it.
        return None
    if is_indoor:
        return GameWeather(
            park=park_code, game_datetime=game_datetime,
            temperature_f=72.0, wind_speed_mph=0.0, wind_direction_deg=0.0,
            precipitation_in=0.0, is_indoor=True, wind_out_to_cf_mph=0.0,
        )

    wind_raw = str(mlb_weather.get("wind", "")).strip()
    speed = 0.0
    phrase = "none"
    if wind_raw:
        parts = wind_raw.split(",", 1)
        try:
            speed = float(parts[0].strip().lower().replace("mph", "").strip())
        except (TypeError, ValueError):
            speed = 0.0
        phrase = parts[1].strip().lower() if len(parts) > 1 else "none"
    out_to_cf = speed * _WIND_PHRASE_TO_CF_MULT.get(phrase, 0.0)
    return GameWeather(
        park=park_code, game_datetime=game_datetime,
        temperature_f=temp_f, wind_speed_mph=speed, wind_direction_deg=0.0,
        precipitation_in=0.0, is_indoor=False, wind_out_to_cf_mph=out_to_cf,
    )


def get_game_weather(
    park_code: str,
    game_datetime: datetime,
    use_cache: bool = True,
    mlb_weather: Optional[dict] = None,
) -> GameWeather:
    """Game-time weather. Prefers the MLB Stats API `weather` block (authoritative
    roof state + field-relative wind); falls back to Open-Meteo.

    Cached per (park, date) under data/raw/weather/. When MLB weather is absent
    AND the park has a retractable roof, we still can't know the roof state, so
    we conservatively assume closed (the legacy fallback) — but with MLB weather
    that guess is no longer the primary path.
    """
    info = get_park_info(park_code)

    mlb = parse_mlb_weather(mlb_weather, park_code, game_datetime)
    if mlb is not None:
        return mlb

    if info.get("retractable_roof"):
        # Fallback only (no MLB signal): assume closed. Wind=0, temp=72, dir=0.
        return GameWeather(
            park=park_code, game_datetime=game_datetime,
            temperature_f=72.0, wind_speed_mph=0.0,
            wind_direction_deg=0.0, precipitation_in=0.0,
            is_indoor=True,
        )

    WEATHER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = WEATHER_CACHE_DIR / f"{park_code}_{game_datetime.date().isoformat()}.json"
    if use_cache and cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    else:
        try:
            payload = _fetch_open_meteo(info["lat"], info["lon"], game_datetime.date())
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
        except Exception as e:    # noqa: BLE001
            # Open-Meteo unreachable after retries — fall back to neutral
            # weather rather than crashing the entire daily cron. A single
            # park's missing weather shouldn't kill 100+ batter projections.
            logger.error(
                "Open-Meteo failed for park=%s; falling back to neutral weather "
                "(70F, no wind). Error: %s: %s",
                park_code, type(e).__name__, e,
            )
            return GameWeather(
                park=park_code, game_datetime=game_datetime,
                temperature_f=70.0, wind_speed_mph=0.0,
                wind_direction_deg=0.0, precipitation_in=0.0,
                is_indoor=False,
            )

    # _select_hour can raise (e.g. malformed payload, missing utc_offset_seconds).
    # Don't let a single park's parsing error nuke the entire daily cron — fall
    # back to neutral weather with a loud log, same as the network-failure path.
    try:
        return _select_hour(payload, park_code, game_datetime)
    except Exception as e:    # noqa: BLE001
        logger.error(
            "Open-Meteo payload parse failed for park=%s; falling back to neutral "
            "weather (70F, no wind). Error: %s: %s",
            park_code, type(e).__name__, e,
        )
        return GameWeather(
            park=park_code, game_datetime=game_datetime,
            temperature_f=70.0, wind_speed_mph=0.0,
            wind_direction_deg=0.0, precipitation_in=0.0,
            is_indoor=False,
        )


OPEN_METEO_TIMEOUT_S = 30          # was 15 — CI runners occasionally see 15s+ TLS handshakes
OPEN_METEO_MAX_RETRIES = 3         # bump to 3 with backoff so transient timeouts don't kill the cron
OPEN_METEO_BACKOFF_S = 2           # 2, 4, 8 seconds between retries


def _fetch_open_meteo(lat: float, lon: float, day: date) -> dict:
    """Pull hourly forecast for the given day. Choose forecast vs archive by date.

    Retries on timeouts / transient network errors with exponential backoff —
    a single read-timeout used to take the entire daily cron down (CI runners
    occasionally see slow TLS handshakes to api.open-meteo.com).
    """
    import time as _time
    today = date.today()
    if day >= today:
        url = OPEN_METEO_FORECAST
    else:
        url = OPEN_METEO_ARCHIVE
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "timezone": "auto",
    }
    last_err: Exception | None = None
    for attempt in range(1, OPEN_METEO_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=OPEN_METEO_TIMEOUT_S)
            resp.raise_for_status()
            return resp.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last_err = e
            if attempt < OPEN_METEO_MAX_RETRIES:
                sleep_s = OPEN_METEO_BACKOFF_S * (2 ** (attempt - 1))
                logger.warning(
                    "Open-Meteo attempt %d/%d failed (%s: %s); retrying in %ds",
                    attempt, OPEN_METEO_MAX_RETRIES, type(e).__name__, e, sleep_s,
                )
                _time.sleep(sleep_s)
            else:
                logger.error(
                    "Open-Meteo gave up after %d attempts; raising %s",
                    OPEN_METEO_MAX_RETRIES, type(e).__name__,
                )
    raise last_err  # type: ignore[misc]


def _select_hour(payload: dict, park_code: str, game_datetime: datetime) -> GameWeather:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        raise ValueError(f"no hourly weather for park={park_code} day={game_datetime.date()}")

    # MLB Stats API returns commence_time as UTC-aware (e.g. "...Z"). Open-Meteo
    # with timezone=auto returns naive ISO strings in the park's *local* time.
    # Normalize: convert target to the park's local clock via utc_offset_seconds,
    # then strip tz so the comparison is naive↔naive.
    target = game_datetime.replace(minute=0, second=0, microsecond=0)
    # Don't `or 0` the offset — silently defaulting to UTC for non-UTC parks
    # picks the wrong hourly index by 4-8h (same shape as the pitcher_hand
    # silent-default bug). Require the field; the caller wraps and falls back
    # to neutral weather if this raises.
    raw_offset = payload.get("utc_offset_seconds")
    if raw_offset is None:
        raise ValueError(
            f"Open-Meteo response missing utc_offset_seconds for park={park_code}; "
            "cannot align game time to local hourly index"
        )
    offset_s = int(raw_offset)
    if target.tzinfo is not None:
        from datetime import timedelta as _td, timezone as _tz
        target = target.astimezone(_tz(_td(seconds=offset_s))).replace(tzinfo=None)

    idx = _nearest_index(times, target)
    return GameWeather(
        park=park_code,
        game_datetime=game_datetime,
        temperature_f=float(hourly["temperature_2m"][idx]),
        wind_speed_mph=float(hourly["wind_speed_10m"][idx]),
        wind_direction_deg=float(hourly["wind_direction_10m"][idx]),
        precipitation_in=float(hourly.get("precipitation", [0.0] * len(times))[idx]),
        is_indoor=False,
    )


def _nearest_index(iso_times: list[str], target: datetime) -> int:
    """Both sides expected to be naive datetimes after _select_hour normalizes them."""
    parsed = [datetime.fromisoformat(t) for t in iso_times]
    return int(np.argmin([abs((p - target).total_seconds()) for p in parsed]))


# ---------------------------------------------------------------------------
# AsOfContext-aware park factor (no leakage; factors are pre-built once)
# ---------------------------------------------------------------------------

def park_weather_features(
    park_code: str,
    batter_hand: str,
    game_datetime: datetime,
    ctx: AsOfContext,
    mlb_weather: Optional[dict] = None,
) -> dict:
    """Combined park + weather feature dict for the daily pipeline.

    The park factor is precomputed (immutable historical artifact). The
    weather is fetched live; for games on/after ctx.cutoff_date we accept
    forecast data — that's not a leakage concern because forecasts are made
    BEFORE the game, which is exactly what we want at pick time.

    `mlb_weather`: the MLB Stats API `weather` block for this game (roof state +
    field-relative wind + official temp). Preferred over Open-Meteo when present.
    """
    info = get_park_info(park_code)
    factor = get_park_factor(park_code, batter_hand)
    wx = get_game_weather(park_code, game_datetime, mlb_weather=mlb_weather)
    cf = float(info["cf_bearing"])
    return {
        "park": park_code,
        "park_name": info["name"],
        "bat_side": batter_hand,
        "park_hr_factor": factor,
        "is_indoor": wx.is_indoor,
        "temperature_f": wx.temperature_f,
        "wind_speed_mph": wx.wind_speed_mph,
        "wind_direction_deg": wx.wind_direction_deg,
        "wind_out_to_cf_mph": wx.out_to_cf_component(cf),
        "precipitation_in": wx.precipitation_in,
        "cf_bearing_deg": cf,
        "game_datetime": game_datetime.isoformat(),
        "as_of": ctx.cutoff_date.isoformat(),
    }
