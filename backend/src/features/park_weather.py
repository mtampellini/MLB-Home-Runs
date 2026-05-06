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


def compute_park_factors_from_statcast(
    start_year: int = 2022,
    end_year: int = 2024,
    output_path: Path = PARK_FACTORS_PATH,
    pitches_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build handedness-specific HR park factors from Statcast.

    Methodology: HR / PA at each park (split by batter handedness), normalized
    so league-average park = 1.0. PA denominator (not BBE) is intentional —
    the factor absorbs both ball-flight effects and batter-pool composition,
    fine because the downstream model already conditions on batter + pitcher.

    `pitches_df`: if provided, use that DataFrame instead of pulling Statcast
    fresh. Must have columns {events, home_team, stand}. Lets callers reuse a
    cached chunked pull (e.g. the research script's chunk cache).

    Writes to data/processed/park_factors.parquet and returns the DataFrame.
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

    grp = all_pa.groupby(["home_team", "stand"]).agg(
        hr=("is_hr", "sum"), pa=("is_hr", "size")
    ).reset_index()
    grp["hr_per_pa"] = grp["hr"] / grp["pa"]
    # Normalize within batter-hand: league average (across parks) = 1.0.
    league_avg = grp.groupby("stand")["hr_per_pa"].transform("mean")
    grp["factor"] = grp["hr_per_pa"] / league_avg
    out = grp.rename(columns={"home_team": "park", "stand": "bat_side"})[
        ["park", "bat_side", "factor", "hr", "pa"]
    ]
    out.to_parquet(output_path, index=False)
    logger.info("wrote park factors to %s (%d rows)", output_path, len(out))
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

    def out_to_cf_component(self, cf_bearing_deg: float) -> float:
        """Wind component pushing toward CF (positive = blowing out / HR-friendly).

        Convention: wind_direction_deg is the direction the wind is *coming from*.
        A wind from home plate toward CF means the wind is *coming from* the
        opposite of CF (i.e. wind_direction = (cf_bearing + 180) mod 360).

        Returns wind_speed_mph projected onto the CF axis, with positive sign
        when wind blows out toward CF.
        """
        if self.is_indoor:
            return 0.0
        # Direction wind is blowing TO:
        wind_to_deg = (self.wind_direction_deg + 180) % 360
        # Angle between "blowing-to" direction and CF bearing:
        diff = math.radians(wind_to_deg - cf_bearing_deg)
        return self.wind_speed_mph * math.cos(diff)


def get_game_weather(
    park_code: str,
    game_datetime: datetime,
    use_cache: bool = True,
) -> GameWeather:
    """Fetch hourly weather at game time. Uses forecast for future, archive for past.

    Cached per (park, date) under data/raw/weather/. Returns indoor placeholder
    for retractable-roof parks (we have no roof-state signal so we conservatively
    assume closed and zero out wind / set typical indoor temperature).
    """
    info = get_park_info(park_code)
    if info.get("retractable_roof"):
        # Without a roof-state feed we assume closed. Wind=0, temp=72, dir=0.
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

    return _select_hour(payload, park_code, game_datetime)


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
    offset_s = int(payload.get("utc_offset_seconds") or 0)
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
) -> dict:
    """Combined park + weather feature dict for the daily pipeline.

    The park factor is precomputed (immutable historical artifact). The
    weather is fetched live; for games on/after ctx.cutoff_date we accept
    forecast data — that's not a leakage concern because forecasts are made
    BEFORE the game, which is exactly what we want at pick time.
    """
    info = get_park_info(park_code)
    factor = get_park_factor(park_code, batter_hand)
    wx = get_game_weather(park_code, game_datetime)
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
