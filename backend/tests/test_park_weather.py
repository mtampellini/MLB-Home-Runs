"""Wind-relative-to-park math + Open-Meteo timezone normalization.

Convention: wind_direction_deg is the meteorological direction the wind is
*coming from* (0 = from N, 90 = from E). cf_bearing is the compass bearing of
CF from home plate. Positive `out_to_cf_component` means wind pushing out to
CF (HR-friendly). Negative means blowing in.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.features.park_weather import GameWeather, _select_hour


def _wx(speed: float, direction_from: float, indoor: bool = False) -> GameWeather:
    return GameWeather(
        park="TEST",
        game_datetime=datetime(2025, 6, 1, 19, 0),
        temperature_f=75.0,
        wind_speed_mph=speed,
        wind_direction_deg=direction_from,
        precipitation_in=0.0,
        is_indoor=indoor,
    )


def test_wind_from_home_to_cf_is_full_positive():
    # CF bearing 90° (east). Wind blowing TO 90° means coming FROM 270°.
    wx = _wx(speed=10, direction_from=270)
    assert wx.out_to_cf_component(cf_bearing_deg=90) == pytest.approx(10.0)


def test_wind_from_cf_to_home_is_full_negative():
    # Wind blowing in (from CF toward home): coming FROM 90° when CF is at 90°.
    wx = _wx(speed=10, direction_from=90)
    assert wx.out_to_cf_component(cf_bearing_deg=90) == pytest.approx(-10.0)


def test_crosswind_zero_component():
    # Wind blowing perpendicular to CF axis. CF at 90° (east),
    # wind from 0° (from N → blowing south) → no out-to-CF component.
    wx = _wx(speed=12, direction_from=0)
    assert wx.out_to_cf_component(cf_bearing_deg=90) == pytest.approx(0.0, abs=1e-9)


def test_45_degree_offset_is_cos45_times_speed():
    # CF at 0° (north). Wind blowing TO 45° means coming FROM 225°.
    wx = _wx(speed=10, direction_from=225)
    expected = 10 * (2 ** 0.5) / 2
    assert wx.out_to_cf_component(cf_bearing_deg=0) == pytest.approx(expected)


def test_indoor_zeroes_wind():
    wx = _wx(speed=20, direction_from=270, indoor=True)
    assert wx.out_to_cf_component(cf_bearing_deg=90) == 0.0


def test_zero_speed_zero_component():
    wx = _wx(speed=0, direction_from=180)
    assert wx.out_to_cf_component(cf_bearing_deg=0) == 0.0


# ---------------------------------------------------------------------------
# _select_hour: tz-aware game time vs naive Open-Meteo local times
# ---------------------------------------------------------------------------

def _meteo_payload(times: list[str], temps: list[float],
                    utc_offset_seconds: int = -14400) -> dict:
    """Mock Open-Meteo response. Default offset = -14400s = US Eastern (EDT)."""
    return {
        "utc_offset_seconds": utc_offset_seconds,
        "timezone": "America/New_York",
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "wind_speed_10m": [5.0] * len(times),
            "wind_direction_10m": [180.0] * len(times),
            "precipitation": [0.0] * len(times),
        },
    }


def test_select_hour_handles_utc_aware_game_time():
    """7:05pm Eastern game ('...23:05Z' UTC) should map to the 19:00 local hour."""
    payload = _meteo_payload(
        times=["2026-05-06T17:00", "2026-05-06T18:00",
                "2026-05-06T19:00", "2026-05-06T20:00"],
        temps=[70.0, 72.0, 75.0, 73.0],   # 19:00 → 75°F
    )
    game_dt = datetime(2026, 5, 6, 23, 5, tzinfo=timezone.utc)
    wx = _select_hour(payload, "NYY", game_dt)
    assert wx.temperature_f == 75.0


def test_select_hour_handles_naive_game_time():
    """Backward-compat: naive game_datetime (no tzinfo) should also work."""
    payload = _meteo_payload(
        times=["2026-05-06T18:00", "2026-05-06T19:00", "2026-05-06T20:00"],
        temps=[72.0, 75.0, 73.0],
    )
    wx = _select_hour(payload, "NYY", datetime(2026, 5, 6, 19, 5))
    assert wx.temperature_f == 75.0


def test_select_hour_floors_to_game_hour():
    """Game at 19:35 reads the 19:00 forecast — we floor to the hour the game
    starts in, not the nearest hour. Treats temperature/wind as 'reading at
    first pitch'."""
    payload = _meteo_payload(
        times=["2026-05-06T19:00", "2026-05-06T20:00"],
        temps=[70.0, 80.0],
    )
    wx = _select_hour(payload, "NYY", datetime(2026, 5, 6, 19, 35))
    assert wx.temperature_f == 70.0


def test_select_hour_raises_when_utc_offset_seconds_missing():
    """Regression: payload used to default to UTC+0 if utc_offset_seconds was
    missing, silently picking the wrong hour by 4-8h for non-UTC parks. Now
    it raises so the wrapper falls back to neutral weather instead."""
    payload = {
        # NO utc_offset_seconds key.
        "hourly": {
            "time": ["2026-05-06T18:00", "2026-05-06T19:00"],
            "temperature_2m": [72.0, 75.0],
            "wind_speed_10m": [5.0, 5.0],
            "wind_direction_10m": [180.0, 180.0],
            "precipitation": [0.0, 0.0],
        },
    }
    game_dt = datetime(2026, 5, 6, 23, 5, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="utc_offset_seconds"):
        _select_hour(payload, "NYY", game_dt)


def test_get_game_weather_falls_back_to_neutral_on_missing_utc_offset(
    tmp_path, monkeypatch,
):
    """Wrap-and-fall-back: if _select_hour raises (e.g. missing utc_offset),
    get_game_weather logs and returns neutral weather, doesn't crash."""
    import json as _json
    from src.features import park_weather as pw_mod

    monkeypatch.setattr(pw_mod, "WEATHER_CACHE_DIR", tmp_path / "weather")
    (tmp_path / "weather").mkdir(parents=True, exist_ok=True)
    # Pre-seed cache with a malformed payload (no utc_offset_seconds).
    cache_path = tmp_path / "weather" / "NYY_2026-05-06.json"
    cache_path.write_text(_json.dumps({
        "hourly": {
            "time": ["2026-05-06T19:00"],
            "temperature_2m": [80.0],
            "wind_speed_10m": [10.0],
            "wind_direction_10m": [90.0],
            "precipitation": [0.0],
        },
    }))
    wx = pw_mod.get_game_weather(
        "NYY", datetime(2026, 5, 6, 23, 5, tzinfo=timezone.utc),
        use_cache=True,
    )
    # Falls back to neutral, NOT the (malformed) cached 80F that would have
    # come out of the silent-default code path.
    assert wx.temperature_f == 70.0
    assert wx.wind_speed_mph == 0.0


def test_get_game_weather_falls_back_to_neutral_when_open_meteo_fails(tmp_path, monkeypatch):
    """If Open-Meteo errors out, we DON'T crash the cron — we return neutral
    weather (70F, no wind) and keep going. A single park's API failure
    shouldn't kill 100+ batter projections."""
    import requests as _requests
    from src.features import park_weather as pw_mod

    def _boom(url, params=None, timeout=None):
        raise _requests.ConnectionError("simulated DNS failure")

    monkeypatch.setattr(pw_mod, "WEATHER_CACHE_DIR", tmp_path / "weather")
    monkeypatch.setattr(pw_mod.requests, "get", _boom)
    # Disable retry sleep so the test is fast.
    monkeypatch.setattr(pw_mod, "OPEN_METEO_MAX_RETRIES", 1)
    monkeypatch.setattr(pw_mod, "OPEN_METEO_BACKOFF_S", 0)

    wx = pw_mod.get_game_weather(
        "NYY", datetime(2026, 5, 6, 19, 5, tzinfo=timezone.utc),
        use_cache=False,
    )
    assert wx.temperature_f == 70.0
    assert wx.wind_speed_mph == 0.0
    assert wx.is_indoor is False


def test_open_meteo_retries_on_transient_failure(tmp_path, monkeypatch):
    """First call fails, second succeeds → we use the second response."""
    import requests as _requests
    from unittest.mock import MagicMock
    from src.features import park_weather as pw_mod

    call_count = {"n": 0}
    good_response = MagicMock()
    good_response.json.return_value = {
        "utc_offset_seconds": -14400,
        "hourly": {
            "time": ["2026-05-06T19:00"],
            "temperature_2m": [78.0],
            "wind_speed_10m": [10.0],
            "wind_direction_10m": [180.0],
            "precipitation": [0.0],
        },
    }
    good_response.raise_for_status = lambda: None

    def _flaky(url, params=None, timeout=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _requests.Timeout("transient")
        return good_response

    monkeypatch.setattr(pw_mod, "WEATHER_CACHE_DIR", tmp_path / "weather")
    monkeypatch.setattr(pw_mod.requests, "get", _flaky)
    monkeypatch.setattr(pw_mod, "OPEN_METEO_BACKOFF_S", 0)   # no test sleep

    wx = pw_mod.get_game_weather(
        "NYY", datetime(2026, 5, 6, 23, 5, tzinfo=timezone.utc),
        use_cache=False,
    )
    assert call_count["n"] == 2                 # one retry
    assert wx.temperature_f == 78.0             # second response delivered


def test_select_hour_works_across_dst_offsets():
    """West-coast park (UTC-7 in PDT) should still map correctly."""
    payload = _meteo_payload(
        times=["2026-05-06T17:00", "2026-05-06T18:00", "2026-05-06T19:00"],
        temps=[68.0, 70.0, 72.0],
        utc_offset_seconds=-25200,        # PDT
    )
    # 7pm Pacific = 02:00 UTC the next day
    game_dt = datetime(2026, 5, 7, 2, 5, tzinfo=timezone.utc)
    wx = _select_hour(payload, "LAD", game_dt)
    assert wx.temperature_f == 72.0
