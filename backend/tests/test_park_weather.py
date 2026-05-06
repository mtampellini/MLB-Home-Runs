"""Wind-relative-to-park math.

Convention: wind_direction_deg is the meteorological direction the wind is
*coming from* (0 = from N, 90 = from E). cf_bearing is the compass bearing of
CF from home plate. Positive `out_to_cf_component` means wind pushing out to
CF (HR-friendly). Negative means blowing in.
"""

from datetime import datetime

import pytest

from src.features.park_weather import GameWeather


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
