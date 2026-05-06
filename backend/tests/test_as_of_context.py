"""Leakage tests for AsOfContext.

The contract is:
- date < cutoff_date → allowed
- date >= cutoff_date → forbidden

We test both the filter (which silently drops bad rows) and the assert
(which raises). Both must agree on what "leakage" means.
"""

from datetime import date, datetime

import pandas as pd
import pytest

from src.backtest.as_of_context import AsOfContext, LeakageError, assert_all_before


@pytest.fixture
def ctx() -> AsOfContext:
    return AsOfContext(cutoff_date=date(2025, 4, 15))


def _df(dates: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"game_date": pd.to_datetime(dates), "value": range(len(dates))})


def test_construct_from_iso_string():
    c = AsOfContext.from_str("2025-04-15")
    assert c.cutoff_date == date(2025, 4, 15)


def test_construct_coerces_datetime_to_date():
    c = AsOfContext(cutoff_date=datetime(2025, 4, 15, 19, 5))
    assert c.cutoff_date == date(2025, 4, 15)
    assert not isinstance(c.cutoff_date, datetime)


def test_last_allowed_date_is_cutoff_minus_one(ctx):
    assert ctx.last_allowed_date == date(2025, 4, 14)


def test_window_start_for_30_day_window(ctx):
    # 30 days ending 2025-04-14 inclusive → starts 2025-03-16
    assert ctx.window_start(30) == date(2025, 3, 16)


def test_season_start_defaults_to_march_1_of_cutoff_year(ctx):
    assert ctx.season_start() == date(2025, 3, 1)


def test_filter_drops_cutoff_date_itself(ctx):
    df = _df(["2025-04-13", "2025-04-14", "2025-04-15", "2025-04-16"])
    out = ctx.filter_df(df)
    assert list(pd.to_datetime(out["game_date"]).dt.date) == [date(2025, 4, 13), date(2025, 4, 14)]


def test_filter_handles_empty(ctx):
    df = pd.DataFrame({"game_date": pd.to_datetime([])})
    out = ctx.filter_df(df)
    assert out.empty


def test_filter_missing_date_column_raises(ctx):
    df = pd.DataFrame({"foo": [1, 2]})
    with pytest.raises(KeyError):
        ctx.filter_df(df)


def test_assert_no_leakage_passes_on_clean_data(ctx):
    df = _df(["2025-04-13", "2025-04-14"])
    ctx.assert_no_leakage(df)  # should not raise


def test_assert_no_leakage_raises_on_cutoff_date(ctx):
    df = _df(["2025-04-13", "2025-04-15"])
    with pytest.raises(LeakageError):
        ctx.assert_no_leakage(df)


def test_assert_no_leakage_raises_on_post_cutoff(ctx):
    df = _df(["2025-04-13", "2025-04-20"])
    with pytest.raises(LeakageError):
        ctx.assert_no_leakage(df)


def test_clip_range_drops_post_cutoff(ctx):
    out = ctx.clip_range(date(2025, 4, 1), date(2025, 4, 20))
    assert out == (date(2025, 4, 1), date(2025, 4, 14))


def test_clip_range_returns_none_when_entirely_post_cutoff(ctx):
    out = ctx.clip_range(date(2025, 4, 15), date(2025, 4, 20))
    assert out is None


def test_clip_range_pass_through_when_all_before(ctx):
    out = ctx.clip_range(date(2025, 3, 1), date(2025, 4, 10))
    assert out == (date(2025, 3, 1), date(2025, 4, 10))


def test_assert_all_before_rejects_cutoff_date(ctx):
    with pytest.raises(LeakageError):
        assert_all_before([date(2025, 4, 14), date(2025, 4, 15)], ctx.cutoff_date)


def test_assert_all_before_passes_clean(ctx):
    assert_all_before([date(2025, 4, 13), "2025-04-14"], ctx.cutoff_date)


def test_frozen_dataclass_is_hashable(ctx):
    s = {ctx, AsOfContext(cutoff_date=date(2025, 4, 15))}
    assert len(s) == 1
