"""Strict temporal cutoff enforcement.

Every feature-query function in this project must accept an AsOfContext and
clip its data so that nothing on or after `cutoff_date` is used. This is the
single mechanism that prevents leakage in walk-forward evaluation.

The contract:
- Data with date STRICTLY LESS THAN cutoff_date is allowed.
- Data with date EQUAL TO OR GREATER THAN cutoff_date is forbidden.

Rationale: cutoff_date represents "what we know going into the game on this
date." A game played on 2025-04-15 cannot use data from 2025-04-15 (that's the
game itself or other games happening simultaneously) when generating its
prediction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd


class LeakageError(AssertionError):
    """Raised when data on/after the cutoff sneaks into a feature pipeline."""


@dataclass(frozen=True)
class AsOfContext:
    cutoff_date: date

    def __post_init__(self) -> None:
        if not isinstance(self.cutoff_date, date) or isinstance(self.cutoff_date, datetime):
            object.__setattr__(self, "cutoff_date", _to_date(self.cutoff_date))

    @classmethod
    def from_str(cls, s: str) -> "AsOfContext":
        return cls(cutoff_date=date.fromisoformat(s))

    @property
    def last_allowed_date(self) -> date:
        """The most recent date whose data may be used (cutoff - 1 day)."""
        return self.cutoff_date - timedelta(days=1)

    def window_start(self, days: int) -> date:
        """Start date for a rolling N-day window ending at last_allowed_date (inclusive)."""
        return self.last_allowed_date - timedelta(days=days - 1)

    def season_start(self, season_year: int | None = None) -> date:
        """Conservative regular-season start: March 1 of the cutoff year (or given year)."""
        year = season_year if season_year is not None else self.cutoff_date.year
        return date(year, 3, 1)

    def filter_df(self, df: pd.DataFrame, date_col: str = "game_date") -> pd.DataFrame:
        """Return rows strictly before cutoff_date. Empty input returns empty."""
        if df is None or df.empty:
            return df
        if date_col not in df.columns:
            raise KeyError(f"date column '{date_col}' not in dataframe; columns={list(df.columns)}")
        dates = pd.to_datetime(df[date_col]).dt.date
        mask = dates < self.cutoff_date
        return df.loc[mask].copy()

    def assert_no_leakage(self, df: pd.DataFrame, date_col: str = "game_date") -> None:
        """Raise LeakageError if any row in df has date >= cutoff_date."""
        if df is None or df.empty:
            return
        if date_col not in df.columns:
            raise KeyError(f"date column '{date_col}' not in dataframe; columns={list(df.columns)}")
        dates = pd.to_datetime(df[date_col]).dt.date
        bad = (dates >= self.cutoff_date).sum()
        if bad:
            raise LeakageError(
                f"{bad} row(s) with {date_col} >= cutoff_date={self.cutoff_date.isoformat()} "
                "leaked into a feature query."
            )

    def clip_range(self, start: date, end: date) -> tuple[date, date] | None:
        """Clip an inclusive [start, end] date range to the as-of window.

        Returns None if the entire range is on/after the cutoff.
        """
        end_clipped = min(end, self.last_allowed_date)
        if start > end_clipped:
            return None
        return start, end_clipped

    def __str__(self) -> str:
        return f"AsOfContext(cutoff={self.cutoff_date.isoformat()})"


def _to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"cannot coerce {type(value).__name__} to date: {value!r}")


def assert_all_before(values: Iterable, cutoff: date, label: str = "value") -> None:
    """Generic guard: every item in `values` (date or ISO string) must be < cutoff."""
    cutoff_d = _to_date(cutoff)
    for v in values:
        d = _to_date(v) if not isinstance(v, date) else v
        if d >= cutoff_d:
            raise LeakageError(f"{label}={d.isoformat()} >= cutoff={cutoff_d.isoformat()}")
