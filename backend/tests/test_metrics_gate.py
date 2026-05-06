"""Backtest metrics + the dormant-mode data gate."""

import numpy as np
import pytest

from src.backtest._data_gate import (
    InsufficientOddsError,
    count_logged_odds_days,
    gate,
)
from src.backtest.metrics import (
    brier_score,
    clv_summary,
    log_loss_safe,
    roi_summary,
)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def test_log_loss_perfect_predictions_near_zero():
    assert log_loss_safe(np.array([0.0001, 0.9999]), np.array([0, 1])) < 0.01


def test_log_loss_handles_extreme_probs():
    """Clipping prevents log(0) blowups."""
    assert log_loss_safe(np.array([0.0, 1.0]), np.array([0, 1])) < 1e-6


def test_brier_score_zero_for_perfect_predictions():
    assert brier_score(np.array([0.0, 1.0]), np.array([0, 1])) == 0.0


def test_brier_score_quadratic():
    # (0.5 - 1)^2 = 0.25
    assert brier_score(np.array([0.5]), np.array([1])) == pytest.approx(0.25)


def test_roi_summary_basic():
    rows = [
        {"outcome": "W", "profit_units": 3.10},
        {"outcome": "L", "profit_units": -1.0},
        {"outcome": "W", "profit_units": 2.0},
        {"outcome": "VOID", "profit_units": 0.0},
    ]
    s = roi_summary(rows)
    assert s.n_picks == 4
    assert s.wins == 2 and s.losses == 1 and s.voids == 1
    assert s.units_staked == 3.0
    assert s.units_profit == pytest.approx(4.10)
    assert s.roi_pct == pytest.approx(136.67, abs=0.01)


def test_clv_summary_handles_empty():
    s = clv_summary([])
    assert s["n"] == 0 and s["mean"] is None


def test_clv_summary_quartiles():
    s = clv_summary([0, 5, 10, 15, 20])
    assert s["n"] == 5
    assert s["mean"] == 10.0
    assert s["median"] == 10.0
    assert s["p25"] == 5.0 and s["p75"] == 15.0


# ---------------------------------------------------------------------------
# Data gate
# ---------------------------------------------------------------------------

def test_count_logged_odds_days(tmp_path):
    # Create 4 snapshots across 3 distinct days.
    for name in (
        "2026-05-06-1500.json", "2026-05-06-1800.json",
        "2026-05-07-1500.json", "2026-05-08-1500.json",
    ):
        (tmp_path / name).write_text("{}")
    assert count_logged_odds_days(tmp_path) == 3


def test_count_logged_odds_days_empty_dir(tmp_path):
    assert count_logged_odds_days(tmp_path) == 0


def test_gate_raises_below_threshold(tmp_path):
    with pytest.raises(InsufficientOddsError):
        gate(min_days=60, odds_dir=tmp_path)


def test_gate_allows_unsafe_override(tmp_path):
    decision = gate(min_days=60, odds_dir=tmp_path, allow_unsafe=True)
    assert decision.sufficient is False
    assert "RESULTS ARE NOT RELIABLE" in decision.warning_text


def test_gate_passes_above_threshold(tmp_path):
    for i in range(60):
        (tmp_path / f"2026-05-{1 + i:02d}-1500.json").write_text("{}") if i < 31 else \
            (tmp_path / f"2026-06-{i - 30:02d}-1500.json").write_text("{}")
    decision = gate(min_days=60, odds_dir=tmp_path)
    assert decision.sufficient is True
    assert decision.days_logged >= 60
