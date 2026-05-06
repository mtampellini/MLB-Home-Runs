"""Backtest metrics: ROI, CLV, log loss, calibration diagnostics.

Pure functions over arrays / lists of dicts. Used by walk_forward.py and the
runtime tracker. No I/O; nothing here writes files.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np

from src.model.calibration import expected_calibration_error, max_decile_gap


@dataclass
class RoiSummary:
    n_picks: int
    wins: int
    losses: int
    voids: int
    units_staked: float
    units_profit: float
    roi_pct: float
    hit_rate: float


def roi_summary(rows: Iterable[dict]) -> RoiSummary:
    """rows: dicts with 'outcome' ∈ {'W','L','VOID'} and 'profit_units'."""
    n = w = l = v = 0
    profit = 0.0
    for r in rows:
        n += 1
        out = r.get("outcome")
        if out == "W": w += 1
        elif out == "L": l += 1
        else: v += 1
        profit += float(r.get("profit_units", 0.0))
    units_staked = float(w + l)
    return RoiSummary(
        n_picks=n, wins=w, losses=l, voids=v,
        units_staked=units_staked, units_profit=profit,
        roi_pct=(profit / units_staked * 100.0) if units_staked else 0.0,
        hit_rate=(w / units_staked) if units_staked else 0.0,
    )


def clv_summary(clvs: Iterable[float]) -> dict:
    """Return mean / median / quartiles of CLV %."""
    arr = np.asarray([c for c in clvs if c is not None], dtype=float)
    if arr.size == 0:
        return {"n": 0, "mean": None, "median": None,
                "p25": None, "p75": None}
    return {
        "n": int(arr.size),
        "mean": round(float(arr.mean()), 3),
        "median": round(float(np.median(arr)), 3),
        "p25": round(float(np.percentile(arr, 25)), 3),
        "p75": round(float(np.percentile(arr, 75)), 3),
    }


# ---------------------------------------------------------------------------
# Probabilistic metrics
# ---------------------------------------------------------------------------

def log_loss_safe(probs: np.ndarray, outcomes: np.ndarray, eps: float = 1e-12) -> float:
    """Binary log-loss with clipping. Returns 0 on empty input."""
    p = np.clip(np.asarray(probs, dtype=float), eps, 1.0 - eps)
    y = np.asarray(outcomes, dtype=int)
    if p.size == 0:
        return 0.0
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def brier_score(probs: np.ndarray, outcomes: np.ndarray) -> float:
    p = np.asarray(probs, dtype=float)
    y = np.asarray(outcomes, dtype=int)
    if p.size == 0:
        return 0.0
    return float(np.mean((p - y) ** 2))


# ---------------------------------------------------------------------------
# Combined diagnostics for walk-forward
# ---------------------------------------------------------------------------

@dataclass
class BacktestMetrics:
    n_picks: int
    log_loss: float
    brier: float
    ece: float
    max_decile_gap: float
    roi: Optional[RoiSummary] = None
    clv: Optional[dict] = None
    notes: list[str] = None


def compute_metrics(
    probs: np.ndarray,
    outcomes: np.ndarray,
    settled_rows: Optional[list[dict]] = None,
    clvs: Optional[list[float]] = None,
    notes: Optional[list[str]] = None,
) -> BacktestMetrics:
    return BacktestMetrics(
        n_picks=int(len(probs)),
        log_loss=log_loss_safe(probs, outcomes),
        brier=brier_score(probs, outcomes),
        ece=expected_calibration_error(probs, outcomes),
        max_decile_gap=max_decile_gap(probs, outcomes),
        roi=roi_summary(settled_rows) if settled_rows else None,
        clv=clv_summary(clvs) if clvs is not None else None,
        notes=notes or [],
    )
