"""Isotonic-regression probability calibration.

Wraps sklearn.isotonic.IsotonicRegression with save/load + a tiny convenience
API. Calibration is a *post-hoc* step applied after a model produces raw
probabilities — fit the calibrator on a held-out set of (raw_prob, outcome),
then transform new raw probabilities.

When to use:
- After training the LightGBM model in train.py.
- After running walk-forward eval if calibration is drifting (decile error > 2pp).

When NOT to use:
- Before we have ≥ 60 days of logged-odds outcomes. The calibrator needs at
  least a few hundred (raw_prob, win/loss) pairs to be reliable. Smaller
  samples make calibration WORSE, not better.

Per project README, validation gate #1 is "predicted vs actual HR rate within
2pp across deciles" — this is the tool that fixes drift if/when that gate fails.
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np


@dataclass(frozen=True)
class CalibrationFit:
    """Container for a fitted isotonic calibrator + sample-size metadata."""
    calibrator: object   # sklearn.isotonic.IsotonicRegression instance
    n_samples: int
    n_positives: int

    def transform(self, raw_probs: np.ndarray) -> np.ndarray:
        return self.calibrator.transform(np.asarray(raw_probs, dtype=float))

    @property
    def positive_rate(self) -> float:
        return self.n_positives / self.n_samples if self.n_samples else 0.0


def fit_isotonic(raw_probs: np.ndarray, outcomes: np.ndarray) -> CalibrationFit:
    """Fit an isotonic regression mapping raw_prob → calibrated_prob.

    Both inputs must be the same length. `outcomes` should be 0/1 (HR or not).
    Sklearn's IsotonicRegression handles edge cases (all-zero, all-one labels)
    gracefully; the resulting calibrator is monotonically non-decreasing.
    """
    from sklearn.isotonic import IsotonicRegression  # type: ignore

    raw = np.asarray(raw_probs, dtype=float)
    out = np.asarray(outcomes, dtype=int)
    if raw.shape != out.shape:
        raise ValueError(f"shape mismatch: raw={raw.shape} outcomes={out.shape}")
    if raw.ndim != 1:
        raise ValueError(f"expected 1-D arrays, got {raw.ndim}-D")
    if not np.all((out == 0) | (out == 1)):
        raise ValueError("outcomes must be 0 or 1")

    cal = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    cal.fit(raw, out)
    return CalibrationFit(
        calibrator=cal,
        n_samples=int(len(out)),
        n_positives=int(out.sum()),
    )


def save(fit: CalibrationFit, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(fit, f)


def load(path: Path) -> CalibrationFit:
    with open(path, "rb") as f:
        return pickle.load(f)


# ---------------------------------------------------------------------------
# Calibration error metrics
# ---------------------------------------------------------------------------

def expected_calibration_error(
    probs: np.ndarray, outcomes: np.ndarray, n_bins: int = 10,
) -> float:
    """ECE: weighted average of |bucket_predicted − bucket_actual| across bins.

    Project gate is 2pp across deciles. ECE ≤ 0.02 means we pass on average.
    """
    p = np.asarray(probs, dtype=float)
    y = np.asarray(outcomes, dtype=int)
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    n = len(p)
    if n == 0:
        return 0.0
    ece = 0.0
    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = (p >= lo) & (p < hi if i < n_bins - 1 else p <= hi)
        if mask.sum() == 0:
            continue
        bucket_pred = float(p[mask].mean())
        bucket_actual = float(y[mask].mean())
        ece += (mask.sum() / n) * abs(bucket_pred - bucket_actual)
    return ece


def max_decile_gap(probs: np.ndarray, outcomes: np.ndarray) -> float:
    """Largest |predicted − actual| across deciles. The gate is ≤ 0.02."""
    p = np.asarray(probs, dtype=float)
    y = np.asarray(outcomes, dtype=int)
    if len(p) == 0:
        return 0.0
    bin_edges = np.linspace(0.0, 1.0, 11)
    worst = 0.0
    for i in range(10):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = (p >= lo) & (p < hi if i < 9 else p <= hi)
        if mask.sum() == 0:
            continue
        worst = max(worst, abs(float(p[mask].mean()) - float(y[mask].mean())))
    return worst
