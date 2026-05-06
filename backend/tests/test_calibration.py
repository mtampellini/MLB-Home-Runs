"""Calibration math: isotonic fit, ECE, max-decile gap."""

import numpy as np
import pytest

from src.model.calibration import (
    expected_calibration_error,
    fit_isotonic,
    max_decile_gap,
)


def test_isotonic_recovers_calibrated_probabilities():
    """Synthetic: raw probs deliberately too high; outcomes match true rate."""
    np.random.seed(20260506)
    n = 5000
    raw = np.random.uniform(0.1, 0.9, size=n)
    # Pretend true probs are half of raw (model is over-confident).
    true_p = raw * 0.5
    outcomes = (np.random.uniform(size=n) < true_p).astype(int)

    fit = fit_isotonic(raw, outcomes)
    calibrated = fit.transform(raw)

    # ECE should drop after calibration.
    ece_raw = expected_calibration_error(raw, outcomes)
    ece_cal = expected_calibration_error(calibrated, outcomes)
    assert ece_cal < ece_raw / 2


def test_isotonic_raises_on_bad_outcomes():
    raw = np.array([0.1, 0.5, 0.9])
    bad = np.array([0, 1, 2])
    with pytest.raises(ValueError):
        fit_isotonic(raw, bad)


def test_isotonic_raises_on_shape_mismatch():
    with pytest.raises(ValueError):
        fit_isotonic(np.array([0.1, 0.5]), np.array([0, 1, 0]))


def test_ece_zero_when_perfectly_calibrated():
    """Bucket boundaries: model says 0.3 → 30% wins, model 0.6 → 60% wins, ..."""
    probs = np.array([0.3] * 100 + [0.6] * 100)
    outcomes = np.array([1] * 30 + [0] * 70 + [1] * 60 + [0] * 40)
    assert expected_calibration_error(probs, outcomes) == pytest.approx(0.0, abs=1e-12)


def test_ece_grows_with_miscalibration():
    probs = np.array([0.3] * 100)
    outcomes = np.array([1] * 50 + [0] * 50)        # actual 50%, predicted 30%
    ece = expected_calibration_error(probs, outcomes)
    assert ece == pytest.approx(0.20, abs=1e-9)


def test_max_decile_gap_handles_empty_input():
    assert max_decile_gap(np.array([]), np.array([])) == 0.0


def test_max_decile_gap_finds_worst_bucket():
    probs = np.array([0.05] * 100 + [0.55] * 100)
    outcomes = np.array([0] * 100 + [1] * 100)      # bucket 0.5–0.6: actual 100% vs predicted 55%
    assert max_decile_gap(probs, outcomes) == pytest.approx(0.45, abs=1e-9)
