"""Dormant ML trainer — runs end-to-end on real logged odds + settled results.

THIS MODEL IS NOT USED IN THE DAILY PIPELINE. The empirical-Bayes baseline
(`src/model/baseline.py`) is what we bet with for the first 60+ days. This
trainer exists so we're ready to flip the switch the moment we have enough
data, NOT to be tuned earlier.

Inputs (assembled from on-disk artifacts):
  - data/processed/picks_*.json     ← model_prob, blended_hr_per_pa, etc.
  - data/processed/results_*.json   ← actual_hr, outcome
  - data/odds/*.json                ← optional CLV diagnostic source

The script joins picks with their settled outcomes, builds a feature matrix
of whatever signals we logged at pick time, and trains LightGBM. It refuses
to run without the `allow_unsafe=True` opt-in if fewer than 60 distinct
days of odds have been logged.

We never train on synthetic odds. We never reverse-engineer odds from HR
rates. Both are project rules, enforced by the gate plus the simple rule
that this script reads only real artifacts on disk.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date as _date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.backtest._data_gate import GateDecision, gate
from src.backtest.metrics import BacktestMetrics, compute_metrics
from src.model.calibration import CalibrationFit, fit_isotonic
from src.results.tracker import PROCESSED_DIR

logger = logging.getLogger(__name__)


FEATURE_COLUMNS = (
    "model_prob",
    "blended_hr_per_pa",
    "breakout_score",
    "market_prob_devig",
    "ev_pct",
)

MODEL_OUT_PATH = PROCESSED_DIR.parent / "models" / "ml_model.pkl"
CALIBRATOR_OUT_PATH = PROCESSED_DIR.parent / "models" / "calibration.pkl"


@dataclass
class TrainResult:
    n_train: int
    n_test: int
    metrics_test: Optional[BacktestMetrics]
    calibrator: Optional[CalibrationFit]
    gate: GateDecision
    notes: list[str] = field(default_factory=list)
    model_path: Optional[Path] = None
    calibrator_path: Optional[Path] = None


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _list_dated_files(prefix: str, processed_dir: Path) -> list[Path]:
    pat = re.compile(rf"{prefix}_(\d{{4}}-\d{{2}}-\d{{2}})\.json")
    return sorted([f for f in processed_dir.glob(f"{prefix}_*.json") if pat.match(f.name)])


def assemble_dataset(processed_dir: Path = PROCESSED_DIR) -> pd.DataFrame:
    """Inner-join picks_* and results_* to get one row per settled pick.

    Drops VOID rows (no usable label). Returns columns:
        as_of_date, batter_id, game_pk, <FEATURE_COLUMNS>, label
    """
    rows: list[dict] = []
    for picks_file in _list_dated_files("picks", processed_dir):
        day = picks_file.stem.split("_", 1)[1]
        results_file = processed_dir / f"results_{day}.json"
        if not results_file.exists():
            continue
        picks = json.loads(picks_file.read_text()).get("picks", []) or []
        results = json.loads(results_file.read_text()).get("results", []) or []
        result_idx = {(int(r["batter_id"]), r.get("game_pk")): r for r in results}
        for p in picks:
            r = result_idx.get((int(p["batter_id"]), p.get("game_pk")))
            if r is None or r["outcome"] == "VOID":
                continue
            rows.append({
                "as_of_date": day,
                "batter_id": int(p["batter_id"]),
                "game_pk": p.get("game_pk"),
                **{c: float(p.get(c, 0.0) or 0.0) for c in FEATURE_COLUMNS},
                "label": 1 if r["outcome"] == "W" else 0,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Train
# ---------------------------------------------------------------------------

def train(
    *,
    processed_dir: Path = PROCESSED_DIR,
    test_fraction: float = 0.25,
    allow_unsafe: bool = False,
    save: bool = True,
) -> TrainResult:
    """Build dataset → time-ordered split → train LightGBM → calibrate → save."""
    decision = gate(allow_unsafe=allow_unsafe)
    notes: list[str] = [decision.warning_text]
    if not decision.sufficient:
        notes.append("output is FOR SCAFFOLDING ONLY — do not interpret as production-quality")

    df = assemble_dataset(processed_dir)
    if df.empty:
        notes.append("no settled picks on disk; nothing to train on.")
        return TrainResult(
            n_train=0, n_test=0, metrics_test=None,
            calibrator=None, gate=decision, notes=notes,
        )

    df = df.sort_values("as_of_date").reset_index(drop=True)
    split = int(len(df) * (1.0 - test_fraction))
    train_df = df.iloc[:split].copy()
    test_df = df.iloc[split:].copy()

    X_train = train_df[list(FEATURE_COLUMNS)].to_numpy(dtype=float)
    y_train = train_df["label"].to_numpy(dtype=int)
    X_test = test_df[list(FEATURE_COLUMNS)].to_numpy(dtype=float)
    y_test = test_df["label"].to_numpy(dtype=int)

    import lightgbm as lgb  # type: ignore

    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    spw = (neg / max(pos, 1)) if pos else 1.0
    logger.info("training LightGBM: n_train=%d (pos=%d) n_test=%d scale_pos_weight=%.2f",
                len(X_train), pos, len(X_test), spw)

    model = lgb.LGBMClassifier(
        n_estimators=400,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=50,
        feature_fraction=0.9,
        bagging_fraction=0.9,
        bagging_freq=5,
        scale_pos_weight=spw,
        random_state=20260506,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train)
    raw_test_probs = model.predict_proba(X_test)[:, 1]

    # Fit isotonic calibrator on the test set itself for the scaffolding stage.
    # Once we have more data, switch to a proper held-out calibration set.
    calibrator = fit_isotonic(raw_test_probs, y_test) if len(y_test) >= 50 else None
    notes.append(
        "calibrator fit on test set — replace with a held-out calibration set "
        "once n_test ≥ 1000."
    )

    metrics = compute_metrics(probs=raw_test_probs, outcomes=y_test, notes=notes)

    model_path: Optional[Path] = None
    cal_path: Optional[Path] = None
    if save:
        MODEL_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        import pickle
        with open(MODEL_OUT_PATH, "wb") as f:
            pickle.dump(model, f)
        model_path = MODEL_OUT_PATH
        if calibrator is not None:
            with open(CALIBRATOR_OUT_PATH, "wb") as f:
                pickle.dump(calibrator, f)
            cal_path = CALIBRATOR_OUT_PATH

    return TrainResult(
        n_train=len(X_train), n_test=len(X_test),
        metrics_test=metrics, calibrator=calibrator,
        gate=decision, notes=notes,
        model_path=model_path, calibrator_path=cal_path,
    )


def main() -> int:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="Dormant ML trainer for HR-Picks-V7.")
    p.add_argument("--allow-unsafe", action="store_true",
                   help="Run despite < 60 days of logged odds. Smoke-tests only.")
    p.add_argument("--no-save", action="store_true",
                   help="Skip writing model + calibrator pickles.")
    args = p.parse_args()
    result = train(allow_unsafe=args.allow_unsafe, save=not args.no_save)
    print(json.dumps({
        "n_train": result.n_train,
        "n_test": result.n_test,
        "gate_sufficient": result.gate.sufficient,
        "days_logged": result.gate.days_logged,
        "metrics": (result.metrics_test.__dict__ | {"roi": None, "clv": None})
                    if result.metrics_test else None,
        "model_path": str(result.model_path) if result.model_path else None,
        "calibrator_path": str(result.calibrator_path) if result.calibrator_path else None,
        "notes": result.notes,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
