"""Walk-forward time-series evaluation. DORMANT until 60+ days of logged odds.

For each "fold" date in a stepwise schedule:
  - Train on every settled pick before that date.
  - Test on settled picks in [fold_date, fold_date + step_days).
  - Compute log loss, Brier, ECE, max-decile gap, ROI, CLV on the test fold.

Returns a DataFrame of per-fold metrics. Aggregating across folds is the
caller's job — this module just runs the slices.

Like train.py, this refuses to run below the 60-day data gate unless
allow_unsafe=True (smoke tests only).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as _date
from datetime import timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.backtest._data_gate import GateDecision, gate
from src.backtest.metrics import BacktestMetrics, compute_metrics
from src.model.train import FEATURE_COLUMNS, assemble_dataset
from src.results.tracker import PROCESSED_DIR

logger = logging.getLogger(__name__)


@dataclass
class WalkForwardResult:
    folds: pd.DataFrame             # one row per fold
    gate: GateDecision
    notes: list[str]


def _train_and_score(train_df: pd.DataFrame, test_df: pd.DataFrame) -> BacktestMetrics:
    import lightgbm as lgb  # type: ignore

    X_train = train_df[list(FEATURE_COLUMNS)].to_numpy(dtype=float)
    y_train = train_df["label"].to_numpy(dtype=int)
    X_test = test_df[list(FEATURE_COLUMNS)].to_numpy(dtype=float)
    y_test = test_df["label"].to_numpy(dtype=int)

    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    spw = (neg / max(pos, 1)) if pos else 1.0
    model = lgb.LGBMClassifier(
        n_estimators=300, learning_rate=0.05, num_leaves=31,
        min_child_samples=50, scale_pos_weight=spw,
        random_state=20260506, n_jobs=-1, verbose=-1,
    )
    model.fit(X_train, y_train)
    test_probs = model.predict_proba(X_test)[:, 1]
    return compute_metrics(probs=test_probs, outcomes=y_test)


def walk_forward(
    *,
    processed_dir: Path = PROCESSED_DIR,
    step_days: int = 7,
    min_train_size: int = 200,
    allow_unsafe: bool = False,
) -> WalkForwardResult:
    decision = gate(allow_unsafe=allow_unsafe)
    notes: list[str] = [decision.warning_text]
    if not decision.sufficient:
        notes.append(
            "fold metrics below the gate are SCAFFOLDING ONLY — do NOT use them "
            "to compare candidate models or tune hyperparameters."
        )

    df = assemble_dataset(processed_dir)
    if df.empty:
        notes.append("no settled picks on disk; no folds to run.")
        return WalkForwardResult(folds=pd.DataFrame(), gate=decision, notes=notes)

    df["as_of_date_dt"] = pd.to_datetime(df["as_of_date"]).dt.date
    df = df.sort_values("as_of_date_dt").reset_index(drop=True)

    start = df["as_of_date_dt"].iloc[0]
    end = df["as_of_date_dt"].iloc[-1]
    fold_starts: list[_date] = []
    cursor = start
    while cursor <= end:
        fold_starts.append(cursor)
        cursor += timedelta(days=step_days)

    fold_rows: list[dict] = []
    for fold_start in fold_starts:
        fold_end = fold_start + timedelta(days=step_days)
        train = df[df["as_of_date_dt"] < fold_start]
        test = df[(df["as_of_date_dt"] >= fold_start) & (df["as_of_date_dt"] < fold_end)]
        if len(train) < min_train_size or len(test) == 0:
            fold_rows.append({
                "fold_start": fold_start, "fold_end": fold_end,
                "n_train": len(train), "n_test": len(test),
                "skipped": True, "skip_reason":
                    "below min_train_size" if len(train) < min_train_size else "empty test",
            })
            continue
        m = _train_and_score(train, test)
        fold_rows.append({
            "fold_start": fold_start, "fold_end": fold_end,
            "n_train": len(train), "n_test": len(test),
            "skipped": False,
            "log_loss": m.log_loss, "brier": m.brier,
            "ece": m.ece, "max_decile_gap": m.max_decile_gap,
        })

    return WalkForwardResult(folds=pd.DataFrame(fold_rows), gate=decision, notes=notes)


def main() -> int:
    import argparse, json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Walk-forward backtest.")
    p.add_argument("--allow-unsafe", action="store_true")
    p.add_argument("--step-days", type=int, default=7)
    args = p.parse_args()
    result = walk_forward(allow_unsafe=args.allow_unsafe, step_days=args.step_days)
    print(json.dumps({
        "gate_sufficient": result.gate.sufficient,
        "days_logged": result.gate.days_logged,
        "n_folds": len(result.folds),
        "folds": result.folds.to_dict(orient="records") if len(result.folds) else [],
        "notes": result.notes,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
