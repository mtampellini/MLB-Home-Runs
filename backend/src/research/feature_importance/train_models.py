"""Train logistic regression + LightGBM for variants A/B/C.

Both models use class-imbalance handling:
- LogisticRegression: class_weight='balanced'
- LightGBM:           scale_pos_weight = neg/pos

Models are pickled per (variant, model). Skipped if already on disk so the
script is resumable.

For LightGBM we save the booster object so analyze.py can pull gain/split
importance and run SHAP.
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.research.feature_importance import progress
from src.research.feature_importance.config import (
    MODELS_DIR,
    RANDOM_SEED,
    all_features_variant_c,
    ensure_dirs,
    features_variant_a,
    features_variant_b,
)


# ---------------------------------------------------------------------------
# Variant registry
# ---------------------------------------------------------------------------

VARIANTS: dict[str, list[str]] = {
    "A": features_variant_a(),
    "B": features_variant_b(),
    "C": all_features_variant_c(),
}

VARIANT_DESCRIPTIONS: dict[str, str] = {
    "A": "blended_hr_per_pa only",
    "B": "blended + 4 'breakout' metrics (barrel, sweet_spot, pull_air, max_ev) -- 30d",
    "C": "all features (every Statcast metric x {season, 30d} + park + pitcher + handedness)",
}


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ModelResult:
    variant: str
    model_name: str
    features: list[str]
    n_train: int
    n_test: int
    auc: float
    log_loss: float
    train_pos_rate: float
    test_pos_rate: float
    extras: dict = field(default_factory=dict)
    artifact_path: Optional[Path] = None


# ---------------------------------------------------------------------------
# Encoding categorical features for sklearn
# ---------------------------------------------------------------------------

def _encode(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """Map categorical columns to numeric. Logreg can't handle strings; LightGBM can but
    we use the same matrix for both to keep results comparable."""
    df = df[features].copy()
    if "batter_handedness" in df.columns:
        df["batter_handedness"] = df["batter_handedness"].map(
            {"L": 0, "R": 1, "S": 2}
        ).astype("float32")
    if "pitcher_handedness" in df.columns:
        df["pitcher_handedness"] = df["pitcher_handedness"].map(
            {"L": 0, "R": 1}
        ).astype("float32")
    return df


# ---------------------------------------------------------------------------
# Logistic regression -- imputes NaN with mean, then standardizes
# ---------------------------------------------------------------------------

def _train_logreg(
    X_train: pd.DataFrame, y_train: np.ndarray,
    X_test: pd.DataFrame, y_test: np.ndarray,
) -> tuple[Pipeline, float, float]:
    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(
            class_weight="balanced",
            max_iter=2000,
            random_state=RANDOM_SEED,
            n_jobs=-1,
        )),
    ])
    pipe.fit(X_train, y_train)
    proba = pipe.predict_proba(X_test)[:, 1]
    return pipe, float(roc_auc_score(y_test, proba)), float(log_loss(y_test, proba))


# ---------------------------------------------------------------------------
# LightGBM -- handles NaN natively, no scaling needed
# ---------------------------------------------------------------------------

def _train_lgbm(
    X_train: pd.DataFrame, y_train: np.ndarray,
    X_test: pd.DataFrame, y_test: np.ndarray,
):
    import lightgbm as lgb  # type: ignore

    pos = int(y_train.sum())
    neg = int(len(y_train) - pos)
    scale_pos_weight = neg / max(pos, 1)
    progress.info(
        f"  LightGBM scale_pos_weight = {scale_pos_weight:.2f} (pos={pos:,}, neg={neg:,})"
    )

    model = lgb.LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.05,
        num_leaves=63,
        min_child_samples=200,
        feature_fraction=0.9,
        bagging_fraction=0.9,
        bagging_freq=5,
        reg_alpha=0.1,
        reg_lambda=0.1,
        scale_pos_weight=scale_pos_weight,
        random_state=RANDOM_SEED,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(0)],
    )
    proba = model.predict_proba(X_test)[:, 1]
    return model, float(roc_auc_score(y_test, proba)), float(log_loss(y_test, proba))


# ---------------------------------------------------------------------------
# Top-level: train every (variant x model)
# ---------------------------------------------------------------------------

def train_all(train_df: pd.DataFrame, test_df: pd.DataFrame) -> list[ModelResult]:
    ensure_dirs()
    results: list[ModelResult] = []
    y_train = train_df["label"].to_numpy()
    y_test  = test_df["label"].to_numpy()
    train_rate = float(y_train.mean())
    test_rate = float(y_test.mean())

    for variant, features in VARIANTS.items():
        with progress.phase(f"variant {variant} -- {VARIANT_DESCRIPTIONS[variant]}"):
            X_train = _encode(train_df, features)
            X_test  = _encode(test_df, features)
            progress.info(
                f"  feature count: {len(features)},  train rows: {len(X_train):,},  "
                f"test rows: {len(X_test):,}"
            )

            # Logistic regression
            lr_path = MODELS_DIR / f"logreg_{variant}.pkl"
            if lr_path.exists():
                progress.info(f"  logreg cached -> loading {lr_path.name}")
                with open(lr_path, "rb") as f:
                    lr_pipe = pickle.load(f)
                proba = lr_pipe.predict_proba(X_test)[:, 1]
                auc_lr, ll_lr = (
                    float(roc_auc_score(y_test, proba)),
                    float(log_loss(y_test, proba)),
                )
            else:
                progress.info("  training logreg...")
                lr_pipe, auc_lr, ll_lr = _train_logreg(X_train, y_train, X_test, y_test)
                with open(lr_path, "wb") as f:
                    pickle.dump(lr_pipe, f)
                progress.info(f"  logreg -> AUC={auc_lr:.4f}  log_loss={ll_lr:.4f}")

            results.append(ModelResult(
                variant=variant, model_name="logreg", features=features,
                n_train=len(X_train), n_test=len(X_test),
                auc=auc_lr, log_loss=ll_lr,
                train_pos_rate=train_rate, test_pos_rate=test_rate,
                artifact_path=lr_path,
            ))

            # LightGBM
            lgb_path = MODELS_DIR / f"lgbm_{variant}.pkl"
            if lgb_path.exists():
                progress.info(f"  LightGBM cached -> loading {lgb_path.name}")
                with open(lgb_path, "rb") as f:
                    lgb_model = pickle.load(f)
                proba = lgb_model.predict_proba(X_test)[:, 1]
                auc_lgb, ll_lgb = (
                    float(roc_auc_score(y_test, proba)),
                    float(log_loss(y_test, proba)),
                )
            else:
                progress.info("  training LightGBM...")
                lgb_model, auc_lgb, ll_lgb = _train_lgbm(X_train, y_train, X_test, y_test)
                with open(lgb_path, "wb") as f:
                    pickle.dump(lgb_model, f)
                progress.info(f"  LightGBM -> AUC={auc_lgb:.4f}  log_loss={ll_lgb:.4f}")

            results.append(ModelResult(
                variant=variant, model_name="lightgbm", features=features,
                n_train=len(X_train), n_test=len(X_test),
                auc=auc_lgb, log_loss=ll_lgb,
                train_pos_rate=train_rate, test_pos_rate=test_rate,
                artifact_path=lgb_path,
            ))

    return results
