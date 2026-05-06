"""Feature-importance and ablation analyses on the trained models.

Outputs:
  - LightGBM importance: gain, split-count, mean |SHAP|. Disagreements flagged.
  - Logreg coefficient magnitudes (after scaling, so cross-feature comparable).
  - Univariate AUC: train a single-feature logreg per feature, score on holdout.
  - Pearson correlation matrix between numeric features (redundancy view).
  - High-univariate-AUC + low-LightGBM-importance flags (redundant predictors).
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.research.feature_importance import progress
from src.research.feature_importance.config import (
    MODELS_DIR,
    RANDOM_SEED,
    SHAP_SAMPLE_SIZE,
    all_features_variant_c,
)
from src.research.feature_importance.train_models import _encode


# ---------------------------------------------------------------------------
# LightGBM importance — gain / split / SHAP
# ---------------------------------------------------------------------------

@dataclass
class LgbmImportance:
    feature: str
    gain: float
    split: int
    mean_abs_shap: float
    rank_gain: int
    rank_split: int
    rank_shap: int
    disagreement: int          # max(rank) - min(rank); high = unstable usage


def lgbm_three_way_importance(
    variant: str,
    X_test: pd.DataFrame,
    feature_names: list[str],
    shap_sample_size: int = SHAP_SAMPLE_SIZE,
) -> list[LgbmImportance]:
    progress.info(f"computing 3-way importance for variant {variant}...")
    with open(MODELS_DIR / f"lgbm_{variant}.pkl", "rb") as f:
        model = pickle.load(f)
    booster = model.booster_

    gain = booster.feature_importance(importance_type="gain")
    split = booster.feature_importance(importance_type="split")

    # SHAP
    progress.info(f"  computing SHAP on {min(shap_sample_size, len(X_test)):,} sample rows...")
    import shap  # type: ignore
    sample = X_test.sample(n=min(shap_sample_size, len(X_test)), random_state=RANDOM_SEED)
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(sample)
    if isinstance(shap_values, list):  # binary classifier returns [class0, class1]
        shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]
    mean_abs_shap = np.abs(shap_values).mean(axis=0)

    df = pd.DataFrame({
        "feature": feature_names,
        "gain": gain,
        "split": split,
        "mean_abs_shap": mean_abs_shap,
    })
    df["rank_gain"]  = df["gain"].rank(ascending=False, method="min").astype(int)
    df["rank_split"] = df["split"].rank(ascending=False, method="min").astype(int)
    df["rank_shap"]  = df["mean_abs_shap"].rank(ascending=False, method="min").astype(int)
    df["disagreement"] = (
        df[["rank_gain", "rank_split", "rank_shap"]].max(axis=1)
        - df[["rank_gain", "rank_split", "rank_shap"]].min(axis=1)
    )

    return [LgbmImportance(**row) for row in df.to_dict(orient="records")]


# ---------------------------------------------------------------------------
# Logreg coefficient magnitude
# ---------------------------------------------------------------------------

def logreg_coef_magnitudes(variant: str, feature_names: list[str]) -> pd.DataFrame:
    with open(MODELS_DIR / f"logreg_{variant}.pkl", "rb") as f:
        pipe = pickle.load(f)
    coef = pipe.named_steps["clf"].coef_[0]
    return (
        pd.DataFrame({"feature": feature_names, "coef": coef,
                      "abs_coef": np.abs(coef)})
          .sort_values("abs_coef", ascending=False)
          .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Univariate AUC — single-feature logreg per feature
# ---------------------------------------------------------------------------

def univariate_auc(
    train_df: pd.DataFrame, test_df: pd.DataFrame, features: list[str],
) -> pd.DataFrame:
    progress.info(f"univariate AUC across {len(features)} features...")
    y_train = train_df["label"].to_numpy()
    y_test  = test_df["label"].to_numpy()
    out: list[dict] = []
    for f in features:
        Xtr = _encode(train_df, [f])
        Xte = _encode(test_df, [f])
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="mean")),
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(class_weight="balanced", max_iter=2000,
                                       random_state=RANDOM_SEED)),
        ])
        try:
            pipe.fit(Xtr, y_train)
            proba = pipe.predict_proba(Xte)[:, 1]
            auc = float(roc_auc_score(y_test, proba))
        except Exception as e:
            progress.warn(f"  {f}: failed ({type(e).__name__}: {e})")
            auc = float("nan")
        out.append({"feature": f, "univariate_auc": auc})
    return pd.DataFrame(out).sort_values("univariate_auc", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Correlation matrix
# ---------------------------------------------------------------------------

def correlation_matrix(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    progress.info("computing Pearson correlation matrix...")
    enc = _encode(df, features)
    return enc.corr(method="pearson", numeric_only=True)


# ---------------------------------------------------------------------------
# Bat-speed availability
# ---------------------------------------------------------------------------

def bat_speed_availability(train_df: pd.DataFrame, test_df: pd.DataFrame) -> dict:
    def _avail(df: pd.DataFrame, col: str) -> float:
        if col not in df.columns:
            return 0.0
        return float(df[col].notna().mean())
    return {
        "train_bat_speed_season_pct": _avail(train_df, "bat_speed_season"),
        "train_bat_speed_30d_pct":    _avail(train_df, "bat_speed_30d"),
        "test_bat_speed_season_pct":  _avail(test_df, "bat_speed_season"),
        "test_bat_speed_30d_pct":     _avail(test_df, "bat_speed_30d"),
    }
