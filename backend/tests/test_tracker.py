"""Tracker aggregation: ROI, hit rate, CLV, calibration buckets."""

import json
from datetime import date
from pathlib import Path

import pytest

from src.results.tracker import build_tracker


def _pick_payload(batter_id, game_pk, *, taken=310, model_prob=0.30, best_book="draftkings"):
    return {
        "batter": f"Player {batter_id}",
        "batter_id": batter_id,
        "team": "NYY",
        "game_pk": game_pk,
        "game_datetime": "2026-05-06T23:05:00+00:00",
        "fd_odds": 290, "dk_odds": taken,
        "best_book": best_book,
        "model_prob": model_prob,
        "market_prob_devig": 0.234,
        "ev_pct": 25.0,
    }


def _settled_payload(batter_id, game_pk, outcome, *, taken=310, model_prob=0.30):
    profit = (
        (taken / 100.0) if taken > 0 else (100.0 / abs(taken))
    ) if outcome == "W" else (-1.0 if outcome == "L" else 0.0)
    return {
        "batter": f"Player {batter_id}",
        "batter_id": batter_id,
        "team": "NYY",
        "game_pk": game_pk,
        "over_american": taken,
        "model_prob": model_prob,
        "market_prob_devig": 0.234,
        "ev_pct": 25.0,
        "actual_hr": 1 if outcome == "W" else 0,
        "outcome": outcome,
        "profit_units": profit,
        "void_reason": None,
    }


def _summary_for(settled):
    n_w = sum(1 for s in settled if s["outcome"] == "W")
    n_l = sum(1 for s in settled if s["outcome"] == "L")
    n_v = sum(1 for s in settled if s["outcome"] == "VOID")
    units_staked = float(n_w + n_l)
    units_profit = sum(float(s["profit_units"]) for s in settled)
    return {
        "n_picks": len(settled),
        "n_wins": n_w, "n_losses": n_l, "n_voids": n_v,
        "units_staked": units_staked,
        "units_profit": round(units_profit, 4),
        "roi_pct": round(units_profit / units_staked * 100.0, 2) if units_staked > 0 else 0.0,
    }


def _write_archive(archives_dir: Path, day: date, *,
                   primary_picks=(), secondary_picks=(), shadow_picks=(),
                   primary_settled=None, secondary_settled=None, shadow_settled=None):
    archives_dir.mkdir(parents=True, exist_ok=True)
    settlement = None
    if any(x is not None for x in (primary_settled, secondary_settled, shadow_settled)):
        settlement = {"settled_at": f"{day.isoformat()}T15:00:00+00:00"}
        if primary_settled is not None:
            settlement["primary_results"] = primary_settled
            settlement["primary_summary"] = _summary_for(primary_settled)
        if secondary_settled is not None:
            settlement["secondary_results"] = secondary_settled
            settlement["secondary_summary"] = _summary_for(secondary_settled)
        if shadow_settled is not None:
            settlement["shadow_results"] = shadow_settled
            settlement["shadow_summary"] = _summary_for(shadow_settled)
    (archives_dir / f"{day.isoformat()}.json").write_text(json.dumps({
        "date": day.isoformat(),
        "primary_picks": list(primary_picks),
        "secondary_picks": list(secondary_picks),
        "shadow_picks": list(shadow_picks),
        "settlement": settlement,
    }))


def _write_snapshot(odds_dir, day: date, hour_min: str,
                     batter: str, fd_over: int, dk_over: int) -> None:
    odds_dir.mkdir(parents=True, exist_ok=True)
    fetched = f"{day.isoformat()}T{hour_min[:2]}:{hour_min[2:]}:00+00:00"
    (odds_dir / f"{day.isoformat()}-{hour_min}.json").write_text(json.dumps({
        "fetched_at": fetched,
        "as_of_date": day.isoformat(),
        "books_filtered": ["fanduel", "draftkings"],
        "market": "batter_home_runs_alternate",
        "quotes": [
            {"book": "fanduel",   "batter_name": batter, "point": 0.5,
             "over_american": fd_over, "under_american": -400},
            {"book": "draftkings","batter_name": batter, "point": 0.5,
             "over_american": dk_over, "under_american": -400},
        ],
    }))


@pytest.fixture
def tmp_layout(tmp_path):
    processed = tmp_path / "processed"
    archives = tmp_path / "daily_archives"
    odds = tmp_path / "odds"
    processed.mkdir(parents=True)
    archives.mkdir(parents=True)
    odds.mkdir(parents=True)
    return {"processed": processed, "archives": archives, "odds": odds}


def _build(tmp_layout, **kw):
    return build_tracker(
        processed_dir=tmp_layout["processed"],
        archives_dir=tmp_layout["archives"],
        odds_dir=tmp_layout["odds"],
        **kw,
    )


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def test_tracker_aggregates_wins_losses_voids_and_roi(tmp_layout):
    day = date(2026, 5, 6)
    picks = [
        _pick_payload(1, 100, taken=310),
        _pick_payload(2, 100, taken=200),
        _pick_payload(3, 200, taken=310),
    ]
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=picks,
                   primary_settled=[
                       _settled_payload(1, 100, "W", taken=310),
                       _settled_payload(2, 100, "L", taken=200),
                       _settled_payload(3, 200, "VOID", taken=310),
                   ])
    out = _build(tmp_layout)
    assert out.summary.total_picks == 3
    assert out.summary.wins == 1
    assert out.summary.losses == 1
    assert out.summary.voids == 1
    assert out.summary.units_staked == 2.0
    # +310 → payout 3.10; -1 loss → net 2.10
    assert out.summary.units_profit == pytest.approx(2.10)
    assert out.summary.roi_pct == pytest.approx(105.0)
    assert out.summary.hit_rate == pytest.approx(0.5)


def test_tracker_writes_tracker_json(tmp_layout):
    day = date(2026, 5, 6)
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=[_pick_payload(1, 100)],
                   primary_settled=[_settled_payload(1, 100, "W")])
    _build(tmp_layout)
    payload = json.loads((tmp_layout["processed"] / "tracker.json").read_text())
    assert "summary" in payload
    assert "calibration" in payload
    assert "by_book" in payload


def test_tracker_calibration_buckets_use_model_prob(tmp_layout):
    day = date(2026, 5, 6)
    settled = [
        _settled_payload(1, 100, "W", model_prob=0.22),
        _settled_payload(2, 100, "L", model_prob=0.22),
        _settled_payload(3, 100, "L", model_prob=0.22),
        _settled_payload(4, 100, "L", model_prob=0.22),
        _settled_payload(5, 200, "W", model_prob=0.40),
    ]
    picks = [_pick_payload(s["batter_id"], s["game_pk"]) for s in settled]
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=picks, primary_settled=settled)
    out = _build(tmp_layout)
    bucket_low = next(b for b in out.calibration if b["model_prob_min"] == 0.20)
    bucket_high = next(b for b in out.calibration if b["model_prob_min"] == 0.40)
    assert bucket_low["n_picks"] == 4 and bucket_low["actual_hit_rate"] == 0.25
    assert bucket_high["n_picks"] == 1 and bucket_high["actual_hit_rate"] == 1.0


# ---------------------------------------------------------------------------
# CLV
# ---------------------------------------------------------------------------

def test_tracker_computes_clv_from_pre_game_snapshot(tmp_layout):
    """Take +310, closing +250 → we beat the close. CLV should be positive."""
    day = date(2026, 5, 6)
    picks = [_pick_payload(1, 100, taken=310, best_book="draftkings")]
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=picks,
                   primary_settled=[_settled_payload(1, 100, "W", taken=310)])
    # Earlier snapshot: same as taken price (irrelevant, won't be picked)
    _write_snapshot(tmp_layout["odds"], day, "1500",
                     "Player 1", fd_over=290, dk_over=310)
    # Closing snapshot (just before commence at 23:05): book moved DOWN to +250
    _write_snapshot(tmp_layout["odds"], day, "2300",
                     "Player 1", fd_over=240, dk_over=250)

    out = _build(tmp_layout)
    # Closing best price = max(240, 250) = 250.
    # decimal(310) = 4.10; decimal(250) = 3.50; CLV = 4.10/3.50 - 1 = 0.1714 → 17.14%
    assert out.summary.avg_clv_pct == pytest.approx(17.14, abs=0.05)
    assert out.summary.n_picks_with_clv == 1


def test_tracker_handles_missing_snapshot_gracefully(tmp_layout):
    day = date(2026, 5, 6)
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=[_pick_payload(1, 100)],
                   primary_settled=[_settled_payload(1, 100, "W")])
    out = _build(tmp_layout)
    assert out.summary.avg_clv_pct is None
    assert out.summary.n_picks_with_clv == 0


def test_tracker_loads_each_tier_from_archive(tmp_layout):
    """Regression: tracker.py used to read shadow_results_*.json for tier='secondary'
    because of an ``if tier == 'primary' else 'shadow_*'`` ternary. With three
    distinct tiers each must resolve to its own settlement block keys.
    """
    day = date(2026, 5, 6)
    _write_archive(
        tmp_layout["archives"], day,
        primary_picks=[_pick_payload(1, 100, taken=310)],
        primary_settled=[_settled_payload(1, 100, "W", taken=310)],
        secondary_picks=[_pick_payload(2, 200, taken=600),
                         _pick_payload(3, 200, taken=600)],
        secondary_settled=[_settled_payload(2, 200, "L", taken=600),
                            _settled_payload(3, 200, "L", taken=600)],
        shadow_picks=[_pick_payload(4, 300, taken=900)],
        shadow_settled=[_settled_payload(4, 300, "W", taken=900)],
    )
    _build(tmp_layout)
    payload = json.loads((tmp_layout["processed"] / "tracker.json").read_text())

    assert payload["summary_primary"]["wins"] == 1
    assert payload["summary_primary"]["losses"] == 0
    assert payload["summary_secondary"]["wins"] == 0
    assert payload["summary_secondary"]["losses"] == 2
    assert payload["summary_shadow"]["wins"] == 1
    assert payload["summary_shadow"]["losses"] == 0

    assert payload["calibration_n_picks_primary"] == 1
    assert payload["calibration_n_picks_secondary"] == 2
    assert payload["calibration_n_picks_shadow"] == 1


def test_tracker_by_book_breakdown(tmp_layout):
    day = date(2026, 5, 6)
    picks = [
        _pick_payload(1, 100, best_book="draftkings"),
        _pick_payload(2, 100, best_book="fanduel"),
    ]
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=picks,
                   primary_settled=[
                       _settled_payload(1, 100, "W"),
                       _settled_payload(2, 100, "L"),
                   ])
    out = _build(tmp_layout)
    assert out.by_book["draftkings"]["wins"] == 1
    assert out.by_book["fanduel"]["losses"] == 1


def test_tracker_skips_unsettled_archives(tmp_layout):
    """Archives without a settlement block contribute nothing."""
    day = date(2026, 5, 6)
    _write_archive(tmp_layout["archives"], day,
                   primary_picks=[_pick_payload(1, 100)])
    out = _build(tmp_layout)
    assert out.summary.total_picks == 0
