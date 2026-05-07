"""Tracker aggregation: ROI, hit rate, CLV, calibration buckets."""

import json
from datetime import date, datetime

import pytest

from src.results.tracker import build_tracker
import src.results.tracker as tracker_mod


def _write_picks(tmp_dir, day: date, picks: list[dict]) -> None:
    (tmp_dir / f"picks_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(),
        "picks": picks,
    }))


def _write_results(tmp_dir, day: date, settled: list[dict]) -> None:
    n_w = sum(1 for s in settled if s["outcome"] == "W")
    n_l = sum(1 for s in settled if s["outcome"] == "L")
    n_v = sum(1 for s in settled if s["outcome"] == "VOID")
    units_staked = float(n_w + n_l)
    units_profit = sum(float(s["profit_units"]) for s in settled)
    (tmp_dir / f"results_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(),
        "settled_at": "2026-05-07T08:00:00+00:00",
        "n_picks": len(settled),
        "n_wins": n_w, "n_losses": n_l, "n_voids": n_v,
        "units_staked": units_staked,
        "units_profit": units_profit,
        "roi_pct": (units_profit / units_staked * 100.0) if units_staked > 0 else 0.0,
        "results": settled,
    }))


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


def _write_snapshot(tmp_odds_dir, day: date, hour_min: str,
                     batter: str, fd_over: int, dk_over: int) -> None:
    fetched = f"{day.isoformat()}T{hour_min[:2]}:{hour_min[2:]}:00+00:00"
    (tmp_odds_dir / f"{day.isoformat()}-{hour_min}.json").write_text(json.dumps({
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
def tmp_layout(tmp_path, monkeypatch):
    processed = tmp_path / "processed"
    odds = tmp_path / "odds"
    processed.mkdir(parents=True)
    odds.mkdir(parents=True)
    monkeypatch.setattr(tracker_mod, "PROCESSED_DIR", processed)
    monkeypatch.setattr(tracker_mod, "ODDS_DIR", odds)
    monkeypatch.setattr(tracker_mod, "TRACKER_PATH", processed / "tracker.json")
    return {"processed": processed, "odds": odds}


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
    _write_picks(tmp_layout["processed"], day, picks)
    _write_results(tmp_layout["processed"], day, [
        _settled_payload(1, 100, "W", taken=310),
        _settled_payload(2, 100, "L", taken=200),
        _settled_payload(3, 200, "VOID", taken=310),
    ])

    out = build_tracker(processed_dir=tmp_layout["processed"])
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
    _write_picks(tmp_layout["processed"], day, [_pick_payload(1, 100)])
    _write_results(tmp_layout["processed"], day, [_settled_payload(1, 100, "W")])
    build_tracker(processed_dir=tmp_layout["processed"])
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
    _write_picks(tmp_layout["processed"], day, picks)
    _write_results(tmp_layout["processed"], day, settled)
    out = build_tracker(processed_dir=tmp_layout["processed"])
    # Two buckets should have rows.
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
    _write_picks(tmp_layout["processed"], day, picks)
    _write_results(tmp_layout["processed"], day,
                    [_settled_payload(1, 100, "W", taken=310)])
    # Earlier snapshot: same as taken price (irrelevant, won't be picked)
    _write_snapshot(tmp_layout["odds"], day, "1500",
                     "Player 1", fd_over=290, dk_over=310)
    # Closing snapshot (just before commence at 23:05): book moved DOWN to +250
    _write_snapshot(tmp_layout["odds"], day, "2300",
                     "Player 1", fd_over=240, dk_over=250)

    out = build_tracker(processed_dir=tmp_layout["processed"])
    # Closing best price = max(240, 250) = 250.
    # decimal(310) = 4.10; decimal(250) = 3.50; CLV = 4.10/3.50 - 1 = 0.1714 → 17.14%
    assert out.summary.avg_clv_pct == pytest.approx(17.14, abs=0.05)
    assert out.summary.n_picks_with_clv == 1


def test_tracker_handles_missing_snapshot_gracefully(tmp_layout):
    day = date(2026, 5, 6)
    picks = [_pick_payload(1, 100)]
    _write_picks(tmp_layout["processed"], day, picks)
    _write_results(tmp_layout["processed"], day,
                    [_settled_payload(1, 100, "W")])
    # No snapshots written.
    out = build_tracker(processed_dir=tmp_layout["processed"])
    assert out.summary.avg_clv_pct is None
    assert out.summary.n_picks_with_clv == 0


def test_tracker_loads_each_tier_from_its_own_files(tmp_layout):
    """Regression: tracker.py used to read shadow_results_*.json for tier='secondary'
    because of an ``if tier == 'primary' else 'shadow_*'`` ternary. With three
    distinct tiers each must resolve to its own filename prefix.
    """
    day = date(2026, 5, 6)
    proc = tmp_layout["processed"]

    # Primary: 1W
    _write_picks(proc, day, [_pick_payload(1, 100, taken=310)])
    _write_results(proc, day, [_settled_payload(1, 100, "W", taken=310)])

    # Secondary: 2L. Different files, different filename prefix.
    sec_picks = [_pick_payload(2, 200, taken=600), _pick_payload(3, 200, taken=600)]
    (proc / f"secondary_picks_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(), "picks": sec_picks,
    }))
    sec_settled = [_settled_payload(2, 200, "L", taken=600),
                    _settled_payload(3, 200, "L", taken=600)]
    (proc / f"secondary_results_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(),
        "settled_at": "2026-05-07T08:00:00+00:00",
        "n_picks": len(sec_settled), "n_wins": 0, "n_losses": 2, "n_voids": 0,
        "units_staked": 2.0, "units_profit": -2.0, "roi_pct": -100.0,
        "results": sec_settled,
    }))

    # Shadow: 1W (different result count from secondary so the bug is loud)
    sh_picks = [_pick_payload(4, 300, taken=900)]
    (proc / f"shadow_picks_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(), "picks": sh_picks,
    }))
    sh_settled = [_settled_payload(4, 300, "W", taken=900)]
    (proc / f"shadow_results_{day.isoformat()}.json").write_text(json.dumps({
        "as_of_date": day.isoformat(),
        "settled_at": "2026-05-07T08:00:00+00:00",
        "n_picks": 1, "n_wins": 1, "n_losses": 0, "n_voids": 0,
        "units_staked": 1.0, "units_profit": 9.0, "roi_pct": 900.0,
        "results": sh_settled,
    }))

    build_tracker(processed_dir=proc)
    payload = json.loads((proc / "tracker.json").read_text())

    # Each tier must reflect ITS OWN files, not be cross-contaminated.
    assert payload["summary_primary"]["wins"] == 1
    assert payload["summary_primary"]["losses"] == 0
    assert payload["summary_secondary"]["wins"] == 0
    assert payload["summary_secondary"]["losses"] == 2
    assert payload["summary_shadow"]["wins"] == 1
    assert payload["summary_shadow"]["losses"] == 0

    # And calibration aggregates ALL three tiers' rows.
    assert payload["calibration_n_picks_primary"] == 1
    assert payload["calibration_n_picks_secondary"] == 2
    assert payload["calibration_n_picks_shadow"] == 1


def test_tracker_by_book_breakdown(tmp_layout):
    day = date(2026, 5, 6)
    picks = [
        _pick_payload(1, 100, best_book="draftkings"),
        _pick_payload(2, 100, best_book="fanduel"),
    ]
    _write_picks(tmp_layout["processed"], day, picks)
    _write_results(tmp_layout["processed"], day, [
        _settled_payload(1, 100, "W"),
        _settled_payload(2, 100, "L"),
    ])
    out = build_tracker(processed_dir=tmp_layout["processed"])
    assert out.by_book["draftkings"]["wins"] == 1
    assert out.by_book["fanduel"]["losses"] == 1
