"""Regression: clv_recover must tolerate archives with explicit null settlement.

Zero-pick days (e.g. odds-quota exhaustion) write `"settlement": null`. The old
`d.get("settlement", {})` returned None for those (the default only applies when
the key is ABSENT), so `None.get(...)` crashed the whole CLV report step in the
settle workflow. This locks the `or {}` fix.
"""
import json

from src.backtest import clv_recover


def test_recover_tolerates_null_settlement(tmp_path, monkeypatch):
    monkeypatch.setattr(clv_recover, "ARCHIVE_DIR", tmp_path)
    # A zero-pick day: picks present but settlement explicitly null.
    (tmp_path / "2026-06-24.json").write_text(json.dumps({
        "primary_picks": [{"batter_id": 1, "game_pk": 9, "game_datetime": "2026-06-24T23:00:00+00:00"}],
        "secondary_picks": [], "shadow_picks": [],
        "settlement": None,
    }))
    # Must not raise; a null-settlement day simply contributes no CLV rows.
    rows = clv_recover.recover("2026-06-24", "2026-06-24")
    assert rows == []


def test_recover_tolerates_missing_settlement_key(tmp_path, monkeypatch):
    monkeypatch.setattr(clv_recover, "ARCHIVE_DIR", tmp_path)
    (tmp_path / "2026-06-25.json").write_text(json.dumps({
        "primary_picks": [], "secondary_picks": [], "shadow_picks": [],
    }))
    assert clv_recover.recover("2026-06-25", "2026-06-25") == []
