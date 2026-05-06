"""Snapshot writer tests.

Locks in the rule that snapshots are NEVER overwritten — collisions get
suffixed -1, -2, ... and existing files are left intact. Filename respects
YYYY-MM-DD-HHMM[-tag] format.
"""

import json
from datetime import date, datetime

import pytest

from src.odds.log import list_snapshots, load_snapshot, write_snapshot


@pytest.fixture
def tmp_odds_dir(tmp_path, monkeypatch):
    """Redirect ODDS_DIR to a per-test temp dir."""
    odds_dir = tmp_path / "odds"
    odds_dir.mkdir(parents=True)
    monkeypatch.setattr("src.odds.log.ODDS_DIR", odds_dir)
    return odds_dir


def _payload(extra: dict | None = None) -> dict:
    base = {
        "fetched_at": "2026-05-06T15:23:01-04:00",
        "as_of_date": "2026-05-06",
        "books_filtered": ["fanduel", "draftkings"],
        "market": "batter_home_runs_alternate",
        "quotes": [],
    }
    if extra:
        base.update(extra)
    return base


def test_write_snapshot_creates_file_with_timestamp_filename(tmp_odds_dir):
    when = datetime(2026, 5, 6, 15, 23)
    p = write_snapshot(_payload(), when=when, output_dir=tmp_odds_dir)
    assert p.name == "2026-05-06-1523.json"
    assert p.exists()


def test_write_snapshot_appends_tag(tmp_odds_dir):
    when = datetime(2026, 5, 6, 15, 23)
    p = write_snapshot(_payload(), when=when, tag="prematch", output_dir=tmp_odds_dir)
    assert p.name == "2026-05-06-1523-prematch.json"


def test_write_snapshot_never_overwrites(tmp_odds_dir):
    when = datetime(2026, 5, 6, 15, 23)
    p1 = write_snapshot(_payload({"v": 1}), when=when, output_dir=tmp_odds_dir)
    p2 = write_snapshot(_payload({"v": 2}), when=when, output_dir=tmp_odds_dir)
    p3 = write_snapshot(_payload({"v": 3}), when=when, output_dir=tmp_odds_dir)
    assert p1.name == "2026-05-06-1523.json"
    assert p2.name == "2026-05-06-1523-1.json"
    assert p3.name == "2026-05-06-1523-2.json"
    # Original survived untouched.
    assert load_snapshot(p1)["v"] == 1
    assert load_snapshot(p2)["v"] == 2
    assert load_snapshot(p3)["v"] == 3


def test_write_snapshot_adds_written_at_marker(tmp_odds_dir):
    p = write_snapshot(_payload(), when=datetime(2026, 5, 6, 12, 0),
                       output_dir=tmp_odds_dir)
    data = load_snapshot(p)
    assert "_snapshot_written_at" in data


def test_write_snapshot_serializes_datetimes(tmp_odds_dir):
    payload = _payload({
        "events": [{"commence_time": datetime(2026, 5, 6, 19, 5)}],
    })
    p = write_snapshot(payload, when=datetime(2026, 5, 6, 12, 0),
                       output_dir=tmp_odds_dir)
    data = load_snapshot(p)
    assert data["events"][0]["commence_time"] == "2026-05-06T19:05:00"


def test_list_snapshots_filters_by_date_range(tmp_odds_dir):
    write_snapshot(_payload(), when=datetime(2026, 5, 1, 10, 0), output_dir=tmp_odds_dir)
    write_snapshot(_payload(), when=datetime(2026, 5, 6, 10, 0), output_dir=tmp_odds_dir)
    write_snapshot(_payload(), when=datetime(2026, 5, 10, 10, 0), output_dir=tmp_odds_dir)

    # Patch ODDS_DIR to test dir for list_snapshots
    import src.odds.log as log_mod
    log_mod.ODDS_DIR = tmp_odds_dir

    files = list_snapshots(start=date(2026, 5, 5), end=date(2026, 5, 7))
    assert len(files) == 1
    assert "2026-05-06" in files[0].stem
