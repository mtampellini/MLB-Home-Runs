"""Outcome backfill onto the full-slate log."""

import json
from pathlib import Path

import pytest

from src.results.full_slate_outcomes import (
    LabelStore,
    backfill,
    build_labeled_dataset,
    coverage,
    harvest_archive_labels,
    load_store,
    save_store,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _slate_file(dirpath: Path, day: str, rows: list[dict],
                model_version: str = "v7-weather-cal2-0.3.0") -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / f"{day}.json").write_text(json.dumps({
        "date": day,
        "generated_at": f"{day}T12:00:00-04:00",
        "model_version": model_version,
        "rows": rows,
    }))


def _row(batter_id: int, game_pk: int, *, matched_odds: bool = True,
         model_prob: float = 0.20) -> dict:
    return {
        "batter_id": batter_id,
        "batter": f"Batter {batter_id}",
        "game_pk": game_pk,
        "model_prob": model_prob,
        "matched_odds": matched_odds,
    }


def _archive_file(dirpath: Path, day: str, results: list[dict],
                  tier: str = "primary") -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    (dirpath / f"{day}.json").write_text(json.dumps({
        "date": day,
        f"{tier}_picks": [],
        "settlement": {f"{tier}_results": results},
    }))


def _result(batter_id: int, game_pk: int, outcome: str, actual_hr: int) -> dict:
    return {
        "batter_id": batter_id,
        "game_pk": game_pk,
        "outcome": outcome,
        "actual_hr": actual_hr,
    }


def _box(entries: dict[int, int | None]) -> dict:
    """entries: {batter_id: hr_count or None for 'on roster, did not bat'}."""
    players = {}
    for bid, hr in entries.items():
        players[f"ID_{bid}"] = {
            "person": {"id": bid, "fullName": f"Batter {bid}"},
            "stats": {"batting": {"homeRuns": hr, "atBats": 4}} if hr is not None else {},
        }
    return {"teams": {"home": {"players": players}, "away": {"players": {}}}}


class FakeClient:
    """Stands in for MlbStatsClient: canned schedule + boxscores, call-counted."""

    def __init__(self, statuses: dict[int, str], boxes: dict[int, dict]):
        self._statuses = statuses
        self._boxes = boxes
        self.boxscore_calls: list[int] = []

    def schedule_for_date(self, d):
        return {"dates": [{"games": [
            {"gamePk": gpk, "status": {"abstractGameState": state}}
            for gpk, state in self._statuses.items()
        ]}]}

    def _get(self, path: str, params=None):
        gpk = int(path.split("/")[2])
        self.boxscore_calls.append(gpk)
        if gpk not in self._boxes:
            raise RuntimeError(f"no canned boxscore for {gpk}")
        return self._boxes[gpk]


# ---------------------------------------------------------------------------
# Store round-trip
# ---------------------------------------------------------------------------

def test_store_roundtrip(tmp_path):
    store = LabelStore()
    store.put(101, 900, 1, source="boxscore")
    store.put(102, 900, 0, source="archive")
    store.games_processed.add(900)

    path = tmp_path / "store.json"
    save_store(store, path)
    loaded = load_store(path)

    assert loaded.get(101, 900) == 1
    assert loaded.get(102, 900) == 0
    assert loaded.games_processed == {900}
    assert loaded.sources["101|900"] == "boxscore"


def test_load_store_missing_file_is_empty(tmp_path):
    store = load_store(tmp_path / "nope.json")
    assert store.labels == {}
    assert store.games_processed == set()


def test_existing_label_is_never_overwritten():
    """Archive labels are settled history; a boxscore re-read must not rewrite them."""
    store = LabelStore()
    store.put(101, 900, 1, source="archive")
    store.put(101, 900, 0, source="boxscore")
    assert store.get(101, 900) == 1
    assert store.sources["101|900"] == "archive"


# ---------------------------------------------------------------------------
# Source 1: archives
# ---------------------------------------------------------------------------

def test_harvest_archive_labels(tmp_path):
    _archive_file(tmp_path, "2026-07-01", [
        _result(101, 900, "W", 1),
        _result(102, 900, "L", 0),
    ])
    store = LabelStore()
    added = harvest_archive_labels(store, tmp_path)

    assert added == 2
    assert store.get(101, 900) == 1
    assert store.get(102, 900) == 0


def test_harvest_skips_voids(tmp_path):
    """A VOID is 'no usable label', not 'did not homer'."""
    _archive_file(tmp_path, "2026-07-01", [
        _result(101, 900, "VOID", 0),
        _result(102, 900, "W", 2),
    ])
    store = LabelStore()
    added = harvest_archive_labels(store, tmp_path)

    assert added == 1
    assert store.get(101, 900) is None
    assert store.get(102, 900) == 2


def test_harvest_is_idempotent(tmp_path):
    _archive_file(tmp_path, "2026-07-01", [_result(101, 900, "W", 1)])
    store = LabelStore()
    assert harvest_archive_labels(store, tmp_path) == 1
    assert harvest_archive_labels(store, tmp_path) == 0
    assert len(store.labels) == 1


def test_harvest_tolerates_unreadable_archive(tmp_path):
    (tmp_path / "2026-07-01.json").write_text("{ not json")
    _archive_file(tmp_path, "2026-07-02", [_result(101, 900, "W", 1)])
    store = LabelStore()
    assert harvest_archive_labels(store, tmp_path) == 1


# ---------------------------------------------------------------------------
# Source 2: boxscores
# ---------------------------------------------------------------------------

def test_fetch_boxscore_labels(tmp_path, monkeypatch):
    slate, archives, store_path = tmp_path / "fs", tmp_path / "ar", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 900), _row(103, 901)])

    client = FakeClient(
        statuses={900: "Final", 901: "Final"},
        boxes={900: _box({101: 1, 102: 0}), 901: _box({103: 3})},
    )
    result = backfill(full_slate_dir=slate, archives_dir=archives,
                      store_path=store_path, client=client)

    assert result["added_from_boxscore"] == 3
    assert result["games_fetched"] == 2
    store = load_store(store_path)
    assert store.get(101, 900) == 1
    assert store.get(102, 900) == 0
    assert store.get(103, 901) == 3


def test_did_not_bat_gets_no_label(tmp_path):
    """A scratched batter must be dropped, never scored as a zero."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 900)])

    client = FakeClient(statuses={900: "Final"},
                        boxes={900: _box({101: 0, 102: None})})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=client)

    store = load_store(store_path)
    assert store.get(101, 900) == 0
    assert store.get(102, 900) is None
    # The game is still marked done so we never re-fetch chasing the missing one.
    assert 900 in store.games_processed


def test_non_final_games_are_skipped_and_retried(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])

    live = FakeClient(statuses={900: "Live"}, boxes={900: _box({101: 1})})
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=live)
    assert result["games_skipped_not_final"] == 1
    assert result["added_from_boxscore"] == 0
    assert live.boxscore_calls == []          # no wasted fetch
    assert load_store(store_path).games_processed == set()

    final = FakeClient(statuses={900: "Final"}, boxes={900: _box({101: 1})})
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=final)
    assert result["added_from_boxscore"] == 1


def test_dead_schedule_api_is_not_reported_as_games_in_progress(tmp_path):
    """A failed schedule call must not look like 'every game is still Live'.

    settle._fetch_game_statuses swallows request errors and returns {}, so
    without an explicit check a total outage counts every game as not-final
    and exits clean — a silent no-op backfill.
    """
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 901)])

    dead = FakeClient(statuses={}, boxes={})       # schedule returns nothing
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=dead)

    assert result["schedule_failures"] == 1
    assert result["games_skipped_not_final"] == 0
    assert dead.boxscore_calls == []
    assert load_store(store_path).games_processed == set()


def test_boxscore_failures_are_counted(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])

    client = FakeClient(statuses={900: "Final"}, boxes={})   # _get raises
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=client)

    assert result["boxscore_failures"] == 1
    assert result["games_fetched"] == 0


def test_processed_games_are_not_refetched(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])

    c1 = FakeClient(statuses={900: "Final"}, boxes={900: _box({101: 1})})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c1)
    assert c1.boxscore_calls == [900]

    c2 = FakeClient(statuses={900: "Final"}, boxes={900: _box({101: 1})})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c2)
    assert c2.boxscore_calls == []


def test_boxscore_failure_keeps_offline_progress(tmp_path):
    """One dead game must not sink the run or lose archive labels."""
    slate, archives, store_path = tmp_path / "fs", tmp_path / "ar", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 901)])
    _archive_file(archives, "2026-07-01", [_result(101, 900, "W", 1)])

    client = FakeClient(statuses={900: "Final", 901: "Final"},
                        boxes={900: _box({101: 1})})     # 901 raises
    result = backfill(full_slate_dir=slate, archives_dir=archives,
                      store_path=store_path, client=client)

    assert result["added_from_archive"] == 1
    store = load_store(store_path)
    assert store.get(101, 900) == 1
    assert 901 not in store.games_processed          # retried next run


def test_max_games_limits_fetching(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 901), _row(103, 902)])

    client = FakeClient(
        statuses={900: "Final", 901: "Final", 902: "Final"},
        boxes={g: _box({100 + i: 0}) for i, g in enumerate((900, 901, 902), start=1)},
    )
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=client, max_games=2)
    assert result["games_fetched"] == 2


def test_offline_only_makes_no_calls(tmp_path):
    slate, archives, store_path = tmp_path / "fs", tmp_path / "ar", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])
    _archive_file(archives, "2026-07-01", [_result(101, 900, "W", 1)])

    client = FakeClient(statuses={900: "Final"}, boxes={900: _box({101: 1})})
    result = backfill(full_slate_dir=slate, archives_dir=archives,
                      store_path=store_path, client=client, offline_only=True)

    assert result["added_from_archive"] == 1
    assert result["games_fetched"] == 0
    assert client.boxscore_calls == []


# ---------------------------------------------------------------------------
# The join
# ---------------------------------------------------------------------------

def test_build_labeled_dataset(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 900), _row(103, 900)])

    store = LabelStore()
    store.put(101, 900, 2, source="boxscore")
    store.put(102, 900, 0, source="boxscore")
    save_store(store, store_path)

    ds = build_labeled_dataset(full_slate_dir=slate, store_path=store_path)

    assert len(ds) == 2                        # 103 unlabeled -> dropped
    by_id = {r["batter_id"]: r for r in ds}
    assert by_id[101]["label"] == 1 and by_id[101]["actual_hr"] == 2
    assert by_id[102]["label"] == 0
    assert by_id[101]["slate_date"] == "2026-07-01"


def test_require_odds_filter(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01",
                [_row(101, 900, matched_odds=True), _row(102, 900, matched_odds=False)])

    store = LabelStore()
    store.put(101, 900, 1, source="boxscore")
    store.put(102, 900, 1, source="boxscore")
    save_store(store, store_path)

    assert len(build_labeled_dataset(full_slate_dir=slate, store_path=store_path)) == 2
    assert len(build_labeled_dataset(full_slate_dir=slate, store_path=store_path,
                                     require_odds=True)) == 1


def test_model_version_filter(tmp_path):
    """model_prob is not comparable across the 2026-07-01 rebuild."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-06-20", [_row(101, 900)], model_version="v7-baseline-0.2.0")
    _slate_file(slate, "2026-07-05", [_row(102, 901)], model_version="v7-weather-cal2-0.3.0")

    store = LabelStore()
    store.put(101, 900, 1, source="boxscore")
    store.put(102, 901, 0, source="boxscore")
    save_store(store, store_path)

    ds = build_labeled_dataset(full_slate_dir=slate, store_path=store_path,
                               model_version="v7-weather-cal2-0.3.0")
    assert [r["batter_id"] for r in ds] == [102]


def test_rows_without_game_pk_are_skipped(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    row = _row(101, 900)
    row["game_pk"] = None
    _slate_file(slate, "2026-07-01", [row])

    save_store(LabelStore(), store_path)
    assert build_labeled_dataset(full_slate_dir=slate, store_path=store_path) == []
    cov = coverage(load_store(store_path), slate)
    assert cov.rows_no_game_pk == 1
    assert cov.rows_unlabeled == 0


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

def test_coverage_counts(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [
        _row(101, 900, matched_odds=True),
        _row(102, 900, matched_odds=True),
        _row(103, 900, matched_odds=False),
    ])

    store = LabelStore()
    store.put(101, 900, 1, source="boxscore")
    store.put(102, 900, 0, source="archive")
    save_store(store, store_path)

    cov = coverage(load_store(store_path), slate)
    assert cov.rows_total == 3
    assert cov.rows_labeled == 2
    assert cov.rows_unlabeled == 1
    assert cov.hr_rate == pytest.approx(0.5)
    assert cov.labeled_by_source == {"boxscore": 1, "archive": 1}
    assert cov.rows_with_odds == 2
    assert cov.rows_with_odds_labeled == 2
    assert cov.date_min == "2026-07-01"


def test_complete_games_only_excludes_partially_labeled_games(tmp_path):
    """A game whose box score hasn't been read has only its PICKS labeled.

    Mixing those rows into an 'unbiased' frame quietly restores the selection
    bias the full-slate log exists to remove.
    """
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900), _row(102, 900),
                                      _row(103, 901), _row(104, 901)])

    store = LabelStore()
    # Game 900 fully read from its box score.
    store.put(101, 900, 1, source="boxscore")
    store.put(102, 900, 0, source="boxscore")
    store.games_processed.add(900)
    # Game 901: only the pick got a label from the archive.
    store.put(103, 901, 1, source="archive")
    save_store(store, store_path)

    mixed = build_labeled_dataset(full_slate_dir=slate, store_path=store_path)
    clean = build_labeled_dataset(full_slate_dir=slate, store_path=store_path,
                                  complete_games_only=True)

    assert {r["batter_id"] for r in mixed} == {101, 102, 103}
    assert {r["batter_id"] for r in clean} == {101, 102}


# ---------------------------------------------------------------------------
# Batting lines (markets beyond home runs)
# ---------------------------------------------------------------------------

def _box_line(batter_id: int, **stats) -> dict:
    return {"teams": {"home": {"players": {f"ID_{batter_id}": {
        "person": {"id": batter_id, "fullName": "X"},
        "stats": {"batting": stats},
    }}}, "away": {"players": {}}}}


def test_total_bases_computed_from_components():
    """`hits` is inclusive, so singles must be derived, and totalBases is not
    present on every boxscore — compute it rather than trust it."""
    from src.results.full_slate_outcomes import batting_line_for_batter
    box = _box_line(101, atBats=5, hits=3, doubles=1, triples=0, homeRuns=1)
    line = batting_line_for_batter(box, 101)
    assert line["1b"] == 1                       # 3 hits - 1 double - 1 HR
    assert line["tb"] == 1 + 2 + 4               # 1B + 2B + HR
    assert line["hr"] == 1


def test_batting_line_none_when_batter_did_not_play():
    from src.results.full_slate_outcomes import batting_line_for_batter
    box = {"teams": {"home": {"players": {"ID_101": {
        "person": {"id": 101}, "stats": {}}}}, "away": {"players": {}}}}
    assert batting_line_for_batter(box, 101) is None
    assert batting_line_for_batter(box, 999) is None


def test_line_capture_reprocesses_already_labeled_games(tmp_path):
    """A game read before lines existed must be re-read once, without
    disturbing the HR label already settled for it."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])

    # Simulate the old schema: labeled and marked processed, but no line.
    old = LabelStore()
    old.put(101, 900, 1, source="archive")
    old.games_processed.add(900)
    save_store(old, store_path)

    client = FakeClient(statuses={900: "Final"},
                        boxes={900: _box_line(101, atBats=4, hits=2,
                                              doubles=1, triples=0, homeRuns=1)})
    result = backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
                      store_path=store_path, client=client)

    assert client.boxscore_calls == [900]        # re-read for the line
    store = load_store(store_path)
    assert store.get(101, 900) == 1              # settled label untouched
    assert store.sources["101|900"] == "archive"
    # hits=2 = one double + one HR, so no singles: TB = 2 + 4.
    assert store.get_line(101, 900)["tb"] == 2 + 4
    assert 900 in store.games_lined


def test_lined_games_are_not_refetched(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])
    box = _box_line(101, atBats=4, hits=1, doubles=0, triples=0, homeRuns=1)

    c1 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c1)
    c2 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c2)
    assert c2.boxscore_calls == []


def test_total_bases_absent_is_none_not_zero(tmp_path):
    """A game whose line hasn't been captured must not read as 0 total bases."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])
    store = LabelStore()
    store.put(101, 900, 1, source="archive")
    save_store(store, store_path)

    ds = build_labeled_dataset(full_slate_dir=slate, store_path=store_path)
    assert ds[0]["total_bases"] is None
    assert ds[0]["label"] == 1


# ---------------------------------------------------------------------------
# Pitching lines
# ---------------------------------------------------------------------------

def _box_pitch(pitcher_id: int, **stats) -> dict:
    return {"teams": {"home": {"players": {f"ID_{pitcher_id}": {
        "person": {"id": pitcher_id, "fullName": "P"},
        "stats": {"pitching": stats},
    }}}, "away": {"players": {}}}}


def test_innings_pitched_parsed_as_thirds_not_decimal():
    """'5.2' means 5 and 2/3 innings, not 5.2 — reading it as a float is a
    silent ~13% error on every start."""
    from src.results.full_slate_outcomes import pitching_line_for_pitcher
    line = pitching_line_for_pitcher(
        _box_pitch(500, inningsPitched="5.2", battersFaced=24, homeRuns=1,
                   strikeOuts=7, earnedRuns=2, hits=5, baseOnBalls=1), 500)
    assert line["outs"] == 17                    # 5*3 + 2
    assert line["ip"] == pytest.approx(5.667, abs=0.001)
    assert line["hr"] == 1 and line["k"] == 7


def test_whole_innings_parse():
    from src.results.full_slate_outcomes import pitching_line_for_pitcher
    line = pitching_line_for_pitcher(_box_pitch(500, inningsPitched="6.0"), 500)
    assert line["outs"] == 18


def test_pitching_line_none_when_did_not_pitch():
    from src.results.full_slate_outcomes import pitching_line_for_pitcher
    box = {"teams": {"home": {"players": {"ID_500": {
        "person": {"id": 500}, "stats": {}}}}, "away": {"players": {}}}}
    assert pitching_line_for_pitcher(box, 500) is None


def test_backfill_captures_the_starting_pitcher(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    row = _row(101, 900)
    row["pitcher_id"] = 500
    _slate_file(slate, "2026-07-01", [row])

    box = {"teams": {"home": {"players": {
        "ID_101": {"person": {"id": 101},
                   "stats": {"batting": {"hits": 1, "homeRuns": 1, "atBats": 4,
                                         "doubles": 0, "triples": 0}}},
        "ID_500": {"person": {"id": 500},
                   "stats": {"pitching": {"inningsPitched": "6.1",
                                          "homeRuns": 2, "strikeOuts": 8,
                                          "battersFaced": 25}}},
    }}, "away": {"players": {}}}}

    client = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=client)

    store = load_store(store_path)
    assert store.get_line(101, 900)["tb"] == 4
    pl = store.get_pitching_line(500, 900)
    assert pl["outs"] == 19 and pl["hr"] == 2 and pl["k"] == 8
    assert 900 in store.games_pitcher_lined


def test_pitcher_lined_games_are_not_refetched(tmp_path):
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    row = _row(101, 900); row["pitcher_id"] = 500
    _slate_file(slate, "2026-07-01", [row])
    box = {"teams": {"home": {"players": {
        "ID_101": {"person": {"id": 101}, "stats": {"batting": {"hits": 0, "homeRuns": 0,
                                                                "atBats": 3, "doubles": 0,
                                                                "triples": 0}}},
        "ID_500": {"person": {"id": 500}, "stats": {"pitching": {"inningsPitched": "5.0"}}},
    }}, "away": {"players": {}}}}
    c1 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c1)
    c2 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c2)
    assert c2.boxscore_calls == []


# ---------------------------------------------------------------------------
# Rosters (joining the odds archive, which keys on NAME)
# ---------------------------------------------------------------------------

def test_normalize_name_folds_accents_punctuation_and_suffixes():
    from src.results.full_slate_outcomes import normalize_name
    assert normalize_name("Ronald Acuña Jr.") == normalize_name("Ronald Acuna Jr")
    assert normalize_name("José Ramírez") == normalize_name("Jose Ramirez")
    assert normalize_name("Michael Harris II") == "michael harris"
    assert normalize_name("  Aaron   Judge ") == "aaron judge"


def test_roster_captures_every_batter_not_just_projected(tmp_path):
    """Odds rows key on name, so the join needs the WHOLE roster — the big book
    disagreements cluster on players the pipeline never projects."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])     # only 101 projected

    box = {"teams": {"home": {"players": {
        "ID_101": {"person": {"id": 101, "fullName": "Aaron Judge"},
                   "stats": {"batting": {"hits": 1, "homeRuns": 1, "atBats": 4,
                                         "doubles": 0, "triples": 0}}},
        "ID_777": {"person": {"id": 777, "fullName": "José Ramírez"},
                   "stats": {"batting": {"hits": 2, "homeRuns": 0, "atBats": 4,
                                         "doubles": 1, "triples": 0}}},
    }}, "away": {"players": {}}}}

    client = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=client)

    roster = load_store(store_path).rosters["900"]
    assert roster["aaron judge"] == 101
    assert roster["jose ramirez"] == 777          # never projected, still mapped


def test_roster_absence_triggers_one_refetch(tmp_path):
    """Games read before rosters existed must be re-read exactly once."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])
    old = LabelStore()
    old.put(101, 900, 1, source="boxscore")
    old.games_processed.add(900)
    old.games_lined.add(900)
    old.games_pitcher_lined.add(900)
    save_store(old, store_path)

    box = {"teams": {"home": {"players": {"ID_101": {
        "person": {"id": 101, "fullName": "Aaron Judge"},
        "stats": {"batting": {"hits": 1, "homeRuns": 1, "atBats": 4,
                              "doubles": 0, "triples": 0}}}}},
        "away": {"players": {}}}}
    c1 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c1)
    assert c1.boxscore_calls == [900]
    assert "900" in load_store(store_path).rosters

    c2 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c2)
    assert c2.boxscore_calls == []


def test_empty_roster_still_stops_the_refetch_loop(tmp_path):
    """A box score with no usable names must not re-fetch forever."""
    slate, store_path = tmp_path / "fs", tmp_path / "s.json"
    _slate_file(slate, "2026-07-01", [_row(101, 900)])
    box = {"teams": {"home": {"players": {"ID_101": {
        "person": {"id": 101},                       # no fullName
        "stats": {"batting": {"hits": 0, "homeRuns": 0, "atBats": 3,
                              "doubles": 0, "triples": 0}}}}},
        "away": {"players": {}}}}
    c1 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c1)
    c2 = FakeClient(statuses={900: "Final"}, boxes={900: box})
    backfill(full_slate_dir=slate, archives_dir=tmp_path / "ar",
             store_path=store_path, client=c2)
    assert c2.boxscore_calls == []
    assert load_store(store_path).rosters["900"] == {}
