"""Resumable Statcast pull, chunked + cached per (year, start, end).

Each chunk lands in its own parquet file under data/research/feature_importance/chunks/.
Chunks already on disk are skipped on re-run, so a 30%-completed run + Ctrl-C +
re-launch picks up where it left off without re-fetching.

The chunk filename is `{year}_{start_iso}_{end_iso}.parquet`. If a chunk is empty
(off-day, all-star break, etc.) we still write a marker file so we don't retry.
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.research.feature_importance import progress
from src.research.feature_importance.config import (
    CHUNKS_DIR,
    SEASON_BOUNDS,
    STATCAST_CHUNK_DAYS,
    ensure_dirs,
)


def _chunk_filename(year: int, start: date, end: date) -> Path:
    return CHUNKS_DIR / f"{year}_{start.isoformat()}_{end.isoformat()}.parquet"


def _empty_marker(year: int, start: date, end: date) -> Path:
    return CHUNKS_DIR / f"{year}_{start.isoformat()}_{end.isoformat()}.empty"


def _enumerate_chunks(year: int) -> list[tuple[date, date]]:
    """Build the list of [start, end] inclusive chunks for one season."""
    season_start, season_end = SEASON_BOUNDS[year]
    out: list[tuple[date, date]] = []
    cur = season_start
    while cur <= season_end:
        chunk_end = min(cur + timedelta(days=STATCAST_CHUNK_DAYS - 1), season_end)
        out.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return out


def pull_year(year: int, retry_per_chunk: int = 3, retry_sleep_s: int = 30) -> int:
    """Pull a single season's Statcast data, chunked + cached. Returns # new chunks fetched."""
    from pybaseball import statcast  # type: ignore

    ensure_dirs()
    chunks = _enumerate_chunks(year)
    total = len(chunks)
    fetched = 0
    skipped = 0
    chunk_throughput_start = time.time()
    chunks_done_in_run = 0

    progress.info(f"year {year}: {total} chunks of {STATCAST_CHUNK_DAYS} days each")

    for i, (start, end) in enumerate(chunks, start=1):
        out_path = _chunk_filename(year, start, end)
        empty_path = _empty_marker(year, start, end)

        if out_path.exists() or empty_path.exists():
            skipped += 1
            if i % 10 == 0 or i == total:
                progress.progress_chunk(
                    i, total, f"{start}..{end} (cached, skipped)",
                )
            continue

        # Throughput estimate based on this run's actual observed pace.
        eta_rate = None
        if chunks_done_in_run > 0:
            elapsed = time.time() - chunk_throughput_start
            if elapsed > 0:
                eta_rate = chunks_done_in_run / elapsed

        progress.progress_chunk(
            i, total, f"fetching {start}..{end}",
            eta_chunks_per_sec=eta_rate,
        )

        df = _fetch_with_retry(statcast, start, end, retry_per_chunk, retry_sleep_s)

        if df is None or df.empty:
            empty_path.touch()
            progress.info(f"  -> empty chunk; wrote marker {empty_path.name}")
        else:
            df.to_parquet(out_path, index=False)
            progress.info(f"  -> {len(df):,} rows -> {out_path.name}")

        fetched += 1
        chunks_done_in_run += 1

    progress.info(
        f"year {year} complete: {fetched} new chunks fetched, {skipped} cached/skipped"
    )
    return fetched


def _fetch_with_retry(statcast_fn, start: date, end: date, max_retries: int, sleep_s: int):
    """Pull one chunk with bounded retries. Re-raises after max_retries failures."""
    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            return statcast_fn(start_dt=start.isoformat(), end_dt=end.isoformat())
        except Exception as e:  # pybaseball raises ValueError, requests errors, etc.
            last_err = e
            progress.warn(
                f"  attempt {attempt}/{max_retries} failed for {start}..{end}: "
                f"{type(e).__name__}: {e}"
            )
            if attempt < max_retries:
                time.sleep(sleep_s)
    progress.error(
        f"  giving up on {start}..{end} after {max_retries} attempts. "
        f"Re-run the script -- already-fetched chunks won't be re-pulled."
    )
    raise last_err  # type: ignore[misc]


def pull_all(years: tuple[int, ...]) -> None:
    """Pull every requested year. Resumable: existing chunks are skipped."""
    with progress.phase(f"Statcast pull {years[0]}..{years[-1]}"):
        for year in years:
            with progress.phase(f"year {year}"):
                pull_year(year)


def load_cached(years: tuple[int, ...]) -> pd.DataFrame:
    """Load all cached chunks for the given years into one DataFrame.

    Reads only the columns we need downstream to keep memory bounded.
    """
    ensure_dirs()
    keep_cols = [
        "game_pk", "game_date", "game_year",
        "batter", "pitcher", "stand", "p_throws",
        "events", "description",
        "launch_speed", "launch_angle", "launch_speed_angle",
        "estimated_woba_using_speedangle",
        "bb_type", "hc_x", "hc_y",
        "home_team",
        "bat_speed",   # 2024+; missing column is handled below
    ]
    frames: list[pd.DataFrame] = []
    files: list[Path] = []
    for year in years:
        files.extend(sorted(CHUNKS_DIR.glob(f"{year}_*.parquet")))

    progress.info(f"loading {len(files)} cached chunks for years {years}")
    for f in files:
        df = pd.read_parquet(f)
        # Defensive: 2022/2023 won't have bat_speed.
        if "bat_speed" not in df.columns:
            df["bat_speed"] = float("nan")
        df = df[[c for c in keep_cols if c in df.columns]]
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=keep_cols)
    out = pd.concat(frames, ignore_index=True)
    out["game_date"] = pd.to_datetime(out["game_date"]).dt.date
    progress.info(f"loaded {len(out):,} pitch rows total")
    return out


if __name__ == "__main__":
    # Allow `python -m src.research.feature_importance.pull_data` as a standalone fetch.
    from src.research.feature_importance.config import YEARS_ALL
    pull_all(YEARS_ALL)
