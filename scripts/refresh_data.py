#!/usr/bin/env python3
"""
refresh_data.py - Refresh Statcast profiles for V6 model
=========================================================
Pulls recent Statcast data and recomputes all profile .parquet files
that the run_pipeline.py uses for V6 features.

Run this weekly (or daily for freshest data).

Usage:
    python scripts/refresh_data.py
    python scripts/refresh_data.py --days 30   # Pull last 30 days only
"""

import argparse, sys, time
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "model" / "data"

PITCH_MAP = {'FF': 'FF', 'FA': 'FF', 'SI': 'SI', 'FC': 'FC', 'SL': 'SL',
    'ST': 'ST', 'SV': 'SL', 'CU': 'CU', 'KC': 'CU', 'CS': 'CU',
    'CH': 'CH', 'FS': 'FS', 'FO': 'FS'}
BUCKETS = ['FF', 'SI', 'FC', 'SL', 'ST', 'CU', 'CH', 'FS']
SHRINKAGE_PA = 100


def pull_statcast(days_back=90):
    """Pull recent Statcast pitch-level data."""
    from pybaseball import statcast
    end = datetime.now()
    start = end - timedelta(days=days_back)
    print(f"Pulling Statcast: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}...")

    chunks = []
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=30), end)
        s, e = cur.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        print(f"  {s} to {e}...")
        try:
            df = statcast(s, e)
            if len(df) > 0:
                chunks.append(df)
                print(f"    {len(df)} pitches")
            time.sleep(2)
        except Exception as ex:
            print(f"    Error: {ex}")
            time.sleep(5)
        cur = chunk_end + timedelta(days=1)

    if not chunks:
        print("No data pulled!")
        return None

    df = pd.concat(chunks, ignore_index=True)
    df['game_date'] = pd.to_datetime(df['game_date'])

    # Precompute flags
    df['is_hr'] = df['events'] == 'home_run'
    df['is_strikeout'] = df['events'].isin(['strikeout', 'strikeout_double_play'])
    df['is_walk'] = df['events'].isin(['walk', 'hit_by_pitch'])
    df['is_whiff'] = df['description'].isin(['swinging_strike', 'swinging_strike_blocked', 'foul_tip'])
    df['is_swing'] = df['description'].isin([
        'swinging_strike', 'swinging_strike_blocked', 'foul_tip',
        'foul', 'foul_bunt', 'hit_into_play', 'hit_into_play_score', 'hit_into_play_no_out'])
    df['is_hard_hit'] = df['launch_speed'] >= 95
    df['is_barrel'] = (df['launch_speed'] >= 98) & (df['launch_angle'].between(26, 30))
    df['pitch_bucket'] = df['pitch_type'].map(PITCH_MAP)

    print(f"Total: {len(df)} pitches, {df['pitcher'].nunique()} pitchers, {df['batter'].nunique()} batters")
    return df


def build_pitcher_pitch_profiles(df):
    """Granular 7-type pitch mix per pitcher."""
    print("Building pitcher pitch profiles...")
    pitch_data = df[df['pitch_bucket'].notna()]
    profiles = []
    for pid in pitch_data['pitcher'].unique():
        pdata = pitch_data[pitch_data['pitcher'] == pid]
        total = len(pdata)
        if total < 100: continue
        pname = pdata['player_name'].iloc[0] if 'player_name' in pdata.columns else str(pid)
        row = {'pitcher_id': pid, 'pitcher_name': pname, 'total_pitches': total}
        for b in BUCKETS:
            bdata = pdata[pdata['pitch_bucket'] == b]
            n = len(bdata)
            row[f'{b}_pct'] = n / total
            if n >= 20:
                row[f'{b}_hr_rate'] = bdata['is_hr'].sum() / n
                sw = bdata['is_swing'].sum(); wh = bdata['is_whiff'].sum()
                row[f'{b}_whiff'] = wh / sw if sw > 0 else 0
            else:
                row[f'{b}_hr_rate'] = np.nan; row[f'{b}_whiff'] = np.nan
            row[f'{b}_danger'] = row[f'{b}_pct'] * (row.get(f'{b}_hr_rate', 0) or 0) * 100
        profiles.append(row)

    out = pd.DataFrame(profiles)
    out.to_parquet(DATA_DIR / "pitcher_pitch_profiles_2025.parquet", index=False)
    print(f"  Saved {len(out)} pitcher pitch profiles")
    return out


def build_rolling_30d(df):
    """Rolling 30-day pitcher stats."""
    print("Building rolling 30-day pitcher stats...")
    daily = df.groupby(['pitcher', 'game_date']).agg(
        pitches=('pitch_type', 'count'), swings=('is_swing', 'sum'), whiffs=('is_whiff', 'sum'),
        hrs=('is_hr', 'sum'), batted=('launch_speed', 'count'), total_ev=('launch_speed', 'sum'),
        barrels=('is_barrel', 'sum'), hard_hits=('is_hard_hit', 'sum'), velo=('release_speed', 'mean'),
    ).reset_index().sort_values(['pitcher', 'game_date'])

    records = []
    for pid, grp in daily.groupby('pitcher'):
        grp = grp.sort_values('game_date')
        dates, pitches = grp['game_date'].values, grp['pitches'].values
        for i in range(len(grp)):
            cd = dates[i]; ws = cd - np.timedelta64(30, 'D')
            mask = (grp['game_date'] >= pd.Timestamp(ws)) & (grp['game_date'] < pd.Timestamp(cd))
            w = grp[mask]
            if len(w) < 1: w = grp[grp['game_date'] < pd.Timestamp(cd)]
            if len(w) == 0: continue
            tp = w['pitches'].sum(); tsw = w['swings'].sum(); twh = w['whiffs'].sum()
            thr = w['hrs'].sum(); tbb = w['batted'].sum(); tev = w['total_ev'].sum()
            tbr = w['barrels'].sum(); thh = w['hard_hits'].sum()
            records.append({'pitcher': pid, 'game_date': cd, 'r30_pitches': tp,
                'r30_whiff_rate': twh / tsw if tsw > 0 else np.nan,
                'r30_hr_per_pitch': thr / tp if tp > 0 else np.nan,
                'r30_avg_ev': tev / tbb if tbb > 0 else np.nan,
                'r30_barrel_rate': tbr / tbb if tbb > 0 else np.nan,
                'r30_hard_hit_rate': thh / tbb if tbb > 0 else np.nan,
                'r30_avg_velo': w['velo'].mean()})

    out = pd.DataFrame(records)
    out['game_date'] = pd.to_datetime(out['game_date'])
    out.to_parquet(DATA_DIR / "pitcher_rolling_30d.parquet", index=False)
    print(f"  Saved {len(out)} rolling records")


def build_bvp(df):
    """Batter vs pitch type with Bayesian shrinkage."""
    print("Building batter vs pitch type profiles...")
    pitch_data = df[df['pitch_bucket'].notna()]
    lg = pitch_data.groupby('pitch_bucket').agg(n=('events', 'count'), hrs=('is_hr', 'sum')).reset_index()
    lg['lg_hr'] = lg['hrs'] / lg['n'].clip(lower=1)
    lg_rates = {r['pitch_bucket']: r['lg_hr'] for _, r in lg.iterrows()}

    bp = pitch_data.groupby(['batter', 'pitch_bucket']).agg(
        n=('events', 'count'), hrs=('is_hr', 'sum'),
        batted=('launch_speed', 'count'), ev_sum=('launch_speed', 'sum')).reset_index()

    records = []
    for _, r in bp.iterrows():
        bid, b, n = r['batter'], r['pitch_bucket'], r['n']
        raw_hr = r['hrs'] / n if n > 0 else 0
        raw_ev = r['ev_sum'] / r['batted'] if r['batted'] > 0 else np.nan
        w = n / (n + SHRINKAGE_PA)
        records.append({'batter': bid, 'pitch_bucket': b,
            f'bvp_hr_rate_{b}': w * raw_hr + (1 - w) * lg_rates.get(b, 0.03),
            f'bvp_avg_ev_{b}': raw_ev if pd.notna(raw_ev) else lg_rates.get(b, 85)})

    bvp_df = pd.DataFrame(records)
    # Pivot
    wide = bvp_df.pivot_table(index='batter', columns='pitch_bucket',
        values=[c for c in bvp_df.columns if c.startswith('bvp_')], aggfunc='first')
    wide.columns = [f'{col[0]}' if col[0].startswith('bvp_') else f'{col[0]}_{col[1]}' for col in wide.columns]
    wide = wide.reset_index()
    wide.to_parquet(DATA_DIR / "batter_vs_pitch_type.parquet", index=False)
    print(f"  Saved {len(wide)} batter BvP profiles")


def build_splits(df):
    """Batter HR rate by pitcher hand."""
    print("Building batter splits...")
    events = df[df['events'].notna()]
    sp = events.groupby(['batter', 'stand', 'p_throws']).agg(
        pa=('events', 'count'), hrs=('is_hr', 'sum')).reset_index()
    sp['split_hr_rate'] = sp['hrs'] / sp['pa']
    sp.to_parquet(DATA_DIR / "batter_splits.parquet", index=False)
    print(f"  Saved {len(sp)} split records")


def build_same_side_weapon(df):
    """Pitcher's best pitch against same-hand batters."""
    print("Building same-side weapons...")
    pitch_data = df[df['pitch_bucket'].notna()]
    same = pitch_data[pitch_data['stand'] == pitch_data['p_throws']]
    grp = same.groupby(['pitcher', 'pitch_bucket']).agg(
        n=('pitch_type', 'count'), wh=('is_whiff', 'sum'),
        sw=('is_swing', 'sum'), hrs=('is_hr', 'sum')).reset_index()
    grp['whiff'] = grp['wh'] / grp['sw'].clip(lower=1)
    grp['hr_r'] = grp['hrs'] / grp['n']
    qual = grp[grp['n'] >= 30]
    best = qual.sort_values('whiff', ascending=False).groupby('pitcher').first().reset_index()
    best = best[['pitcher', 'pitch_bucket', 'whiff', 'hr_r']].rename(
        columns={'pitch_bucket': 'p_best_same_pitch', 'whiff': 'p_same_side_whiff', 'hr_r': 'p_same_side_hr'})
    best.to_parquet(DATA_DIR / "pitcher_same_side_weapon.parquet", index=False)
    print(f"  Saved {len(best)} same-side weapon profiles")


def build_bullpen(df):
    """Team bullpen vulnerability scores."""
    print("Building bullpen profiles...")
    # Identify relievers
    game_entries = df.groupby(['game_pk', 'pitcher']).agg(
        first_inning=('inning', 'min'), pitches=('pitch_type', 'count')).reset_index()
    pitcher_role = game_entries.groupby('pitcher').agg(
        median_entry=('first_inning', 'median'), appearances=('game_pk', 'count'),
        avg_pitches=('pitches', 'mean')).reset_index()
    pitcher_role['is_reliever'] = pitcher_role['median_entry'] > 1
    reliever_ids = set(pitcher_role[pitcher_role['is_reliever']]['pitcher'])

    # Reliever stats
    rp_data = df[df['pitcher'].isin(reliever_ids)]
    rp_prof = rp_data.groupby(['pitcher', 'player_name']).agg(
        total_pitches=('pitch_type', 'count'), hrs=('is_hr', 'sum'),
        avg_ev=('launch_speed', 'mean'), swings=('is_swing', 'sum'),
        whiffs=('is_whiff', 'sum')).reset_index()
    rp_prof['hr_per_pitch'] = rp_prof['hrs'] / rp_prof['total_pitches']
    rp_prof['whiff_rate'] = rp_prof['whiffs'] / rp_prof['swings'].clip(lower=1)
    rp_prof = rp_prof.merge(pitcher_role[pitcher_role['is_reliever']][['pitcher', 'appearances']], on='pitcher')

    # Team assignment
    if 'fielding_team' in df.columns:
        tc = 'fielding_team'
    else:
        rp_games = df[df['pitcher'].isin(reliever_ids)].copy()
        rp_games['team'] = np.where(rp_games['inning_topbot'] == 'Top', rp_games['home_team'], rp_games['away_team'])
        pt = rp_games.groupby('pitcher')['team'].agg(lambda x: x.value_counts().index[0]).reset_index()
        rp_prof = rp_prof.merge(pt, on='pitcher', how='left')

    rp_qual = rp_prof[rp_prof['total_pitches'] >= 50]

    team_bp = []
    for team, group in rp_qual.groupby('team'):
        ta = group['appearances'].sum()
        group = group.copy()
        group['w'] = group['appearances'] / ta
        whr = (group['hr_per_pitch'] * group['w']).sum()
        wev = (group['avg_ev'].fillna(85) * group['w']).sum()
        wwh = (group['whiff_rate'].fillna(0.2) * group['w']).sum()
        vuln = whr * 5000 + (wev - 80) * 2 + (1 - wwh) * 10
        team_bp.append({'team': team, 'n_relievers': len(group), 'weighted_hr_rate': whr,
            'weighted_avg_ev': wev, 'weighted_whiff_rate': wwh, 'bp_vulnerability': round(vuln, 2)})

    out = pd.DataFrame(team_bp)
    out.to_parquet(DATA_DIR / "team_bullpen_vulnerability_2025.parquet", index=False)
    rp_prof.to_parquet(DATA_DIR / "reliever_profiles_2025.parquet", index=False)
    print(f"  Saved {len(out)} team BP scores, {len(rp_prof)} reliever profiles")


def refresh_savant_leaderboards():
    """Pull latest Savant percentile ranks and expected stats."""
    print("Refreshing Savant leaderboards...")
    try:
        from pybaseball import statcast_batter_percentile_ranks, statcast_batter_expected_stats
        from pybaseball import statcast_pitcher_expected_stats

        pct = statcast_batter_percentile_ranks(2025)
        pct.to_parquet(DATA_DIR / "batter_percentiles_2025.parquet", index=False)
        print(f"  Batter percentiles: {len(pct)} (includes bat_speed, squared_up)")

        exp = statcast_batter_expected_stats(2025)
        exp.to_parquet(DATA_DIR / "batter_expected_2025.parquet", index=False)
        print(f"  Batter expected stats: {len(exp)} (real xSLG, xwOBA)")

        p_exp = statcast_pitcher_expected_stats(2025)
        p_exp.to_parquet(DATA_DIR / "pitcher_expected_2025_full.parquet", index=False)
        print(f"  Pitcher expected stats: {len(p_exp)}")
    except Exception as e:
        print(f"  Error refreshing leaderboards: {e}")
        print("  Existing files will be used.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=90, help='Days of Statcast data to pull')
    parser.add_argument('--skip-statcast', action='store_true', help='Skip Statcast pull, rebuild from existing data')
    args = parser.parse_args()

    print(f"=== V6 Data Refresh ===\n")

    # Step 1: Refresh Savant leaderboards (bat speed, xSLG, etc.)
    refresh_savant_leaderboards()

    # Step 2: Pull Statcast pitch-level data
    if not args.skip_statcast:
        df = pull_statcast(args.days)
        if df is None:
            print("Failed to pull Statcast data. Exiting.")
            return
    else:
        print("Skipping Statcast pull. Looking for cached data...")
        cached = DATA_DIR.parent / "pitches_cache.parquet"
        if cached.exists():
            df = pd.read_parquet(cached)
            print(f"  Loaded {len(df)} cached pitches")
        else:
            print("  No cached data found. Run without --skip-statcast.")
            return

    # Step 3: Build all profile files
    build_pitcher_pitch_profiles(df)
    build_rolling_30d(df)
    build_bvp(df)
    build_splits(df)
    build_same_side_weapon(df)
    build_bullpen(df)

    print(f"\n=== Refresh complete ===")
    print(f"All profile files updated in {DATA_DIR}")
    print(f"Now run: python scripts/run_pipeline.py")


if __name__ == "__main__":
    main()
