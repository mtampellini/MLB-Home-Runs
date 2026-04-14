#!/usr/bin/env python3
"""
run_pipeline.py - Fully automated daily HR picks pipeline
==========================================================
Runs in GitHub Actions. No local machine needed.

1. Pulls today's games + lineups from MLB API
2. Pulls HR prop odds from FanDuel/DraftKings via The Odds API
3. Runs the HR model on all batter-pitcher matchups
4. Grades yesterday's picks against actual results
5. Updates picks.json with everything
"""

import json, os, pickle, requests, sys
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

# Paths relative to repo root
REPO_ROOT = Path(__file__).parent.parent
MODEL_DIR = REPO_ROOT / "model" / "model_artifacts"
DATA_DIR = REPO_ROOT / "model" / "data"
PICKS_FILE = REPO_ROOT / "src" / "data" / "picks.json"

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
MIN_ROI = 10.0

TEAM_MAP = {"ARI": "AZ", "ATH": "OAK", "WSN": "WSH"}
INDOOR = {"TB", "TOR", "TEX", "HOU", "MIA", "AZ", "SEA", "MIL"}
PARK_HR = {
    "NYY": (1.25, 1.08, 55), "BOS": (0.92, 1.18, 20), "TB": (0.85, 0.90, 42),
    "BAL": (1.15, 1.02, 33), "TOR": (1.10, 1.00, 250), "CLE": (0.92, 0.98, 650),
    "CHW": (1.08, 0.98, 595), "DET": (1.05, 0.95, 600), "KC": (0.88, 0.95, 800),
    "MIN": (1.08, 0.98, 815), "HOU": (1.05, 0.95, 41), "LAA": (0.92, 0.98, 160),
    "OAK": (0.88, 0.92, 10), "SEA": (0.88, 0.95, 17), "TEX": (1.08, 1.02, 545),
    "ATL": (1.05, 0.98, 1050), "MIA": (0.82, 0.88, 10), "NYM": (0.98, 0.92, 17),
    "PHI": (1.12, 1.08, 20), "WSH": (1.00, 0.95, 25), "CHC": (1.05, 1.12, 600),
    "CIN": (1.22, 1.15, 490), "MIL": (1.05, 1.00, 635), "PIT": (0.88, 0.95, 730),
    "STL": (0.92, 0.98, 465), "AZ": (1.12, 1.05, 1080), "COL": (1.45, 1.35, 5200),
    "LAD": (0.95, 1.12, 515), "SD": (0.85, 0.95, 17), "SF": (0.78, 0.85, 7),
}


# ── MODEL ──
class HRModel:
    def __init__(self):
        self.model = pickle.load(open(MODEL_DIR / "hr_model_v4.pkl", "rb"))
        self.meta = json.load(open(MODEL_DIR / "hr_meta_v4.json"))
        self.medians = json.load(open(MODEL_DIR / "hr_medians_v4.json"))
        self.features = self.meta['features']

    def predict(self, df):
        X = df[self.features].copy()
        for c in X.columns:
            X[c] = pd.to_numeric(X[c], errors='coerce')
            X[c] = X[c].fillna(self.medians.get(c, 0))
        return self.model.predict_proba(X)[:, 1]


# ── ODDS ──
def fetch_odds(api_key):
    """Pull HR props from FanDuel and DraftKings only."""
    print("Fetching FD/DK odds...")
    r = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/events", params={"apiKey": api_key})
    if r.status_code != 200:
        print(f"  API error: {r.status_code}")
        return {}

    events = r.json()
    ALLOWED = {'fanduel', 'draftkings'}
    all_odds = {}

    for ev in events:
        eid = ev['id']
        away, home = ev.get('away_team', ''), ev.get('home_team', '')
        try:
            er = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/events/{eid}/odds",
                params={"apiKey": api_key, "regions": "us", "markets": "batter_home_runs", "oddsFormat": "american"},
                timeout=10)
            if er.status_code != 200:
                continue
            for book in er.json().get('bookmakers', []):
                if book['key'] not in ALLOWED:
                    continue
                for mkt in book.get('markets', []):
                    if mkt['key'] != 'batter_home_runs':
                        continue
                    for out in mkt['outcomes']:
                        if out.get('name') == 'Over' and out.get('point') == 0.5:
                            player, price = out['description'], out['price']
                            if player not in all_odds or price > all_odds[player]['odds']:
                                all_odds[player] = {'odds': price, 'book': book['title']}
        except:
            continue

    fd = sum(1 for v in all_odds.values() if 'FanDuel' in v['book'])
    dk = sum(1 for v in all_odds.values() if 'DraftKings' in v['book'])
    print(f"  FanDuel: {fd} | DraftKings: {dk}")
    return all_odds


# ── PROFILES ──
def load_profiles():
    bp_pct = pd.read_parquet(DATA_DIR / "batter_percentiles_2025.parquet")
    bp_exp = pd.read_parquet(DATA_DIR / "batter_expected_2025.parquet")
    bp = bp_pct.merge(bp_exp[['player_id', 'est_woba', 'est_slg', 'pa']], on='player_id', how='left', suffixes=('_pct', ''))
    batter_map = {}
    for _, r in bp.iterrows():
        batter_map[r['player_name']] = r.to_dict()
        batter_map[r['player_id']] = r.to_dict()

    p_hr = pd.read_parquet(DATA_DIR / "pitcher_hr_profiles.parquet")
    p_hr_map = {r['name']: r.to_dict() for _, r in p_hr.iterrows()}

    p_mix = pd.read_parquet(DATA_DIR / "pitcher_mix_hr.parquet")
    p_mix_map = {r['pitcher_id']: r.to_dict() for _, r in p_mix.iterrows()}

    p_exp = {}
    for path in [DATA_DIR / "pitcher_expected_2025_full.parquet", DATA_DIR / "statcast_pitchers_2025.parquet"]:
        if path.exists():
            for _, r in pd.read_parquet(path).iterrows():
                p_exp[r.get('last_name, first_name', '')] = {
                    'p_xwoba': r.get('est_woba'), 'p_xslg': r.get('est_slg'), 'p_xera': r.get('xera')}
            break

    return batter_map, p_hr_map, p_mix_map, p_exp


def match_pitcher(name, *lookups):
    parts = name.split()
    if len(parts) < 2:
        return {}
    rev = f"{parts[-1]}, {' '.join(parts[:-1])}"
    last = parts[-1].lower()
    result = {}
    for lk in lookups:
        if rev in lk:
            result.update(lk[rev] if isinstance(lk[rev], dict) else {})
        else:
            for k, v in lk.items():
                if isinstance(k, str) and last in k.lower():
                    result.update(v if isinstance(v, dict) else {})
                    break
    return result


# ── GENERATE PICKS ──
def generate_picks(date_str, api_key):
    model = HRModel()
    batter_map, p_hr_map, p_mix_map, p_exp_map = load_profiles()
    live_odds = fetch_odds(api_key) if api_key else {}

    r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
        params={"sportId": 1, "date": date_str, "hydrate": "probablePitcher,team,venue"})
    games = r.json().get('dates', [{}])[0].get('games', [])
    print(f"Games: {len(games)}")

    rows = []
    for g in games:
        gpk = g['gamePk']
        away, home = g['teams']['away'], g['teams']['home']
        at = TEAM_MAP.get(away['team'].get('abbreviation', ''), away['team'].get('abbreviation', ''))
        ht = TEAM_MAP.get(home['team'].get('abbreviation', ''), home['team'].get('abbreviation', ''))
        asp = away.get('probablePitcher', {}).get('fullName', 'TBD')
        hsp = home.get('probablePitcher', {}).get('fullName', 'TBD')
        park = PARK_HR.get(ht, (1.0, 1.0, 500))
        is_dome = ht in INDOOR

        lineups = {'away': [], 'home': []}
        try:
            lr = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gpk}/feed/live", timeout=5)
            live = lr.json()
            for side in ['away', 'home']:
                box = live.get('liveData', {}).get('boxscore', {}).get('teams', {}).get(side, {})
                for pid in box.get('battingOrder', []):
                    pinfo = box.get('players', {}).get(f"ID{pid}", {}).get('person', {})
                    lineups[side].append({'id': pid, 'name': pinfo.get('fullName', '')})
        except:
            pass

        if not lineups['away'] and not lineups['home']:
            for side, tdata in [('away', away), ('home', home)]:
                tid = tdata['team'].get('id')
                if not tid: continue
                try:
                    rr = requests.get(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster/active", timeout=5)
                    for p in rr.json().get('roster', []):
                        if p.get('position', {}).get('type', '') != 'Pitcher':
                            lineups[side].append({'id': p['person']['id'], 'name': p['person']['fullName']})
                except:
                    pass

        for side, pitcher_name in [('away', hsp), ('home', asp)]:
            if pitcher_name == 'TBD': continue
            p_data = match_pitcher(pitcher_name, p_hr_map, p_exp_map, p_mix_map)

            for i, b in enumerate(lineups.get(side, [])[:9]):
                bp = batter_map.get(b['id'])
                if not bp:
                    parts = b['name'].split()
                    if len(parts) >= 2:
                        bp = batter_map.get(f"{parts[-1]}, {' '.join(parts[:-1])}")
                if not bp: continue

                rows.append({
                    'batter_name': b['name'], 'game': f"{at} @ {ht}", 'vs_pitcher': pitcher_name, 'park': ht,
                    'b_xslg': bp.get('est_slg'), 'b_xwoba': bp.get('est_woba'),
                    'b_barrel_pct': bp.get('brl_percent'), 'b_max_ev': bp.get('max_ev'),
                    'b_exit_velo': bp.get('exit_velocity'), 'b_hard_hit': bp.get('hard_hit_percent'),
                    'b_bat_speed': bp.get('bat_speed'), 'b_squared_up': bp.get('squared_up_rate'),
                    'b_k_pct': bp.get('k_percent'), 'b_bb_pct': bp.get('bb_percent'),
                    'b_chase_pct': bp.get('chase_percent'), 'b_whiff_pct': bp.get('whiff_percent'),
                    'b_pa': bp.get('pa'),
                    'p_xwoba': p_data.get('p_xwoba') or p_data.get('est_woba'),
                    'p_xslg': p_data.get('p_xslg') or p_data.get('est_slg'),
                    'p_xera': p_data.get('p_xera') or p_data.get('xera'),
                    'p_hr_per_pa': p_data.get('p_hr_per_pa'), 'p_hr_fb_rate': p_data.get('p_hr_fb_rate'),
                    'p_fb_rate': p_data.get('p_fb_rate'), 'p_gb_rate': p_data.get('p_gb_rate'),
                    'p_avg_ev_allowed': p_data.get('p_avg_ev_allowed'),
                    'p_barrel_pct_allowed': p_data.get('p_barrel_pct_allowed'),
                    'p_hard_hit_allowed': p_data.get('p_hard_hit_allowed'),
                    'p_fb_velo': p_data.get('p_fb_velo'),
                    'p_fastball_pct': p_data.get('p_fastball_pct'), 'p_sinker_pct': p_data.get('p_sinker_pct'),
                    'p_breaking_pct': p_data.get('p_breaking_pct'), 'p_offspeed_pct': p_data.get('p_offspeed_pct'),
                    'p_hr_danger_score': p_data.get('p_hr_danger_score'),
                    'park_hr_factor': park[0], 'park_altitude': park[2],
                    'wx_temp': 72 if is_dome else 75, 'wx_wind': 0 if is_dome else 8,
                    'wx_humidity': 50 if is_dome else 55, 'wx_dome': 1 if is_dome else 0,
                    'platoon': 0, 'n_pa': 4, 'max_tto': 3 if i < 4 else 2, 'lineup_pos': i + 1,
                })

    if not rows:
        print("No matchups to score")
        return []

    df = pd.DataFrame(rows)
    df['model_prob'] = model.predict(df)
    df = df.sort_values('model_prob', ascending=False).drop_duplicates('batter_name', keep='first')

    # Match odds (FD/DK only)
    df['book_odds'] = None
    df['book_name'] = None
    for idx, row in df.iterrows():
        name = row['batter_name']
        if name in live_odds:
            df.at[idx, 'book_odds'] = live_odds[name]['odds']
            df.at[idx, 'book_name'] = live_odds[name]['book']
        else:
            last = name.split()[-1].lower() if ' ' in name else name.lower()
            for oname, odata in live_odds.items():
                clean = oname.split('(')[0].strip()
                if last == clean.split()[-1].lower():
                    df.at[idx, 'book_odds'] = odata['odds']
                    df.at[idx, 'book_name'] = odata['book']
                    break

    # Calculate ROI, filter to FD/DK only
    df['implied_prob'] = df['book_odds'].apply(lambda x: 100/(x+100) if pd.notna(x) and x > 0 else None)
    df['edge'] = df['model_prob'] - df['implied_prob']
    df['projected_roi'] = df.apply(
        lambda r: (r['model_prob'] * (r['book_odds']/100) - (1-r['model_prob'])) * 100
        if pd.notna(r['book_odds']) else None, axis=1)

    qualified = df[df['projected_roi'].notna() & (df['projected_roi'] >= MIN_ROI)].sort_values('projected_roi', ascending=False)

    picks = []
    for _, r in qualified.iterrows():
        picks.append({
            "batter": r['batter_name'], "game": r['game'], "vs_pitcher": r['vs_pitcher'], "park": r['park'],
            "model_prob": round(float(r['model_prob']), 3),
            "book_odds": int(r['book_odds']), "book": r['book_name'],
            "implied_prob": round(float(r['implied_prob']), 3),
            "edge": round(float(r['edge']), 3),
            "projected_roi": round(float(r['projected_roi']), 1),
            "result": None, "hit_hr": None, "pnl": None,
        })

    print(f"Qualified picks (FD/DK, ROI >= {MIN_ROI}%): {len(picks)}")
    for p in picks:
        print(f"  {p['batter']:<22} +{p['book_odds']:<6} {p['book']:<12} ROI: +{p['projected_roi']:.1f}%")

    return picks


# ── GRADE RESULTS ──
def grade_yesterday(db):
    """Grade the most recent unsettled day."""
    for date_str in sorted(db['dates'].keys(), reverse=True):
        day = db['dates'][date_str]
        if day.get('settled') or not day.get('picks'):
            continue

        # Check if games are final
        r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": date_str, "hydrate": "team"})
        games = r.json().get('dates', [{}])[0].get('games', [])
        all_final = all('Final' in g.get('status', {}).get('detailedState', '') or
                        'Completed' in g.get('status', {}).get('detailedState', '')
                        for g in games)
        if not all_final and games:
            print(f"  {date_str}: games still in progress, skipping grade")
            continue

        # Get HR hitters
        hr_hitters = set()
        for g in games:
            try:
                br = requests.get(f"https://statsapi.mlb.com/api/v1/game/{g['gamePk']}/boxscore", timeout=5)
                for side in ['away', 'home']:
                    for pid, pdata in br.json().get('teams', {}).get(side, {}).get('players', {}).items():
                        if pdata.get('stats', {}).get('batting', {}).get('homeRuns', 0) > 0:
                            hr_hitters.add(pdata.get('person', {}).get('fullName', ''))
            except:
                continue

        print(f"  Grading {date_str}: {len(hr_hitters)} HR hitters found")

        wins, day_pnl = 0, 0.0
        for pick in day['picks']:
            hit = any(pick['batter'].lower() in h.lower() or h.lower() in pick['batter'].lower() for h in hr_hitters)
            payout = pick['book_odds'] / 100
            pnl = payout if hit else -1.0
            pick['hit_hr'] = hit
            pick['result'] = 'HIT' if hit else 'MISS'
            pick['pnl'] = round(pnl, 2)
            day_pnl += pnl
            if hit: wins += 1
            print(f"    {'HIT' if hit else 'MISS'} {pick['batter']:<22} +{pick['book_odds']:<6} P&L: {pnl:+.1f}u")

        day['settled'] = True
        day['summary'] = {'wins': wins, 'losses': len(day['picks']) - wins, 'pnl': round(day_pnl, 2)}
        print(f"  {date_str}: {wins}W-{len(day['picks'])-wins}L, P&L: {day_pnl:+.1f}u")

    # Recalculate cumulative
    total_bets, total_wins, total_pnl, winning_days, losing_days = 0, 0, 0.0, 0, 0
    for d, ddata in db['dates'].items():
        if not ddata.get('settled'): continue
        s = ddata.get('summary', {})
        total_bets += s.get('wins', 0) + s.get('losses', 0)
        total_wins += s.get('wins', 0)
        total_pnl += s.get('pnl', 0)
        if s.get('pnl', 0) > 0: winning_days += 1
        elif s.get('pnl', 0) < 0: losing_days += 1

    db['cumulative'] = {
        'total_bets': total_bets, 'total_wins': total_wins,
        'total_pnl': round(total_pnl, 2), 'total_wagered': total_bets,
        'roi': round(total_pnl / total_bets * 100, 1) if total_bets > 0 else 0,
        'winning_days': winning_days, 'losing_days': losing_days,
    }


# ── MAIN ──
def main():
    api_key = os.environ.get('ODDS_API_KEY')
    today = datetime.now().strftime('%Y-%m-%d')

    print(f"=== HR Picks Pipeline: {today} ===\n")

    # Load existing data
    if PICKS_FILE.exists():
        db = json.load(open(PICKS_FILE))
    else:
        db = {
            "dates": {},
            "cumulative": {"total_bets": 0, "total_wins": 0, "total_pnl": 0.0,
                           "total_wagered": 0, "roi": 0, "winning_days": 0, "losing_days": 0},
            "config": {"min_roi_threshold": MIN_ROI, "model_version": "hr_v4_mix_tto_lineup",
                       "backtest_roi": 48.4, "backtest_months_profitable": "5/5", "unit_size": 1.0},
        }

    # Grade any unsettled days
    print("Grading unsettled days...")
    grade_yesterday(db)

    # Generate today's picks
    print(f"\nGenerating picks for {today}...")
    picks = generate_picks(today, api_key)

    if picks:
        db['dates'][today] = {"picks": picks, "settled": False, "summary": None}
    elif today not in db['dates']:
        # No FD/DK odds yet, store empty
        db['dates'][today] = {"picks": [], "settled": False, "summary": None}

    # Ensure config exists
    if 'config' not in db:
        db['config'] = {"min_roi_threshold": MIN_ROI, "model_version": "hr_v4_mix_tto_lineup",
                        "backtest_roi": 48.4, "backtest_months_profitable": "5/5", "unit_size": 1.0}

    # Save
    PICKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(db, open(PICKS_FILE, 'w'), indent=2, default=str)
    print(f"\nSaved to {PICKS_FILE}")
    print(f"Cumulative: {db['cumulative']['total_bets']} bets, {db['cumulative']['total_pnl']:+.1f}u, {db['cumulative']['roi']:+.1f}% ROI")


if __name__ == "__main__":
    main()
