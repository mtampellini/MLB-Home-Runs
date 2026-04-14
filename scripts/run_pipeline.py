#!/usr/bin/env python3
"""
run_pipeline.py - V6 HR Picks Pipeline
=======================================
Pulls games, lineups, odds, weather. Runs V6 model. Publishes picks.
Caches FD/DK odds for future backtesting.

Usage:
    python scripts/run_pipeline.py                  # Auto (ODDS_API_KEY env var)
    python scripts/run_pipeline.py --api-key KEY    # Explicit key
    python scripts/run_pipeline.py --date 2026-04-15
"""

import json, os, pickle, requests, sys, argparse
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).parent.parent
MODEL_DIR = REPO_ROOT / "model" / "model_artifacts"
DATA_DIR = REPO_ROOT / "model" / "data"
PICKS_FILE = REPO_ROOT / "src" / "data" / "picks.json"
ODDS_HISTORY = REPO_ROOT / "odds_history"
ODDS_HISTORY.mkdir(exist_ok=True)

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
MIN_ROI = 25.0
MODEL_VERSION = "v6"

TEAM_MAP = {"ARI": "AZ", "ATH": "OAK", "WSN": "WSH"}
INDOOR = {"TB", "TOR", "TEX", "HOU", "MIA", "AZ", "SEA", "MIL", "MIN"}
PARK_HR = {
    "NYY": (1.25, 55), "BOS": (0.92, 20), "TB": (0.85, 42), "BAL": (1.15, 33),
    "TOR": (1.10, 250), "CLE": (0.92, 650), "CHW": (1.08, 595), "DET": (1.05, 600),
    "KC": (0.88, 800), "MIN": (1.08, 815), "HOU": (1.05, 41), "LAA": (0.92, 160),
    "OAK": (0.88, 10), "SEA": (0.88, 17), "TEX": (1.08, 545), "ATL": (1.05, 1050),
    "MIA": (0.82, 10), "NYM": (0.98, 17), "PHI": (1.12, 20), "WSH": (1.00, 25),
    "CHC": (1.05, 600), "CIN": (1.22, 490), "MIL": (1.05, 635), "PIT": (0.88, 730),
    "STL": (0.92, 465), "AZ": (1.12, 1080), "COL": (1.45, 5200), "LAD": (0.95, 515),
    "SD": (0.85, 17), "SF": (0.78, 7),
}
PA_EST = {1: 4.5, 2: 4.4, 3: 4.3, 4: 4.2, 5: 4.1, 6: 3.9, 7: 3.8, 8: 3.6, 9: 3.5}
TTO_EST = {1: 3.0, 2: 2.9, 3: 2.9, 4: 2.8, 5: 2.7, 6: 2.6, 7: 2.5, 8: 2.4, 9: 2.3}
PITCH_BUCKETS = ['FF', 'SI', 'FC', 'SL', 'ST', 'CU', 'CH', 'FS']


class HRModel:
    def __init__(self):
        v = MODEL_VERSION
        self.model = pickle.load(open(MODEL_DIR / f"hr_model_{v}.pkl", "rb"))
        self.meta = json.load(open(MODEL_DIR / f"hr_meta_{v}.json"))
        self.medians = json.load(open(MODEL_DIR / f"hr_medians_{v}.json"))
        self.features = self.meta['features']

    def predict(self, df):
        X = df[self.features].copy()
        for c in X.columns:
            X[c] = pd.to_numeric(X[c], errors='coerce')
            X[c] = X[c].fillna(self.medians.get(c, 0))
        return self.model.predict_proba(X)[:, 1]


def fetch_odds(api_key):
    print("Fetching FD/DK odds...")
    r = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/events", params={"apiKey": api_key})
    if r.status_code != 200:
        print(f"  API error: {r.status_code}")
        return {}
    events = r.json()
    all_odds = {}
    remaining = '?'
    for ev in events:
        eid = ev['id']
        away, home = ev.get('away_team', ''), ev.get('home_team', '')
        try:
            er = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/events/{eid}/odds",
                params={"apiKey": api_key, "regions": "us",
                        "markets": "batter_home_runs,batter_home_runs_alternate",
                        "oddsFormat": "american", "bookmakers": "fanduel,draftkings"},
                timeout=10)
            remaining = er.headers.get('x-requests-remaining', '?')
            if er.status_code != 200: continue
            for book in er.json().get('bookmakers', []):
                for mkt in book.get('markets', []):
                    if mkt['key'] not in ('batter_home_runs', 'batter_home_runs_alternate'): continue
                    for out in mkt['outcomes']:
                        if out.get('name') == 'Over' and out.get('point') == 0.5:
                            player, price = out['description'], out['price']
                            if player not in all_odds or price > all_odds[player]['odds']:
                                all_odds[player] = {'odds': price, 'book': book['title'],
                                    'game': f"{away} vs {home}", 'fd_odds': None, 'dk_odds': None}
                            if book['key'] == 'fanduel': all_odds[player]['fd_odds'] = price
                            elif book['key'] == 'draftkings': all_odds[player]['dk_odds'] = price
        except: continue
    fd = sum(1 for v in all_odds.values() if v['book'] == 'FanDuel')
    dk = sum(1 for v in all_odds.values() if v['book'] == 'DraftKings')
    print(f"  FanDuel: {fd} | DraftKings: {dk} | API remaining: {remaining}")
    return all_odds


def cache_odds(all_odds, date_str):
    cache_file = ODDS_HISTORY / f"{date_str}.json"
    cache_data = {'date': date_str, 'timestamp': datetime.now().isoformat(),
        'n_players': len(all_odds),
        'odds': {name: {'best_odds': d['odds'], 'best_book': d['book'],
            'fd_odds': d.get('fd_odds'), 'dk_odds': d.get('dk_odds'),
            'game': d.get('game', '')} for name, d in all_odds.items()}}
    json.dump(cache_data, open(cache_file, 'w'), indent=2)
    print(f"  Cached {len(all_odds)} odds to odds_history/{date_str}.json")


def load_profiles():
    bp_pct = pd.read_parquet(DATA_DIR / "batter_percentiles_2025.parquet")
    bp_exp = pd.read_parquet(DATA_DIR / "batter_expected_2025.parquet")
    bp = bp_pct.merge(bp_exp[['player_id', 'est_woba', 'est_slg', 'pa']],
                      on='player_id', how='left', suffixes=('_pct', ''))
    batter_map = {}
    for _, r in bp.iterrows():
        batter_map[r['player_name']] = r.to_dict()
        batter_map[r['player_id']] = r.to_dict()

    p_hr_map = {}
    if (DATA_DIR / "pitcher_hr_profiles.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "pitcher_hr_profiles.parquet").iterrows():
            p_hr_map[r['name']] = r.to_dict()

    p_exp = {}
    for path in [DATA_DIR / "pitcher_expected_2025_full.parquet", DATA_DIR / "statcast_pitchers_2025.parquet"]:
        if path.exists():
            for _, r in pd.read_parquet(path).iterrows():
                p_exp[r.get('last_name, first_name', '')] = {
                    'p_xwoba': r.get('est_woba'), 'p_xslg': r.get('est_slg'), 'p_xera': r.get('xera')}
            break

    p_mix_map = {}
    if (DATA_DIR / "pitcher_mix_hr.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "pitcher_mix_hr.parquet").iterrows():
            p_mix_map[r['pitcher_id']] = r.to_dict()

    pitch_prof = {}
    if (DATA_DIR / "pitcher_pitch_profiles_2025.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "pitcher_pitch_profiles_2025.parquet").iterrows():
            pitch_prof[int(r['pitcher_id'])] = r.to_dict()
            pitch_prof[r.get('pitcher_name', '')] = r.to_dict()

    bp_vuln = {}
    if (DATA_DIR / "team_bullpen_vulnerability_2025.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "team_bullpen_vulnerability_2025.parquet").iterrows():
            bp_vuln[r['team']] = r.to_dict()

    batter_bvp = {}
    if (DATA_DIR / "batter_vs_pitch_type.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "batter_vs_pitch_type.parquet").iterrows():
            batter_bvp[int(r['batter'])] = r.to_dict()

    splits = {}
    if (DATA_DIR / "batter_splits.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "batter_splits.parquet").iterrows():
            splits[(r['batter'], r['stand'], r['p_throws'])] = r.to_dict()

    pitcher_rolling = {}
    if (DATA_DIR / "pitcher_rolling_30d.parquet").exists():
        roll_df = pd.read_parquet(DATA_DIR / "pitcher_rolling_30d.parquet")
        roll_df['game_date'] = pd.to_datetime(roll_df['game_date'])
        latest = roll_df.sort_values('game_date').groupby('pitcher').last()
        for pid, r in latest.iterrows():
            pitcher_rolling[int(pid)] = r.to_dict()

    same_side = {}
    if (DATA_DIR / "pitcher_same_side_weapon.parquet").exists():
        for _, r in pd.read_parquet(DATA_DIR / "pitcher_same_side_weapon.parquet").iterrows():
            same_side[int(r['pitcher'])] = r.to_dict()

    return {'batter': batter_map, 'p_hr': p_hr_map, 'p_exp': p_exp, 'p_mix_old': p_mix_map,
            'pitch_prof': pitch_prof, 'bp_vuln': bp_vuln, 'bvp': batter_bvp,
            'splits': splits, 'rolling': pitcher_rolling, 'same_side': same_side}


def match_pitcher(name, *lookups):
    parts = name.split()
    if len(parts) < 2: return {}
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


def fetch_weather(game_pk):
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live", timeout=5)
        if r.status_code != 200: return {}
        w = r.json().get('gameData', {}).get('weather', {})
        v = r.json().get('gameData', {}).get('venue', {})
        try: temp = float(w.get('temp', ''))
        except: temp = None
        wind_str = w.get('wind', '')
        try: wind_speed = float(wind_str.split(' ')[0])
        except: wind_speed = None
        wd = wind_str.lower()
        wind_boost = 1.0 if 'out' in wd else (-1.0 if 'in' in wd else 0.0)
        try: humidity = float(w.get('humidity', ''))
        except: humidity = None
        roof = v.get('fieldInfo', {}).get('roofType', '')
        is_dome = 1.0 if roof in ['Dome', 'Retractable'] else 0.0
        return {'wx_temp_real': temp, 'wx_wind_speed': wind_speed, 'wx_wind_hr_boost': wind_boost,
                'wx_humidity_real': humidity, 'wx_dome_real': is_dome}
    except: return {}


def generate_picks(date_str, api_key):
    model = HRModel()
    prof = load_profiles()
    batter_map = prof['batter']
    live_odds = fetch_odds(api_key) if api_key else {}
    if live_odds: cache_odds(live_odds, date_str)

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
        park = PARK_HR.get(ht, (1.0, 500))
        is_dome = ht in INDOOR
        wx = fetch_weather(gpk) or {
            'wx_temp_real': 72 if is_dome else 75, 'wx_wind_speed': 0 if is_dome else 8,
            'wx_wind_hr_boost': 0, 'wx_humidity_real': 50 if is_dome else 55,
            'wx_dome_real': 1 if is_dome else 0}

        lineups = {'away': [], 'home': []}
        try:
            lr = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gpk}/feed/live", timeout=5)
            for side in ['away', 'home']:
                box = lr.json().get('liveData', {}).get('boxscore', {}).get('teams', {}).get(side, {})
                for pid in box.get('battingOrder', []):
                    pinfo = box.get('players', {}).get(f"ID{pid}", {}).get('person', {})
                    lineups[side].append({'id': pid, 'name': pinfo.get('fullName', '')})
        except: pass

        if not lineups['away'] and not lineups['home']:
            for side, tdata in [('away', away), ('home', home)]:
                tid = tdata['team'].get('id')
                if not tid: continue
                try:
                    rr = requests.get(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster/active", timeout=5)
                    for p in rr.json().get('roster', []):
                        if p.get('position', {}).get('type', '') != 'Pitcher':
                            lineups[side].append({'id': p['person']['id'], 'name': p['person']['fullName']})
                except: pass

        for side, pitcher_name, opp_team in [('away', hsp, ht), ('home', asp, at)]:
            if pitcher_name == 'TBD': continue
            p_data = match_pitcher(pitcher_name, prof['p_hr'], prof['p_exp'], prof['p_mix_old'])
            p_pitch = match_pitcher(pitcher_name, prof['pitch_prof'])
            p_roll = {}; p_ss = {}
            for pid_key, pd_val in prof['rolling'].items():
                pp_name = prof['pitch_prof'].get(pid_key, {}).get('pitcher_name', '')
                if pp_name and pitcher_name.split()[-1].lower() in pp_name.lower():
                    p_roll = pd_val; p_ss = prof['same_side'].get(pid_key, {}); break
            bp_data = prof['bp_vuln'].get(opp_team, {})

            for i, b in enumerate(lineups.get(side, [])[:9]):
                bp = batter_map.get(b['id'])
                if not bp:
                    parts = b['name'].split()
                    if len(parts) >= 2: bp = batter_map.get(f"{parts[-1]}, {' '.join(parts[:-1])}")
                if not bp: continue
                lpos = i + 1; bid = b['id']
                bvp_d = prof['bvp'].get(bid, {})

                rows.append({
                    'batter_name': b['name'], 'game': f"{at} @ {ht}",
                    'vs_pitcher': pitcher_name, 'park': ht,
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
                    'park_hr_factor': park[0], 'park_altitude': park[1],
                    'wx_temp_real': wx.get('wx_temp_real'), 'wx_wind_speed': wx.get('wx_wind_speed'),
                    'wx_wind_hr_boost': wx.get('wx_wind_hr_boost', 0),
                    'wx_humidity_real': wx.get('wx_humidity_real'), 'wx_dome_real': wx.get('wx_dome_real'),
                    'platoon': 0, 'n_pa': PA_EST.get(lpos, 3.8),
                    'p_fastball_pct': p_data.get('p_fastball_pct'), 'p_sinker_pct': p_data.get('p_sinker_pct'),
                    'p_breaking_pct': p_data.get('p_breaking_pct'), 'p_offspeed_pct': p_data.get('p_offspeed_pct'),
                    'p_hr_danger_score': p_data.get('p_hr_danger_score'),
                    'max_tto': TTO_EST.get(lpos, 2.5), 'lineup_pos': lpos,
                    'r30_whiff_rate': p_roll.get('r30_whiff_rate'), 'r30_hr_per_pitch': p_roll.get('r30_hr_per_pitch'),
                    'r30_avg_ev': p_roll.get('r30_avg_ev'), 'r30_barrel_rate': p_roll.get('r30_barrel_rate'),
                    'r30_hard_hit_rate': p_roll.get('r30_hard_hit_rate'), 'r30_avg_velo': p_roll.get('r30_avg_velo'),
                    'bvp_hr_FF': bvp_d.get('bvp_hr_rate_FF'), 'bvp_hr_SL': bvp_d.get('bvp_hr_rate_SL'),
                    'bvp_hr_CH': bvp_d.get('bvp_hr_rate_CH'), 'bvp_hr_SI': bvp_d.get('bvp_hr_rate_SI'),
                    'bvp_hr_FC': bvp_d.get('bvp_hr_rate_FC'),
                    'bvp_ev_FF': bvp_d.get('bvp_avg_ev_FF'), 'bvp_ev_SL': bvp_d.get('bvp_avg_ev_SL'),
                    'bvp_ev_CH': bvp_d.get('bvp_avg_ev_CH'),
                    'split_hr_rate': None, 'p_same_side_whiff': p_ss.get('p_same_side_whiff'),
                    'p_same_side_hr': p_ss.get('p_same_side_hr'),
                    **{f'{bk}_pct': p_pitch.get(f'{bk}_pct') for bk in PITCH_BUCKETS},
                    **{f'{bk}_hr_rate': p_pitch.get(f'{bk}_hr_rate') for bk in PITCH_BUCKETS},
                    'bp_vuln_adj': bp_data.get('bp_vulnerability'), 'bp_weighted_hr_rate': bp_data.get('weighted_hr_rate'),
                })

    if not rows:
        print("No matchups to score"); return []

    df = pd.DataFrame(rows)
    print(f"  Scoring {len(df)} matchups with V6...")
    df['model_prob'] = model.predict(df)
    df = df.sort_values('model_prob', ascending=False).drop_duplicates('batter_name', keep='first')

    df['book_odds'] = None; df['book_name'] = None
    for idx, row in df.iterrows():
        name = row['batter_name']
        if name in live_odds:
            df.at[idx, 'book_odds'] = live_odds[name]['odds']
            df.at[idx, 'book_name'] = live_odds[name]['book']
        else:
            last = name.split()[-1].lower() if ' ' in name else name.lower()
            for oname, odata in live_odds.items():
                if last == oname.split()[-1].split('(')[0].strip().split()[-1].lower():
                    df.at[idx, 'book_odds'] = odata['odds']
                    df.at[idx, 'book_name'] = odata['book']; break

    df['implied_prob'] = df['book_odds'].apply(lambda x: 100 / (x + 100) if pd.notna(x) and x > 0 else None)
    df['projected_roi'] = df.apply(
        lambda r: (r['model_prob'] * (r['book_odds'] / 100) - (1 - r['model_prob'])) * 100
        if pd.notna(r['book_odds']) else None, axis=1)

    qualified = df[df['projected_roi'].notna() & (df['projected_roi'] >= MIN_ROI)].sort_values('projected_roi', ascending=False)

    picks = []
    for _, r in qualified.iterrows():
        picks.append({"batter": r['batter_name'], "game": r['game'], "vs_pitcher": r['vs_pitcher'], "park": r['park'],
            "model_prob": round(float(r['model_prob']), 3), "book_odds": int(r['book_odds']), "book": r['book_name'],
            "implied_prob": round(float(r['implied_prob']), 3), "edge": round(float(r['model_prob'] - r['implied_prob']), 3),
            "projected_roi": round(float(r['projected_roi']), 1), "result": None, "hit_hr": None, "pnl": None})

    print(f"Qualified picks (ROI >= {MIN_ROI}%): {len(picks)}")
    for p in picks[:15]:
        print(f"  {p['batter']:<22} +{p['book_odds']:<6} {p['book']:<12} Model: {p['model_prob']*100:.1f}% ROI: +{p['projected_roi']:.1f}%")
    if len(picks) > 15: print(f"  ... and {len(picks) - 15} more")
    return picks


def grade_yesterday(db):
    for date_str in sorted(db['dates'].keys(), reverse=True):
        day = db['dates'][date_str]
        if day.get('settled') or not day.get('picks'): continue
        r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": date_str, "hydrate": "team"})
        games = r.json().get('dates', [{}])[0].get('games', [])
        all_final = all('Final' in g.get('status', {}).get('detailedState', '') or
                        'Completed' in g.get('status', {}).get('detailedState', '') for g in games)
        if not all_final and games: print(f"  {date_str}: games in progress"); continue

        hr_hitters = set()
        for g in games:
            try:
                br = requests.get(f"https://statsapi.mlb.com/api/v1/game/{g['gamePk']}/boxscore", timeout=5)
                for side in ['away', 'home']:
                    for pid, pdata in br.json().get('teams', {}).get(side, {}).get('players', {}).items():
                        if pdata.get('stats', {}).get('batting', {}).get('homeRuns', 0) > 0:
                            hr_hitters.add(pdata.get('person', {}).get('fullName', ''))
            except: continue

        print(f"  Grading {date_str}: {len(hr_hitters)} HR hitters")
        wins, day_pnl = 0, 0.0
        for pick in day['picks']:
            hit = any(pick['batter'].lower() in h.lower() or h.lower() in pick['batter'].lower() for h in hr_hitters)
            pnl = pick['book_odds'] / 100 if hit else -1.0
            pick['hit_hr'] = hit; pick['result'] = 'HIT' if hit else 'MISS'; pick['pnl'] = round(pnl, 2)
            day_pnl += pnl
            if hit: wins += 1
        day['settled'] = True
        day['summary'] = {'wins': wins, 'losses': len(day['picks']) - wins, 'pnl': round(day_pnl, 2)}
        print(f"  {date_str}: {wins}W-{len(day['picks']) - wins}L, P&L: {day_pnl:+.1f}u")

    total_bets = total_wins = winning_days = losing_days = 0; total_pnl = 0.0
    for d, ddata in db['dates'].items():
        if not ddata.get('settled'): continue
        s = ddata.get('summary', {})
        total_bets += s.get('wins', 0) + s.get('losses', 0)
        total_wins += s.get('wins', 0); total_pnl += s.get('pnl', 0)
        if s.get('pnl', 0) > 0: winning_days += 1
        elif s.get('pnl', 0) < 0: losing_days += 1
    db['cumulative'] = {'total_bets': total_bets, 'total_wins': total_wins,
        'total_pnl': round(total_pnl, 2), 'total_wagered': total_bets,
        'roi': round(total_pnl / total_bets * 100, 1) if total_bets > 0 else 0,
        'winning_days': winning_days, 'losing_days': losing_days}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', default=os.environ.get('ODDS_API_KEY'))
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    print(f"=== HR Picks Pipeline V6: {args.date} ===\n")

    if PICKS_FILE.exists():
        db = json.load(open(PICKS_FILE))
    else:
        db = {"dates": {},
            "cumulative": {"total_bets": 0, "total_wins": 0, "total_pnl": 0.0,
                           "total_wagered": 0, "roi": 0, "winning_days": 0, "losing_days": 0},
            "config": {"min_roi_threshold": MIN_ROI, "model_version": "hr_v6_rolling_bvp_weather_splits_bp",
                       "backtest_months_profitable": "5/5", "unit_size": 1.0}}

    print("Grading unsettled days...")
    grade_yesterday(db)

    print(f"\nGenerating V6 picks for {args.date}...")
    picks = generate_picks(args.date, args.api_key)

    if picks: db['dates'][args.date] = {"picks": picks, "settled": False, "summary": None}
    elif args.date not in db['dates']:
        db['dates'][args.date] = {"picks": [], "settled": False, "summary": None}

    db['config'] = {"min_roi_threshold": MIN_ROI, "model_version": "hr_v6_rolling_bvp_weather_splits_bp",
                    "backtest_months_profitable": "5/5", "unit_size": 1.0}

    PICKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(db, open(PICKS_FILE, 'w'), indent=2, default=str)
    print(f"\nSaved to {PICKS_FILE}")
    print(f"Cumulative: {db['cumulative']['total_bets']} bets, {db['cumulative']['total_pnl']:+.1f}u, {db['cumulative']['roi']:+.1f}% ROI")


if __name__ == "__main__":
    main()
