"""
HR Prop Daily Pipeline
======================
Pulls live sportsbook odds, runs the HR model, calculates
projected ROI for every batter, and outputs a bet sheet.

Usage:
    python hr_daily.py                          # Today, no odds (model only)
    python hr_daily.py --api-key YOUR_KEY       # Today with live odds
    python hr_daily.py --date 2026-04-14        # Specific date
    python hr_daily.py --min-roi 10             # Only show 10%+ ROI picks
    python hr_daily.py --top 20                 # Show top 20 picks

Setup:
    1. Sign up at https://the-odds-api.com (free tier = 500 req/month)
    2. Copy your API key
    3. Either pass --api-key or set ODDS_API_KEY env variable
"""

import sys, os, json, pickle, warnings, requests
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

BASE = Path(__file__).parent
MODEL_DIR = BASE / "model_artifacts"
DATA_DIR = BASE / "data"
OUTPUT_DIR = BASE / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ══════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════
DEFAULT_TOP_N = 25
DEFAULT_MIN_ROI = 5.0   # Minimum projected ROI to flag as BET
MIN_MODEL_PROB = 0.12   # Don't even score batters below this
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

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


# ══════════════════════════════════════════════════════════
# ODDS API
# ══════════════════════════════════════════════════════════
def fetch_hr_odds(api_key):
    """Pull HR prop odds from The Odds API for all today's MLB games."""
    print("  Fetching live HR prop odds...")

    # Step 1: Get today's event IDs
    r = requests.get(f"{ODDS_API_BASE}/sports/baseball_mlb/events",
                     params={"apiKey": api_key}, timeout=10)
    if r.status_code != 200:
        print(f"    Events API error: {r.status_code} - {r.text[:200]}")
        return {}

    events = r.json()
    print(f"    Found {len(events)} MLB events")

    # Step 2: For each event, pull batter_home_runs market
    # ONLY FanDuel and DraftKings. Everything else is ignored.
    ALLOWED = {'fanduel', 'draftkings'}
    all_odds = {}

    for event in events:
        eid = event['id']
        away = event.get('away_team', '')
        home = event.get('home_team', '')

        try:
            er = requests.get(
                f"{ODDS_API_BASE}/sports/baseball_mlb/events/{eid}/odds",
                params={
                    "apiKey": api_key,
                    "regions": "us",
                    "markets": "batter_home_runs",
                    "oddsFormat": "american",
                },
                timeout=10,
            )
            if er.status_code != 200:
                continue

            data = er.json()
            for book in data.get('bookmakers', []):
                book_key = book['key']
                book_name = book['title']

                # Skip anything that isn't FanDuel or DraftKings
                if book_key not in ALLOWED:
                    continue

                for market in book.get('markets', []):
                    if market['key'] != 'batter_home_runs':
                        continue
                    for outcome in market.get('outcomes', []):
                        if outcome.get('name') == 'Over' and outcome.get('point', 0) == 0.5:
                            player = outcome['description']
                            price = outcome['price']
                            existing = all_odds.get(player)

                            # Keep best price between FD and DK
                            if existing is None or price > existing['odds']:
                                all_odds[player] = {
                                    'odds': price, 'book': book_name,
                                    'book_key': book_key, 'game': f"{away} vs {home}",
                                }
        except Exception:
            continue

    remaining = er.headers.get('x-requests-remaining', '?') if 'er' in dir() else '?'
    fd = sum(1 for v in all_odds.values() if v['book_key'] == 'fanduel')
    dk = sum(1 for v in all_odds.values() if v['book_key'] == 'draftkings')
    print(f"    FanDuel: {fd} players | DraftKings: {dk} players | API remaining: {remaining}")
    if len(all_odds) == 0:
        print("    No FD/DK props posted yet. Run again closer to game time (~4-5pm ET).")
    return all_odds


# ══════════════════════════════════════════════════════════
# MODEL + DATA
# ══════════════════════════════════════════════════════════
class HRModel:
    def __init__(self):
        self.model = pickle.load(open(MODEL_DIR / "hr_model_v4.pkl", "rb"))
        self.meta = json.load(open(MODEL_DIR / "hr_meta_v4.json"))
        self.medians = json.load(open(MODEL_DIR / "hr_medians_v4.json"))
        self.features = self.meta['features']

    def predict(self, rows_df):
        X = rows_df[self.features].copy()
        for c in X.columns:
            X[c] = pd.to_numeric(X[c], errors='coerce')
        for c in X.columns:
            X[c] = X[c].fillna(self.medians.get(c, 0))
        return self.model.predict_proba(X)[:, 1]


def load_profiles():
    """Load batter + pitcher profiles."""
    bp_pct = pd.read_parquet(DATA_DIR / "batter_percentiles_2025.parquet")
    bp_exp = pd.read_parquet(DATA_DIR / "batter_expected_2025.parquet")
    bp = bp_pct.merge(bp_exp[['player_id', 'est_woba', 'est_slg', 'pa']],
                      on='player_id', how='left', suffixes=('_pct', ''))
    batter_map = {}
    for _, r in bp.iterrows():
        batter_map[r['player_name']] = r.to_dict()
        batter_map[r['player_id']] = r.to_dict()

    pitcher_hr = pd.read_parquet(DATA_DIR / "pitcher_hr_profiles.parquet")
    p_hr_map = {}
    for _, r in pitcher_hr.iterrows():
        p_hr_map[r['name']] = r.to_dict()
        p_hr_map[r['pitcher_id']] = r.to_dict()

    pitcher_mix = pd.read_parquet(DATA_DIR / "pitcher_mix_hr.parquet")
    p_mix_map = {r['pitcher_id']: r.to_dict() for _, r in pitcher_mix.iterrows()}

    # Build pitcher expected stats lookup
    p_exp = {}
    for path in [DATA_DIR / "pitcher_expected_2025_full.parquet",
                 DATA_DIR / "statcast_pitchers_2025.parquet"]:
        if path.exists():
            for _, r in pd.read_parquet(path).iterrows():
                name = r.get('last_name, first_name', '')
                p_exp[name] = {
                    'p_xwoba': r.get('est_woba'),
                    'p_xslg': r.get('est_slg'),
                    'p_xera': r.get('xera'),
                }
            break

    return batter_map, p_hr_map, p_mix_map, p_exp


def match_pitcher(name, *lookups):
    """Fuzzy match a pitcher name across lookup dicts."""
    parts = name.split()
    if len(parts) < 2:
        return {}
    reversed_name = f"{parts[-1]}, {' '.join(parts[:-1])}"
    last = parts[-1].lower()

    result = {}
    for lookup in lookups:
        if reversed_name in lookup:
            result.update(lookup[reversed_name])
        else:
            for k, v in lookup.items():
                if isinstance(k, str) and last in k.lower():
                    result.update(v if isinstance(v, dict) else {})
                    break
    return result


# ══════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════
def run(date_str=None, api_key=None, top_n=DEFAULT_TOP_N, min_roi=DEFAULT_MIN_ROI):
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    print(f"\n{'='*80}")
    print(f"HR PROP EDGE SHEET -- {date_str}")
    print(f"{'='*80}")

    # Load model + profiles
    model = HRModel()
    batter_map, p_hr_map, p_mix_map, p_exp_map = load_profiles()
    print(f"  Model: {model.meta['version']} | {model.meta['n_samples']} training samples")
    print(f"  Profiles: {len(batter_map)//2} batters")

    # Fetch odds
    live_odds = {}
    if api_key:
        live_odds = fetch_hr_odds(api_key)
    else:
        print("  No API key -- running model only (no odds comparison)")

    # Fetch games
    r = requests.get("https://statsapi.mlb.com/api/v1/schedule", params={
        "sportId": 1, "date": date_str,
        "hydrate": "probablePitcher,team,venue"
    })
    games = r.json().get('dates', [{}])[0].get('games', [])
    print(f"  Games: {len(games)}")

    # Fetch lineups, score only batters actually in today's games
    all_rows = []

    for g in games:
        gpk = g['gamePk']
        away = g['teams']['away']
        home = g['teams']['home']
        at = TEAM_MAP.get(away['team'].get('abbreviation', ''),
                         away['team'].get('abbreviation', ''))
        ht = TEAM_MAP.get(home['team'].get('abbreviation', ''),
                         home['team'].get('abbreviation', ''))
        asp = away.get('probablePitcher', {}).get('fullName', 'TBD')
        hsp = home.get('probablePitcher', {}).get('fullName', 'TBD')
        park = PARK_HR.get(ht, (1.0, 1.0, 500))
        is_dome = ht in INDOOR

        # Get lineups from live feed or roster
        lineups = {'away': [], 'home': []}
        try:
            lr = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{gpk}/feed/live", timeout=5)
            live = lr.json()
            for side in ['away', 'home']:
                box = live.get('liveData', {}).get('boxscore', {}).get('teams', {}).get(side, {})
                order = box.get('battingOrder', [])
                players = box.get('players', {})
                for pid in order:
                    pinfo = players.get(f"ID{pid}", {}).get('person', {})
                    lineups[side].append({'id': pid, 'name': pinfo.get('fullName', '')})
        except:
            pass

        # Fallback: roster if no lineup yet
        if not lineups['away'] and not lineups['home']:
            for side, tdata in [('away', away), ('home', home)]:
                tid = tdata['team'].get('id')
                if not tid:
                    continue
                try:
                    rr = requests.get(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster/active", timeout=5)
                    for p in rr.json().get('roster', []):
                        if p.get('position', {}).get('type', '') != 'Pitcher':
                            lineups[side].append({'id': p['person']['id'], 'name': p['person']['fullName']})
                except:
                    pass

        # Score: away batters vs home SP, home batters vs away SP
        for side, pitcher_name in [('away', hsp), ('home', asp)]:
            if pitcher_name == 'TBD':
                continue
            p_data = match_pitcher(pitcher_name, p_hr_map, p_exp_map, p_mix_map)
            batters = lineups.get(side, [])

            for i, b in enumerate(batters[:9]):
                bp = batter_map.get(b['id'])
                if not bp:
                    parts = b['name'].split()
                    if len(parts) >= 2:
                        bp = batter_map.get(f"{parts[-1]}, {' '.join(parts[:-1])}")
                if not bp:
                    continue

                lineup_pos = i + 1
                est_tto = 3 if lineup_pos <= 4 else 2

                row = {
                    'batter_name': b['name'],
                    'game': f"{at} @ {ht}",
                    'vs_pitcher': pitcher_name,
                    'park': ht,

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
                    'p_hr_per_pa': p_data.get('p_hr_per_pa'),
                    'p_hr_fb_rate': p_data.get('p_hr_fb_rate'),
                    'p_fb_rate': p_data.get('p_fb_rate'),
                    'p_gb_rate': p_data.get('p_gb_rate'),
                    'p_avg_ev_allowed': p_data.get('p_avg_ev_allowed'),
                    'p_barrel_pct_allowed': p_data.get('p_barrel_pct_allowed'),
                    'p_hard_hit_allowed': p_data.get('p_hard_hit_allowed'),
                    'p_fb_velo': p_data.get('p_fb_velo'),
                    'p_fastball_pct': p_data.get('p_fastball_pct'),
                    'p_sinker_pct': p_data.get('p_sinker_pct'),
                    'p_breaking_pct': p_data.get('p_breaking_pct'),
                    'p_offspeed_pct': p_data.get('p_offspeed_pct'),
                    'p_hr_danger_score': p_data.get('p_hr_danger_score'),

                    'park_hr_factor': park[0], 'park_altitude': park[2],
                    'wx_temp': 72 if is_dome else 75,
                    'wx_wind': 0 if is_dome else 8,
                    'wx_humidity': 50 if is_dome else 55,
                    'wx_dome': 1 if is_dome else 0,
                    'platoon': 0, 'n_pa': 4,
                    'max_tto': est_tto, 'lineup_pos': lineup_pos,
                }
                all_rows.append(row)

    if not all_rows:
        print("  No matchups to score")
        return

    df = pd.DataFrame(all_rows)
    print(f"  Scoring {len(df)} batter-pitcher matchups...")

    # Run model
    probs = model.predict(df)
    df['model_prob'] = probs

    # Deduplicate: keep highest prob per batter (they might face multiple pitchers)
    df = df.sort_values('model_prob', ascending=False).drop_duplicates('batter_name', keep='first')

    # Match to live odds
    def find_odds(batter_name):
        # Direct match (both "First Last" format)
        if batter_name in live_odds:
            return live_odds[batter_name]
        # Handle "Last, First" -> "First Last"
        parts = batter_name.split(', ')
        if len(parts) == 2:
            full = f"{parts[1]} {parts[0]}"
            if full in live_odds:
                return live_odds[full]
        # Last name partial match
        last = batter_name.split()[-1] if ' ' in batter_name else batter_name
        for oname, odata in live_odds.items():
            # Strip suffixes like "(2002)" from odds names
            clean = oname.split('(')[0].strip()
            if last.lower() == clean.split()[-1].lower():
                return odata
        return None

    df['book_odds'] = None
    df['book_name'] = None
    for idx, row in df.iterrows():
        match = find_odds(row['batter_name'])
        if match:
            df.at[idx, 'book_odds'] = match['odds']
            df.at[idx, 'book_name'] = match['book']

    # Calculate edge and projected ROI
    df['implied_prob'] = df['book_odds'].apply(
        lambda x: 100 / (x + 100) if pd.notna(x) and x > 0 else None)
    df['edge'] = df['model_prob'] - df['implied_prob']
    df['projected_roi'] = df.apply(
        lambda r: (r['model_prob'] * (r['book_odds'] / 100) - (1 - r['model_prob'])) * 100
        if pd.notna(r['book_odds']) else None, axis=1)
    df['breakeven_odds'] = df['model_prob'].apply(
        lambda p: int((1 - p) / p * 100) if p > 0 else 9999)
    df['min_odds'] = (df['breakeven_odds'] * 1.10).astype(int)

    # Sort: only show picks that have FD/DK odds. No odds = no pick.
    has_odds = df[df['book_odds'].notna()].sort_values('projected_roi', ascending=False)
    
    if len(has_odds) == 0 and api_key:
        print("\n  No picks with FD/DK odds. Props may not be posted yet.")
        print("  Run again closer to game time (~4-5pm ET).")
        # Show top model picks without odds as reference
        no_odds = df.sort_values('model_prob', ascending=False).head(10)
        print(f"\n  Top model picks (no odds yet):")
        for _, r in no_odds.iterrows():
            min_o = int((1 - r['model_prob']) / r['model_prob'] * 100 * 1.10)
            print(f"    {r['batter_name']:<22} {r['game']:<13} Model: {r['model_prob']:.1%} | Bet if FD/DK offers +{min_o}+")
    
    df_sorted = has_odds.head(top_n)

    # Generate output
    _print_sheet(date_str, df_sorted, top_n, min_roi, bool(live_odds))
    _generate_html(date_str, df_sorted, top_n, min_roi, bool(live_odds))

    # Save JSON
    out = df_sorted.head(top_n * 2).to_dict('records')
    clean = []
    for r in out:
        cr = {}
        for k, v in r.items():
            if isinstance(v, float) and np.isnan(v):
                cr[k] = None
            elif isinstance(v, np.generic):
                cr[k] = v.item()
            else:
                cr[k] = v
            # Skip model internal features
            if k.startswith(('b_', 'p_', 'wx_', 'park_', 'max_tto', 'lineup', 'platoon', 'n_pa')):
                continue
            cr[k] = cr.get(k)
        clean.append({k: v for k, v in cr.items()
                      if not k.startswith(('b_', 'p_', 'wx_', 'park_', 'max_tto', 'lineup', 'platoon', 'n_pa'))})
    json.dump(clean, open(OUTPUT_DIR / f"hr_picks_{date_str}.json", 'w'), indent=2, default=str)

    return df_sorted


def _print_sheet(date, df, top_n, min_roi, has_live):
    bets = df[df['projected_roi'].notna() & (df['projected_roi'] >= min_roi)]
    leans = df[df['projected_roi'].notna() & (df['projected_roi'] > 0) & (df['projected_roi'] < min_roi)]
    passes = df[df['projected_roi'].notna() & (df['projected_roi'] <= 0)]

    print(f"\n{'Batter':<22} {'Game':<13} {'vs':<16} {'Model':>6} {'Odds':>6} {'Book':>6} {'Edge':>7} {'ProjROI':>8} {'CALL':>6}")
    print("─" * 98)

    for _, r in df.head(top_n).iterrows():
        o = f"+{int(r['book_odds'])}" if pd.notna(r.get('book_odds')) else "  ---"
        bk = r.get('book_name', '')[:4] if pd.notna(r.get('book_name')) else " ---"
        ed = f"{r['edge']*100:+.1f}pp" if pd.notna(r.get('edge')) else "   ---"
        roi = f"{r['projected_roi']:+.1f}%" if pd.notna(r.get('projected_roi')) else "   ---"

        if pd.notna(r.get('projected_roi')) and r['projected_roi'] >= min_roi:
            call = "BET"
        elif pd.notna(r.get('projected_roi')) and r['projected_roi'] > 0:
            call = "LEAN"
        elif pd.notna(r.get('projected_roi')):
            call = "PASS"
        else:
            call = f"+{r['min_odds']}"

        print(f"{r['batter_name']:<22} {r['game']:<13} {r['vs_pitcher']:<16} "
              f"{r['model_prob']:>5.1%} {o:>6} {bk:>6} {ed:>7} {roi:>8} {call:>6}")

    if len(bets) > 0:
        print(f"\n  BETS ({len(bets)}): {', '.join(bets['batter_name'].tolist())}")
        print(f"  Avg projected ROI: {bets['projected_roi'].mean():+.1f}%")


def _generate_html(date, df, top_n, min_roi, has_live):
    """Dark-themed HTML bet sheet."""
    rows_html = ""
    for _, r in df.head(top_n).iterrows():
        o = f"+{int(r['book_odds'])}" if pd.notna(r.get('book_odds')) else "---"
        ed = f"{r['edge']*100:+.1f}" if pd.notna(r.get('edge')) else "---"
        roi_val = r.get('projected_roi')
        roi = f"{roi_val:+.1f}%" if pd.notna(roi_val) else "---"

        if pd.notna(roi_val) and roi_val >= min_roi:
            cls = "bet"
            call = "BET"
        elif pd.notna(roi_val) and roi_val > 0:
            cls = "lean"
            call = "LEAN"
        elif pd.notna(roi_val):
            cls = "pass"
            call = "PASS"
        else:
            cls = "need"
            call = f"+{r['min_odds']}"

        rows_html += f"""<tr class="{cls}">
<td class="bn">{r['batter_name']}</td>
<td>{r['game']}</td>
<td>{r['vs_pitcher']}</td>
<td class="n">{r['model_prob']:.1%}</td>
<td class="n">{o}</td>
<td class="n">{ed}</td>
<td class="n roi">{roi}</td>
<td class="call">{call}</td>
</tr>"""

    bets = df[df['projected_roi'].notna() & (df['projected_roi'] >= min_roi)]
    bet_count = len(bets)
    avg_roi = f"{bets['projected_roi'].mean():+.1f}%" if bet_count > 0 else "n/a"

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>HR Props {date}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#06090f;color:#94a3b8;font-family:'DM Sans',sans-serif;padding:16px;max-width:900px;margin:0 auto}}
h1{{font-size:22px;font-weight:700;color:#f1f5f9;letter-spacing:-0.5px}}
.sub{{font-size:11px;color:#475569;font-family:'JetBrains Mono',monospace;margin:4px 0 16px}}
.stats{{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap}}
.stat{{background:#0f1724;border:1px solid #1e293b;border-radius:8px;padding:10px 14px;min-width:120px}}
.stat-label{{font-size:9px;color:#475569;text-transform:uppercase;letter-spacing:1px;font-family:'JetBrains Mono',monospace}}
.stat-value{{font-size:20px;font-weight:700;color:#f1f5f9;margin-top:2px}}
.stat-value.green{{color:#22c55e}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{padding:8px 6px;text-align:left;color:#475569;font-weight:600;font-size:9px;letter-spacing:.5px;font-family:'JetBrains Mono',monospace;border-bottom:2px solid #1e293b}}
th.n{{text-align:right}}
td{{padding:7px 6px;border-bottom:1px solid #111827}}
.n{{text-align:right;font-family:'JetBrains Mono',monospace}}
.bn{{font-weight:600;color:#e2e8f0}}
.roi{{font-weight:700}}
.call{{font-weight:700;text-align:center;font-size:11px;letter-spacing:.5px}}
tr.bet{{background:rgba(34,197,94,0.06)}}
tr.bet .call{{color:#22c55e}}
tr.bet .roi{{color:#22c55e}}
tr.lean .call{{color:#facc15}}
tr.lean .roi{{color:#facc15}}
tr.pass .call{{color:#475569}}
tr.pass .roi{{color:#ef4444}}
tr.need .call{{color:#64748b;font-size:10px}}
tr:hover{{background:#111827}}
.ft{{margin-top:16px;font-size:10px;color:#334155;border-top:1px solid #1e293b;padding-top:10px;line-height:1.6}}
</style></head><body>
<h1>HR Prop Edge Sheet</h1>
<div class="sub">{date} &middot; v4 model &middot; walk-forward validated &middot; 5/5 months profitable &middot; {'live odds' if has_live else 'no odds API key'}</div>
<div class="stats">
<div class="stat"><div class="stat-label">Qualified Bets</div><div class="stat-value green">{bet_count}</div></div>
<div class="stat"><div class="stat-label">Avg Proj ROI</div><div class="stat-value green">{avg_roi}</div></div>
<div class="stat"><div class="stat-label">Min ROI Threshold</div><div class="stat-value">{min_roi:.0f}%</div></div>
<div class="stat"><div class="stat-label">Matchups Scored</div><div class="stat-value">{len(df)}</div></div>
</div>
<div style="overflow-x:auto"><table>
<thead><tr><th>Batter</th><th>Game</th><th>vs Pitcher</th><th class="n">Model%</th><th class="n">Odds</th><th class="n">Edge</th><th class="n">Proj ROI</th><th>Call</th></tr></thead>
<tbody>{rows_html}</tbody></table></div>
<div class="ft">
BET = projected ROI &ge; {min_roi:.0f}% | LEAN = positive ROI under threshold | PASS = negative ROI | +NNN = min odds needed (no live odds found)<br>
Model trained on 23K+ batter-games, walk-forward validated May-Sep 2025 (5/5 months profitable, +48% ROI on top 3% at +500).<br>
Edge = model probability minus implied probability from sportsbook odds. Projected ROI = (prob &times; payout) - (1-prob).<br>
Always verify odds are current before placing bets. Lines move.
</div></body></html>"""

    path = OUTPUT_DIR / f"hr_props_{date}.html"
    with open(path, 'w') as f:
        f.write(html)
    print(f"\n  HTML saved: {path}")


# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="HR Prop Daily Pipeline")
    parser.add_argument("--date", default=None, help="Date (YYYY-MM-DD)")
    parser.add_argument("--api-key", default=os.environ.get("ODDS_API_KEY"), help="The Odds API key")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help="Number of picks to show")
    parser.add_argument("--min-roi", type=float, default=DEFAULT_MIN_ROI, help="Minimum ROI to flag as BET")
    args = parser.parse_args()

    run(args.date, args.api_key, args.top, args.min_roi)
