#!/usr/bin/env python3
"""
grade_results.py
================
Checks yesterday's (or any date's) picks against actual MLB results.
Updates the picks database with hit/miss outcomes and recalculates
cumulative ROI.

Usage:
    python scripts/grade_results.py                     # Grade yesterday
    python scripts/grade_results.py --date 2026-04-13   # Specific date
"""

import json, requests, sys
from pathlib import Path
from datetime import datetime, timedelta

SCRIPT_DIR = Path(__file__).parent
APP_DIR = SCRIPT_DIR.parent
PICKS_FILE = APP_DIR / "src" / "data" / "picks.json"


def get_hr_hitters(date_str):
    """Get all batters who hit HRs on a given date from MLB API."""
    print(f"  Fetching results for {date_str}...")
    r = requests.get("https://statsapi.mlb.com/api/v1/schedule", params={
        "sportId": 1, "date": date_str, "hydrate": "scoringplays,team"
    }, timeout=10)

    hr_hitters = set()
    for game in r.json().get('dates', [{}])[0].get('games', []):
        status = game.get('status', {}).get('detailedState', '')
        if 'Final' not in status:
            continue

        scoring = game.get('scoringPlays', [])
        for play in scoring:
            desc = play.get('result', {}).get('description', '')
            event = play.get('result', {}).get('eventType', '')
            if 'home_run' in event.lower() or 'homers' in desc.lower() or 'home run' in desc.lower():
                # Extract batter name from description
                # Format: "First Last homers (N) on a..."
                parts = desc.split(' homers')
                if parts:
                    name = parts[0].strip()
                    hr_hitters.add(name)
                else:
                    parts = desc.split(' hits a')
                    if parts:
                        name = parts[0].strip()
                        hr_hitters.add(name)

    # Also try box score approach for more reliable data
    for game in r.json().get('dates', [{}])[0].get('games', []):
        gpk = game['gamePk']
        status = game.get('status', {}).get('detailedState', '')
        if 'Final' not in status:
            continue
        try:
            br = requests.get(f"https://statsapi.mlb.com/api/v1/game/{gpk}/boxscore", timeout=5)
            box = br.json()
            for side in ['away', 'home']:
                players = box.get('teams', {}).get(side, {}).get('players', {})
                for pid, pdata in players.items():
                    stats = pdata.get('stats', {}).get('batting', {})
                    if stats.get('homeRuns', 0) > 0:
                        name = pdata.get('person', {}).get('fullName', '')
                        if name:
                            hr_hitters.add(name)
        except:
            continue

    print(f"    Found {len(hr_hitters)} HR hitters: {', '.join(sorted(hr_hitters)[:10])}{'...' if len(hr_hitters) > 10 else ''}")
    return hr_hitters


def grade(date_str):
    """Grade picks for a specific date."""
    if not PICKS_FILE.exists():
        print("No picks file found")
        return

    db = json.load(open(PICKS_FILE))

    if date_str not in db['dates']:
        print(f"No picks found for {date_str}")
        return

    day = db['dates'][date_str]
    if day.get('settled'):
        print(f"{date_str} already settled")
        return

    picks = day['picks']
    if not picks:
        print(f"No picks for {date_str}")
        return

    # Get actual HR hitters
    hr_hitters = get_hr_hitters(date_str)

    # Grade each pick
    wins = 0
    day_pnl = 0.0
    for pick in picks:
        batter = pick['batter']
        # Check if batter hit a HR (fuzzy match)
        hit = any(batter.lower() in h.lower() or h.lower() in batter.lower()
                  for h in hr_hitters)
        pick['hit_hr'] = hit
        pick['result'] = 'HIT' if hit else 'MISS'

        payout = pick['book_odds'] / 100 if pick.get('book_odds') else 5.0
        pnl = payout if hit else -1.0
        pick['pnl'] = round(pnl, 2)
        day_pnl += pnl
        if hit:
            wins += 1

    # Update day summary
    day['settled'] = True
    day['summary'] = {
        'wins': wins,
        'losses': len(picks) - wins,
        'pnl': round(day_pnl, 2),
        'roi': round(day_pnl / len(picks) * 100, 1) if picks else 0,
    }

    # Recalculate cumulative stats
    total_bets = 0
    total_wins = 0
    total_pnl = 0.0
    winning_days = 0
    losing_days = 0

    for d, ddata in db['dates'].items():
        if not ddata.get('settled'):
            continue
        s = ddata.get('summary', {})
        total_bets += s.get('wins', 0) + s.get('losses', 0)
        total_wins += s.get('wins', 0)
        total_pnl += s.get('pnl', 0)
        if s.get('pnl', 0) > 0:
            winning_days += 1
        elif s.get('pnl', 0) < 0:
            losing_days += 1

    db['cumulative'] = {
        'total_bets': total_bets,
        'total_wins': total_wins,
        'total_pnl': round(total_pnl, 2),
        'total_wagered': total_bets,
        'roi': round(total_pnl / total_bets * 100, 1) if total_bets > 0 else 0,
        'winning_days': winning_days,
        'losing_days': losing_days,
    }

    # Save
    json.dump(db, open(PICKS_FILE, 'w'), indent=2, default=str)

    # Print results
    print(f"\n  {date_str} RESULTS:")
    for p in picks:
        icon = "HIT" if p['hit_hr'] else "MISS"
        odds_str = f"+{p['book_odds']}" if p.get('book_odds') else "+???"
        print(f"    {icon} {p['batter']:<22} {odds_str:>6}  P&L: {p['pnl']:+.2f}u")

    print(f"\n  Day: {wins}W-{len(picks)-wins}L | P&L: {day_pnl:+.2f}u")
    print(f"  Cumulative: {total_bets} bets | {total_wins}W-{total_bets-total_wins}L | P&L: {total_pnl:+.2f}u | ROI: {db['cumulative']['roi']:+.1f}%")
    print(f"\n  Saved. Git commit and push to update Vercel.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
    args = parser.parse_args()
    grade(args.date)
