#!/usr/bin/env python3
"""
publish_picks.py
================
Runs the HR model, pulls live odds, and publishes qualified picks
to the Vercel app's data file.

Usage:
    python scripts/publish_picks.py                     # Today
    python scripts/publish_picks.py --date 2026-04-14   # Specific date

Requires:
    - ODDS_API_KEY env variable (or --api-key flag)
    - Model artifacts in ../f5_model/model_artifacts/
    - Batter/pitcher data in ../f5_model/data/
"""

import sys, os, json, subprocess
from pathlib import Path
from datetime import datetime

# Paths
SCRIPT_DIR = Path(__file__).parent
APP_DIR = SCRIPT_DIR.parent
MODEL_DIR = APP_DIR.parent / "f5_model"  # Adjust if needed
PICKS_FILE = APP_DIR / "src" / "data" / "picks.json"

MIN_ROI = 25.0  # Minimum projected ROI to qualify as a pick


def run_model(date_str, api_key):
    """Run hr_daily.py and capture output picks."""
    cmd = [sys.executable, str(MODEL_DIR / "hr_daily.py"),
           "--date", date_str, "--top", "50", "--min-roi", str(MIN_ROI)]
    if api_key:
        cmd.extend(["--api-key", api_key])

    print(f"Running model for {date_str}...")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(MODEL_DIR))
    print(result.stdout[-500:] if result.stdout else "No output")
    if result.returncode != 0:
        print(f"Error: {result.stderr[-500:]}")

    # Load the generated picks JSON
    picks_path = MODEL_DIR / "output" / f"hr_picks_{date_str}.json"
    if picks_path.exists():
        return json.load(open(picks_path))
    return []


def load_existing():
    """Load existing picks database."""
    if PICKS_FILE.exists():
        return json.load(open(PICKS_FILE))
    return {
        "dates": {},
        "cumulative": {
            "total_bets": 0, "total_wins": 0, "total_pnl": 0.0,
            "total_wagered": 0.0, "roi": 0.0, "winning_days": 0, "losing_days": 0,
        },
        "config": {
            "min_roi_threshold": MIN_ROI,
            "model_version": "hr_v4_mix_tto_lineup",
            "backtest_roi": 48.4,
            "backtest_months_profitable": "5/5",
            "unit_size": 1.0,
        },
    }


def publish(date_str, api_key=None):
    """Full publish pipeline."""
    # Run model
    raw_picks = run_model(date_str, api_key)

    # Filter to qualified picks (positive ROI with odds, or top model picks without)
    qualified = []
    for p in raw_picks:
        roi = p.get('projected_roi')
        if roi is not None and roi >= MIN_ROI:
            qualified.append({
                "batter": p.get('batter_name', p.get('name', '')),
                "game": p.get('game', ''),
                "vs_pitcher": p.get('vs_pitcher', ''),
                "park": p.get('park', ''),
                "model_prob": round(p.get('model_prob', 0), 3),
                "book_odds": p.get('book_odds'),
                "book": p.get('book_name', ''),
                "implied_prob": round(p.get('implied_prob', 0), 3) if p.get('implied_prob') else None,
                "edge": round(p.get('edge', 0), 3) if p.get('edge') else None,
                "projected_roi": round(roi, 1),
                "result": None,
                "hit_hr": None,
            })

    print(f"\n  Qualified picks (ROI >= {MIN_ROI}%): {len(qualified)}")
    for p in qualified:
        print(f"    {p['batter']:<22} vs {p['vs_pitcher']:<16} +{p['book_odds']}  ROI: +{p['projected_roi']:.1f}%")

    # Update database
    db = load_existing()
    db['dates'][date_str] = {
        "picks": qualified,
        "settled": False,
        "summary": None,
    }

    # Save
    PICKS_FILE.parent.mkdir(parents=True, exist_ok=True)
    json.dump(db, open(PICKS_FILE, 'w'), indent=2, default=str)
    print(f"\n  Saved to {PICKS_FILE}")
    print(f"  Git commit and push to deploy to Vercel")

    return qualified


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument("--api-key", default=os.environ.get("ODDS_API_KEY"))
    args = parser.parse_args()

    publish(args.date, args.api_key)
