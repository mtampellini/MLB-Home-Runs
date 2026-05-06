"""MLBAM team-id → our internal team/park code map.

Codes match the keys in `data/park_metadata.json`. Used to translate the
MLB Stats API's numeric team_id into the 2–3 letter code our park factors,
park metadata, and front-end use.
"""

# MLBAM team IDs → internal codes.
TEAM_CODE_BY_MLBAM_ID: dict[int, str] = {
    108: "LAA",  # Los Angeles Angels
    109: "ARI",  # Arizona Diamondbacks
    110: "BAL",  # Baltimore Orioles
    111: "BOS",  # Boston Red Sox
    112: "CHC",  # Chicago Cubs
    113: "CIN",  # Cincinnati Reds
    114: "CLE",  # Cleveland Guardians
    115: "COL",  # Colorado Rockies
    116: "DET",  # Detroit Tigers
    117: "HOU",  # Houston Astros
    118: "KC",   # Kansas City Royals
    119: "LAD",  # Los Angeles Dodgers
    120: "WSH",  # Washington Nationals
    121: "NYM",  # New York Mets
    133: "OAK",  # Oakland Athletics (now Sutter Health Park)
    134: "PIT",  # Pittsburgh Pirates
    135: "SD",   # San Diego Padres
    136: "SEA",  # Seattle Mariners
    137: "SF",   # San Francisco Giants
    138: "STL",  # St. Louis Cardinals
    139: "TB",   # Tampa Bay Rays
    140: "TEX",  # Texas Rangers
    141: "TOR",  # Toronto Blue Jays
    142: "MIN",  # Minnesota Twins
    143: "PHI",  # Philadelphia Phillies
    144: "ATL",  # Atlanta Braves
    145: "CHW",  # Chicago White Sox
    146: "MIA",  # Miami Marlins
    147: "NYY",  # New York Yankees
    158: "MIL",  # Milwaukee Brewers
}
