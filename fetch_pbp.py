"""Step 2: download the play-by-play of the 2016 NBA Finals Game 7 and clean it for replay.

Run inside the client container:
    python fetch_pbp.py

Outputs:
    data/pbp_0041500407_raw.csv   raw response from stats.nba.com (467 events)
    data/pbp_0041500407.csv       cleaned events used by the Step 3+ replay scripts
"""
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import playbyplayv3

GAME_ID = "0041500407"  # 004 = playoffs, 15 = 2015-16 season, 00407 = round 4 (Finals), game 7
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# 1. Call the stats.nba.com endpoint. Omitting start_period/end_period returns the whole game.
response = playbyplayv3.PlayByPlayV3(game_id=GAME_ID, timeout=60)
raw = response.play_by_play.get_data_frame()
raw.to_csv(DATA_DIR / f"pbp_{GAME_ID}_raw.csv", index=False)
print(f"downloaded {len(raw)} events")

# 2. Keep the columns the replay needs.
df = raw[["actionNumber", "period", "clock", "teamTricode", "personId", "playerName",
          "actionType", "subType", "shotResult", "isFieldGoal",
          "scoreHome", "scoreAway", "description"]].copy()

# 2a. Scores are only filled on scoring plays (other rows hold ""), so carry the last known
#     score forward to give every event the running score at that moment.
for col in ["scoreHome", "scoreAway"]:
    df[col] = pd.to_numeric(df[col].str.strip().replace("", pd.NA)).ffill().fillna(0).astype(int)

# 2b. The clock is an ISO-8601 duration, e.g. "PT11M39.00S" = 11:39 left in the period.
parts = df["clock"].str.extract(r"PT(\d+)M([\d.]+)S").astype(float)
df["clockSeconds"] = parts[0] * 60 + parts[1]

# 2c. Seconds of game time elapsed since tip-off (12-minute periods; this game had no overtime).
df["gameSeconds"] = (df["period"] - 1) * 720 + (720 - df["clockSeconds"])

# 2d. Replay order 1..N.
df.insert(0, "seq", range(1, len(df) + 1))

df.to_csv(DATA_DIR / f"pbp_{GAME_ID}.csv", index=False)

# 3. Sanity checks.
last = df.iloc[-1]
game_len = df["gameSeconds"].max()
print(f"final score: GSW (home) {last.scoreHome} - CLE (away) {last.scoreAway}")
print(f"{game_len:.0f} s of game time, one event every {game_len / len(df):.1f} s on average")
print(f"events that carry a score: {raw['scoreHome'].str.strip().ne('').sum()} of {len(raw)}")
print(df[["seq", "period", "clock", "teamTricode", "actionType", "scoreHome", "scoreAway", "description"]]
      .tail(5).to_string(index=False))
