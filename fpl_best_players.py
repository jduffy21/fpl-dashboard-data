"""
FPL Best Players — Weekly Snapshot Logger
==========================================
Generates fpl_data/best_players_log.csv — a running log where each week's
top players are appended with a snapshot_date column.

This means you get a full time-series in one file, perfect for Tableau/Power BI
trend charts showing how player rankings shift across the season.

Improvements over v1:
  - Minutes filter (>450 min) — ignores bit-part players
  - Fixture difficulty weighting — penalises tough upcoming fixtures
  - Differential bonus — rewards low-ownership hidden gems
  - Appends to log instead of overwriting — enables time-series charts

Usage:
    python fpl_best_players.py
"""

import pandas as pd
import os
from datetime import date

DATA_DIR = "fpl_data"
LOG_FILE = os.path.join(DATA_DIR, "best_players_log.csv")

# ── Scoring weights (tune these to your strategy) ────────────────────────────
W_FORM            =  3.0   # Recent gameweek point average — most important signal
W_PPG             =  2.0   # Season-long consistency
W_ICT             =  0.1   # Underlying stats: influence, creativity, threat
W_COST            = -0.5   # Penalise expensive players (price in £m)
W_OWNERSHIP       =  0.05  # Mild crowd-wisdom signal
W_DIFFERENTIAL    =  0.15  # Bonus for low-ownership picks (<15% selected)
W_FIXTURE         = -0.4   # Penalise tough upcoming fixtures (difficulty 1–5)
MINUTES_THRESHOLD =  450   # Ignore players with fewer minutes (filters fringe/injured)
TOP_N_PER_POS     =  10    # How many players to keep per position


def load_fixtures(bootstrap_teams):
    """
    Load upcoming fixtures and compute average difficulty for each team
    over their next 3 games. Returns a dict: team_name -> avg_difficulty.
    """
    path = os.path.join(DATA_DIR, "fixtures.csv")
    if not os.path.exists(path):
        print("  ⚠ fixtures.csv not found — skipping fixture difficulty weighting")
        return {}

    fixtures = pd.read_csv(path)

    # Only unplayed fixtures
    upcoming = fixtures[fixtures["finished"] == False].copy()
    if upcoming.empty:
        return {}

    difficulty = {}

    for team in bootstrap_teams:
        # Fixtures where this team plays (home or away)
        home = upcoming[upcoming["team_h_name"] == team][["event", "team_h_difficulty"]].rename(
            columns={"team_h_difficulty": "difficulty"}
        )
        away = upcoming[upcoming["team_a_name"] == team][["event", "team_a_difficulty"]].rename(
            columns={"team_a_difficulty": "difficulty"}
        )
        team_fixtures = pd.concat([home, away]).sort_values("event").head(3)

        if not team_fixtures.empty:
            difficulty[team] = round(team_fixtures["difficulty"].mean(), 2)

    return difficulty


def score_player(row, fixture_difficulty):
    """
    Composite FPL value score. Higher = better player to own this week.
    """
    score = 0

    score += float(row.get("form", 0))             * W_FORM
    score += float(row.get("points_per_game", 0))  * W_PPG
    score += float(row.get("ict_index", 0))        * W_ICT
    score += float(row.get("now_cost_millions", 6))* W_COST
    score += float(row.get("selected_by_percent", 0)) * W_OWNERSHIP

    # Differential bonus: reward players owned by fewer than 15% of managers
    ownership = float(row.get("selected_by_percent", 100))
    if ownership < 15:
        score += (15 - ownership) * W_DIFFERENTIAL

    # Fixture difficulty: penalise hard upcoming run
    team = row.get("team_name", "")
    avg_difficulty = fixture_difficulty.get(team, 3.0)  # default to mid difficulty
    score += avg_difficulty * W_FIXTURE

    # Hard penalise unavailable players
    if row.get("status") not in ("a", None, ""):
        score -= 10

    return round(score, 2)


def main():
    players_path = os.path.join(DATA_DIR, "players.csv")
    if not os.path.exists(players_path):
        print("❌ players.csv not found — run fpl_extract.py first")
        return

    df = pd.read_csv(players_path)

    # ── 1. Minutes filter ────────────────────────────────────────────────────
    before = len(df)
    df = df[pd.to_numeric(df.get("minutes", 0), errors="coerce").fillna(0) >= MINUTES_THRESHOLD]
    print(f"  Minutes filter: {before} → {len(df)} players (removed {before - len(df)} fringe/injured)")

    # ── 2. Clean numeric columns ─────────────────────────────────────────────
    for col in ["form", "points_per_game", "ict_index",
                "now_cost_millions", "selected_by_percent", "minutes"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    # ── 3. Fixture difficulty lookup ─────────────────────────────────────────
    all_teams = df["team_name"].dropna().unique().tolist()
    fixture_difficulty = load_fixtures(all_teams)
    df["avg_next3_difficulty"] = df["team_name"].map(fixture_difficulty).fillna(3.0)

    # ── 4. Score every player ────────────────────────────────────────────────
    df["value_score"] = df.apply(lambda row: score_player(row, fixture_difficulty), axis=1)

    # ── 5. Top N per position ────────────────────────────────────────────────
    positions = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
    frames = []
    for pos in positions:
        top = (df[df["position"] == pos]
               .sort_values("value_score", ascending=False)
               .head(TOP_N_PER_POS)
               .copy())
        top["rank_in_position"] = range(1, len(top) + 1)
        frames.append(top)

    best = pd.concat(frames)

    # ── 6. Add snapshot date ─────────────────────────────────────────────────
    best["snapshot_date"] = date.today().isoformat()

    # ── 7. Select output columns ─────────────────────────────────────────────
    cols = [
        "snapshot_date", "position", "rank_in_position",
        "full_name", "team_name", "now_cost_millions",
        "total_points", "points_per_game", "form", "minutes",
        "ict_index", "selected_by_percent", "avg_next3_difficulty",
        "value_score", "status", "news"
    ]
    cols = [c for c in cols if c in best.columns]
    best = best[cols].sort_values(["position", "rank_in_position"])

    # ── 8. Append to running log ──────────────────────────────────────────────
    today = date.today().isoformat()

    if os.path.exists(LOG_FILE):
        existing = pd.read_csv(LOG_FILE)

        # Remove today's entries if this is a re-run (idempotent)
        existing = existing[existing["snapshot_date"] != today]
        updated = pd.concat([existing, best], ignore_index=True)
        print(f"  Appending to existing log ({len(existing)} existing rows → {len(updated)} total)")
    else:
        updated = best
        print(f"  Creating new log with {len(updated)} rows")

    updated.to_csv(LOG_FILE, index=False)
    print(f"✅ Saved best_players_log.csv  ({len(updated):,} total rows, {len(best)} added today)")

    # ── 9. Also write a simple "latest only" snapshot for quick Tableau views ─
    best.to_csv(os.path.join(DATA_DIR, "best_players_latest.csv"), index=False)
    print(f"✅ Saved best_players_latest.csv  (current week only)")


if __name__ == "__main__":
    main()
