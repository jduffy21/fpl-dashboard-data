"""
Generates fpl_data/best_players.csv — a ranked, opinionated
"best players to own" report, refreshed every week.
Scoring weights can be tuned to your own preferences.
"""

import pandas as pd
import os

DATA_DIR = "fpl_data"

def score_player(row):
    """
    Composite FPL value score (tune weights to taste).
    Higher = better player to own right now.
    """
    score = 0
    score += float(row.get("form", 0)) * 3.0          # Recent form is king
    score += float(row.get("points_per_game", 0)) * 2.0
    score += float(row.get("ict_index", 0)) * 0.1
    score -= float(row.get("now_cost_millions", 6)) * 0.5  # Penalise high cost
    score += float(row.get("selected_by_percent", 0)) * 0.05  # Ownership signal
    # Penalise injured/suspended players
    if row.get("status") not in ("a", None, ""):
        score -= 5
    return round(score, 2)

def main():
    path = os.path.join(DATA_DIR, "players.csv")
    if not os.path.exists(path):
        print("players.csv not found — run fpl_extract.py first")
        return

    df = pd.read_csv(path)

    # Clean numeric columns
    for col in ["form", "points_per_game", "ict_index",
                "now_cost_millions", "selected_by_percent"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)

    df["value_score"] = df.apply(score_player, axis=1)

    # Top 10 per position
    positions = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
    frames = []
    for pos in positions:
        top = (df[df["position"] == pos]
               .sort_values("value_score", ascending=False)
               .head(10)
               .copy())
        frames.append(top)

    best = pd.concat(frames)

    cols = [
        "position", "full_name", "team_name", "now_cost_millions",
        "total_points", "points_per_game", "form", "ict_index",
        "selected_by_percent", "value_score", "status", "news"
    ]
    cols = [c for c in cols if c in best.columns]
    best = best[cols].sort_values(["position", "value_score"], ascending=[True, False])

    out = os.path.join(DATA_DIR, "best_players.csv")
    best.to_csv(out, index=False)
    print(f"✅ Saved best_players.csv ({len(best)} players)")

if __name__ == "__main__":
    main()
