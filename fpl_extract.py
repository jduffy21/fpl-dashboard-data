"""
FPL Data Extraction Pipeline
Pulls data from the Fantasy Premier League API and saves to CSV files
optimised for Tableau / Power BI import.

Usage:
    python fpl_extract.py                      # Pull all data
    python fpl_extract.py --gameweek 25        # Pull specific gameweek
    python fpl_extract.py --team-id 1234567    # Also pull your team data
"""

import requests
import pandas as pd
import os
import json
import time
import argparse
from datetime import datetime

BASE_URL = "https://fantasy.premierleague.com/api"
OUTPUT_DIR = "fpl_data"


def get_bootstrap():
    """Fetch the main bootstrap-static endpoint (players, teams, gameweeks)."""
    print("Fetching bootstrap data...")
    r = requests.get(f"{BASE_URL}/bootstrap-static/", timeout=15)
    r.raise_for_status()
    return r.json()


def get_fixtures():
    """Fetch all fixtures for the season."""
    print("Fetching fixtures...")
    r = requests.get(f"{BASE_URL}/fixtures/", timeout=15)
    r.raise_for_status()
    return r.json()


def get_player_detail(player_id):
    """Fetch per-player history and fixture details."""
    r = requests.get(f"{BASE_URL}/element-summary/{player_id}/", timeout=15)
    r.raise_for_status()
    return r.json()


def get_gameweek_live(gameweek):
    """Fetch live points data for a gameweek."""
    print(f"Fetching live data for GW{gameweek}...")
    r = requests.get(f"{BASE_URL}/event/{gameweek}/live/", timeout=15)
    r.raise_for_status()
    return r.json()


def get_manager_team(team_id, gameweek):
    """Fetch a manager's team for a specific gameweek."""
    r = requests.get(f"{BASE_URL}/entry/{team_id}/event/{gameweek}/picks/", timeout=15)
    r.raise_for_status()
    return r.json()


def get_manager_history(team_id):
    """Fetch a manager's overall history."""
    r = requests.get(f"{BASE_URL}/entry/{team_id}/history/", timeout=15)
    r.raise_for_status()
    return r.json()


# ── Transformation helpers ──────────────────────────────────────────────────

def build_players_df(bootstrap):
    """Clean players table with team name joined in."""
    teams_lookup = {t["id"]: t["name"] for t in bootstrap["teams"]}
    position_lookup = {
        1: "Goalkeeper", 2: "Defender", 3: "Midfielder", 4: "Forward"
    }

    players = pd.DataFrame(bootstrap["elements"])
    players["team_name"] = players["team"].map(teams_lookup)
    players["position"] = players["element_type"].map(position_lookup)
    players["full_name"] = players["first_name"] + " " + players["second_name"]
    players["now_cost_millions"] = players["now_cost"] / 10

    cols = [
        "id", "full_name", "web_name", "team_name", "position",
        "now_cost_millions", "total_points", "points_per_game",
        "selected_by_percent", "form", "minutes", "goals_scored",
        "assists", "clean_sheets", "goals_conceded", "yellow_cards",
        "red_cards", "bonus", "bps", "influence", "creativity",
        "threat", "ict_index", "transfers_in_event", "transfers_out_event",
        "transfers_in", "transfers_out", "status", "chance_of_playing_next_round",
        "news", "ep_next", "ep_this"
    ]
    # Only keep columns that exist
    cols = [c for c in cols if c in players.columns]
    return players[cols]


def build_teams_df(bootstrap):
    """Clean teams table."""
    teams = pd.DataFrame(bootstrap["teams"])
    cols = [
        "id", "name", "short_name", "strength", "strength_overall_home",
        "strength_overall_away", "strength_attack_home", "strength_attack_away",
        "strength_defence_home", "strength_defence_away", "win", "draw", "loss",
        "points", "position"
    ]
    cols = [c for c in cols if c in teams.columns]
    return teams[cols]


def build_gameweeks_df(bootstrap):
    """Clean gameweeks / events table."""
    gws = pd.DataFrame(bootstrap["events"])
    cols = [
        "id", "name", "deadline_time", "average_entry_score",
        "highest_score", "most_selected", "most_transferred_in",
        "top_element", "transfers_made", "finished", "is_current", "is_next"
    ]
    cols = [c for c in cols if c in gws.columns]
    return gws[cols]


def build_fixtures_df(fixtures, bootstrap):
    """Clean fixtures with team names."""
    teams_lookup = {t["id"]: t["name"] for t in bootstrap["teams"]}
    df = pd.DataFrame(fixtures)
    df["team_h_name"] = df["team_h"].map(teams_lookup)
    df["team_a_name"] = df["team_a"].map(teams_lookup)
    cols = [
        "id", "event", "team_h_name", "team_a_name",
        "team_h_score", "team_a_score", "team_h_difficulty",
        "team_a_difficulty", "kickoff_time", "finished", "started"
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols]


def build_player_history_df(bootstrap, max_players=None):
    """
    Pull per-gameweek history for every player.
    Set max_players for a quick test run (None = all players).
    This is the most time-consuming step (~650 API calls).
    """
    players = bootstrap["elements"]
    if max_players:
        players = players[:max_players]

    all_history = []
    total = len(players)

    for i, p in enumerate(players):
        pid = p["id"]
        name = p["web_name"]
        if (i + 1) % 50 == 0 or i == 0:
            print(f"  Fetching player history {i+1}/{total}: {name}")
        try:
            detail = get_player_detail(pid)
            for row in detail.get("history", []):
                row["player_id"] = pid
                row["player_name"] = name
                all_history.append(row)
            time.sleep(0.05)  # Be polite to the API
        except Exception as e:
            print(f"  ⚠ Skipped player {pid} ({name}): {e}")

    if not all_history:
        return pd.DataFrame()

    df = pd.DataFrame(all_history)
    cols = [
        "player_id", "player_name", "round", "opponent_team",
        "total_points", "was_home", "kickoff_time", "team_h_score",
        "team_a_score", "minutes", "goals_scored", "assists",
        "clean_sheets", "goals_conceded", "own_goals", "penalties_saved",
        "penalties_missed", "yellow_cards", "red_cards", "saves",
        "bonus", "bps", "influence", "creativity", "threat", "ict_index",
        "value", "transfers_balance", "selected", "transfers_in", "transfers_out"
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]
    df["value_millions"] = df["value"] / 10
    return df


def build_manager_history_df(team_id):
    """Pull a manager's season-by-season and gameweek-by-gameweek history."""
    data = get_manager_history(team_id)
    gw_df = pd.DataFrame(data.get("current", []))
    if not gw_df.empty:
        gw_df["team_id"] = team_id
    season_df = pd.DataFrame(data.get("past", []))
    if not season_df.empty:
        season_df["team_id"] = team_id
    return gw_df, season_df


# ── Save helpers ─────────────────────────────────────────────────────────────

def save(df, filename):
    """Save DataFrame to CSV."""
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  ✓ Saved {filename}  ({len(df):,} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FPL Data Pipeline")
    parser.add_argument("--gameweek", type=int, help="Gameweek for live data (default: current)")
    parser.add_argument("--team-id", type=int, help="Your FPL manager team ID")
    parser.add_argument("--player-history", action="store_true",
                        help="Pull full per-GW player history (slow, ~650 requests)")
    parser.add_argument("--max-players", type=int, default=None,
                        help="Limit player history fetch (for testing)")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\n{'='*50}")
    print(f"  FPL Data Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    # 1. Bootstrap
    bootstrap = get_bootstrap()

    # 2. Core tables
    print("\n── Core tables ──")
    save(build_players_df(bootstrap), "players.csv")
    save(build_teams_df(bootstrap), "teams.csv")
    save(build_gameweeks_df(bootstrap), "gameweeks.csv")

    # 3. Fixtures
    print("\n── Fixtures ──")
    fixtures = get_fixtures()
    save(build_fixtures_df(fixtures, bootstrap), "fixtures.csv")

    # 4. Gameweek live data
    events = bootstrap["events"]
    current_gw = next((e["id"] for e in events if e.get("is_current")), None)
    gw = args.gameweek or current_gw
    if gw:
        print(f"\n── Gameweek {gw} live data ──")
        try:
            live = get_gameweek_live(gw)
            live_rows = []
            for el in live["elements"]:
                row = {"player_id": el["id"]}
                row.update(el["stats"])
                live_rows.append(row)
            save(pd.DataFrame(live_rows), f"gw{gw}_live.csv")
        except Exception as e:
            print(f"  ⚠ Could not fetch live data: {e}")

    # 5. Optional: full player history
    if args.player_history:
        print(f"\n── Player GW history (this may take a few minutes) ──")
        hist_df = build_player_history_df(bootstrap, max_players=args.max_players)
        if not hist_df.empty:
            save(hist_df, "player_history.csv")

    # 6. Optional: manager data
    if args.team_id:
        print(f"\n── Manager {args.team_id} history ──")
        try:
            gw_df, season_df = build_manager_history_df(args.team_id)
            if not gw_df.empty:
                save(gw_df, f"manager_{args.team_id}_gw_history.csv")
            if not season_df.empty:
                save(season_df, f"manager_{args.team_id}_seasons.csv")
        except Exception as e:
            print(f"  ⚠ Could not fetch manager data: {e}")

    print(f"\n✅ Done! All files saved to ./{OUTPUT_DIR}/\n")


if __name__ == "__main__":
    main()
