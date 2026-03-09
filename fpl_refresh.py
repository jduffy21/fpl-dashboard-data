"""
FPL Scheduled Refresh
Run this script on a schedule (e.g. cron, Task Scheduler, GitHub Actions)
to keep your CSVs up to date throughout the season.

Recommended schedule:
  - Full refresh: Once per week (Tuesdays, after GW deadline passes)
  - Live GW data: Every 15 min on match days

Cron example (weekly Tuesday 10am + player history):
  0 10 * * 2 cd /your/path && python fpl_refresh.py --full

Windows Task Scheduler: point to python.exe and this script.
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime


def run(args):
    cmd = [sys.executable, "fpl_extract.py"] + args
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true",
                        help="Full refresh including player GW history")
    parser.add_argument("--team-id", type=int, help="Your FPL manager ID")
    parser.add_argument("--gameweek", type=int)
    cli = parser.parse_args()

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FPL Refresh starting...\n")

    base_args = []
    if cli.gameweek:
        base_args += ["--gameweek", str(cli.gameweek)]
    if cli.team_id:
        base_args += ["--team-id", str(cli.team_id)]
    if cli.full:
        base_args += ["--player-history"]

    code = run(base_args)
    if code == 0:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ Refresh complete.\n")
    else:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ❌ Refresh failed (exit {code}).\n")
        sys.exit(code)


if __name__ == "__main__":
    main()
