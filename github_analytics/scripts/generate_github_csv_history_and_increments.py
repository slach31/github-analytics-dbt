#!/usr/bin/env python3
"""
generate_github_csv.py
======================
This script queries the GitHub API and generates structured
CSV files in history + incremental mode:
  - 1 "history" file per endpoint (last 3 months of 2025:
    from October 1 to December 31, 2025)
  - 10 daily files per endpoint (from January 1 to February 10, 2026)

Result:
  (1 history + 10 daily) x 4 endpoints = 44 CSV files

Usage:
  python generate_github_csv.py [--token GITHUB_TOKEN]

With a token, the rate limit is 5000 req/h (instead of 60).
A token is STRONGLY recommended for this script.
"""

import requests
import csv
import os
import time
import json
import argparse
import sys
import random
import copy
from datetime import datetime, date, timedelta, timezone

# ============================================================
# CONFIGURATION
# ============================================================
REPOS = [
    "facebook/react",
    "tensorflow/tensorflow",
    "microsoft/vscode",
    "torvalds/linux",
    "golang/go",
    "apache/spark",
    "langchain-ai/langchain",
    "pallets/flask",
    "docker/compose",
    "duckdb/duckdb",
]

BASE_URL = "https://api.github.com"
PER_PAGE = 100
MAX_PAGES = 5  # more pages since we have a token

# Reference dates
# History: October 1 -> December 31, 2025 (92 days)
# Daily:   January 1 -> February 10, 2026 (10 files)
HISTORY_START = date(2025, 10, 1)
REF_DATE = date(2025, 12, 31)       # end of history
DAILY_END = date(2026, 2, 10)

HISTORY_DAYS = (REF_DATE - HISTORY_START).days  # 92
INCREMENTAL_DAYS = (DAILY_END - REF_DATE).days  # 10
TOTAL_DAYS = HISTORY_DAYS + INCREMENTAL_DAYS    # 102

# Output directory
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "generated_csv"
)

# ============================================================
# HELPERS
# ============================================================
class GitHubClient:
    """GitHub API client with rate limit handling."""

    def __init__(self, token=None):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github.v3+json",
        })
        if token:
            self.session.headers["Authorization"] = f"token {token}"
        self.request_count = 0

    def get(self, url, params=None):
        """GET with automatic rate limit handling."""
        resp = self.session.get(url, params=params)
        self.request_count += 1
        remaining = int(
            resp.headers.get("X-RateLimit-Remaining", 999))
        if remaining <= 5:
            reset_ts = int(
                resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_ts - int(time.time()), 0) + 5
            print(f"  [RATE LIMIT] {remaining} remaining, "
                  f"pausing {wait}s...")
            time.sleep(wait)
        if resp.status_code == 403:
            reset_ts = int(
                resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_ts - int(time.time()), 0) + 10
            print(f"  [403 RATE LIMIT] Forced pause {wait}s...")
            time.sleep(wait)
            return self.get(url, params)  # retry
        if resp.status_code != 200:
            print(f"  [ERROR {resp.status_code}] {url}")
            return None
        return resp.json()

    def get_paginated(self, url, params=None,
                      max_pages=MAX_PAGES):
        """Paginated GET."""
        all_items = []
        if params is None:
            params = {}
        params["per_page"] = PER_PAGE
        for page in range(1, max_pages + 1):
            params["page"] = page
            data = self.get(url, params)
            if data is None or len(data) == 0:
                break
            all_items.extend(data)
            if len(data) < PER_PAGE:
                break
        return all_items


def write_csv(filepath, rows, fieldnames):
    """Write a list of dicts to CSV."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="",
              encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames,
            extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def parse_date(date_str):
    """Parse an ISO 8601 date string into a date object."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(
            date_str.replace("Z", "+00:00"))
        return dt.date()
    except (ValueError, AttributeError):
        return None


def split_by_date(rows, date_field, ref_date, history_days,
                  incremental_days):
    """
    Split rows into:
    - history: everything within [ref_date - history_days, ref_date]
    - daily:   dict {date_str: [rows]} for each day after ref_date
               over incremental_days days
    """
    history_start = ref_date - timedelta(days=history_days)
    history_end = ref_date
    incr_end = ref_date + timedelta(days=incremental_days)

    history = []
    daily = {}

    for row in rows:
        d = parse_date(row.get(date_field))
        if d is None:
            # Without a date, put in history
            history.append(row)
            continue

        if history_start <= d <= history_end:
            history.append(row)
        elif history_end < d <= incr_end:
            day_str = d.isoformat()
            if day_str not in daily:
                daily[day_str] = []
            daily[day_str].append(row)

    return history, daily


def generate_daily_files(daily_dict, ref_date,
                         incremental_days, base_name,
                         fieldnames, output_subdir):
    """
    Generate daily files (one per day), even if empty.
    """
    count = 0
    for i in range(1, incremental_days + 1):
        day = ref_date + timedelta(days=i)
        day_str = day.isoformat()
        rows = daily_dict.get(day_str, [])
        filepath = os.path.join(
            output_subdir,
            f"{base_name}_{day_str}.csv"
        )
        write_csv(filepath, rows, fieldnames)
        count += 1
    return count


# ============================================================
# EXTRACTORS
# ============================================================
def extract_repositories(client):
    """
    Endpoint: GET /repos/{owner}/{repo}
    Repository metadata is a snapshot (no time series).
    We simulate realistic evolution:
    - Fetch current values from the API
    - Back-project to October 1, 2025
    - Advance day by day with random deltas
      (stars, forks, open_issues, subscribers)
    - History file = snapshot as of December 31, 2025
    - Daily files = January 1 to February 10, 2026
    """
    print("\n[1/4] Extracting metadata...")

    fields = [
        "full_name", "name", "owner_login",
        "description", "language",
        "created_at", "updated_at", "pushed_at",
        "stargazers_count", "watchers_count",
        "forks_count", "open_issues_count",
        "size", "default_branch",
        "has_wiki", "has_pages",
        "archived", "disabled",
        "license_name", "topics",
        "network_count", "subscribers_count",
        "snapshot_date",
    ]

    # ----------------------------------------------------------
    # 1. Fetch current values from the API
    # ----------------------------------------------------------
    current_rows = []
    for repo in REPOS:
        print(f"  {repo}")
        data = client.get(f"{BASE_URL}/repos/{repo}")
        if data is None:
            continue
        row = {
            "full_name": data.get("full_name"),
            "name": data.get("name"),
            "owner_login": data.get(
                "owner", {}).get("login"),
            "description": (
                data.get("description") or "")[:500],
            "language": data.get("language"),
            "created_at": data.get("created_at"),
            "updated_at": data.get("updated_at"),
            "pushed_at": data.get("pushed_at"),
            "stargazers_count": data.get(
                "stargazers_count", 0),
            "watchers_count": data.get(
                "watchers_count", 0),
            "forks_count": data.get("forks_count", 0),
            "open_issues_count": data.get(
                "open_issues_count", 0),
            "size": data.get("size", 0),
            "default_branch": data.get("default_branch"),
            "has_wiki": data.get("has_wiki", False),
            "has_pages": data.get("has_pages", False),
            "archived": data.get("archived", False),
            "disabled": data.get("disabled", False),
            "license_name": (
                data.get("license") or {}
            ).get("spdx_id", "Unknown"),
            "topics": json.dumps(
                data.get("topics", [])),
            "network_count": data.get(
                "network_count", 0),
            "subscribers_count": data.get(
                "subscribers_count", 0),
        }
        current_rows.append(row)

    # ----------------------------------------------------------
    # 2. Back-project to HISTORY_START and simulate evolution
    #    day by day until DAILY_END
    # ----------------------------------------------------------
    total_days = (DAILY_END - HISTORY_START).days  # ~102

    # Evolving metrics and their simulation parameters:
    # (field, daily_rate_per_1000, can_decrease)
    # The rate is calibrated for ~1000 current units.
    EVOLVING_METRICS = {
        "stargazers_count": {"daily_rate_per_1k": 0.8,
                             "can_decrease": False},
        "forks_count":      {"daily_rate_per_1k": 0.15,
                             "can_decrease": False},
        "open_issues_count": {"daily_rate_per_1k": 0.4,
                              "can_decrease": True},
        "subscribers_count": {"daily_rate_per_1k": 0.05,
                              "can_decrease": False},
        "watchers_count":    {"daily_rate_per_1k": 0.05,
                              "can_decrease": False},
        "network_count":     {"daily_rate_per_1k": 0.1,
                              "can_decrease": False},
    }

    random.seed(42)  # reproducibility

    # For each repo, generate the complete timeline
    # Structure: {full_name: {day_offset: row_dict}}
    timelines = {}

    for row in current_rows:
        fname = row["full_name"]

        # Current values (= simulation endpoint)
        current_vals = {
            k: row[k] for k in EVOLVING_METRICS
        }

        # Back-calculate the starting value (HISTORY_START)
        start_vals = {}
        for metric, cfg in EVOLVING_METRICS.items():
            val = current_vals[metric]
            rate = cfg["daily_rate_per_1k"]
            # Estimated total growth over the period
            daily_delta = max(1, val / 1000) * rate
            total_growth = int(daily_delta * total_days)
            if cfg["can_decrease"]:
                # open_issues fluctuates: remove ~half
                total_growth = int(total_growth * 0.5)
            start_val = max(0, val - total_growth)
            start_vals[metric] = start_val

        # Simulate day by day from HISTORY_START
        day_rows = {}
        vals = dict(start_vals)

        for d in range(total_days + 1):
            day = HISTORY_START + timedelta(days=d)
            snap = copy.deepcopy(row)

            # Apply current day's values
            for metric in EVOLVING_METRICS:
                snap[metric] = int(vals[metric])

            # Update updated_at and pushed_at
            snap["updated_at"] = (
                day.isoformat() + "T12:00:00Z")
            snap["pushed_at"] = (
                day.isoformat() + "T10:00:00Z")
            snap["snapshot_date"] = day.isoformat()

            day_rows[day] = snap

            # Calculate delta for the next day
            if d < total_days:
                for metric, cfg in EVOLVING_METRICS.items():
                    base = max(1, vals[metric])
                    rate = cfg["daily_rate_per_1k"]
                    expected = max(0.3, base / 1000) * rate

                    if cfg["can_decrease"]:
                        # open_issues: +/- with slight positive bias
                        delta = random.gauss(
                            expected * 0.3, expected * 1.5)
                    else:
                        # stars/forks: always positive
                        # with Poisson-like variance
                        delta = max(0, random.gauss(
                            expected, expected * 0.6))

                    vals[metric] = max(
                        0, vals[metric] + delta)

        timelines[fname] = day_rows

    # ----------------------------------------------------------
    # 3. Write history file (snapshot at REF_DATE)
    # ----------------------------------------------------------
    ref_date = REF_DATE
    subdir = os.path.join(OUTPUT_DIR, "repositories")

    history_rows = []
    for fname, day_rows in timelines.items():
        if ref_date in day_rows:
            history_rows.append(day_rows[ref_date])

    filepath = os.path.join(
        subdir, "raw_repositories_history.csv")
    n = write_csv(filepath, history_rows, fields)
    print(f"  => history: {n} rows "
          f"(snapshot as of {ref_date})")

    # ----------------------------------------------------------
    # 4. Write daily files (Jan 1 - Feb 10)
    # ----------------------------------------------------------
    count = 0
    for i in range(1, INCREMENTAL_DAYS + 1):
        day = ref_date + timedelta(days=i)
        day_str = day.isoformat()
        day_rows_list = []
        for fname, day_rows in timelines.items():
            if day in day_rows:
                day_rows_list.append(day_rows[day])
        fp = os.path.join(
            subdir,
            f"raw_repositories_{day_str}.csv"
        )
        write_csv(fp, day_rows_list, fields)
        count += 1
    print(f"  => {count} daily files generated")

    # Log an example of evolution
    sample = list(timelines.keys())[0]
    t = timelines[sample]
    d0 = t[HISTORY_START]
    d1 = t[REF_DATE]
    d2 = t[DAILY_END]
    print(f"  Example [{sample}]:")
    print(f"    stars  : {d0['stargazers_count']:>8} "
          f"-> {d1['stargazers_count']:>8} "
          f"-> {d2['stargazers_count']:>8}")
    print(f"    forks  : {d0['forks_count']:>8} "
          f"-> {d1['forks_count']:>8} "
          f"-> {d2['forks_count']:>8}")
    print(f"    issues : {d0['open_issues_count']:>8} "
          f"-> {d1['open_issues_count']:>8} "
          f"-> {d2['open_issues_count']:>8}")

    return current_rows


def extract_commits(client):
    """
    Endpoint: GET /repos/{owner}/{repo}/commits
    Natural time series via author_date.
    """
    print("\n[2/4] Extracting commits...")

    fields = [
        "repo_full_name", "sha",
        "author_login", "author_date",
        "committer_login", "committer_date",
        "message",
    ]

    all_rows = []
    for repo in REPOS:
        print(f"  {repo}")
        commits = client.get_paginated(
            f"{BASE_URL}/repos/{repo}/commits",
            params={
                "since": HISTORY_START.isoformat() + "T00:00:00Z",
                "until": (DAILY_END + timedelta(days=1)).isoformat() + "T00:00:00Z",
            })
        for c in commits:
            cd = c.get("commit", {})
            row = {
                "repo_full_name": repo,
                "sha": c.get("sha"),
                "author_login": (
                    c.get("author") or {}
                ).get("login", "unknown"),
                "author_date": cd.get(
                    "author", {}).get("date"),
                "committer_login": (
                    c.get("committer") or {}
                ).get("login", "unknown"),
                "committer_date": cd.get(
                    "committer", {}).get("date"),
                "message": (
                    cd.get("message") or ""
                )[:300].replace("\n", " "),
            }
            all_rows.append(row)

    # Split by date
    ref_date = REF_DATE
    subdir = os.path.join(OUTPUT_DIR, "commits")

    history, daily = split_by_date(
        all_rows, "author_date", ref_date,
        HISTORY_DAYS, INCREMENTAL_DAYS)

    fp = os.path.join(subdir, "raw_commits_history.csv")
    n = write_csv(fp, history, fields)
    print(f"  => history: {n} rows")

    nd = generate_daily_files(
        daily, ref_date, INCREMENTAL_DAYS,
        "raw_commits", fields, subdir)
    print(f"  => {nd} daily files generated")

    return all_rows


def extract_pull_requests(client):
    """
    Endpoint: GET /repos/{owner}/{repo}/pulls?state=all
    Time series via created_at.
    """
    print("\n[3/4] Extracting pull requests...")

    fields = [
        "repo_full_name", "pr_number", "title",
        "state", "user_login",
        "created_at", "updated_at",
        "closed_at", "merged_at",
        "draft", "comments",
        "review_comments", "labels",
    ]

    all_rows = []
    for repo in REPOS:
        print(f"  {repo}")
        prs = client.get_paginated(
            f"{BASE_URL}/repos/{repo}/pulls",
            params={
                "state": "all",
                "sort": "created",
                "direction": "desc",
            })
        for pr in prs:
            labels = [
                l.get("name", "")
                for l in pr.get("labels", [])
            ]
            row = {
                "repo_full_name": repo,
                "pr_number": pr.get("number"),
                "title": (pr.get("title") or "")[:300],
                "state": pr.get("state"),
                "user_login": (
                    pr.get("user") or {}
                ).get("login", "unknown"),
                "created_at": pr.get("created_at"),
                "updated_at": pr.get("updated_at"),
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "draft": pr.get("draft", False),
                "comments": pr.get("comments", 0),
                "review_comments": pr.get(
                    "review_comments", 0),
                "labels": json.dumps(labels),
            }
            all_rows.append(row)

    ref_date = REF_DATE
    subdir = os.path.join(OUTPUT_DIR, "pull_requests")

    history, daily = split_by_date(
        all_rows, "created_at", ref_date,
        HISTORY_DAYS, INCREMENTAL_DAYS)

    fp = os.path.join(
        subdir, "raw_pull_requests_history.csv")
    n = write_csv(fp, history, fields)
    print(f"  => history: {n} rows")

    nd = generate_daily_files(
        daily, ref_date, INCREMENTAL_DAYS,
        "raw_pull_requests", fields, subdir)
    print(f"  => {nd} daily files generated")

    return all_rows


def extract_issues(client):
    """
    Endpoint: GET /repos/{owner}/{repo}/issues?state=all
    Time series via created_at.
    """
    print("\n[4/4] Extracting issues...")

    fields = [
        "repo_full_name", "issue_number", "title",
        "state", "user_login",
        "created_at", "updated_at", "closed_at",
        "comments", "labels", "is_pull_request",
    ]

    all_rows = []
    for repo in REPOS:
        print(f"  {repo}")
        issues = client.get_paginated(
            f"{BASE_URL}/repos/{repo}/issues",
            params={
                "state": "all",
                "since": HISTORY_START.isoformat() + "T00:00:00Z",
                "sort": "created",
                "direction": "desc",
            })
        for issue in issues:
            labels = [
                l.get("name", "")
                for l in issue.get("labels", [])
            ]
            row = {
                "repo_full_name": repo,
                "issue_number": issue.get("number"),
                "title": (
                    issue.get("title") or "")[:300],
                "state": issue.get("state"),
                "user_login": (
                    issue.get("user") or {}
                ).get("login", "unknown"),
                "created_at": issue.get("created_at"),
                "updated_at": issue.get("updated_at"),
                "closed_at": issue.get("closed_at"),
                "comments": issue.get("comments", 0),
                "labels": json.dumps(labels),
                "is_pull_request": (
                    "pull_request" in issue),
            }
            all_rows.append(row)

    ref_date = REF_DATE
    subdir = os.path.join(OUTPUT_DIR, "issues")

    history, daily = split_by_date(
        all_rows, "created_at", ref_date,
        HISTORY_DAYS, INCREMENTAL_DAYS)

    fp = os.path.join(subdir, "raw_issues_history.csv")
    n = write_csv(fp, history, fields)
    print(f"  => history: {n} rows")

    nd = generate_daily_files(
        daily, ref_date, INCREMENTAL_DAYS,
        "raw_issues", fields, subdir)
    print(f"  => {nd} daily files generated")

    return all_rows


# ============================================================
# MAIN
# ============================================================
def print_summary():
    """Display the generated file tree."""
    print("\n" + "=" * 60)
    print("GENERATED FILE TREE")
    print("=" * 60)
    total_files = 0
    for root, dirs, files in os.walk(OUTPUT_DIR):
        level = root.replace(OUTPUT_DIR, "").count(os.sep)
        indent = "  " * level
        folder = os.path.basename(root)
        csv_files = [f for f in files if f.endswith(".csv")]
        total_files += len(csv_files)
        if csv_files:
            # Display the folder with a summary
            hist = [f for f in csv_files if "history" in f]
            daily = [f for f in csv_files if "history" not in f]
            print(f"{indent}{folder}/")
            for h in hist:
                print(f"{indent}  {h}")
            if daily:
                first = sorted(daily)[0]
                last = sorted(daily)[-1]
                print(f"{indent}  {first}")
                print(f"{indent}  ... ({len(daily) - 2} "
                      f"intermediate files)")
                print(f"{indent}  {last}")

    print(f"\nTotal: {total_files} CSV files")
    print(f"  = 4 endpoints x "
          f"(1 history + {INCREMENTAL_DAYS} daily)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate GitHub CSVs for the dbt lab")
    parser.add_argument(
        "--token", "-t",
        help="GitHub token (recommended for 5000 req/h)",
        default=None)
    parser.add_argument(
        "--output", "-o",
        help="Output directory",
        default=OUTPUT_DIR)
    args = parser.parse_args()

    out_dir = args.output

    client = GitHubClient(token=args.token)

    print("=" * 60)
    print("GITHUB CSV GENERATION — INSTRUCTOR SCRIPT")
    print("=" * 60)
    if args.token:
        print("  Mode: authenticated (5000 req/h)")
    else:
        print("  Mode: unauthenticated (60 req/h)")
        print("  WARNING: a token is recommended!")

    print(f"  Repos: {len(REPOS)}")
    print(f"  Endpoints: 4 (repos, commits, PRs, issues)")
    print(f"  History: {HISTORY_START} -> {REF_DATE} "
          f"({HISTORY_DAYS} days)")
    print(f"  Daily files: {REF_DATE + timedelta(days=1)} -> "
          f"{DAILY_END} ({INCREMENTAL_DAYS} files)")
    print(f"  Expected files: "
          f"{4 * (1 + INCREMENTAL_DAYS)}")
    print(f"  Output: {out_dir}")

    os.makedirs(out_dir, exist_ok=True)

    # extract_repositories(client)
    # extract_commits(client)
    extract_pull_requests(client)
    extract_issues(client)

    print_summary()

    print(f"\nAPI requests made: "
          f"{client.request_count}")
    print("GENERATION COMPLETE")


if __name__ == "__main__":
    main()