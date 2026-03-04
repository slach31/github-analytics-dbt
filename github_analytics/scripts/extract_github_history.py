#!/usr/bin/env python3
"""
extract_github_history.py
=========================
Queries the GitHub API and generates 4 history CSV files
covering 3 months (October 1 to December 31, 2025):
  - raw_repositories_history.csv  (snapshot as of Dec 31, 2025)
  - raw_commits_history.csv       (commits from Oct 1 to Dec 31)
  - raw_pull_requests_history.csv (PRs created from Oct 1 to Dec 31)
  - raw_issues_history.csv        (issues created from Oct 1 to Dec 31)

Usage:
  python extract_github_history.py [--token GITHUB_TOKEN]

With a token, the rate limit is 5000 req/h (instead of 60).
A token is STRONGLY recommended.
"""

import requests
import csv
import os
import time
import json
import argparse
import random
import copy
from datetime import datetime, date, timedelta

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
MAX_PAGES = 5

# History window: October 1 -> December 31, 2025
HISTORY_START = date(2025, 10, 1)
HISTORY_END = date(2025, 12, 31)
HISTORY_DAYS = (HISTORY_END - HISTORY_START).days  # 92

# Output directory
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data", "raw"
)


# ============================================================
# GITHUB CLIENT
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


# ============================================================
# HELPERS
# ============================================================
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


def filter_by_date_range(rows, date_field):
    """Keep only rows within [HISTORY_START, HISTORY_END]."""
    filtered = []
    for row in rows:
        d = parse_date(row.get(date_field))
        if d is None:
            filtered.append(row)  # no date -> keep
        elif HISTORY_START <= d <= HISTORY_END:
            filtered.append(row)
    return filtered


# ============================================================
# EXTRACTORS
# ============================================================
def extract_repositories(client):
    """
    Endpoint: GET /repos/{owner}/{repo}
    Returns a snapshot (current state, not historical).
    We simulate a realistic snapshot as of HISTORY_END by
    back-projecting current API values over the history window.
    """
    print("\n[1/4] Extracting repository metadata...")

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
    ]

    # Metrics that evolve over time and their simulation params
    EVOLVING_METRICS = {
        "stargazers_count":  {"daily_rate_per_1k": 0.8,
                              "can_decrease": False},
        "forks_count":       {"daily_rate_per_1k": 0.15,
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

    

    # 3. Write CSV
    filepath = os.path.join(OUTPUT_DIR, "raw_repositories_history.csv")
    n = write_csv(filepath, current_rows, fields)
    print(f"  => {n} rows written")


def extract_commits(client):
    """
    Endpoint: GET /repos/{owner}/{repo}/commits
    Time series via author_date.
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
                "since": HISTORY_START.isoformat()
                         + "T00:00:00Z",
                "until": (HISTORY_END + timedelta(days=1)
                          ).isoformat() + "T00:00:00Z",
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

    # Filter to history window and write
    history = filter_by_date_range(all_rows, "author_date")
    filepath = os.path.join(OUTPUT_DIR, "raw_commits_history.csv")
    n = write_csv(filepath, history, fields)
    print(f"  => {n} rows written")


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

    # Filter to history window and write
    history = filter_by_date_range(all_rows, "created_at")
    filepath = os.path.join(
        OUTPUT_DIR, "raw_pull_requests_history.csv")
    n = write_csv(filepath, history, fields)
    print(f"  => {n} rows written")


def extract_issues(client):
    """
    Endpoint: GET /repos/{owner}/{repo}/issues?state=all
    Time series via created_at.
    Note: this endpoint also returns PRs (filtered in Silver).
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
                "since": HISTORY_START.isoformat()
                         + "T00:00:00Z",
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

    # Filter to history window and write
    history = filter_by_date_range(all_rows, "created_at")
    filepath = os.path.join(OUTPUT_DIR, "raw_issues_history.csv")
    n = write_csv(filepath, history, fields)
    print(f"  => {n} rows written")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Extract GitHub history CSVs for the dbt lab")
    parser.add_argument(
        "--token", "-t",
        help="GitHub token (recommended for 5000 req/h)",
        default=None)
    args = parser.parse_args()

    client = GitHubClient(token=args.token)

    print("=" * 60)
    print("GITHUB HISTORY CSV EXTRACTION")
    print("=" * 60)
    if args.token:
        print("  Mode: authenticated (5000 req/h)")
    else:
        print("  Mode: unauthenticated (60 req/h)")
        print("  WARNING: a token is recommended!")
    print(f"  Repos: {len(REPOS)}")
    print(f"  History window: {HISTORY_START} -> "
          f"{HISTORY_END} ({HISTORY_DAYS} days)")
    print(f"  Output: {OUTPUT_DIR}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    extract_repositories(client)
    extract_commits(client)
    extract_pull_requests(client)
    extract_issues(client)

    # Summary
    print("\n" + "=" * 60)
    print("FILES GENERATED")
    print("=" * 60)
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith(".csv"):
            path = os.path.join(OUTPUT_DIR, f)
            size = os.path.getsize(path)
            print(f"  {f:.<40s} {size:>10,} bytes")

    print(f"\nAPI requests made: {client.request_count}")
    print("EXTRACTION COMPLETE")


if __name__ == "__main__":
    main()
