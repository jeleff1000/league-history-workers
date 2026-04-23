#!/usr/bin/env python3
"""
Resolve the database name for a league.

Single source of truth for database name resolution across all import workflows.
Prints the resolved name to stdout (last line).

Usage:
    python .github/scripts/resolve_db_name.py \
        --league-id <id> --league-name <name> --platform <yahoo|sleeper|espn> \
        [--database-name <pre-computed>]
"""

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.request


def fly_query(sql, database="___ops"):
    """Query via Fly.io database server."""
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/") + "/query"
    token = os.environ["DATABASE_READ_TOKEN"]
    data = json.dumps({"sql": sql, "database": database}).encode()
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def slugify(name: str) -> str:
    """Convert league name to a valid database name.
    Matches sanitize_database_name() in db_utils.py."""
    x = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not x:
        return "league"
    if x[0].isdigit():
        x = f"l_{x}"
    return x[:63]


def extract_yahoo_league_number(yahoo_league_key: str) -> str | None:
    """Extract stable numeric suffix from Yahoo league key.
    '449.l.198278' -> '198278', '198278' -> '198278', else None."""
    if not yahoo_league_key:
        return None
    key_str = str(yahoo_league_key).strip()
    m = re.match(r"^\d+\.l\.(\d+)$", key_str)
    if m:
        return m.group(1)
    if key_str.isdigit():
        return key_str
    return None


def check_db_exists(db_name: str) -> bool:
    """Check if a database exists via Fly.io."""
    try:
        result = fly_query(
            "SELECT 1 FROM information_schema.schemata "
            f"WHERE catalog_name = '{db_name}' LIMIT 1",
            database=db_name
        )
        return len(result) > 0
    except Exception:
        return False


def check_league_id_in_db(db_name: str, league_id: str, platform: str) -> bool:
    """Check if a league_id exists in a database's matchup table."""
    try:
        if platform == "yahoo":
            yahoo_number = extract_yahoo_league_number(league_id)
            if yahoo_number:
                result = fly_query(
                    f"SELECT 1 FROM public.matchup "
                    f"WHERE CAST(league_id AS VARCHAR) LIKE '%.l.{yahoo_number}' "
                    "LIMIT 1",
                    database=db_name
                )
                return len(result) > 0

        result = fly_query(
            f"SELECT 1 FROM public.matchup "
            f"WHERE CAST(league_id AS VARCHAR) = '{league_id}' "
            "LIMIT 1",
            database=db_name
        )
        return len(result) > 0
    except Exception:
        return False


def lookup_mapping_table(league_id: str, platform: str) -> str | None:
    """Check the ___ops mapping table for an existing database name."""
    try:
        if platform == "yahoo":
            yahoo_number = extract_yahoo_league_number(league_id)
            if yahoo_number:
                result = fly_query(
                    "SELECT database_name FROM main.league_credentials "
                    f"WHERE CAST(league_id AS VARCHAR) LIKE '%.l.{yahoo_number}' "
                    "LIMIT 1"
                )
                if result and result[0].get("database_name"):
                    return result[0]["database_name"]
        elif platform == "sleeper":
            result = fly_query(
                "SELECT database_name FROM main.sleeper_leagues "
                f"WHERE sleeper_league_id = '{league_id}' "
                "LIMIT 1"
            )
            if result and result[0].get("database_name"):
                return result[0]["database_name"]
        elif platform == "espn":
            result = fly_query(
                "SELECT database_name FROM main.espn_leagues "
                f"WHERE espn_league_id = {int(league_id)} "
                "LIMIT 1"
            )
            if result and result[0].get("database_name"):
                return result[0]["database_name"]
    except Exception as e:
        print(f"[resolve] Mapping table lookup failed for {platform}: {e}", file=sys.stderr)
    return None


def check_registry_collision(base_name: str, league_id: str, platform: str) -> bool:
    """Check if base_name is registered to a DIFFERENT league in ANY platform's registry.

    Returns True if another league owns the name (collision), False if free.
    """
    registry_queries = [
        (
            "yahoo",
            "SELECT league_id FROM main.league_credentials "
            f"WHERE database_name = '{base_name}' LIMIT 1",
        ),
        (
            "sleeper",
            "SELECT sleeper_league_id as league_id FROM main.sleeper_leagues "
            f"WHERE database_name = '{base_name}' LIMIT 1",
        ),
        (
            "espn",
            "SELECT espn_league_id as league_id FROM main.espn_leagues "
            f"WHERE database_name = '{base_name}' LIMIT 1",
        ),
    ]
    for reg_platform, query in registry_queries:
        try:
            result = fly_query(query)
            if result and result[0].get("league_id"):
                registered_id = str(result[0]["league_id"])
                # Same platform + same league = reimport, not collision
                if reg_platform == platform:
                    if platform == "yahoo":
                        own_number = extract_yahoo_league_number(league_id)
                        reg_number = extract_yahoo_league_number(registered_id)
                        if own_number and reg_number and own_number == reg_number:
                            continue
                    elif registered_id == str(league_id):
                        continue
                # Different league or different platform owns this name
                print(
                    f"[resolve] Registry collision: {base_name} owned by "
                    f"{reg_platform} league {registered_id}",
                    file=sys.stderr,
                )
                return True
        except Exception:
            pass
    return False


def resolve(league_id: str, league_name: str, platform: str, pre_computed_db: str = "") -> str:
    """Resolve the database name. Returns the name string."""

    server_url = os.environ.get("DATABASE_SERVER_URL", "")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        print("[resolve] WARNING: No DATABASE_SERVER_URL/DATABASE_READ_TOKEN, using slugified name", file=sys.stderr)
        return slugify(league_name)

    try:
        # 1. Check mapping table for THIS league (source of truth for reimports)
        mapped = lookup_mapping_table(league_id, platform)
        if mapped:
            print(f"[resolve] Found in {platform} mapping table: {mapped}", file=sys.stderr)
            return mapped

        # 2. Trust an explicit database_name when it does not belong to another league.
        # In Fly-first centralized storage the db_name may be canonical without existing as
        # a separate catalog, so registry ownership is a better idempotence signal than
        # catalog existence.
        if pre_computed_db:
            if check_registry_collision(pre_computed_db, league_id, platform):
                print(
                    f"[resolve] Pre-computed '{pre_computed_db}' collides with another league, resolving fresh",
                    file=sys.stderr,
                )
            else:
                if check_db_exists(pre_computed_db):
                    print(f"[resolve] Using pre-computed database_name: {pre_computed_db}", file=sys.stderr)
                else:
                    print(
                        f"[resolve] Trusting pre-computed database_name without catalog: {pre_computed_db}",
                        file=sys.stderr,
                    )
                return pre_computed_db

        # 3. Slugify and check for collisions
        base_name = slugify(league_name)
        print(f"[resolve] Not in mapping table, checking base name: {base_name}", file=sys.stderr)

        # 3a. Check ALL registry tables for ownership (catches deleted-but-registered DBs)
        if check_registry_collision(base_name, league_id, platform):
            hash_suffix = hashlib.md5(str(league_id).encode()).hexdigest()[:6]
            hashed_name = f"{base_name}_{hash_suffix}"
            print(f"[resolve] Registry collision, using: {hashed_name}", file=sys.stderr)
            return hashed_name

        if not check_db_exists(base_name):
            print(f"[resolve] Database {base_name} does not exist, will create", file=sys.stderr)
            return base_name

        # Base DB exists - check if it's the same league (reimport)
        if check_league_id_in_db(base_name, league_id, platform):
            print(f"[resolve] League {league_id} found in {base_name}, reusing (reimport)", file=sys.stderr)
            return base_name

        # Different league owns the base name - collision
        hash_suffix = hashlib.md5(str(league_id).encode()).hexdigest()[:6]
        hashed_name = f"{base_name}_{hash_suffix}"
        print(f"[resolve] Collision detected, using: {hashed_name}", file=sys.stderr)
        return hashed_name

    except Exception as e:
        print(f"[resolve] Error during resolution: {e}", file=sys.stderr)
        return slugify(league_name)


def main():
    parser = argparse.ArgumentParser(description="Resolve database name for a league")
    parser.add_argument("--league-id", required=True, help="Platform-specific league ID")
    parser.add_argument("--league-name", required=True, help="Human-readable league name")
    parser.add_argument("--platform", required=True, choices=["yahoo", "sleeper", "espn"])
    parser.add_argument("--database-name", default="", help="Pre-computed database name (trust if DB exists)")
    args = parser.parse_args()

    resolved = resolve(args.league_id, args.league_name, args.platform, args.database_name)
    print(resolved)


if __name__ == "__main__":
    main()
