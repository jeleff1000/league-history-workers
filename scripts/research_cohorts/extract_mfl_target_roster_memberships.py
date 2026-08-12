"""Fetch raw roster membership only for flagged MFL league-weeks.

The extractor is source-side only.  It writes roster-membership and MFL player
directory artifacts for the bridge receipt; it never opens or writes the
canonical cache.  Each target tries the settings league ID and the historical
seed ID encoded in ``smpl_mfl_<seed year>_<seed id>`` against both the target
season and seed season.  Every attempt is retained in the report.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Protocol


_DB_SEED = re.compile(r"^smpl_mfl_(?P<year>\d{4})_(?P<league_id>\d+)$", re.IGNORECASE)


class MFLClient(Protocol):
    def fetch_league(self, league_id: str, year: int) -> dict[str, Any] | None: ...
    def fetch_weekly_results(self, league_id: str, year: int, week: int) -> dict[str, Any] | None: ...
    def fetch_players(self, league_id: str, year: int) -> dict[str, dict[str, Any]]: ...


def _clean(value: object) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _as_list(value: object) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _get(mapping: object, *names: str) -> object:
    if not isinstance(mapping, dict):
        return None
    for name in names:
        if name in mapping:
            return mapping[name]
    return None


def _seed(db_name: str) -> tuple[int, str] | None:
    match = _DB_SEED.match(db_name)
    return (int(match["year"]), match["league_id"]) if match else None


def source_candidates(group: dict[str, Any]) -> list[dict[str, Any]]:
    """Return deterministic, de-duplicated source endpoint candidates."""
    db_name = str(group["db_name"])
    target_year = int(group["year"])
    settings_id = _clean(group.get("source_id"))
    seed = _seed(db_name)
    candidates: list[tuple[str | None, int, str]] = [
        (settings_id, target_year, "settings_id_target_year"),
    ]
    if seed:
        seed_year, seed_id = seed
        candidates.extend([
            (seed_id, target_year, "seed_id_target_year"),
            (settings_id, seed_year, "settings_id_seed_year"),
            (seed_id, seed_year, "seed_id_seed_year"),
        ])
    seen: set[tuple[str, int]] = set()
    out: list[dict[str, Any]] = []
    for league_id, source_year, provenance in candidates:
        if not league_id or (league_id, source_year) in seen:
            continue
        seen.add((league_id, source_year))
        out.append({"source_id": league_id, "source_year": int(source_year), "candidate_provenance": provenance})
    return out


def _franchises(weekly: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for matchup in _as_list(weekly.get("matchup")):
        if isinstance(matchup, dict):
            entries.extend(item for item in _as_list(matchup.get("franchise")) if isinstance(item, dict))
    entries.extend(item for item in _as_list(weekly.get("franchise")) if isinstance(item, dict))
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in entries:
        franchise_id = _clean(_get(item, "id", "franchise_id", "franchiseId"))
        if franchise_id and franchise_id not in seen:
            seen.add(franchise_id)
            output.append(item)
    return output


def _franchise_lookup(league: dict[str, Any]) -> dict[str, dict[str, Any]]:
    franchises = _get(_get(league, "franchises") or {}, "franchise")
    result: dict[str, dict[str, Any]] = {}
    for franchise in _as_list(franchises):
        if isinstance(franchise, dict) and (fid := _clean(_get(franchise, "id", "franchise_id"))):
            result[fid] = franchise
    return result


def _source_manager(entry: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> tuple[str | None, str | None]:
    fid = _clean(_get(entry, "id", "franchise_id", "franchiseId"))
    base = lookup.get(fid or "", {})
    for key in ("owner_name", "ownerName", "username"):
        value = _clean(_get(entry, key)) or _clean(_get(base, key))
        if value:
            return value, "mfl_franchise_owner_name"
    value = _clean(_get(entry, "name")) or _clean(_get(base, "name"))
    return (value, "mfl_franchise_name") if value else (None, None)


def _player_directory_rows(source_year: int, players: dict[str, dict[str, Any]], wanted_ids: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for player_id in sorted(wanted_ids, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value)):
        raw = players.get(player_id) or {}
        rows.append({
            "source_year": source_year,
            "mfl_player_id": player_id,
            "espn_id": _clean(raw.get("espn_id")),
            "raw_name": _clean(raw.get("name")),
            "raw_position": _clean(raw.get("position")),
            "raw_team": _clean(raw.get("team")),
        })
    return rows


def extract_group(group: dict[str, Any], client: MFLClient) -> dict[str, Any]:
    """Extract one targeted MFL league-week or return an explicit source status."""
    db_name, year, week = str(group["db_name"]), int(group["year"]), int(group["week"])
    attempts: list[dict[str, Any]] = []
    for candidate in source_candidates(group):
        source_id, source_year = candidate["source_id"], candidate["source_year"]
        attempt = {"source_id": source_id, "source_year": source_year}
        try:
            league = client.fetch_league(source_id, source_year)
        except Exception as exc:  # source evidence, never a successful lookup
            attempts.append({**attempt, "status": "league_error", "error": type(exc).__name__})
            continue
        if not isinstance(league, dict) or not league:
            attempts.append({**attempt, "status": "no_league"})
            continue
        try:
            weekly = client.fetch_weekly_results(source_id, source_year, week)
        except Exception as exc:
            attempts.append({**attempt, "status": "weekly_error", "error": type(exc).__name__})
            continue
        franchises = _franchises(weekly or {}) if isinstance(weekly, dict) else []
        if not franchises or not any(_as_list(franchise.get("player")) for franchise in franchises):
            attempts.append({**attempt, "status": "no_weekly_roster"})
            continue
        attempts.append({**attempt, "status": "resolved"})
        lookup = _franchise_lookup(league)
        memberships: list[dict[str, Any]] = []
        wanted_ids: set[str] = set()
        for franchise in franchises:
            franchise_id = _clean(_get(franchise, "id", "franchise_id", "franchiseId"))
            manager, manager_origin = _source_manager(franchise, lookup)
            for raw_player in _as_list(franchise.get("player")):
                if not isinstance(raw_player, dict) or not (player_id := _clean(raw_player.get("id"))):
                    continue
                wanted_ids.add(player_id)
                memberships.append({
                    "db_name": db_name,
                    "year": year,
                    "week": week,
                    "source_id": source_id,
                    "source_year": source_year,
                    "source_franchise_id": franchise_id,
                    "source_manager": manager,
                    "source_manager_origin": manager_origin,
                    "mfl_player_id": player_id,
                    "is_started": int(str(raw_player.get("status") or "").strip().lower() == "starter"),
                })
        try:
            players = client.fetch_players(source_id, source_year) or {}
        except Exception as exc:
            players = {}
            player_directory_status = f"player_directory_error:{type(exc).__name__}"
        else:
            player_directory_status = "resolved"
        return {
            "status": "resolved",
            "db_name": db_name,
            "year": year,
            "week": week,
            "source_id_used": source_id,
            "source_year_used": source_year,
            "candidate_provenance": candidate["candidate_provenance"],
            "attempted_candidates": attempts,
            "memberships": memberships,
            "player_directory_status": player_directory_status,
            "player_directory": _player_directory_rows(source_year, players, wanted_ids),
        }
    return {
        "status": "source_unresolved",
        "db_name": db_name,
        "year": year,
        "week": week,
        "attempted_candidates": attempts,
        "memberships": [],
        "player_directory": [],
    }


def _selected(group: dict[str, Any], shard: int, shards: int) -> bool:
    value = f"{group['db_name']}|{group['year']}|{group['week']}"
    return int(hashlib.sha256(value.encode()).hexdigest()[:12], 16) % shards == shard


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--out-memberships", type=Path, required=True)
    parser.add_argument("--out-player-directory", type=Path, required=True)
    parser.add_argument("--out-report", type=Path, required=True)
    args = parser.parse_args()
    if not 0 <= args.shard < args.shards:
        raise SystemExit("shard must be in [0, shards)")
    from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIClient
    import duckdb
    import pandas as pd

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    groups = [group for group in manifest["groups"] if _selected(group, args.shard, args.shards)]
    client = MFLAPIClient()
    results = [extract_group(group, client) for group in groups]
    memberships = [row for result in results for row in result["memberships"]]
    directory = [row for result in results for row in result["player_directory"]]
    args.out_memberships.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("memberships", pd.DataFrame(memberships))
    con.register("directory", pd.DataFrame(directory))
    con.execute("COPY memberships TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(args.out_memberships)])
    con.execute("COPY directory TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(args.out_player_directory)])
    con.close()
    report = {
        "read_only": True,
        "cache_mutated": False,
        "new_lineage": False,
        "shard": args.shard,
        "shards": args.shards,
        "groups": len(groups),
        "resolved_groups": sum(result["status"] == "resolved" for result in results),
        "source_unresolved_groups": sum(result["status"] != "resolved" for result in results),
        "memberships": len(memberships),
        "player_directory_rows": len(directory),
        "results": [{key: value for key, value in result.items() if key not in {"memberships", "player_directory"}} for result in results],
    }
    args.out_report.parent.mkdir(parents=True, exist_ok=True)
    args.out_report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in report if key != "results"}, sort_keys=True))


if __name__ == "__main__":
    main()
