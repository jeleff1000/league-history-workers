Exit code: 0
Wall time: 1.6 seconds
Output:
"""Discover a unique identity key for the fixed canonical player table."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


CANONICAL = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar", "manager",
    "team_points", "final_playoff_seed", "is_playoffs", "has_po_signal", "player",
    "position", "fantasy_position", "platform", "team_key", "team_name", "nfl_team_api",
    "yahoo_player_id", "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def atom(name: str) -> str:
    return f"COALESCE(CAST({q(name)} AS VARCHAR), '<NULL>')"


def pid() -> str:
    fallback = "COALESCE(" + ", ".join(
        f"NULLIF(TRIM(CAST({q(c)} AS VARCHAR)), '')"
        for c in ("NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "mfl_player_id", "espn_player_id", "yahoo_player_id")
    ) + ")"
    platform = "LOWER(NULLIF(TRIM(CAST(\"platform\" AS VARCHAR)), ''))"
    return (
        "COALESCE(CASE " + platform +
        " WHEN 'sleeper' THEN NULLIF(TRIM(CAST(\"sleeper_player_id\" AS VARCHAR)), '')" +
        " WHEN 'fleaflicker' THEN NULLIF(TRIM(CAST(\"fleaflicker_player_id\" AS VARCHAR)), '')" +
        " WHEN 'mfl' THEN NULLIF(TRIM(CAST(\"mfl_player_id\" AS VARCHAR)), '')" +
        " END, " + fallback + ")"
    )


def key_expr(columns: list[str], include_pid: bool = True) -> str:
    parts = [atom(c) for c in columns]
    if include_pid:
        parts.append(f"COALESCE(CAST(({pid()}) AS VARCHAR), '<NULL>')")
    return "CONCAT_WS('|', " + ", ".join(parts) + ")"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    con = duckdb.connect(str(args.base), read_only=True)
    try:
        cols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
        if cols != CANONICAL:
            raise SystemExit(json.dumps({"schema_ok": False, "columns": cols}))
        variants = [
            ("manager_team_pid", ["db_name", "year", "week", "manager", "team_name", "team_key"], True),
            ("manager_team_pid_fantasy_position", ["db_name", "year", "week", "manager", "team_name", "team_key", "fantasy_position"], True),
            ("manager_team_pid_position_fantasy_position", ["db_name", "year", "week", "manager", "team_name", "team_key", "position", "fantasy_position"], True),
            ("manager_team_pid_player_position_fantasy_position", ["db_name", "year", "week", "manager", "team_name", "team_key", "player", "position", "fantasy_position"], True),
            ("manager_team_pid_player_position_fantasy_position_started_rostered", ["db_name", "year", "week", "manager", "team_name", "team_key", "player", "position", "fantasy_position", "is_started", "is_rostered"], True),
            ("manager_team_platform_pid_fantasy_position", ["db_name", "year", "week", "manager", "team_name", "team_key", "platform", "fantasy_position"], True),
        ]
        results = []
        for name, columns, include_pid in variants:
            key = key_expr(columns, include_pid)
            total, distinct, duplicate_groups = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT {key}), COUNT(*) FROM (SELECT {key} AS k FROM public.player_fantasy GROUP BY k HAVING COUNT(*) > 1)"
            ).fetchone()
            results.append({
                "name": name,
                "columns": columns + (["coalesced_player_id"] if include_pid else []),
                "duplicate_rows": int(total or 0),
                "distinct_keys": int(distinct or 0),
                "duplicate_key_groups": int(duplicate_groups or 0),
                "unique": int(total or 0) == 0,
            })
        chosen = next((r for r in results if r["unique"]), None)
        out = {"read_only": True, "schema_ok": True, "canonical_columns": cols, "variants": results, "chosen": chosen, "cache_mutated": False, "new_lineage": False}
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(out, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()


