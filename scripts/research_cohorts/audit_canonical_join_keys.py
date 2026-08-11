"""Profile the actual cache-side keys for player and team-week evidence.

This is a read-only contract audit.  It deliberately separates two grains:

* player facts: one cache row, requiring a player identity and a team identity;
* matchup facts: one fantasy team in one league-week, which legitimately fans
  out to every player row on that team.

It does not choose a fallback by itself.  The report says exactly which cache
keys are unique, which are usable only as a one-to-many fan-out, and when a
manager based bridge is unsafe because the manager owns multiple teams in the
same league-week.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalised_text(name: str) -> str:
    return f"NULLIF(REGEXP_REPLACE(TRIM(LOWER(CAST({_q(name)} AS VARCHAR))), '\\s+', ' ', 'g'), '')"


def _non_null_filter(columns: list[str]) -> str:
    return " AND ".join(f"{_q(column)} IS NOT NULL" for column in columns)


def _key_stats(con: duckdb.DuckDBPyConnection, relation: str, columns: list[str]) -> dict[str, int | bool]:
    where = _non_null_filter(columns)
    grouped = f"SELECT {', '.join(_q(column) for column in columns)}, COUNT(*) AS n FROM {relation} WHERE {where} GROUP BY 1,2,3,4,5" if len(columns) == 5 else (
        f"SELECT {', '.join(_q(column) for column in columns)}, COUNT(*) AS n FROM {relation} WHERE {where} GROUP BY {', '.join(str(index + 1) for index in range(len(columns)))}"
    )
    rows, groups, duplicate_groups, duplicate_rows = con.execute(f"""
        SELECT
          COALESCE(SUM(n), 0),
          COUNT(*),
          COUNT(*) FILTER (WHERE n > 1),
          COALESCE(SUM(n - 1) FILTER (WHERE n > 1), 0)
        FROM ({grouped})
    """).fetchone()
    return {
        "joinable_rows": int(rows or 0),
        "distinct_keys": int(groups or 0),
        "duplicate_key_groups": int(duplicate_groups or 0),
        "duplicate_rows_after_first": int(duplicate_rows or 0),
        "unique": int(duplicate_groups or 0) == 0,
    }


def _team_fanout_stats(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, int | float]:
    where = _non_null_filter(["db_name", "year", "week", "team_key"])
    groups = f"""
        SELECT db_name, "year", week, team_key, COUNT(*) AS player_rows
        FROM {relation}
        WHERE {where}
        GROUP BY 1,2,3,4
    """
    team_groups, player_rows, min_fanout, max_fanout, avg_fanout = con.execute(f"""
        SELECT COUNT(*), COALESCE(SUM(player_rows), 0), MIN(player_rows), MAX(player_rows), AVG(player_rows)
        FROM ({groups})
    """).fetchone()
    return {
        "team_groups": int(team_groups or 0),
        "player_rows": int(player_rows or 0),
        "min_player_fanout": int(min_fanout or 0),
        "max_player_fanout": int(max_fanout or 0),
        "avg_player_fanout": float(avg_fanout or 0),
    }


def _manager_bridge_stats(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    *,
    include_team_name: bool,
) -> dict[str, int]:
    select_identities = [f"{_normalised_text('manager')} AS manager_key"]
    fields = ["db_name", '"year"', "week", "manager_key"]
    if include_team_name:
        select_identities.append(f"{_normalised_text('team_name')} AS team_name_key")
        fields.append("team_name_key")
    grouped = f"""
        WITH prepared AS (
          SELECT db_name, "year", week, team_key, {', '.join(select_identities)}
          FROM {relation}
        ), groups AS (
          SELECT {', '.join(fields)}, COUNT(DISTINCT team_key) AS n_team_keys
          FROM prepared
          WHERE manager_key IS NOT NULL AND team_key IS NOT NULL
          {'AND team_name_key IS NOT NULL' if include_team_name else ''}
          GROUP BY {', '.join(str(index + 1) for index in range(len(fields)))}
        )
        SELECT
          COUNT(*),
          COUNT(*) FILTER (WHERE n_team_keys=1),
          COUNT(*) FILTER (WHERE n_team_keys>1),
          COALESCE(MAX(n_team_keys), 0)
        FROM groups
    """
    total, single, multi, max_teams = con.execute(grouped).fetchone()
    return {
        "joinable_groups": int(total or 0),
        "single_team_groups": int(single or 0),
        "multi_team_groups": int(multi or 0),
        "max_team_keys_in_group": int(max_teams or 0),
    }


def profile_relation(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, Any]:
    columns = {row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "platform", "manager", "team_key", "team_name"}
    missing = sorted(required - columns)
    if missing:
        raise ValueError(f"missing required player columns: {missing}")
    player_keys = {
        "player_week_nfl": _key_stats(con, relation, ["db_name", "year", "week", "NFL_player_id"]),
        "player_week_nfl_team_key": _key_stats(con, relation, ["db_name", "year", "week", "NFL_player_id", "team_key"]),
        "player_week_nfl_manager": _key_stats(con, relation, ["db_name", "year", "week", "NFL_player_id", "manager"]),
        "player_week_nfl_manager_team_name": _key_stats(con, relation, ["db_name", "year", "week", "NFL_player_id", "manager", "team_name"]),
        "player_week_platform_nfl_team_key": _key_stats(con, relation, ["db_name", "year", "week", "platform", "NFL_player_id", "team_key"]),
    }
    return {
        "rows": int(con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]),
        "player_keys": player_keys,
        "team_keys": {"team_week_team_key": _team_fanout_stats(con, relation)},
        "manager_bridges": {
            "manager_only": _manager_bridge_stats(con, relation, include_team_name=False),
            "manager_team_name": _manager_bridge_stats(con, relation, include_team_name=True),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--relation", default="public.player_fantasy")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    con = duckdb.connect(str(args.base), read_only=True)
    try:
        result = profile_relation(con, args.relation)
    finally:
        con.close()
    result.update({
        "read_only": True,
        "relation": args.relation,
        "cache_mutated": False,
        "new_lineage": False,
    })
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
