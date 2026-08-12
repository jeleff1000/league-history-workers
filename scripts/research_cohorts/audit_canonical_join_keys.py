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


def _fanout_stats(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    *,
    columns: list[str],
    group_label: str,
) -> dict[str, int | float]:
    where = _non_null_filter(columns)
    grouped = f"""
        SELECT {', '.join(_q(column) for column in columns)}, COUNT(*) AS player_rows
        FROM {relation}
        WHERE {where}
        GROUP BY {', '.join(str(index + 1) for index in range(len(columns)))}
    """
    count, player_rows, min_fanout, max_fanout, avg_fanout = con.execute(f"""
        SELECT COUNT(*), COALESCE(SUM(player_rows), 0), MIN(player_rows), MAX(player_rows), AVG(player_rows)
        FROM ({grouped})
    """).fetchone()
    return {
        group_label: int(count or 0),
        "player_rows": int(player_rows or 0),
        "min_player_fanout": int(min_fanout or 0),
        "max_player_fanout": int(max_fanout or 0),
        "avg_player_fanout": float(avg_fanout or 0),
    }


def _mfl_id_coverage(
    con: duckdb.DuckDBPyConnection, relation: str, columns: set[str]
) -> list[dict[str, int | str]]:
    """Report whether MFL player IDs survive in canonical rows.

    A raw MFL roster bridge can use a direct native-ID recipient only if this
    field is populated. This is an observation, not a fallback join rule.
    """
    mfl_id = (
        f"NULLIF(TRIM(CAST({_q('mfl_player_id')} AS VARCHAR)), '')"
        if "mfl_player_id" in columns else "NULL::VARCHAR"
    )
    nfl_id = f"NULLIF(TRIM(CAST({_q('NFL_player_id')} AS VARCHAR)), '')"
    rows = con.execute(f"""
        SELECT
          LOWER(TRIM(CAST({_q('platform')} AS VARCHAR))) AS platform,
          COUNT(*) AS rows,
          COUNT(*) FILTER (WHERE {nfl_id} IS NOT NULL) AS rows_with_nfl_player_id,
          COUNT(*) FILTER (WHERE {mfl_id} IS NOT NULL) AS rows_with_mfl_player_id,
          COUNT(*) FILTER (WHERE {mfl_id} IS NOT NULL AND {nfl_id} IS NOT NULL)
            AS rows_with_mfl_and_nfl_player_id
        FROM {relation}
        WHERE LOWER(TRIM(CAST({_q('platform')} AS VARCHAR))) = 'mfl'
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    return [
        {
            "platform": str(platform),
            "rows": int(count or 0),
            "rows_with_nfl_player_id": int(nfl_count or 0),
            "rows_with_mfl_player_id": int(mfl_count or 0),
            "rows_with_mfl_and_nfl_player_id": int(both_count or 0),
        }
        for platform, count, nfl_count, mfl_count, both_count in rows
    ]


def _native_player_id_expression(columns: set[str]) -> str:
    def source_id(name: str) -> str:
        return f"NULLIF(TRIM(CAST({_q(name)} AS VARCHAR)), '')" if name in columns else "NULL::VARCHAR"
    return """
        COALESCE(
          CASE LOWER(NULLIF(TRIM(CAST("platform" AS VARCHAR)), ''))
            WHEN 'sleeper' THEN {sleeper}
            WHEN 'fleaflicker' THEN {fleaflicker}
            WHEN 'mfl' THEN {mfl}
            WHEN 'espn' THEN {espn}
            WHEN 'yahoo' THEN {yahoo}
          END,
          {nfl}
        )
    """.format(
        sleeper=source_id("sleeper_player_id"),
        fleaflicker=source_id("fleaflicker_player_id"),
        mfl=source_id("mfl_player_id"),
        espn=source_id("espn_player_id"),
        yahoo=source_id("yahoo_player_id"),
        nfl=source_id("NFL_player_id"),
    )


def profile_relation(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, Any]:
    columns = {row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "platform", "manager", "team_key", "team_name"}
    missing = sorted(required - columns)
    if missing:
        raise ValueError(f"missing required player columns: {missing}")
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW canonical_join_key_audit AS
        SELECT *,
               {_normalised_text('manager')} AS manager_key,
               {_normalised_text('team_name')} AS team_name_key,
               {_native_player_id_expression(columns)} AS native_player_id
        FROM {relation}
    """)
    prepared = "canonical_join_key_audit"
    player_keys = {
        "player_week_nfl": _key_stats(con, prepared, ["db_name", "year", "week", "NFL_player_id"]),
        "player_week_nfl_team_key": _key_stats(con, prepared, ["db_name", "year", "week", "NFL_player_id", "team_key"]),
        "player_week_nfl_manager": _key_stats(con, prepared, ["db_name", "year", "week", "NFL_player_id", "manager_key"]),
        "player_week_nfl_manager_team_name": _key_stats(con, prepared, ["db_name", "year", "week", "NFL_player_id", "manager_key", "team_name_key"]),
        "player_week_platform_native_id_manager": _key_stats(con, prepared, ["db_name", "year", "week", "platform", "native_player_id", "manager_key"]),
    }
    component_population = {
        column: int(con.execute(f"SELECT COUNT(*) FROM {prepared} WHERE {_q(column)} IS NOT NULL").fetchone()[0])
        for column in ("NFL_player_id", "native_player_id", "manager_key", "team_key", "team_name_key")
    }
    return {
        "rows": int(con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]),
        "non_null_component_rows": component_population,
        "player_keys": player_keys,
        "platform_id_coverage": _mfl_id_coverage(con, prepared, columns),
        "team_keys": {"team_week_team_key": _team_fanout_stats(con, prepared)},
        "manager_fanout": {
            "manager_week": _fanout_stats(
                con, prepared, columns=["db_name", "year", "week", "manager_key"], group_label="manager_groups",
            ),
            "manager_team_name_week": _fanout_stats(
                con, prepared, columns=["db_name", "year", "week", "manager_key", "team_name_key"], group_label="manager_groups",
            ),
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
