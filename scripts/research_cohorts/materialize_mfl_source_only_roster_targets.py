"""Materialize exact MFL roster-fetch targets for source-only team signals.

The input is matchup-grain evidence: it can repair existing player rows by a
safe team/week fan-out, but it cannot create a player recipient on its own.
This script selects only MFL league-weeks where at least one team signal has
no safe canonical player recipient.  It is read-only and never opens or
modifies the cache beyond a read-only DuckDB connection.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _normalised(expression: str) -> str:
    return (
        "NULLIF(REGEXP_REPLACE(TRIM(LOWER(CAST("
        + expression
        + " AS VARCHAR))), '\\s+', ' ', 'g'), '')"
    )


def _team_key(expression: str) -> str:
    raw = f"LOWER(TRIM(CAST({expression} AS VARCHAR)))"
    return (
        "NULLIF(CASE WHEN INSTR(" + raw + ", '_') > 0 "
        "THEN REGEXP_EXTRACT(" + raw + ", '([^_]+)$', 1) ELSE " + raw + " END, '')"
    )


def build(*, base: Path, source: Path, out: Path) -> dict[str, Any]:
    con = duckdb.connect(str(base), read_only=True)
    try:
        target_columns = _columns(con, "public.player_fantasy")
        required_target = {"db_name", "year", "week", "manager", "team_key", "team_name", "platform"}
        missing_target = sorted(required_target - target_columns)
        if missing_target:
            raise ValueError(f"canonical player table missing required columns: {missing_target}")
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_source AS SELECT * FROM read_parquet({_path(source)})")
        source_columns = _columns(con, "raw_source")
        required_source = {"db_name", "year", "week", "league_id", "team_key", "manager", "team_name", "platform"}
        missing_source = sorted(required_source - source_columns)
        if missing_source:
            raise ValueError(f"raw team source missing required columns: {missing_source}")

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_signals AS
            SELECT DISTINCT
              CAST(db_name AS VARCHAR) AS db_name,
              TRY_CAST(year AS INTEGER) AS year,
              TRY_CAST(week AS INTEGER) AS week,
              NULLIF(TRIM(CAST(league_id AS VARCHAR)), '') AS source_id,
              NULLIF(TRIM(CAST(team_key AS VARCHAR)), '') AS team_key,
              {_team_key('team_key')} AS team_key_norm,
              {_normalised('manager')} AS manager_key,
              {_normalised('team_name')} AS team_name_key
            FROM raw_source
            WHERE LOWER(NULLIF(TRIM(CAST(platform AS VARCHAR)), '')) = 'mfl'
              AND TRY_CAST(year AS INTEGER) IS NOT NULL
              AND TRY_CAST(week AS INTEGER) IS NOT NULL
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE source_manager_counts AS
            SELECT db_name, year, week, manager_key,
                   COUNT(DISTINCT COALESCE(team_key_norm, team_name_key)) AS source_team_count
            FROM source_signals
            WHERE manager_key IS NOT NULL
            GROUP BY 1,2,3,4
        """)
        cache_manager = _normalised("p.manager")
        cache_team_name = _normalised("p.team_name")
        cache_team_key = _team_key("p.team_key")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE source_resolution AS
            SELECT s.*,
              EXISTS (
                SELECT 1 FROM public.player_fantasy p
                WHERE CAST(p.db_name AS VARCHAR) IS NOT DISTINCT FROM s.db_name
                  AND TRY_CAST(p.year AS INTEGER) IS NOT DISTINCT FROM s.year
                  AND TRY_CAST(p.week AS INTEGER) IS NOT DISTINCT FROM s.week
                  AND LOWER(NULLIF(TRIM(CAST(p.platform AS VARCHAR)), '')) = 'mfl'
                  AND (
                    (s.team_key IS NOT NULL AND s.team_key = NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), ''))
                    OR (s.team_key_norm IS NOT NULL AND s.team_key_norm = {cache_team_key})
                    OR (
                      s.manager_key IS NOT NULL
                      AND s.manager_key IS NOT DISTINCT FROM {cache_manager}
                      AND (
                        (s.team_name_key IS NOT NULL
                         AND s.team_name_key IS NOT DISTINCT FROM {cache_team_name})
                        OR COALESCE(mc.source_team_count, 0) = 1
                      )
                    )
                  )
              ) AS has_safe_player_recipient
            FROM source_signals s
            LEFT JOIN source_manager_counts mc
              ON mc.db_name IS NOT DISTINCT FROM s.db_name
             AND mc.year IS NOT DISTINCT FROM s.year
             AND mc.week IS NOT DISTINCT FROM s.week
             AND mc.manager_key IS NOT DISTINCT FROM s.manager_key
        """)
        source_team_weeks = int(con.execute("SELECT COUNT(*) FROM source_resolution").fetchone()[0])
        safe_recipient_team_weeks = int(con.execute(
            "SELECT COUNT(*) FROM source_resolution WHERE has_safe_player_recipient"
        ).fetchone()[0])
        source_only_team_weeks = source_team_weeks - safe_recipient_team_weeks
        invalid_source_ids = int(con.execute("""
            SELECT COUNT(*) FROM (
              SELECT db_name, year, week
              FROM source_resolution
              WHERE NOT has_safe_player_recipient
              GROUP BY 1,2,3
              HAVING COUNT(DISTINCT source_id) FILTER (WHERE source_id IS NOT NULL) > 1
            )
        """).fetchone()[0])
        if invalid_source_ids:
            raise ValueError(f"{invalid_source_ids} target league-weeks have conflicting MFL source IDs")
        groups = [
            {"db_name": db_name, "year": int(year), "week": int(week), "source_id": source_id}
            for db_name, year, week, source_id in con.execute("""
                SELECT db_name, year, week, MIN(source_id) AS source_id
                FROM source_resolution
                WHERE NOT has_safe_player_recipient
                GROUP BY 1,2,3
                ORDER BY 1,2,3
            """).fetchall()
        ]
        payload = {
            "groups": groups,
            "read_only": True,
            "source_only_team_weeks": source_only_team_weeks,
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "source_team_weeks": source_team_weeks,
            "safe_recipient_team_weeks": safe_recipient_team_weeks,
            "source_only_team_weeks": source_only_team_weeks,
            "target_league_weeks": len(groups),
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(base=args.base, source=args.source, out=args.out), sort_keys=True))


if __name__ == "__main__":
    main()
