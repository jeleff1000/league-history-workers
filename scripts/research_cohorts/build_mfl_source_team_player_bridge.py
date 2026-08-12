"""Build a source-proven MFL team-to-player bridge without touching the cache.

MFL roster membership identifies a franchise's native player IDs.  The raw
team/week artifact identifies that franchise's outcome signals.  This script
combines those two source facts through the year-specific MFL→NFL crosswalk.
It deliberately does *not* require the canonical cache's current manager
label: that label is the field whose missing/incorrect identity prevented the
old bridge from carrying valid source evidence forward.

The emitted parquet has the exact player-bridge contract consumed by
``audit_team_signal_candidates.py``.  That later read-only audit still decides
whether a source team can fan out to one existing canonical player row.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


MEMBERSHIP_REQUIRED = {
    "db_name", "year", "week", "source_year", "source_franchise_id", "mfl_player_id",
}
CROSSWALK_REQUIRED = {"source_year", "mfl_player_id", "NFL_player_id"}
SIGNAL_REQUIRED = {"db_name", "year", "week", "team_key", "manager", "team_name"}
OUTPUT_COLUMNS = ("db_name", "year", "week", "NFL_player_id", "team_key", "manager", "team_name")


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _require(*, actual: set[str], required: set[str], label: str) -> None:
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _normalised(expr: str) -> str:
    return "NULLIF(TRIM(LOWER(CAST(" + expr + " AS VARCHAR))), '')"


def build(
    *, memberships: Path, crosswalk: Path, team_signals: Path, out: Path, report: Path,
) -> dict[str, Any]:
    """Emit only unambiguous direct MFL franchise→NFL-player→source-team rows."""
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TEMP VIEW memberships_raw AS SELECT * FROM read_parquet({_path(memberships)})")
        con.execute(f"CREATE TEMP VIEW crosswalk_raw AS SELECT * FROM read_parquet({_path(crosswalk)})")
        con.execute(f"CREATE TEMP VIEW signals_raw AS SELECT * FROM read_parquet({_path(team_signals)})")
        _require(actual=_columns(con, "memberships_raw"), required=MEMBERSHIP_REQUIRED, label="MFL memberships")
        _require(actual=_columns(con, "crosswalk_raw"), required=CROSSWALK_REQUIRED, label="MFL crosswalk")
        _require(actual=_columns(con, "signals_raw"), required=SIGNAL_REQUIRED, label="MFL team signals")

        con.execute(f"""
          CREATE TEMP TABLE crosswalk_one AS
          SELECT
            TRY_CAST(source_year AS INTEGER) AS source_year,
            NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') AS mfl_player_id,
            COUNT(DISTINCT NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), ''))
              FILTER (WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL) AS nfl_id_count,
            MIN(NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), ''))
              FILTER (WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL) AS NFL_player_id
          FROM crosswalk_raw
          GROUP BY 1, 2
        """)
        con.execute(f"""
          CREATE TEMP TABLE source_teams AS
          SELECT
            CAST(db_name AS VARCHAR) AS db_name,
            TRY_CAST(year AS INTEGER) AS year,
            TRY_CAST(week AS INTEGER) AS week,
            {_normalised('team_key')} AS team_key_norm,
            COUNT(*) AS raw_signal_rows,
            COUNT(DISTINCT NULLIF(TRIM(CAST(team_key AS VARCHAR)), '')) AS team_key_count,
            MIN(NULLIF(TRIM(CAST(team_key AS VARCHAR)), '')) AS team_key,
            COUNT(DISTINCT NULLIF(TRIM(CAST(manager AS VARCHAR)), '')) AS manager_count,
            MIN(NULLIF(TRIM(CAST(manager AS VARCHAR)), '')) AS manager,
            COUNT(DISTINCT NULLIF(TRIM(CAST(team_name AS VARCHAR)), '')) AS team_name_count,
            MIN(NULLIF(TRIM(CAST(team_name AS VARCHAR)), '')) AS team_name
          FROM signals_raw
          WHERE {_normalised('team_key')} IS NOT NULL
          GROUP BY 1, 2, 3, 4
        """)
        con.execute(f"""
          CREATE TEMP TABLE members AS
          SELECT
            CAST(m.db_name AS VARCHAR) AS db_name,
            TRY_CAST(m.year AS INTEGER) AS year,
            TRY_CAST(m.week AS INTEGER) AS week,
            TRY_CAST(m.source_year AS INTEGER) AS source_year,
            {_normalised('m.source_franchise_id')} AS franchise_key_norm,
            NULLIF(TRIM(CAST(m.mfl_player_id AS VARCHAR)), '') AS mfl_player_id
          FROM memberships_raw m
          WHERE {_normalised('m.source_franchise_id')} IS NOT NULL
        """)
        con.execute("""
          CREATE TEMP TABLE source_members AS
          SELECT
            m.db_name, m.year, m.week, m.franchise_key_norm, m.mfl_player_id,
            c.nfl_id_count, c.NFL_player_id,
            s.team_key, s.manager, s.team_name,
            COALESCE(s.team_key_count, 0) AS team_key_count,
            COALESCE(s.manager_count, 0) AS manager_count,
            COALESCE(s.team_name_count, 0) AS team_name_count
          FROM members m
          LEFT JOIN crosswalk_one c
            ON c.source_year=m.source_year AND c.mfl_player_id=m.mfl_player_id
          LEFT JOIN source_teams s
            ON s.db_name=m.db_name AND s.year=m.year AND s.week=m.week
           AND s.team_key_norm=m.franchise_key_norm
        """)
        con.execute("""
          CREATE TEMP TABLE candidate_members AS
          SELECT *,
            COUNT(DISTINCT team_key) FILTER (
              WHERE nfl_id_count=1 AND NFL_player_id IS NOT NULL
                AND team_key_count=1 AND manager_count<=1 AND team_name_count<=1
            ) OVER (PARTITION BY db_name, year, week, NFL_player_id) AS player_team_count
          FROM source_members
        """)
        emitted = int(con.execute("""
          SELECT COUNT(*) FROM (
            SELECT DISTINCT db_name, year, week, NFL_player_id, team_key, manager, team_name
            FROM candidate_members
            WHERE nfl_id_count=1 AND NFL_player_id IS NOT NULL
              AND team_key_count=1 AND manager_count<=1 AND team_name_count<=1
              AND player_team_count=1
          )
        """).fetchone()[0])
        ambiguous = int(con.execute("""
          SELECT COUNT(*) FROM candidate_members
          WHERE nfl_id_count=1 AND NFL_player_id IS NOT NULL
            AND team_key_count=1 AND manager_count<=1 AND team_name_count<=1
            AND player_team_count>1
        """).fetchone()[0])
        unmatched = int(con.execute("""
          SELECT COUNT(*) FROM candidate_members
          WHERE team_key_count=0
        """).fetchone()[0])
        crosswalk_blocked = int(con.execute("""
          SELECT COUNT(*) FROM candidate_members
          WHERE COALESCE(nfl_id_count, 0) <> 1
        """).fetchone()[0])
        malformed_signal = int(con.execute("""
          SELECT COUNT(*) FROM candidate_members
          WHERE team_key_count>1 OR manager_count>1 OR team_name_count>1
        """).fetchone()[0])
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
          COPY (
            SELECT DISTINCT db_name, year, week, NFL_player_id, team_key, manager, team_name
            FROM candidate_members
            WHERE nfl_id_count=1 AND NFL_player_id IS NOT NULL
              AND team_key_count=1 AND manager_count<=1 AND team_name_count<=1
              AND player_team_count=1
            ORDER BY db_name, year, week, NFL_player_id, team_key
          ) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "bridge_contract": list(OUTPUT_COLUMNS),
            "source_membership_rows": int(con.execute("SELECT COUNT(*) FROM members").fetchone()[0]),
            "emitted_bridge_rows": emitted,
            "ambiguous_player_team_rows": ambiguous,
            "source_team_unmatched_rows": unmatched,
            "crosswalk_blocked_rows": crosswalk_blocked,
            "malformed_source_team_rows": malformed_signal,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--memberships", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--team-signals", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(**vars(args)), sort_keys=True))


if __name__ == "__main__":
    main()
