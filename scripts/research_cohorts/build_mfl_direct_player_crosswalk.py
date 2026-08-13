"""Resolve only exact historical MFL player identities without touching a cache.

MFL's early player directory frequently omits ESPN IDs even when it provides a
player's native MFL ID, name, NFL team, and position.  This builder turns that
evidence into a *candidate* crosswalk only when those four fields identify one
and only one NFL player in the same historical season.  It does not overwrite
an existing ESPN-derived identity, fetch source data, or mutate either cache.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


DIRECTORY_REQUIRED = {"source_year", "mfl_player_id", "raw_name", "raw_position", "raw_team"}
CROSSWALK_REQUIRED = {"source_year", "mfl_player_id", "NFL_player_id", "crosswalk_status"}
NON_NFL_PRODUCTS = {"COACH", "TMQB", "TMPK"}
OUTPUT_COLUMNS = ("source_year", "mfl_player_id", "NFL_player_id", "crosswalk_status")
HISTORICAL_TEAM_COLUMNS = ("nfl_team_api", "team", "nfl_team", "team_abbr", "team_abbreviation", "posteam")


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _require(*, actual: set[str], required: set[str], label: str) -> None:
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _name_key(expr: str) -> str:
    # MFL uses ``Last, First``; the ops table uses ``First Last``.
    reordered = (
        f"CASE WHEN INSTR(CAST({expr} AS VARCHAR), ',') > 0 "
        f"THEN SPLIT_PART(CAST({expr} AS VARCHAR), ',', 2) || ' ' || "
        f"SPLIT_PART(CAST({expr} AS VARCHAR), ',', 1) ELSE CAST({expr} AS VARCHAR) END"
    )
    return f"NULLIF(TRIM(REGEXP_REPLACE(LOWER({reordered}), '[^a-z0-9]+', ' ', 'g')), '')"


def _team_key(expr: str) -> str:
    return f"NULLIF(REGEXP_REPLACE(UPPER(TRIM(CAST({expr} AS VARCHAR))), '[^A-Z0-9]+', '', 'g'), '')"


def _position_key(expr: str) -> str:
    cleaned = f"UPPER(TRIM(CAST({expr} AS VARCHAR)))"
    return (
        "CASE "
        f"WHEN {cleaned} IN ('PK', 'K') THEN 'K' "
        f"WHEN {cleaned} IN ('DEF', 'DST') THEN 'DEF' "
        f"WHEN {cleaned} IN ('DE', 'DT', 'DL') THEN 'DL' "
        f"WHEN {cleaned} IN ('CB', 'S', 'DB') THEN 'DB' "
        f"ELSE NULLIF({cleaned}, '') END"
    )


def consolidate_candidates(paths: list[Path], out: Path) -> dict[str, int]:
    """Deduplicate repeated source catalogs while rejecting identity disagreements."""
    if not paths:
        raise ValueError("no direct candidate files supplied")
    con = duckdb.connect()
    try:
        files = ",".join(_path(path) for path in paths)
        con.execute(f"CREATE TEMP VIEW candidates AS SELECT * FROM read_parquet([{files}])")
        input_rows = int(con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0])
        conflicts = int(con.execute("""
            SELECT COUNT(*)
            FROM (
              SELECT source_year, mfl_player_id
              FROM candidates
              GROUP BY 1, 2
              HAVING COUNT(DISTINCT NFL_player_id) > 1
            )
        """).fetchone()[0])
        if conflicts:
            raise ValueError(f"conflicting MFL native identities: {conflicts}")
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT source_year, mfl_player_id, NFL_player_id, crosswalk_status
              FROM candidates
              GROUP BY 1, 2, 3, 4
              ORDER BY 1, 2
            ) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        return {
            "input_rows": input_rows,
            "candidate_rows": int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(out)]).fetchone()[0]),
            "conflicting_native_keys": conflicts,
        }
    finally:
        con.close()


def build(
    *, directory: Path, existing_crosswalk: Path, ops_cache: Path, out: Path, report: Path,
) -> dict[str, Any]:
    """Emit unresolved MFL IDs with exactly one historical ops identity."""
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TEMP VIEW raw_directory AS SELECT * FROM read_parquet({_path(directory)})")
        con.execute(f"CREATE TEMP VIEW existing AS SELECT * FROM read_parquet({_path(existing_crosswalk)})")
        _require(actual=_columns(con, "raw_directory"), required=DIRECTORY_REQUIRED, label="MFL directory")
        _require(actual=_columns(con, "existing"), required=CROSSWALK_REQUIRED, label="existing MFL crosswalk")
        con.execute(f"ATTACH {_path(ops_cache)} AS ops (READ_ONLY)")
        stats_columns = _columns(con, "ops.nfl_historical.nfl_player_stats_all")
        _require(
            actual=stats_columns,
            required={"NFL_player_id", "year", "player", "position"},
            label="ops historical player stats",
        )
        historical_team_column = next(
            (name for name in HISTORICAL_TEAM_COLUMNS if name in stats_columns), None
        )
        if historical_team_column is None:
            raise ValueError(
                "ops historical player stats has no supported historical team field; "
                f"available columns: {sorted(stats_columns)}"
            )

        con.execute("""
            CREATE TEMP TABLE directory_one AS
            SELECT
              TRY_CAST(source_year AS INTEGER) AS source_year,
              NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') AS mfl_player_id,
              MIN(raw_name) AS raw_name,
              MIN(raw_position) AS raw_position,
              MIN(raw_team) AS raw_team,
              COUNT(DISTINCT COALESCE(CAST(raw_name AS VARCHAR), '')) AS name_count,
              COUNT(DISTINCT COALESCE(CAST(raw_position AS VARCHAR), '')) AS position_count,
              COUNT(DISTINCT COALESCE(CAST(raw_team AS VARCHAR), '')) AS team_count
            FROM raw_directory
            WHERE NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        """)
        conflicts = int(con.execute("""
            SELECT COUNT(*) FROM directory_one
            WHERE name_count > 1 OR position_count > 1 OR team_count > 1
        """).fetchone()[0])
        if conflicts:
            raise ValueError(f"MFL directory has {conflicts} conflicting source-year/native-ID records")
        con.execute("""
            CREATE TEMP TABLE existing_one AS
            SELECT
              TRY_CAST(source_year AS INTEGER) AS source_year,
              NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') AS mfl_player_id,
              COUNT(DISTINCT NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), ''))
                FILTER (WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL) AS nfl_id_count,
              MIN(NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), ''))
                FILTER (WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL) AS NFL_player_id
            FROM existing
            GROUP BY 1, 2
        """)
        con.execute(f"""
            CREATE TEMP TABLE unresolved AS
            SELECT
              d.source_year, d.mfl_player_id, d.raw_name, d.raw_position, d.raw_team,
              {_name_key('d.raw_name')} AS name_key,
              {_position_key('d.raw_position')} AS position_key,
              {_team_key('d.raw_team')} AS team_key,
              CASE
                WHEN UPPER(TRIM(COALESCE(CAST(d.raw_position AS VARCHAR), ''))) IN ('COACH', 'TMQB', 'TMPK')
                  THEN 'excluded_non_nfl_product'
                ELSE NULL
              END AS forced_status
            FROM directory_one d
            LEFT JOIN existing_one e USING (source_year, mfl_player_id)
            WHERE COALESCE(e.nfl_id_count, 0) = 0
        """)
        con.execute(f"""
            CREATE TEMP TABLE historical_players AS
            SELECT DISTINCT
              NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id,
              TRY_CAST(year AS INTEGER) AS source_year,
              {_name_key('player')} AS name_key,
              {_position_key('position')} AS position_key,
              {_team_key(_q(historical_team_column))} AS team_key
            FROM ops.nfl_historical.nfl_player_stats_all
            WHERE NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL
              AND TRY_CAST(year AS INTEGER) IS NOT NULL
              AND {_name_key('player')} IS NOT NULL
              AND {_position_key('position')} IS NOT NULL
              AND {_team_key(_q(historical_team_column))} IS NOT NULL
        """)
        con.execute("""
            CREATE TEMP TABLE candidates AS
            SELECT
              u.source_year, u.mfl_player_id, u.forced_status,
              COUNT(DISTINCT h.NFL_player_id) AS nfl_id_count,
              MIN(h.NFL_player_id) AS NFL_player_id
            FROM unresolved u
            LEFT JOIN historical_players h
              ON h.source_year=u.source_year
             AND h.name_key=u.name_key
             AND h.position_key=u.position_key
             AND h.team_key=u.team_key
            GROUP BY 1, 2, 3
        """)
        con.execute("""
            CREATE TEMP TABLE result AS
            SELECT
              source_year, mfl_player_id,
              CASE WHEN forced_status IS NULL AND nfl_id_count = 1 THEN NFL_player_id ELSE NULL::VARCHAR END AS NFL_player_id,
              CASE
                WHEN forced_status IS NOT NULL THEN forced_status
                WHEN nfl_id_count = 0 THEN 'no_name_position_team_year_match'
                WHEN nfl_id_count > 1 THEN 'ambiguous_name_position_team_year'
                ELSE 'resolved_name_position_team_year'
              END AS crosswalk_status
            FROM candidates
        """)
        status_counts = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT crosswalk_status, COUNT(*) FROM result GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT {', '.join(OUTPUT_COLUMNS)} FROM result
              WHERE crosswalk_status = 'resolved_name_position_team_year'
              ORDER BY source_year, mfl_player_id
            ) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "directory_keys": int(con.execute("SELECT COUNT(*) FROM directory_one").fetchone()[0]),
            "already_resolved_by_espn": int(con.execute("SELECT COUNT(*) FROM existing_one WHERE nfl_id_count = 1").fetchone()[0]),
            "candidate_rows": status_counts.get("resolved_name_position_team_year", 0),
            "status_counts": status_counts,
            "crosswalk_key": ["source_year", "raw_name", "raw_position", "raw_team"],
            "historical_team_column": historical_team_column,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--existing-crosswalk", type=Path, required=True)
    parser.add_argument("--ops-cache", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(**vars(args)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
