"""Map raw targeted MFL roster-player IDs through the protected ESPN crosswalk.

This produces identity evidence only.  It neither opens the canonical research
cache nor guesses from names.  A raw MFL record is resolved only when its ESPN
ID maps to one and only one ``NFL_player_id`` in the ops player bio table.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


REQUIRED = {"source_year", "mfl_player_id", "espn_id"}


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(*, directory: Path, ops_cache: Path, out: Path, report: Path) -> dict[str, Any]:
    con = duckdb.connect()
    try:
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_directory AS SELECT * FROM read_parquet({_path(directory)})")
        missing = sorted(REQUIRED - _columns(con, "raw_directory"))
        if missing:
            raise ValueError(f"MFL directory missing required columns: {missing}")
        con.execute(f"ATTACH {_path(ops_cache)} AS ops (READ_ONLY)")
        bio_columns = _columns(con, "ops.nfl_historical.player_bio")
        required_bio = {"espn_id", "NFL_player_id"}
        missing_bio = sorted(required_bio - bio_columns)
        if missing_bio:
            raise ValueError(f"ops player_bio missing required columns: {missing_bio}")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE directory_keys AS
            SELECT
              TRY_CAST(source_year AS INTEGER) AS source_year,
              NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') AS mfl_player_id,
              NULLIF(TRIM(CAST(espn_id AS VARCHAR)), '') AS espn_id
            FROM raw_directory
            WHERE NULLIF(TRIM(CAST(mfl_player_id AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2, 3
        """)
        duplicate_keys = int(con.execute("""
            SELECT COUNT(*) FROM (
              SELECT source_year, mfl_player_id
              FROM directory_keys GROUP BY 1, 2 HAVING COUNT(*) > 1
            )
        """).fetchone()[0])
        if duplicate_keys:
            raise ValueError(f"raw MFL directory has {duplicate_keys} conflicting source-year/native-ID keys")
        con.execute("""
            CREATE OR REPLACE TEMP TABLE bio AS
            SELECT
              NULLIF(TRIM(CAST(espn_id AS VARCHAR)), '') AS espn_id,
              NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') AS NFL_player_id
            FROM ops.nfl_historical.player_bio
            WHERE NULLIF(TRIM(CAST(espn_id AS VARCHAR)), '') IS NOT NULL
              AND NULLIF(TRIM(CAST(NFL_player_id AS VARCHAR)), '') IS NOT NULL
            GROUP BY 1, 2
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE bio_summary AS
            SELECT espn_id, COUNT(DISTINCT NFL_player_id) AS nfl_id_count,
                   MIN(NFL_player_id) AS NFL_player_id
            FROM bio GROUP BY 1
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE crosswalk AS
            SELECT
              d.source_year, d.mfl_player_id, d.espn_id,
              CASE WHEN b.nfl_id_count = 1 THEN b.NFL_player_id ELSE NULL::VARCHAR END AS NFL_player_id,
              CASE
                WHEN d.espn_id IS NULL THEN 'missing_espn_id'
                WHEN COALESCE(b.nfl_id_count, 0) = 0 THEN 'espn_not_in_ops_bio'
                WHEN b.nfl_id_count > 1 THEN 'ambiguous_espn_to_nfl'
                ELSE 'resolved_espn_to_nfl'
              END AS crosswalk_status
            FROM directory_keys d
            LEFT JOIN bio_summary b USING (espn_id)
        """)
        status_counts = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT crosswalk_status, COUNT(*) FROM crosswalk GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY (SELECT * FROM crosswalk ORDER BY source_year, mfl_player_id) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "directory_keys": int(con.execute("SELECT COUNT(*) FROM directory_keys").fetchone()[0]),
            "status_counts": status_counts,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--ops-cache", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(
        directory=args.directory, ops_cache=args.ops_cache, out=args.out, report=args.report,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
