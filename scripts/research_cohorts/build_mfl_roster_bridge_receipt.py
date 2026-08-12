"""Build a fail-closed MFL source-roster to canonical-player bridge receipt.

This is deliberately a read-only candidate builder.  It cannot write the
canonical cache or add a column.  A row is emitted only when all of these are
independently true:

1. the raw MFL source identifies exactly one franchise for its manager in the
   target league-week;
2. the raw MFL player id maps to exactly one NFL player id through the
   protected source-year crosswalk; and
3. that NFL player plus the exact normalised source manager identifies exactly
   one canonical player row in the same league-week.

The output is membership evidence, not an outcome update.  A later candidate
builder can attach a source team's outcome to these resolved recipients.  All
other inputs remain in the receipt with an explicit non-promotable status.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


MEMBERSHIP_REQUIRED = {
    "db_name", "year", "week", "source_year", "source_franchise_id",
    "source_manager", "mfl_player_id",
}
CROSSWALK_REQUIRED = {"source_year", "mfl_player_id", "NFL_player_id"}
CANONICAL_REQUIRED = {"db_name", "year", "week", "NFL_player_id", "manager"}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("\\", "/").replace("'", "''") + "'"


def _normalised(expression: str) -> str:
    return f"NULLIF(REGEXP_REPLACE(TRIM(LOWER(CAST({expression} AS VARCHAR))), '\\s+', ' ', 'g'), '')"


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def _require(*, actual: set[str], required: set[str], label: str) -> None:
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def build(
    *,
    base: Path,
    memberships: Path,
    crosswalk: Path,
    out: Path,
    report: Path,
) -> dict[str, Any]:
    """Create a bridge receipt without mutating ``base``."""
    con = duckdb.connect(str(base), read_only=True)
    try:
        _require(
            actual=_columns(con, "public.player_fantasy"),
            required=CANONICAL_REQUIRED,
            label="canonical player table",
        )
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_memberships AS SELECT * FROM read_parquet({_path(memberships)})")
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_crosswalk AS SELECT * FROM read_parquet({_path(crosswalk)})")
        _require(actual=_columns(con, "raw_memberships"), required=MEMBERSHIP_REQUIRED, label="MFL roster memberships")
        _require(actual=_columns(con, "raw_crosswalk"), required=CROSSWALK_REQUIRED, label="MFL player crosswalk")

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE memberships AS
            SELECT
              ROW_NUMBER() OVER (
                ORDER BY CAST({_q('db_name')} AS VARCHAR), TRY_CAST({_q('year')} AS INTEGER),
                         TRY_CAST({_q('week')} AS INTEGER), TRY_CAST({_q('source_year')} AS INTEGER),
                         CAST({_q('source_franchise_id')} AS VARCHAR),
                         CAST({_q('mfl_player_id')} AS VARCHAR),
                         CAST({_q('source_manager')} AS VARCHAR)
              ) AS membership_ordinal,
              CAST({_q('db_name')} AS VARCHAR) AS db_name,
              TRY_CAST({_q('year')} AS INTEGER) AS year,
              TRY_CAST({_q('week')} AS INTEGER) AS week,
              TRY_CAST({_q('source_year')} AS INTEGER) AS source_year,
              NULLIF(TRIM(CAST({_q('source_franchise_id')} AS VARCHAR)), '') AS source_franchise_id,
              NULLIF(TRIM(CAST({_q('source_manager')} AS VARCHAR)), '') AS source_manager,
              {_normalised(_q('source_manager'))} AS source_manager_key,
              NULLIF(TRIM(CAST({_q('mfl_player_id')} AS VARCHAR)), '') AS mfl_player_id
            FROM raw_memberships
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE membership_manager_cardinality AS
            SELECT db_name, year, week, source_manager_key,
                   COUNT(DISTINCT source_franchise_id) AS source_franchise_count
            FROM memberships
            WHERE source_manager_key IS NOT NULL AND source_franchise_id IS NOT NULL
            GROUP BY 1, 2, 3, 4
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE crosswalk_summary AS
            SELECT
              TRY_CAST({_q('source_year')} AS INTEGER) AS source_year,
              NULLIF(TRIM(CAST({_q('mfl_player_id')} AS VARCHAR)), '') AS mfl_player_id,
              COUNT(DISTINCT NULLIF(TRIM(CAST({_q('NFL_player_id')} AS VARCHAR)), ''))
                FILTER (WHERE NULLIF(TRIM(CAST({_q('NFL_player_id')} AS VARCHAR)), '') IS NOT NULL) AS nfl_id_count,
              MIN(NULLIF(TRIM(CAST({_q('NFL_player_id')} AS VARCHAR)), ''))
                FILTER (WHERE NULLIF(TRIM(CAST({_q('NFL_player_id')} AS VARCHAR)), '') IS NOT NULL) AS NFL_player_id
            FROM raw_crosswalk
            GROUP BY 1, 2
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE canonical_candidates AS
            SELECT
              m.membership_ordinal,
              COUNT(p.rowid) AS canonical_recipient_count,
              MIN(p.rowid) AS canonical_player_rowid,
              MIN(CAST(p.manager AS VARCHAR)) AS canonical_manager
            FROM memberships m
            JOIN membership_manager_cardinality mc
              ON mc.db_name IS NOT DISTINCT FROM m.db_name
             AND mc.year IS NOT DISTINCT FROM m.year
             AND mc.week IS NOT DISTINCT FROM m.week
             AND mc.source_manager_key IS NOT DISTINCT FROM m.source_manager_key
            JOIN crosswalk_summary c
              ON c.source_year IS NOT DISTINCT FROM m.source_year
             AND c.mfl_player_id IS NOT DISTINCT FROM m.mfl_player_id
             AND c.nfl_id_count = 1
            JOIN public.player_fantasy p
              ON CAST(p.db_name AS VARCHAR) IS NOT DISTINCT FROM m.db_name
             AND TRY_CAST(p."year" AS INTEGER) IS NOT DISTINCT FROM m.year
             AND TRY_CAST(p.week AS INTEGER) IS NOT DISTINCT FROM m.week
             AND NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '') IS NOT DISTINCT FROM c.NFL_player_id
             AND """ + _normalised("p.manager") + """ IS NOT DISTINCT FROM m.source_manager_key
            WHERE m.source_franchise_id IS NOT NULL
              AND m.source_manager_key IS NOT NULL
              AND mc.source_franchise_count = 1
            GROUP BY 1
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE receipt AS
            SELECT
              m.membership_ordinal,
              m.db_name, m.year, m.week, m.source_year,
              m.source_franchise_id, m.source_manager, m.source_manager_key,
              m.mfl_player_id,
              c.NFL_player_id,
              COALESCE(c.nfl_id_count, 0) AS crosswalk_nfl_id_count,
              COALESCE(mc.source_franchise_count, 0) AS source_franchise_count_for_manager,
              COALESCE(cc.canonical_recipient_count, 0) AS canonical_recipient_count,
              cc.canonical_player_rowid,
              cc.canonical_manager,
              CASE
                WHEN m.source_franchise_id IS NULL THEN 'missing_source_franchise'
                WHEN m.source_manager_key IS NULL THEN 'missing_source_manager'
                WHEN COALESCE(mc.source_franchise_count, 0) <> 1 THEN 'ambiguous_source_franchise_manager'
                WHEN COALESCE(c.nfl_id_count, 0) = 0 THEN 'missing_player_crosswalk'
                WHEN c.nfl_id_count > 1 THEN 'ambiguous_player_crosswalk'
                WHEN COALESCE(cc.canonical_recipient_count, 0) = 0 THEN 'no_canonical_recipient'
                WHEN cc.canonical_recipient_count > 1 THEN 'ambiguous_canonical_recipient'
                ELSE 'resolved_exact_player_team'
              END AS bridge_status
            FROM memberships m
            LEFT JOIN membership_manager_cardinality mc
              ON mc.db_name IS NOT DISTINCT FROM m.db_name
             AND mc.year IS NOT DISTINCT FROM m.year
             AND mc.week IS NOT DISTINCT FROM m.week
             AND mc.source_manager_key IS NOT DISTINCT FROM m.source_manager_key
            LEFT JOIN crosswalk_summary c
              ON c.source_year IS NOT DISTINCT FROM m.source_year
             AND c.mfl_player_id IS NOT DISTINCT FROM m.mfl_player_id
            LEFT JOIN canonical_candidates cc
              ON cc.membership_ordinal = m.membership_ordinal
        """)
        input_memberships = int(con.execute("SELECT COUNT(*) FROM memberships").fetchone()[0])
        status_counts = {
            str(status): int(count)
            for status, count in con.execute(
                "SELECT bridge_status, COUNT(*) FROM receipt GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (
              SELECT * EXCLUDE (bridge_status)
              FROM receipt
              WHERE bridge_status = 'resolved_exact_player_team'
              ORDER BY membership_ordinal
            ) TO {_path(out)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        result = {
            "read_only": True,
            "cache_mutated": False,
            "new_lineage": False,
            "bridge_key": [
                "source franchise", "source MFL player id", "source-year crosswalk",
                "canonical db_name/year/week/NFL_player_id/manager",
            ],
            "input_memberships": input_memberships,
            "resolved_exact_player_team": status_counts.get("resolved_exact_player_team", 0),
            "status_counts": status_counts,
        }
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--memberships", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(
        base=args.base, memberships=args.memberships, crosswalk=args.crosswalk,
        out=args.out, report=args.report,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
